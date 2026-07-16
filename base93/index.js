// Derived from https://github.com/ticlo/arrow-code/blob/master/src/base93.ts
// Apache-2.0. JSON-safe alphabet: space is included; double quote and
// backslash are intentionally excluded.

export const BASE93_ALPHABET =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!#$%&'()*+,-./:;<=>?@[]^_`{|}~ "

const ENCODING_TABLE = (() => {
  const out = new Uint16Array(93)
  for (let i = 0; i < out.length; i++) out[i] = BASE93_ALPHABET.charCodeAt(i)
  return out
})()

const DECODING_TABLE = (() => {
  const out = new Int16Array(128)
  out.fill(-1)
  for (let i = 0; i < BASE93_ALPHABET.length; i++) out[BASE93_ALPHABET.charCodeAt(i)] = i
  return out
})()

function codesToString (codes, length) {
  const chunkLength = 16384
  let result = ''
  for (let i = 0; i < length; i += chunkLength) {
    result += String.fromCharCode.apply(
      null,
      Array.prototype.slice.call(codes, i, Math.min(i + chunkLength, length))
    )
  }
  return result
}

function asBytes (bytes) {
  if (bytes instanceof Uint8Array) return bytes
  if (ArrayBuffer.isView(bytes)) return new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  if (bytes instanceof ArrayBuffer) return new Uint8Array(bytes)
  return Uint8Array.from(bytes)
}

export class Base93Encoder {
  constructor (prefix = '') {
    this.queuedBits = 0
    this.bitCount = 0
    this.parts = prefix ? [String(prefix)] : []
    this.finished = false
  }

  update (bytes) {
    if (this.finished) throw new Error('Base93 encoder is already finalized.')
    const source = asBytes(bytes)
    const output = new Uint16Array(Math.ceil(source.length * 8 / 6.5) + 4)
    let position = 0
    let queuedBits = this.queuedBits
    let bitCount = this.bitCount

    for (let i = 0; i < source.length; i++) {
      queuedBits |= source[i] << bitCount
      bitCount += 8
      if (bitCount > 13) {
        let value = queuedBits & 0x1fff
        if (value > 456) {
          queuedBits >>>= 13
          bitCount -= 13
        } else {
          value = queuedBits & 0x3fff
          queuedBits >>>= 14
          bitCount -= 14
        }
        output[position++] = ENCODING_TABLE[value % 93]
        output[position++] = ENCODING_TABLE[Math.floor(value / 93)]
      }
    }

    this.queuedBits = queuedBits
    this.bitCount = bitCount
    if (position > 0) this.parts.push(codesToString(output, position))
    return this
  }

  getEncoded () {
    if (!this.finished) {
      if (this.bitCount > 0) {
        const output = new Uint16Array(2)
        let position = 0
        output[position++] = ENCODING_TABLE[this.queuedBits % 93]
        if (this.bitCount > 7 || this.queuedBits > 92) {
          output[position++] = ENCODING_TABLE[Math.floor(this.queuedBits / 93)]
        }
        this.parts.push(codesToString(output, position))
      }
      this.finished = true
      this.queuedBits = 0
      this.bitCount = 0
    }
    return this.parts.join('')
  }
}

export function encode (bytes) {
  return new Base93Encoder().update(bytes).getEncoded()
}

function decodeUnchecked (value) {
  const output = new Uint8Array(Math.ceil(value.length * 7 / 8))
  let queuedBits = 0
  let bitCount = 0
  let first = -1
  let position = 0

  for (let i = 0; i < value.length; i++) {
    const code = value.charCodeAt(i)
    if (code >= DECODING_TABLE.length || DECODING_TABLE[code] < 0) {
      throw new Error(`Invalid Base93 character at offset ${i}.`)
    }
    const decoded = DECODING_TABLE[code]
    if (first === -1) {
      first = decoded
      continue
    }

    const pair = first + decoded * 93
    first = -1
    queuedBits |= pair << bitCount
    bitCount += (pair & 0x1fff) > 456 ? 13 : 14
    while (bitCount > 7) {
      output[position++] = queuedBits & 0xff
      queuedBits >>>= 8
      bitCount -= 8
    }
  }

  if (first !== -1) output[position++] = (queuedBits | (first << bitCount)) & 0xff
  return output.slice(0, position)
}

export function decode (text, offset = 0, length = -1) {
  if (typeof text !== 'string') throw new TypeError('Base93 input must be a string.')
  if (!Number.isSafeInteger(offset) || offset < 0 || offset > text.length) throw new RangeError('Invalid Base93 offset.')
  if (!Number.isSafeInteger(length) || length < -1) throw new RangeError('Invalid Base93 length.')

  const end = length < 0 ? text.length : Math.min(text.length, offset + length)
  const encoded = text.slice(offset, end)
  const bytes = decodeUnchecked(encoded)
  if (encode(bytes) !== encoded) throw new Error('Non-canonical or truncated Base93 input.')
  return bytes
}

function normalizeSource (source) {
  if (typeof source === 'function') source = source()
  if (typeof source === 'string') return [source]
  if (source?.[Symbol.asyncIterator] || source?.[Symbol.iterator]) return source
  throw new TypeError('Base93 stream source must be iterable.')
}

export class Base93Decoder {
  constructor (source, { mimeType = '', preferTextStreamDecoding = false } = {}) {
    this.source = normalizeSource(source)
    this.asText = preferTextStreamDecoding && mimeType.startsWith('text/')
  }

  async * decodedValues () {
    const textDecoder = this.asText ? new TextDecoder() : null
    for await (const encoded of this.source) {
      const bytes = decode(encoded)
      const value = textDecoder ? textDecoder.decode(bytes, { stream: true }) : bytes
      if (value.length > 0) yield value
    }
    if (textDecoder) {
      const tail = textDecoder.decode()
      if (tail.length > 0) yield tail
    }
  }

  getDecoded () {
    const iterator = this.decodedValues()[Symbol.asyncIterator]()
    return new ReadableStream({
      async pull (controller) {
        try {
          const { value, done } = await iterator.next()
          if (done) controller.close()
          else controller.enqueue(value)
        } catch (error) {
          controller.error(error)
        }
      },
      async cancel () {
        await iterator.return?.()
      }
    })
  }
}

export default Base93Decoder
