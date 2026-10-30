import { serializeEventChunks } from '../../event/helpers/serialize.js'
import { wasmBase64, wasmExports, wasmImports } from './generated.js'

const MEMORY_BYTES = 256 * 1024
const INPUT_BYTES = 16 * 1024
const PRIVATE_KEY_BYTES = 32
const KEYPAIR_BYTES = 96
const XONLY_KEY_BYTES = 64
const SIGNATURE_BYTES = 64
const PUBLIC_KEY_BYTES = 32
const HASH_CONTEXT_BYTES = 4
const SHA256_STATE_BYTES = 104
const CONTEXT_SIGN = 1 | (1 << 9)
const CONTEXT_VERIFY = 1 | (1 << 8)
const textEncoder = new TextEncoder()

function decodeBase64 (value) {
  if (typeof atob !== 'function') throw Error('Base64 decoding is unavailable')
  const binary = atob(value)
  const bytes = new Uint8Array(binary.length)
  for (let index = 0; index < binary.length; index++) {
    bytes[index] = binary.charCodeAt(index)
  }
  return bytes
}

function requireBytes (value, length, label) {
  if (!(value instanceof Uint8Array) || value.length !== length) {
    throw TypeError(`${label} must be a ${length}-byte Uint8Array`)
  }
}

export async function createSecp256k1WasmBackend () {
  if (typeof WebAssembly !== 'object') throw Error('WebAssembly is unavailable')

  const heap = { bytes: null, words: null }
  const importObject = {
    a: {
      [wasmImports.abort] () {
        throw Error('secp256k1 WASM aborted')
      },
      [wasmImports.resize] (size) {
        throw Error(`secp256k1 WASM requested heap growth to ${size} bytes`)
      },
      [wasmImports.fd_write] (_fd, vectors, count, written) {
        let total = 0
        for (let index = 0; index < count; index++) {
          total += heap.words[(vectors + 4) >> 2]
          vectors += 8
        }
        heap.words[written >> 2] = total
        return 0
      }
    }
  }
  const { instance } = await WebAssembly.instantiate(decodeBase64(wasmBase64), importObject)
  const api = {}
  for (const [name, symbol] of Object.entries(wasmExports)) {
    api[name] = instance.exports[symbol]
  }
  heap.bytes = new Uint8Array(api.memory.buffer)
  heap.words = new Uint32Array(api.memory.buffer)
  const bytes = heap.bytes
  if (bytes.length !== MEMORY_BYTES) {
    throw Error(`Unexpected secp256k1 WASM memory size: ${bytes.length}`)
  }
  api.init()

  const allocate = size => {
    const pointer = api.malloc(size)
    if (!pointer) throw Error(`Failed to allocate ${size} WASM bytes`)
    return pointer
  }
  const context = api.context_create(CONTEXT_SIGN | CONTEXT_VERIFY)
  if (!context) throw Error('Failed to create secp256k1 context')
  const secretKey = allocate(PRIVATE_KEY_BYTES)
  const entropy = allocate(PRIVATE_KEY_BYTES)
  const messageHash = allocate(PRIVATE_KEY_BYTES)
  const publicKey = allocate(PUBLIC_KEY_BYTES)
  const signature = allocate(SIGNATURE_BYTES)
  const keypair = allocate(KEYPAIR_BYTES)
  const xonlyPublicKey = allocate(XONLY_KEY_BYTES)
  const hashContext = allocate(HASH_CONTEXT_BYTES)
  const sha256State = allocate(SHA256_STATE_BYTES)
  const input = allocate(INPUT_BYTES)
  api.hash_ctx_init(hashContext)

  const clearKeyMaterial = () => {
    bytes.fill(0, secretKey, secretKey + PRIVATE_KEY_BYTES)
    bytes.fill(0, entropy, entropy + PRIVATE_KEY_BYTES)
    bytes.fill(0, keypair, keypair + KEYPAIR_BYTES)
  }

  const withKeypair = (secret, callback) => {
    requireBytes(secret, PRIVATE_KEY_BYTES, 'Secret key')
    try {
      bytes.set(secret, secretKey)
      if (api.keypair_create(context, keypair, secretKey) !== 1) {
        throw Error('Invalid secp256k1 secret key')
      }
      return callback()
    } finally {
      clearKeyMaterial()
    }
  }

  const derivePublicKey = () => {
    if (api.keypair_xonly_pub(context, xonlyPublicKey, null, keypair) !== 1) {
      throw Error('Failed to derive secp256k1 public key')
    }
    if (api.xonly_pubkey_serialize(context, publicKey, xonlyPublicKey) !== 1) {
      throw Error('Failed to serialize secp256k1 public key')
    }
    return bytes.slice(publicKey, publicKey + PUBLIC_KEY_BYTES)
  }

  const writeBytesToHash = data => {
    for (let offset = 0; offset < data.length; offset += INPUT_BYTES) {
      const chunk = data.subarray(offset, Math.min(offset + INPUT_BYTES, data.length))
      bytes.set(chunk, input)
      api.sha256_write(hashContext, sha256State, input, chunk.length)
    }
  }

  const writeTextToHash = text => {
    if (typeof textEncoder.encodeInto !== 'function') {
      writeBytesToHash(textEncoder.encode(text))
      return
    }

    let offset = 0
    while (offset < text.length) {
      const target = bytes.subarray(input, input + INPUT_BYTES)
      const { read, written } = textEncoder.encodeInto(text.slice(offset), target)
      if (read === 0 && written === 0) throw Error('UTF-8 encoding stalled')
      api.sha256_write(hashContext, sha256State, input, written)
      offset += read
    }
  }

  const dispose = () => {
    clearKeyMaterial()
  }

  return {
    memoryBytes: bytes.length,

    getPublicKey (secret) {
      return withKeypair(secret, derivePublicKey)
    },

    hashEvent (event) {
      api.sha256_initialize(sha256State)
      for (const chunk of serializeEventChunks(event, { validate: false })) {
        writeTextToHash(chunk)
      }
      api.sha256_finalize(hashContext, sha256State, messageHash)
      return bytes.slice(messageHash, messageHash + PRIVATE_KEY_BYTES)
    },

    signHash (hash, secret, auxiliaryRandom) {
      requireBytes(hash, PRIVATE_KEY_BYTES, 'Message hash')
      requireBytes(auxiliaryRandom, PRIVATE_KEY_BYTES, 'Auxiliary randomness')
      return withKeypair(secret, () => {
        bytes.set(hash, messageHash)
        bytes.set(auxiliaryRandom, entropy)
        if (
          api.schnorrsig_sign32(
            context,
            signature,
            messageHash,
            keypair,
            entropy
          ) !== 1
        ) {
          throw Error('Failed to sign secp256k1 message')
        }
        return bytes.slice(signature, signature + SIGNATURE_BYTES)
      })
    },

    signEventHash (secret, prepareHash, auxiliaryRandom) {
      requireBytes(auxiliaryRandom, PRIVATE_KEY_BYTES, 'Auxiliary randomness')
      return withKeypair(secret, () => {
        const publicKeyBytes = derivePublicKey()
        const hash = prepareHash(publicKeyBytes)
        requireBytes(hash, PRIVATE_KEY_BYTES, 'Message hash')
        bytes.set(hash, messageHash)
        bytes.set(auxiliaryRandom, entropy)
        if (
          api.schnorrsig_sign32(
            context,
            signature,
            messageHash,
            keypair,
            entropy
          ) !== 1
        ) {
          throw Error('Failed to sign secp256k1 message')
        }
        return {
          publicKey: publicKeyBytes,
          signature: bytes.slice(signature, signature + SIGNATURE_BYTES)
        }
      })
    },

    verifyHash (signatureBytes, hash, publicKeyBytes) {
      requireBytes(signatureBytes, SIGNATURE_BYTES, 'Signature')
      requireBytes(hash, PRIVATE_KEY_BYTES, 'Message hash')
      requireBytes(publicKeyBytes, PUBLIC_KEY_BYTES, 'Public key')
      bytes.set(signatureBytes, signature)
      bytes.set(hash, messageHash)
      bytes.set(publicKeyBytes, publicKey)
      if (api.xonly_pubkey_parse(context, xonlyPublicKey, publicKey) !== 1) {
        return false
      }
      return api.schnorrsig_verify(
        context,
        signature,
        messageHash,
        PRIVATE_KEY_BYTES,
        xonlyPublicKey
      ) === 1
    },

    dispose
  }
}
