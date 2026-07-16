import assert from 'node:assert/strict'
import test from 'node:test'

import {
  Base93Decoder,
  Base93Encoder,
  decode,
  encode
} from '../base93/index.js'

async function readStream (stream) {
  const values = []
  for await (const value of stream) values.push(value)
  return values
}

test('Base93 has stable binary vectors', () => {
  const vectors = [
    [[], ''],
    [[0], 'AA'],
    [[1], 'BA'],
    [[255], ')C'],
    [[0, 1, 2, 3, 254, 255], '*CLhl`)C']
  ]
  for (const [input, output] of vectors) {
    assert.equal(encode(Uint8Array.from(input)), output)
    assert.deepEqual([...decode(output)], input)
  }
})

test('streaming encoder is independent of update boundaries', () => {
  const bytes = Uint8Array.from({ length: 51000 }, (_, index) => index % 251)
  const encoder = new Base93Encoder()
  encoder.update(bytes.slice(0, 17)).update(bytes.slice(17, 4097)).update(bytes.slice(4097))
  assert.equal(encoder.getEncoded(), encode(bytes))
  assert.throws(() => encoder.update([1]), /finalized/)
})

test('strict decoder rejects invalid and non-canonical input', () => {
  assert.throws(() => decode('A"A'), /Invalid Base93 character/)
  assert.throws(() => decode('A\\A'), /Invalid Base93 character/)
  assert.throws(() => decode('A\nA'), /Invalid Base93 character/)
  assert.throws(() => decode('~'), /Non-canonical|truncated/)
  assert.throws(() => decode('~~'), /Non-canonical|truncated/)
})

test('streaming decoder handles binary and UTF-8 sources', async () => {
  const binary = await readStream(new Base93Decoder([
    encode(Uint8Array.of(1, 2)),
    encode(Uint8Array.of(3))
  ]).getDecoded())
  assert.deepEqual(binary.map(value => [...value]), [[1, 2], [3]])

  const utf8 = new TextEncoder().encode('ação')
  const text = await readStream(new Base93Decoder([
    encode(utf8.slice(0, 3)),
    encode(utf8.slice(3))
  ], { mimeType: 'text/plain', preferTextStreamDecoding: true }).getDecoded())
  assert.equal(text.join(''), 'ação')
})
