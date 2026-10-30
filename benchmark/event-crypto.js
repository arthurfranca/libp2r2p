// Node-only development benchmark.
import {
  finalizeEvent,
  getEventHash,
  initializeEventCrypto,
  isValidEvent
} from '../event/index.js'
import {
  finalizeEvent as finalizeEventWithJavaScript,
  getEventHash as getEventHashWithJavaScript,
  isValidEvent as isValidEventWithJavaScript
} from '../event/js/index.js'
import { getPublicKey } from '../key/index.js'
import { getPublicKey as getPublicKeyWithJavaScript } from '../key/js/index.js'

const sampleMs = Number(process.env.BENCH_SAMPLE_MS ?? 150)
const sampleCount = Number(process.env.BENCH_SAMPLES ?? 7)
const warmupMs = Number(process.env.BENCH_WARMUP_MS ?? 250)
let sink = 0

function measure (fn, durationMs) {
  let operations = 0
  const start = process.hrtime.bigint()
  const stop = start + BigInt(Math.ceil(durationMs * 1e6))
  do {
    const value = fn()
    sink ^= typeof value === 'boolean'
      ? Number(value)
      : typeof value === 'string'
        ? value.charCodeAt(0)
        : value?.id?.charCodeAt(0) ?? value?.[0] ?? 0
    operations++
  } while (process.hrtime.bigint() < stop)
  return Number(process.hrtime.bigint() - start) / operations / 1000
}

function benchmark (fn) {
  measure(fn, warmupMs)
  const values = []
  for (let index = 0; index < sampleCount; index++) {
    if (globalThis.gc) globalThis.gc()
    values.push(measure(fn, sampleMs))
  }
  values.sort((a, b) => a - b)
  return values[Math.floor(values.length / 2)]
}

const backend = await initializeEventCrypto()
if (backend !== 'wasm') throw Error(`WASM benchmark requires the wasm backend, got ${backend}`)

const secretKey = new Uint8Array(32).fill(7)
const operations = [
  ['getPublicKey', () => getPublicKeyWithJavaScript(secretKey), () => getPublicKey(secretKey)]
]
for (const size of [8 * 1024, 256 * 1024, 1024 * 1024]) {
  const makeTemplate = () => ({
    kind: 1,
    created_at: 1_700_000_000,
    tags: [['t', 'benchmark']],
    content: 'a'.repeat(size)
  })
  const event = finalizeEventWithJavaScript(makeTemplate(), secretKey)
  const suffix = size === 8 * 1024 ? '8 KiB' : `${size / 1024} KiB`
  operations.push(
    [`getEventHash ${suffix}`, () => getEventHashWithJavaScript(event), () => getEventHash(event)],
    [`isValidEvent ${suffix}`, () => isValidEventWithJavaScript(event), () => isValidEvent(event)],
    [`finalizeEvent ${suffix}`, () => finalizeEventWithJavaScript(makeTemplate(), secretKey), () => finalizeEvent(makeTemplate(), secretKey)]
  )
}

const results = operations.map(([name, javaScript, wasm]) => {
  const javascriptMicroseconds = benchmark(javaScript)
  const wasmMicroseconds = benchmark(wasm)
  return {
    name,
    javascriptMicroseconds,
    wasmMicroseconds,
    speedup: javascriptMicroseconds / wasmMicroseconds
  }
})

for (const result of results) {
  console.log(
    `${result.name}: JS ${result.javascriptMicroseconds.toFixed(1)} us, ` +
    `WASM ${result.wasmMicroseconds.toFixed(1)} us, ${result.speedup.toFixed(2)}x`
  )
}
console.log(JSON.stringify({ backend, results, sink }, null, 2))
