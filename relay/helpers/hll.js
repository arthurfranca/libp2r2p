const REGISTER_COUNT = 256
const HLL_HEX_LENGTH = REGISTER_COUNT * 2

function assertRegisters (registers) {
  if (!(registers instanceof Uint8Array) || registers.length !== REGISTER_COUNT) {
    throw new Error('INVALID_HLL_REGISTERS')
  }
}

// NIP-45 serializes one uint8 register for each of the 256 HLL buckets.
export function decodeHll (value) {
  if (typeof value !== 'string' || value.length !== HLL_HEX_LENGTH || !/^[0-9a-fA-F]+$/.test(value)) {
    return null
  }

  const registers = new Uint8Array(REGISTER_COUNT)
  for (let index = 0; index < REGISTER_COUNT; index++) {
    registers[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16)
  }
  return registers
}

export function encodeHll (registers) {
  assertRegisters(registers)

  let value = ''
  for (const register of registers) value += register.toString(16).padStart(2, '0')
  return value
}

// Combining independent relay sketches keeps the largest observation per bucket.
export function mergeHll (target, source) {
  assertRegisters(target)
  assertRegisters(source)

  for (let index = 0; index < REGISTER_COUNT; index++) {
    if (source[index] > target[index]) target[index] = source[index]
  }
  return target
}

// Small sketches use linear counting; larger ones use the standard HLL estimate.
export function estimateHllCount (registers) {
  assertRegisters(registers)

  let zeroes = 0
  let sum = 0
  for (const register of registers) {
    if (register === 0) zeroes++
    sum += 1 / 2 ** register
  }

  if (zeroes > 0) {
    const linearCount = REGISTER_COUNT * Math.log(REGISTER_COUNT / zeroes)
    if (linearCount <= 220) return Math.floor(linearCount)
    if (sum > 0 && (0.7182725932495458 * REGISTER_COUNT * REGISTER_COUNT) / sum <= REGISTER_COUNT * 3) {
      return Math.floor(linearCount)
    }
  }

  return Math.floor((0.7182725932495458 * REGISTER_COUNT * REGISTER_COUNT) / sum)
}
