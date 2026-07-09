import { afterEach, test } from 'node:test'
import assert from 'node:assert/strict'
import {
  cleanupTemporaryStorage,
  getTemporaryItem,
  removeTemporaryItems,
  setTemporaryItem,
  TEMPORARY_STORAGE_KEYS_KEY
} from '../temporary-storage/index.js'

const data = new Map()
let failOnSetKey = ''

globalThis.localStorage = {
  clear: () => data.clear(),
  getItem: key => data.has(String(key)) ? data.get(String(key)) : null,
  removeItem: key => { data.delete(String(key)) },
  setItem: (key, value) => {
    if (key === failOnSetKey) throw new Error('set failed')
    data.set(String(key), String(value))
  }
}

afterEach(() => {
  failOnSetKey = ''
  globalThis.localStorage.clear()
})

function trackedKeys () {
  const raw = globalThis.localStorage.getItem(TEMPORARY_STORAGE_KEYS_KEY)
  return raw ? JSON.parse(raw) : []
}

test('setTemporaryItem tracks a key before storing the value', () => {
  failOnSetKey = 'tmp.fail'

  assert.throws(() => setTemporaryItem('tmp.fail', 'secret'), /set failed/)

  assert.deepEqual(trackedKeys(), ['tmp.fail'])
  assert.equal(globalThis.localStorage.getItem('tmp.fail'), null)
})

test('cleanupTemporaryStorage removes tracked fields and the tracker field', () => {
  setTemporaryItem('tmp.one', 'a')
  setTemporaryItem('tmp.two', 'b')
  globalThis.localStorage.setItem('permanent', 'keep')

  cleanupTemporaryStorage()

  assert.equal(getTemporaryItem('tmp.one'), null)
  assert.equal(getTemporaryItem('tmp.two'), null)
  assert.equal(globalThis.localStorage.getItem(TEMPORARY_STORAGE_KEYS_KEY), null)
  assert.equal(globalThis.localStorage.getItem('permanent'), 'keep')
})

test('removeTemporaryItems untracks fields cleaned during normal operation', () => {
  setTemporaryItem('tmp.one', 'a')
  setTemporaryItem('tmp.two', 'b')

  removeTemporaryItems(['tmp.one'])

  assert.equal(getTemporaryItem('tmp.one'), null)
  assert.equal(getTemporaryItem('tmp.two'), 'b')
  assert.deepEqual(trackedKeys(), ['tmp.two'])
})
