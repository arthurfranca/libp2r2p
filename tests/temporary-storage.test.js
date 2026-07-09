import { afterEach, test } from 'node:test'
import assert from 'node:assert/strict'
import {
  cleanupTemporaryStorage,
  createTemporaryStorage,
  getTemporaryItem,
  removeTemporaryItems,
  setTemporaryItem,
  TEMPORARY_STORAGE_KEYS_KEY
} from '../temporary-storage/index.js'

const localData = new Map()
const sessionData = new Map()
let failOnSetKey = ''

function storageFor (data) {
  return {
    clear: () => data.clear(),
    getItem: key => data.has(String(key)) ? data.get(String(key)) : null,
    removeItem: key => { data.delete(String(key)) },
    setItem: (key, value) => {
      if (key === failOnSetKey) throw new Error('set failed')
      data.set(String(key), String(value))
    }
  }
}

globalThis.localStorage = storageFor(localData)
globalThis.sessionStorage = storageFor(sessionData)

afterEach(() => {
  failOnSetKey = ''
  globalThis.localStorage.clear()
  globalThis.sessionStorage.clear()
})

function trackedKeys () {
  const raw = globalThis.sessionStorage.getItem(TEMPORARY_STORAGE_KEYS_KEY)
  return raw ? JSON.parse(raw) : []
}

test('setTemporaryItem tracks a key before storing the value', () => {
  failOnSetKey = 'tmp.fail'

  assert.throws(() => setTemporaryItem('tmp.fail', 'secret'), /set failed/)

  assert.deepEqual(trackedKeys(), ['tmp.fail'])
  assert.equal(globalThis.sessionStorage.getItem('tmp.fail'), null)
  assert.equal(globalThis.localStorage.getItem('tmp.fail'), null)
})

test('cleanupTemporaryStorage removes tracked fields and the tracker field', () => {
  setTemporaryItem('tmp.one', 'a')
  setTemporaryItem('tmp.two', 'b')
  globalThis.sessionStorage.setItem('permanent', 'keep')

  cleanupTemporaryStorage()

  assert.equal(getTemporaryItem('tmp.one'), null)
  assert.equal(getTemporaryItem('tmp.two'), null)
  assert.equal(globalThis.sessionStorage.getItem(TEMPORARY_STORAGE_KEYS_KEY), null)
  assert.equal(globalThis.sessionStorage.getItem('permanent'), 'keep')
})

test('removeTemporaryItems untracks fields cleaned during normal operation', () => {
  setTemporaryItem('tmp.one', 'a')
  setTemporaryItem('tmp.two', 'b')

  removeTemporaryItems(['tmp.one'])

  assert.equal(getTemporaryItem('tmp.one'), null)
  assert.equal(getTemporaryItem('tmp.two'), 'b')
  assert.deepEqual(trackedKeys(), ['tmp.two'])
})

test('createTemporaryStorage isolates a caller-supplied area', () => {
  const storage = createTemporaryStorage({ storageArea: globalThis.localStorage })

  storage.setItem('tmp.local', 'value')

  assert.equal(globalThis.localStorage.getItem('tmp.local'), 'value')
  assert.equal(globalThis.localStorage.getItem(TEMPORARY_STORAGE_KEYS_KEY), JSON.stringify(['tmp.local']))
  assert.equal(globalThis.sessionStorage.getItem(TEMPORARY_STORAGE_KEYS_KEY), null)

  storage.cleanup()

  assert.equal(globalThis.localStorage.getItem('tmp.local'), null)
  assert.equal(globalThis.localStorage.getItem(TEMPORARY_STORAGE_KEYS_KEY), null)
})
