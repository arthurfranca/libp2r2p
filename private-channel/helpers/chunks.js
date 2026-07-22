import { generateSecretKey, getPublicKey } from '../../key/index.js'
import { bytesToBase64, base64ToBytes } from '../../base64/index.js'
import { bytesToHex } from '../../base16/index.js'
import { verifyIykcProof } from '../../content-key/event/index.js'
import * as nip44v3 from '../../nip44-v3/index.js'
import { JSONL_CHUNK_BYTES } from './chunk-size.js'
import { ROUTER_KIND } from '../constants/index.js'
import { createTemporaryStorage } from '../../temporary-storage/index.js'

const encoder = new TextEncoder()
const decoder = new TextDecoder()
const STORAGE_PREFIX = 'libp2r2p:private-channel:'

function appendBytes (left, right) {
  const out = new Uint8Array(left.length + right.length)
  out.set(left)
  out.set(right, left.length)
  return out
}

function tempKey (id, index) {
  return `${STORAGE_PREFIX}${id}:${index}`
}

function rowTempKey (id, index) {
  return `${STORAGE_PREFIX}${id}:row:${index}`
}

function normalizeContentKey ({ receiverPubkey, iykcPubkey = '', iykcProof = '' } = {}) {
  if (!iykcPubkey) return { iykcPubkey: '', iykcProof: '' }
  if (!verifyIykcProof({ receiverPubkey, iykcPubkey, iykcProof })) throw new Error('INVALID_IYKC_PROOF')
  return { iykcPubkey, iykcProof }
}

function receiverRecord (receiver, receiverContentKeys) {
  if (typeof receiver === 'string') {
    const fetchedContentKey = normalizeContentKey({ receiverPubkey: receiver, ...receiverContentKeys[receiver] })
    return {
      receiverPubkey: receiver,
      ...fetchedContentKey
    }
  }

  if (Array.isArray(receiver)) {
    const [receiverPubkey, iykcPubkey = '', iykcProof = ''] = receiver
    const explicitContentKey = normalizeContentKey({ receiverPubkey, iykcPubkey, iykcProof })
    const fetchedContentKey = normalizeContentKey({ receiverPubkey, ...receiverContentKeys[receiverPubkey] })
    const contentKey = explicitContentKey.iykcPubkey ? explicitContentKey : fetchedContentKey
    return {
      receiverPubkey,
      ...contentKey
    }
  }

  const receiverPubkey = receiver?.receiverPubkey || receiver?.pubkey || ''
  const explicitContentKey = normalizeContentKey({ receiverPubkey, ...receiver })
  const fetchedContentKey = normalizeContentKey({ receiverPubkey, ...receiverContentKeys[receiverPubkey] })
  const resolvedContentKey = explicitContentKey.iykcPubkey ? explicitContentKey : fetchedContentKey
  return {
    receiverPubkey,
    ...resolvedContentKey
  }
}

function buildRecipientRow ({ receiverPubkey, iykcPubkey, iykcProof }, ciphertext) {
  const line = [receiverPubkey, ciphertext]
  if (iykcPubkey) line.push(iykcPubkey, iykcProof)
  return JSON.stringify(line)
}

function buildPayloadRow (ciphertext) {
  return JSON.stringify([ciphertext])
}

function encryptedPayload ({ messageSecretKey, event }) {
  const messagePubkey = getPublicKey(messageSecretKey)
  return nip44v3.encrypt(messageSecretKey, messagePubkey, ROUTER_KIND, '', JSON.stringify(event))
}

function appendLine (chunk, line, id, chunkIndex, temporaryStorage) {
  while (line.length) {
    const available = JSONL_CHUNK_BYTES - chunk.length
    chunk = appendBytes(chunk, line.slice(0, available))
    line = line.slice(available)
    if (chunk.length === JSONL_CHUNK_BYTES) {
      temporaryStorage.setItem(tempKey(id, chunkIndex++), bytesToBase64(chunk))
      chunk = new Uint8Array()
    }
  }
  return { chunk, chunkIndex }
}

function appendRow (chunk, row, id, chunkIndex, temporaryStorage) {
  return appendLine(chunk, encoder.encode(`${row}\n`), id, chunkIndex, temporaryStorage)
}

function joinByteChunks (parts) {
  let length = 0
  const decoded = parts.map(part => {
    const bytes = base64ToBytes(part)
    length += bytes.length
    return bytes
  })
  const out = new Uint8Array(length)
  let offset = 0
  for (const bytes of decoded) {
    out.set(bytes, offset)
    offset += bytes.length
  }
  return out
}

function storageFor (temporaryStorage) {
  return temporaryStorage || createTemporaryStorage()
}

export function readChunkContent (id, index, temporaryStorage) {
  return storageFor(temporaryStorage).getItem(tempKey(id, index))
}

export function decodeChunkLines (content) {
  return decodeChunkText(content).split('\n').filter(Boolean)
}

export function decodeChunkText (content) {
  return decoder.decode(base64ToBytes(content))
}

export function joinChunksAsBase64 (parts) {
  return bytesToBase64(joinByteChunks(parts))
}

export function receiverPubkeys (receivers) {
  return receivers.map(receiver => receiverRecord(receiver, {}).receiverPubkey).filter(Boolean)
}

export function receiverPubkeysWithoutContentKeys (receivers) {
  return receivers
    .map(receiver => receiverRecord(receiver, {}))
    .filter(receiver => receiver.receiverPubkey && !receiver.iykcPubkey)
    .map(receiver => receiver.receiverPubkey)
}

function temporaryId () {
  return `${Date.now()}:${Math.random().toString(16).slice(2)}`
}

function cleanupPreparedRows (id, totalRows, temporaryStorage) {
  const keys = []
  for (let i = 0; i < totalRows; i++) keys.push(rowTempKey(id, i))
  storageFor(temporaryStorage).removeItems(keys)
}

function setPreparedRow (id, index, row, temporaryStorage) {
  storageFor(temporaryStorage).setItem(rowTempKey(id, index), row)
}

function readPreparedRow (preparedRows, index) {
  const row = storageFor(preparedRows.temporaryStorage).getItem(rowTempKey(preparedRows.id, index))
  if (typeof row !== 'string') throw new Error('MISSING_PREPARED_ROW')
  return row
}

async function prepareEnvelopeRowsOnce ({ id, senderSigner, receivers, receiverContentKeys = {}, event, rowScope = '', temporaryStorage }) {
  const useDoubleDh = typeof senderSigner?.nip44EncryptDoubleDH === 'function'
  let foundOwnContentPubkey = false
  let usedOwnContentPubkey = ''
  const messageSecretKey = generateSecretKey()
  const messageSeckey = bytesToHex(messageSecretKey)
  const rowIndexes = []
  const receiverPubkeys = []
  const receiverRowIndexesByPubkey = {}

  setPreparedRow(id, 0, buildPayloadRow(encryptedPayload({ messageSecretKey, event })), temporaryStorage)

  for (const receiver of receivers) {
    const row = receiverRecord(receiver, useDoubleDh ? receiverContentKeys : {})
    let ciphertext
    if (useDoubleDh) {
      const encrypted = await senderSigner.nip44EncryptDoubleDH(
        row.receiverPubkey,
        ROUTER_KIND,
        rowScope,
        bytesToBase64(encoder.encode(messageSeckey)),
        row.iykcPubkey
      )
      ciphertext = encrypted[0]
      const nextContentPubkey = encrypted[1] || ''
      if (foundOwnContentPubkey && nextContentPubkey !== usedOwnContentPubkey) {
        throw new Error('INCONSISTENT_IMKC_PUBKEY')
      }
      foundOwnContentPubkey = true
      if (nextContentPubkey) usedOwnContentPubkey = nextContentPubkey
    } else {
      ciphertext = await senderSigner.nip44v3Encrypt(row.receiverPubkey, ROUTER_KIND, rowScope, bytesToBase64(encoder.encode(messageSeckey)))
    }
    const rowIndex = rowIndexes.length + 1
    setPreparedRow(id, rowIndex, buildRecipientRow(row, ciphertext), temporaryStorage)
    rowIndexes.push(rowIndex)
    receiverPubkeys.push(row.receiverPubkey)
    if (row.receiverPubkey && receiverRowIndexesByPubkey[row.receiverPubkey] === undefined) {
      receiverRowIndexesByPubkey[row.receiverPubkey] = rowIndex
    }
  }

  return {
    id,
    totalRows: rowIndexes.length + 1,
    rowIndexes,
    receiverPubkeys,
    receiverRowIndexesByPubkey,
    ownContentPubkey: usedOwnContentPubkey,
    temporaryStorage
  }
}

export async function prepareEnvelopeRows ({ temporaryStorageArea, ...options } = {}) {
  const temporaryStorage = createTemporaryStorage({ storageArea: temporaryStorageArea })
  const maxRows = (options.receivers?.length || 0) + 1
  for (let attempt = 0; attempt < 2; attempt++) {
    const id = temporaryId()
    try {
      return await prepareEnvelopeRowsOnce({ ...options, id, temporaryStorage })
    } catch (err) {
      cleanupPreparedRows(id, maxRows, temporaryStorage)
      if (err?.message === 'INCONSISTENT_IMKC_PUBKEY' && attempt === 0) continue
      throw err
    }
  }
}

export function cleanupEnvelopeRows (preparedRows) {
  if (!preparedRows?.id || !Number.isSafeInteger(preparedRows.totalRows)) return
  cleanupPreparedRows(preparedRows.id, preparedRows.totalRows, preparedRows.temporaryStorage)
}

export function preparedRowIndexesForReceivers (preparedRows, receivers) {
  const indexes = []
  const seen = new Set()
  for (const receiver of receivers || []) {
    const pubkey = receiverRecord(receiver, {}).receiverPubkey
    const index = preparedRows?.receiverRowIndexesByPubkey?.[pubkey]
    if (!pubkey || index === undefined) throw new Error('MISSING_PREPARED_RECEIVER')
    if (seen.has(index)) continue
    seen.add(index)
    indexes.push(index)
  }
  return indexes
}

export function writeChunksFromPreparedRows (preparedRows, rowIndexes = preparedRows?.rowIndexes || []) {
  const temporaryStorage = storageFor(preparedRows?.temporaryStorage)
  const id = temporaryId()
  let chunk = new Uint8Array()
  let chunkIndex = 0
  try {
    ;({ chunk, chunkIndex } = appendRow(chunk, readPreparedRow(preparedRows, 0), id, chunkIndex, temporaryStorage))
    for (const rowIndex of rowIndexes) {
      ;({ chunk, chunkIndex } = appendRow(chunk, readPreparedRow(preparedRows, rowIndex), id, chunkIndex, temporaryStorage))
    }
    if (chunk.length || chunkIndex === 0) temporaryStorage.setItem(tempKey(id, chunkIndex++), bytesToBase64(chunk))
    return { id, total: chunkIndex, ownContentPubkey: preparedRows.ownContentPubkey || '' }
  } catch (err) {
    cleanupChunks(id, chunkIndex + 1, temporaryStorage)
    throw err
  }
}

export async function writeChunks (options) {
  const preparedRows = await prepareEnvelopeRows(options)
  try {
    return writeChunksFromPreparedRows(preparedRows)
  } finally {
    cleanupEnvelopeRows(preparedRows)
  }
}

export function cleanupChunks (id, total, temporaryStorage) {
  const keys = []
  for (let i = 0; i < total; i++) keys.push(tempKey(id, i))
  storageFor(temporaryStorage).removeItems(keys)
}
