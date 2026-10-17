// Select only operationally useful public coordinates. Never attach a packet,
// signer, plaintext, scope or message key to an error that can reach a logger.
export function decryptionError (cause, context) {
  const error = new Error(cause.message, { cause })
  error.name = cause.name
  const code = cause.code || (cause.message === 'CONTENT_KEY_NOT_FOUND' ? 'CONTENT_KEY_NOT_FOUND' : cause.message === 'invalid MAC' ? 'INVALID_MAC' : undefined)
  if (code) error.code = code
  for (const key of ['stage', 'channelPubkey', 'eventId', 'eventKind', 'senderPubkey', 'receiverPubkey', 'ownContentPubkey', 'peerContentPubkey', 'hasOwnContentKey']) {
    const value = context?.[key] ?? cause[key]
    if (['string', 'number', 'boolean'].includes(typeof value)) error[key] = value
  }
  return error
}

export const isDeferredDecryptionError = error => ['CONTENT_KEY_NOT_FOUND', 'INVALID_MAC'].includes(error?.code)
