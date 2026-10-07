// Keep actual permissions and cryptography in the signer. Background presence
// only yields before its next RPC, without interrupting an operation in flight.
export function backgroundSigner (signer, wait, signal) {
  if (!signer || !wait) return signer
  return new Proxy(signer, {
    get (target, key) {
      const value = Reflect.get(target, key, target)
      if (typeof value !== 'function') return value
      return async (...args) => {
        await wait(signal)
        signal?.throwIfAborted()
        return value.apply(target, args)
      }
    }
  })
}
