const ERROR_CODE = /^[A-Z][A-Z0-9_]*$/

export class ValidationError extends Error {
  constructor (code, messageOrOptions = code, causeOrOptions) {
    if (typeof code !== 'string' || !ERROR_CODE.test(code)) {
      throw new TypeError('Validation error code should be uppercase snake case')
    }
    const objectOptions = messageOrOptions && typeof messageOrOptions === 'object'
      ? messageOrOptions
      : null
    const message = objectOptions === messageOrOptions
      ? (objectOptions.message ?? code)
      : (messageOrOptions ?? code)
    const cause = objectOptions
      ? objectOptions.cause
      : (causeOrOptions && typeof causeOrOptions === 'object' && Object.hasOwn(causeOrOptions, 'cause')
          ? causeOrOptions.cause
          : causeOrOptions)
    super(message, cause === undefined ? undefined : { cause })
    Object.defineProperty(this, 'name', {
      configurable: true,
      value: 'ValidationError',
      writable: true
    })
    Object.defineProperty(this, 'code', {
      configurable: false,
      enumerable: true,
      value: code,
      writable: false
    })
  }
}
