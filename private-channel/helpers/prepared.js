import { ValidationError } from '../../error/index.js'

export function normalizePreparedPublications ({ preparedPublication, preparedPublications } = {}) {
  if (preparedPublication !== undefined && preparedPublications !== undefined) {
    throw new ValidationError('CONFLICTING_PREPARED_PUBLICATIONS')
  }
  if (preparedPublications !== undefined) {
    if (!Array.isArray(preparedPublications) || preparedPublications.length === 0) {
      throw new ValidationError('INVALID_PREPARED_PUBLICATIONS')
    }
    return preparedPublications
  }
  return preparedPublication ? [preparedPublication] : []
}
