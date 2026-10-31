import { ValidationError } from '../../error/index.js'

export function normalizePreparedPublications ({ preparedPublication, preparedPublications } = {}) {
  if (preparedPublication !== undefined) throw new ValidationError('LEGACY_PREPARED_PUBLICATION')
  if (preparedPublications !== undefined) {
    if (!Array.isArray(preparedPublications) || preparedPublications.length === 0) {
      throw new ValidationError('INVALID_PREPARED_PUBLICATIONS')
    }
    return preparedPublications
  }
  return []
}
