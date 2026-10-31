import { ValidationError } from '../../../error/index.js'

export function normalizePreparedState (prepared) {
  if (!prepared) return { prepared, migrated: false }
  const legacy = prepared.publication
  const publications = prepared.publications !== undefined
    ? prepared.publications
    : legacy !== undefined
      ? [legacy]
      : null
  if (!Array.isArray(publications) || publications.length === 0) {
    throw new ValidationError('INVALID_PREPARED_PUBLICATIONS')
  }
  prepared.publications = publications
  delete prepared.publication
  return { prepared, migrated: legacy !== undefined }
}
