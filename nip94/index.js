import { FILE_METADATA } from '../kind/index.js'
import { nfileDecode } from '../nip19/index.js'
import { ValidationError } from '../error/index.js'

// NIP-94 fields plus the IRFS r/service and ThumbHash extensions. SHA-256
// x/ox are optional in this profile; an MMR root is never substituted for x.
export function decodeFileMetadata (event) {
  if (event?.kind !== FILE_METADATA || typeof event.content !== 'string' || !Array.isArray(event.tags)) throw new ValidationError('INVALID_FILE_METADATA')
  const field = name => {
    const tags = event.tags.filter(tag => Array.isArray(tag) && tag[0] === name)
    if (tags.length > 1 || (tags.length && (tags[0].length < 2 || typeof tags[0][1] !== 'string'))) throw new ValidationError('INVALID_FILE_METADATA_TAG')
    return tags[0]?.[1]
  }
  const downloadTags = event.tags.filter(tag => Array.isArray(tag) && tag[0] === 'download')
  const download = downloadTags[0]
  if (downloadTags.length > 1 || (download && (download.length > 2 || (download.length === 2 && !['0', '1'].includes(download[1]))))) throw new ValidationError('INVALID_FILE_METADATA_DOWNLOAD')
  const result = { url: field('url'), mime: field('m'), caption: event.content, download: download ? download.length === 1 ? '1' : download[1] : '0' }
  let url
  try { url = new URL(result.url) } catch { throw new ValidationError('INVALID_FILE_METADATA_URL') }
  if (!['https:', 'http:'].includes(url.protocol) || url.username || url.password) throw new ValidationError('INVALID_FILE_METADATA_URL')
  if (typeof result.mime !== 'string' || !/^[a-z0-9!#$&^_.+-]+\/[a-z0-9!#$&^_.+-]+$/.test(result.mime)) throw new ValidationError('INVALID_FILE_METADATA_MIME')
  for (const [tag, key] of [['r', 'root'], ['service', 'service'], ['thumbhash', 'thumbhash'], ['x', 'sha256'], ['ox', 'originalSha256'], ['alt', 'alt']]) {
    const value = field(tag)
    if (value !== undefined) result[key] = value
  }
  for (const key of ['root', 'sha256', 'originalSha256']) if (result[key] !== undefined && !/^[0-9a-f]{64}$/.test(result[key])) throw new ValidationError('INVALID_FILE_METADATA_HASH')
  const size = field('size')
  if (size !== undefined) {
    if (!/^(0|[1-9][0-9]*)$/.test(size) || !Number.isSafeInteger(Number(size))) throw new ValidationError('INVALID_FILE_METADATA_SIZE')
    result.size = Number(size)
  }
  const dim = field('dim')
  if (dim !== undefined) {
    if (!/^[1-9][0-9]*x[1-9][0-9]*$/.test(dim)) throw new ValidationError('INVALID_FILE_METADATA_DIMENSIONS')
    ;[result.width, result.height] = dim.split('x').map(Number)
    if (![result.width, result.height].every(Number.isSafeInteger)) throw new ValidationError('INVALID_FILE_METADATA_DIMENSIONS')
  }
  if (result.thumbhash !== undefined && !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(result.thumbhash)) throw new ValidationError('INVALID_FILE_METADATA_THUMBHASH')
  if (url.origin === 'https://nostr.alt') {
    const file = nfileDecode(url.pathname.slice(1))
    if ((result.root && result.root !== file.root) || (file.mime && file.mime !== result.mime)) throw new ValidationError('FILE_METADATA_NFILE_MISMATCH')
    result.filename = file.filename
  }
  return result
}

export function createFileMetadata ({ caption = '', created_at: createdAt = Math.floor(Date.now() / 1000), tags = [], ...metadata }) {
  if (!Number.isSafeInteger(createdAt) || createdAt < 0 || !Array.isArray(tags)) throw new ValidationError('INVALID_FILE_METADATA')
  if (metadata.download !== undefined && !['0', '1'].includes(metadata.download)) throw new ValidationError('INVALID_FILE_METADATA_DOWNLOAD')
  const fields = [['download', metadata.download], ['url', metadata.url], ['m', metadata.mime], ['r', metadata.root], ['size', metadata.size], ['service', metadata.service], ['thumbhash', metadata.thumbhash], ['x', metadata.sha256], ['ox', metadata.originalSha256], ['alt', metadata.alt]]
  if (metadata.width !== undefined || metadata.height !== undefined) fields.push(['dim', `${metadata.width}x${metadata.height}`])
  const event = { kind: FILE_METADATA, created_at: createdAt, content: caption, tags: [...fields.filter(([, value]) => value !== undefined).map(([key, value]) => [key, String(value)]), ...tags.map(tag => [...tag])] }
  decodeFileMetadata(event)
  return event
}
