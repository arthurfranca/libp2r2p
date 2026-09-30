import { test } from 'node:test'
import assert from 'node:assert/strict'
import { encodeRecoveryRecord, decodeRecoveryRecord, createPersonalCopyRecoveryStorage } from 'libp2r2p/private-messenger/event-store'
const a = 'a'.repeat(64), b = 'b'.repeat(64), c = 'c'.repeat(64), d = 'd'.repeat(64)
const grant = { controlChannelPubkey: a, fileChannelPubkey: b, peerPubkey: c, receiverPubkey: c, root: d, size: 51001, sharedAt: 100, expiresAt: 604900 }
const seed = { recordType: 'routerEnvelopeRow_v1', channelPubkey: a, receiverPubkey: c, router: { kind: 26300, pubkey: b, created_at: 100, tags: [['f', a], ['p', c], ['extra', '~u=9', '%7E']] }, firstSeenAt: 100, lastSeenAt: 200, payloadRow: '["payload"]', row: JSON.stringify([c, 'encrypted-key', a, 'proof']), expiresAt: 604900 }

test('recovery codec authenticates complete immutable snapshots and survives CRDT decorations', () => {
  for (const [type, row] of [['grant', grant], ['seed', seed]]) {
    const event = encodeRecoveryRecord(type, row)
    assert.equal(event.content, '')
    assert.ok(!event.tags[0][1].includes(':v1:'))
    const decorated = { ...event, tags: event.tags.map(t => [...t, '~u=123;o=abcd']).concat([['~', 'u=123']]) }
    const decoded = decodeRecoveryRecord(decorated)
    assert.equal(decoded.type, type)
    assert.deepEqual(encodeRecoveryRecord(type, decoded.row), event)
    if (type === 'seed') assert.deepEqual(decoded.row.router.tags, row.router.tags)
    const corrupt = structuredClone(event)
    corrupt.tags.find(t => t[0] === 'expiration')[1] = '999999'
    assert.throws(() => decodeRecoveryRecord(corrupt), { code: 'INVALID_RECOVERY_RECORD' })
  }
})
test('daily candidates include every crossed day and switch to literal wildcard after 32', () => {
  const tags = encodeRecoveryRecord('seed', { ...seed, firstSeenAt: 86400 - 1, lastSeenAt: 86400 * 3, expiresAt: 86400 * 50 }).tags
  assert.deepEqual(tags.filter(t => t[0] === 'D').map(t => t[1]), ['0', '1', '2', '3'])
  assert.deepEqual(encodeRecoveryRecord('seed', { ...seed, firstSeenAt: 0, lastSeenAt: 86400 * 32, expiresAt: 86400 * 50 }).tags.filter(t => t[0] === 'D'), [['D', '*']])
})
function fixture () {
  const rows = new Map(), deleted = new Set(), calls = []
  const signer = { getPublicKey: async () => a, obfuscate: async (value, kind, scope) => `${scope}|${value}`, nip44v3: { decrypt: async (owner, kind, scope, text) => new TextEncoder().encode(text).buffer } }
  const eventStore = {
    async addPersonalCopy (event) {
      calls.push(event)
      if (event.kind === 5) {
        for (const tag of event.tags.filter(t => t[0] === 'a')) { const key = tag[1].slice(`30078:${a}:`.length); deleted.add(key); rows.delete(key) }
        return { result: { ok: true } }
      }
      const key = event.tags[0][1]
      if (deleted.has(key)) return { result: { ok: false, code: 'blocked' } }
      const tags = [['k', '30078'], ['c', '|'], ['v', '1'], ...event.tags.filter(t => t[0] === 'expiration'), ...event.tags.filter(t => t[0].length === 1).map(t => ['o', `#${t[0]}|${t[1]}`])]
      const wrapper = { id: key, pubkey: a, kind: 1006, created_at: event.created_at, content: JSON.stringify(event), tags }
      rows.set(key, wrapper)
      return { event: wrapper, result: { ok: true, stored: true } }
    },
    async query (filter) {
      const matches = [...rows.values()].filter(e => (filter.until === undefined || e.created_at <= filter.until) && !filter['!ids']?.includes(e.id) && Object.entries(filter).every(([key, values]) => !['#', '&'].includes(key[0]) || (key[0] === '#' ? values.some(v => e.tags.some(t => t[0] === key.slice(1) && t[1] === v)) : values.every(v => e.tags.some(t => t[0] === key.slice(1) && t[1] === v)))))
      return { results: matches.sort((x, y) => y.created_at - x.created_at || x.id.localeCompare(y.id)).slice(0, filter.limit) }
    },
    async remove (targets) { for (const [, id] of targets) rows.delete(id) }
  }
  return { rows, calls, signer, eventStore }
}
test('paired adapters see new grants, deduplicate writes, revoke and reject replay', async () => {
  const f = fixture()
  const one = createPersonalCopyRecoveryStorage({ ...f, now: () => 200 })
  const two = createPersonalCopyRecoveryStorage({ ...f, now: () => 200 })
  await one.authorizations.put(grant)
  await one.authorizations.put(grant)
  assert.equal(f.calls.length, 1)
  const received = await two.authorizations.find(grant)
  assert.equal(received.sharedAt, 100)
  await two.authorizations.revoke([received])
  assert.equal(await one.authorizations.find(grant), undefined)
  await assert.rejects(one.authorizations.put(grant), { code: 'blocked' })
  assert.ok(f.calls.find(e => e.kind === 5).tags.some(t => t[0] === 'a' && t[1].startsWith(`30078:${a}:libp2r2p:recovery:grant:`)))
  one.close(); two.close()
})
test('paged same-second seeds are all returned; day prefilter retains wildcard records', async () => {
  const f = fixture(), storage = createPersonalCopyRecoveryStorage({ ...f, now: () => 200 })
  for (let i = 0; i < 260; i++) await storage.seeds.put({ ...seed, payloadRow: JSON.stringify([String(i)]) })
  const list = []; for await (const row of storage.seeds.iterate({ channelPubkey: a, receiverPubkey: c, since: 100, until: 200 })) list.push(row)
  assert.equal(list.length, 260)
  assert.equal(new Set(list.map(r => r.recordId)).size, 260)
  storage.close()
})

test('nym snapshots preserve signed carriers and ordered repeated tags', async () => {
  const { finalizeEvent } = await import('libp2r2p/event')
  const carrier = finalizeEvent({ kind: 26400, created_at: 100, tags: [['id', a], ['c', '0', '1'], ['extra', '~u=4']], content: 'encrypted-carrier' }, new Uint8Array(32).fill(9))
  const row = { recordType: 'nymCarrier_v1', channelPubkey: b, carriers: [carrier], expiresAt: 604900 }
  const event = encodeRecoveryRecord('seed', row)
  const decoded = decodeRecoveryRecord(event)
  assert.deepEqual(decoded.row.carriers, [carrier])
  const bad = structuredClone(event); bad.tags.find(t => t[0] === 'carrier')[7] = 'changed'
  assert.throws(() => decodeRecoveryRecord(bad), { code: 'INVALID_RECOVERY_RECORD' })
})

test('a transient signer failure does not poison later storage attempts', async () => {
  const f = fixture()
  const original = f.signer.getPublicKey
  let fail = true
  f.signer.getPublicKey = async () => { if (fail) { fail = false; throw new Error('SIGNER_LOCKED') } return original() }
  const storage = createPersonalCopyRecoveryStorage({ ...f, now: () => 200 })
  await assert.rejects(storage.authorizations.put(grant), /SIGNER_LOCKED/)
  await storage.authorizations.put(grant)
  assert.equal((await storage.authorizations.find(grant)).root, grant.root)
  storage.close()
})

test('expiry cleanup uses wrapper metadata without decrypting retained seeds', async () => {
  const f = fixture()
  const storage = createPersonalCopyRecoveryStorage({ ...f, now: () => 200 })
  await storage.seeds.put(seed)
  f.signer.nip44v3.decrypt = async () => { throw new Error('CLEANUP_MUST_NOT_DECRYPT') }
  await storage.seeds.prune({ now: 300 })
  assert.equal(f.rows.size, 1)
  await storage.seeds.prune({ now: seed.expiresAt })
  assert.equal(f.rows.size, 0)
  storage.close()
})
