import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { DelegationRegistry } from '../src/delegation-registry.mjs'
import { SessionCommands } from '../src/session-commands.mjs'

const MODEL = { provider: 'deepseek-official', model: 'deepseek-flash' }

function event(seq, type, data = {}) {
  return { type, seq, time: seq, data }
}

function page(events, request = {}) {
  const through = request.throughSeq ?? Infinity
  const before = request.beforeSeq ?? Infinity
  const selected = events.filter(item => item.seq <= through && item.seq < before)
  return {
    records: selected.map(item => ({ type: 'event', event: item })),
    hasMore: false,
    cursor: events.at(-1)?.seq ?? -1,
  }
}

async function commandsFor(root, label, events) {
  const registry = new DelegationRegistry({ filePath: join(root, `${label}.json`) })
  await registry.beginCreate({
    delegationId: label,
    cwd: join(root, label),
    workspaceId: 'workspace-test',
    sessionId: `session-${label}`,
    title: label,
    expectedModel: MODEL,
  })
  await registry.markReady(label)
  return new SessionCommands({
    registry,
    adapter: { sessionEvents: request => Promise.resolve(page(events, request)) },
    historyPageMessages: 2,
  })
}

test('verification requires a real request in the target turn', { timeout: 30_000 }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-request-window-'))
  t.after(async () => { await rm(root, { recursive: true, force: true }) })
  const header = event(0, 'request/header', { header: { config: MODEL } })

  await t.test('a canceled turn with no model request cannot inherit an older header', async () => {
    const commands = await commandsFor(root, 'canceled', [
      header,
      event(1, 'user/message', { source: { rpcId: 'canceled-request' } }),
      event(2, 'step/start', { turn: 1, step: 1 }),
      event(3, 'step/end', { turn: 1, step: 1 }),
      event(4, 'turn/end', { turn: 1, reason: { kind: 'aborted' } }),
    ])
    await assert.rejects(
      commands.verifyRequestModel({ delegationId: 'canceled', requestId: 'canceled-request' }),
      error => {
        assert.equal(error?.code, 'delegation/request-not-proven')
        return true
      },
    )
  })

  await t.test('a real request with an inherited unchanged header still verifies', async () => {
    const commands = await commandsFor(root, 'inherited', [
      header,
      event(1, 'user/message', { source: { rpcId: 'first-request' } }),
      event(2, 'assistant/message', {}),
      event(3, 'turn/end', { turn: 1, reason: { kind: 'completed' } }),
      event(4, 'user/message', { source: { rpcId: 'second-request' } }),
      event(5, 'assistant/message', {}),
      event(6, 'turn/end', { turn: 2, reason: { kind: 'completed' } }),
    ])
    const verified = await commands.verifyRequestModel({
      delegationId: 'inherited',
      requestId: 'second-request',
    })
    assert.deepEqual({ provider: verified.provider, model: verified.model }, MODEL)
    assert.equal(verified.seq, 0, 'the inherited header is the older proof')
    assert.equal(verified.turnEndSeq, 6)
  })
})
