import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { mkdir, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { after, test } from 'node:test'
import { loadUpstream } from '../src/upstream.mjs'
import { DelegationRegistry } from '../src/delegation-registry.mjs'
import { SessionCommands } from '../src/session-commands.mjs'
import { createRemoteAdapter } from '../src/operator-adapter.mjs'
import {
  EventReader, extractMessages, extractRequestHeaders, extractToolActivity, readSubagentStatus,
} from '../src/event-reader.mjs'
import { StateWaiter, deriveStatus } from '../src/state-waiter.mjs'
import { startScriptedAgentServer } from './support/scripted-agent-server.mjs'
import { createScratchRoot } from './support/scratch.mjs'
import { startDshWeb } from './support/dsh-web-harness.mjs'
import { connectOperator } from './support/operator.mjs'

function sleep(milliseconds) {
  return new Promise(resolve => setTimeout(resolve, milliseconds))
}

function eventsOf(page) {
  return page.records.map(record => record.event)
}

function errorCode(code) {
  return error => {
    assert.equal(error?.code, code, `expected ${code}, got ${String(error?.code)}: ${error?.message}`)
    return true
  }
}

async function waitForTurn(adapter, sessionId, requestId) {
  const deadline = Date.now() + 60_000
  while (Date.now() < deadline) {
    const state = await adapter.sessionState({ sessionId })
    const page = await adapter.sessionEvents({ address: { kind: 'session', sessionId }, maxMessages: 500 })
    const events = eventsOf(page)
    const user = events.find(event => event.type === 'user/message' && event.data.source?.rpcId === requestId)
    const finished = user !== undefined
      && events.some(event => event.type === 'turn/end' && event.seq > user.seq)
    if (finished && !state.running && (state.queue === null || state.queue.length === 0)) {
      return { state, events }
    }
    await sleep(100)
  }
  throw new Error(`turn for ${requestId} in ${sessionId} did not finish`)
}

// ─── Synthetic pagination fixtures (no Host) ────────────────────────────────

function wireEvent(seq, type = 'fixture/event') {
  return { type, seq, time: 1_000 + seq, data: { seq } }
}

function historyPage(cursor, overrides = {}) {
  const records = []
  for (let seq = 0; seq <= cursor; seq += 1) {
    const event = overrides[seq] ?? wireEvent(seq)
    records.push({ type: 'event', event })
  }
  return { cursor, hasMore: false, records }
}

function turnEndEvent(seq) {
  return {
    type: 'turn/end',
    seq,
    time: 1,
    data: { turn: 1, reason: { kind: 'completed' } },
  }
}

function fixturePage(cursor, seqs, hasMore = false) {
  return {
    cursor,
    hasMore,
    records: seqs.map(seq => ({ type: 'event', event: wireEvent(seq) })),
  }
}

function scriptedEventAdapter(pages) {
  const requests = []
  return {
    requests,
    async sessionEvents(request) {
      requests.push(request)
      const page = pages.shift()
      if (page === undefined) throw new Error('unscripted session.events call')
      return page
    },
  }
}

test('slice3: EventReader fails loudly on gaps, duplicates, disorder and nonadvancing pages', async () => {
  const reader = (pages) => new EventReader({ adapter: scriptedEventAdapter(pages) })

  await assert.rejects(
    reader([fixturePage(4, [0, 1, 2, 4])]).readRange({ sessionId: 'session-gap', afterSeq: -1 }),
    error => {
      errorCode('events/sequence-gap')(error)
      assert.equal(error.details.kind, 'missing')
      assert.equal(error.details.expectedSeq, 3)
      assert.equal(error.details.actualSeq, 4)
      assert.equal(error.details.throughSeq, 4)
      return true
    },
  )

  await assert.rejects(
    reader([fixturePage(2, [0, 1, 1, 2])]).readRange({ sessionId: 'session-dup', afterSeq: -1 }),
    error => {
      errorCode('events/sequence-gap')(error)
      assert.equal(error.details.kind, 'duplicate')
      assert.equal(error.details.actualSeq, 1)
      return true
    },
  )

  await assert.rejects(
    reader([fixturePage(2, [0, 2, 1])]).readRange({ sessionId: 'session-order', afterSeq: -1 }),
    error => {
      errorCode('events/sequence-gap')(error)
      assert.equal(error.details.kind, 'order')
      assert.equal(error.details.actualSeq, 1)
      return true
    },
  )

  const stalled = scriptedEventAdapter([
    fixturePage(5, [3, 4, 5], true),
    fixturePage(5, [3, 4, 5], true),
  ])
  await assert.rejects(
    new EventReader({ adapter: stalled }).readRange({ sessionId: 'session-stall', afterSeq: -1 }),
    error => {
      errorCode('events/pagination-stalled')(error)
      assert.equal(error.details.beforeSeq, 3)
      assert.equal(error.details.actualSeq, 3)
      return true
    },
  )
  assert.equal(stalled.requests[1].beforeSeq, 3)
  assert.equal(stalled.requests[1].throughSeq, 5)

  await assert.rejects(
    reader([fixturePage(5, [3, 4, 5], true), fixturePage(5, [], true)])
      .readRange({ sessionId: 'session-empty', afterSeq: -1 }),
    errorCode('events/pagination-stalled'),
  )

  await assert.rejects(
    reader([fixturePage(5, [3, 4, 5], true), fixturePage(6, [0, 1, 2], false)])
      .readRange({ sessionId: 'session-moved', afterSeq: -1 }),
    error => {
      errorCode('events/cursor-moved')(error)
      assert.equal(error.details.pinnedCursor, 5)
      assert.equal(error.details.actualCursor, 6)
      return true
    },
  )
})

test('slice3: EventReader pins the tail cursor and reconstructs [afterSeq + 1 .. cursor] across pages', async () => {
  const adapter = scriptedEventAdapter([
    fixturePage(6, [4, 5, 6], true),
    fixturePage(6, [1, 2, 3], false),
  ])
  const result = await new EventReader({ adapter }).readRange({
    sessionId: 'session-paged',
    afterSeq: 1,
    pageSize: 2,
  })

  assert.equal(result.cursor, 6)
  assert.equal(result.pinnedCursor, 6)
  assert.equal(result.pagesRead, 2)
  assert.deepEqual(result.events.map(event => event.seq), [2, 3, 4, 5, 6])
  assert.deepEqual(adapter.requests.map(request => [
    request.throughSeq ?? null,
    request.beforeSeq ?? null,
    request.maxMessages,
  ]), [[null, null, 2], [6, 4, 2]])

  const empty = await new EventReader({
    adapter: scriptedEventAdapter([fixturePage(3, [0, 1, 2, 3])]),
  }).readRange({ sessionId: 'session-caught-up', afterSeq: 3 })
  assert.deepEqual(empty.events, [])
  assert.equal(empty.cursor, 3)
})

test('slice3: EventReader treats beyond-head cursors as empty and inverted bounds as bad args', async () => {
  const beyond = await new EventReader({
    adapter: scriptedEventAdapter([fixturePage(3, [0, 1, 2, 3])]),
  }).readRange({ sessionId: 'session-beyond-head', afterSeq: 9 })
  assert.deepEqual(beyond.events, [])
  assert.equal(beyond.cursor, 3)

  await assert.rejects(
    new EventReader({ adapter: scriptedEventAdapter([]) }).readRange({
      sessionId: 'session-inverted',
      afterSeq: 5,
      throughSeq: 3,
    }),
    error => {
      errorCode('events/bad-range')(error)
      assert.equal(error.details.afterSeq, 5)
      assert.equal(error.details.throughSeq, 3)
      return true
    },
  )
})

test('slice3: deriveStatus and input-required waits resolve without probing subagents', async () => {
  assert.equal(deriveStatus({ pendingInput: ['question'], running: true, queue: [{ id: 'q' }] }), 'input-required')
  assert.equal(deriveStatus({ pendingInput: [], running: true, queue: [{ id: 'q' }] }), 'running')
  assert.equal(deriveStatus({ pendingInput: [], running: false, queue: [{ id: 'q' }] }), 'queued')
  assert.equal(deriveStatus({ pendingInput: [], running: false, queue: [] }), 'idle')
  assert.equal(deriveStatus({ pendingInput: [], running: false }), 'idle')

  const registry = {
    async get(delegationId) {
      return { delegationId, sessionId: 'session-input', state: 'ready' }
    },
  }
  let probes = 0
  const adapter = {
    async sessionWait(request) {
      return {
        reason: 'input-required',
        state: { cursor: 7, active: true, running: false, queue: [], pendingInput: ['approval'] },
        output: { records: [], truncated: false },
      }
    },
    async sessionState() { probes += 1; throw new Error('must not probe state') },
    async sessionProjections() { probes += 1; throw new Error('must not probe projections') },
    async sessionEvents() { probes += 1; throw new Error('must not read events') },
  }
  const waiter = new StateWaiter({ registry, adapter })
  const result = await waiter.wait({ delegationId: 'input', afterSeq: -1, timeoutMs: 5_000 })
  assert.equal(result.reason, 'input-required')
  assert.deepEqual(result.pendingInput, ['approval'])
  assert.equal(result.slices, 1)
  assert.equal(result.subagents, null)
  assert.equal(probes, 0)
})

test('slice3: waitForSubagents retries unavailable child status and returns explicit indeterminate', async () => {
  const registry = {
    async get(delegationId) {
      return { delegationId, sessionId: 'session-indeterminate', state: 'ready' }
    },
  }
  let now = 0
  let probes = 0
  const idleState = { cursor: 4, active: true, running: false, queue: [], pendingInput: [] }
  const adapter = {
    async sessionWait() {
      return { reason: 'idle', state: idleState, output: { records: [], truncated: false } }
    },
    async sessionProjections() {
      probes += 1
      const error = new Error('projection read failed')
      error.code = 'gateway/internal'
      throw error
    },
    async sessionEvents() { throw new Error('events are not read without truncation') },
  }
  const waiter = new StateWaiter({
    registry,
    adapter,
    clock: () => now,
    sleep: async milliseconds => { now += milliseconds },
    subagentPollMs: 100,
  })
  const result = await waiter.wait({
    delegationId: 'indeterminate',
    timeoutMs: 500,
    sliceTimeoutMs: 200,
    waitForSubagents: true,
  })
  assert.equal(result.reason, 'timeout', 'unavailable child status must not be reported as idle')
  assert.equal(result.indeterminate, true)
  assert.equal(result.indeterminateReason, 'subagent-status-unavailable')
  assert.equal(result.subagents.available, false)
  assert.ok(probes >= 2, 'the waiter retries the projection within the deadline')
})

test('slice3: waitForSubagents recovers from transient child-status failure', async () => {
  const registry = {
    async get(delegationId) {
      return { delegationId, sessionId: 'session-recovering', state: 'ready' }
    },
  }
  let now = 0
  let probes = 0
  const idleState = { cursor: 4, active: true, running: false, queue: [], pendingInput: [] }
  const adapter = {
    async sessionWait() {
      return { reason: 'idle', state: idleState, output: { records: [], truncated: false } }
    },
    async sessionProjections() {
      probes += 1
      if (probes === 1) throw new Error('transient projection failure')
      return { kind: 'sequenced', asOfSeq: 4, values: { subagentCatalog: [] } }
    },
    async sessionEvents() { throw new Error('events are not read without truncation') },
  }
  const waiter = new StateWaiter({
    registry,
    adapter,
    clock: () => now,
    sleep: async milliseconds => { now += milliseconds },
    subagentPollMs: 100,
  })
  const result = await waiter.wait({
    delegationId: 'recovering',
    timeoutMs: 2_000,
    sliceTimeoutMs: 200,
    waitForSubagents: true,
  })
  assert.equal(result.reason, 'idle')
  assert.equal(result.indeterminate, false)
  assert.equal(result.indeterminateReason, null)
  assert.ok(result.slices >= 2, 'the waiter retried before declaring idle')
  assert.equal(probes, 2)
})

test('slice3: waitForSubagents reports indeterminate when a local child is unreadable', async () => {
  const registry = {
    async get(delegationId) {
      return { delegationId, sessionId: 'session-child-unreadable', state: 'ready' }
    },
  }
  let now = 0
  const idleState = { cursor: 6, active: true, running: false, queue: [], pendingInput: [] }
  const adapter = {
    async sessionWait() {
      return { reason: 'idle', state: idleState, output: { records: [], truncated: false } }
    },
    async sessionProjections({ sessionId }) {
      if (sessionId === 'child-unreadable') {
        return { kind: 'sequenced', asOfSeq: 2, values: {} }
      }
      return {
        kind: 'sequenced',
        asOfSeq: 6,
        values: {
          subagentCatalog: [{ id: 'child-unreadable', mode: 'one-shot', label: 'child', createdAt: 1 }],
        },
      }
    },
    async sessionState() {
      const error = new Error('child state unavailable')
      error.code = 'session/not-found'
      throw error
    },
    async sessionEvents() { throw new Error('events are not read without truncation') },
  }
  const waiter = new StateWaiter({
    registry,
    adapter,
    clock: () => now,
    sleep: async milliseconds => { now += milliseconds },
    subagentPollMs: 100,
  })
  const result = await waiter.wait({
    delegationId: 'child-unreadable',
    timeoutMs: 500,
    sliceTimeoutMs: 200,
    waitForSubagents: true,
  })
  assert.equal(result.reason, 'timeout')
  assert.equal(result.indeterminate, true)
  assert.equal(result.indeterminateReason, 'subagent-status-unavailable')
  assert.deepEqual(result.activeSubagents, [])
  assert.deepEqual(result.subagents.unavailableSubagents.map(entry => entry.id), ['child-unreadable'])
})

test('slice3: nonempty root queue with running=false is queued and never idle in wait', async () => {
  const queuedState = {
    cursor: 9,
    active: true,
    running: false,
    queue: [{ id: 'queued-message', placement: 'queued' }],
    pendingInput: [],
  }
  const registry = {
    async get(delegationId) {
      return { delegationId, sessionId: 'session-queued', state: 'ready' }
    },
  }
  const statusWaiter = new StateWaiter({
    registry,
    adapter: {
      async sessionState() { return queuedState },
      async sessionWait() { throw new Error('status does not wait') },
      async sessionEvents() { throw new Error('status does not read events') },
      async sessionProjections() {
        return { kind: 'sequenced', asOfSeq: 9, values: { subagentCatalog: [] } }
      },
    },
  })
  const status = await statusWaiter.getStatus({ delegationId: 'queued-status' })
  assert.equal(status.status, 'queued')
  assert.deepEqual(status.subagentCatalog, [])

  let now = 0
  const waiter = new StateWaiter({
    registry,
    adapter: {
      async sessionWait() {
        return { reason: 'idle', state: queuedState, output: { records: [], truncated: false } }
      },
      async sessionProjections() {
        throw new Error('subagent status is not needed without waitForSubagents')
      },
      async sessionEvents() { throw new Error('events are not read without truncation') },
    },
    clock: () => now,
    sleep: async milliseconds => { now += milliseconds },
    subagentPollMs: 100,
  })
  const result = await waiter.wait({
    delegationId: 'queued-wait',
    timeoutMs: 500,
    sliceTimeoutMs: 200,
  })
  assert.equal(result.reason, 'timeout', 'queued work must not be reported as idle')
  assert.equal(result.state.queue.length, 1)
  assert.equal(result.indeterminate, false)
})

test('slice3: waitForSubagents joins live grandchildren behind an idle direct child', async () => {
  const registry = {
    async get(delegationId) {
      return { delegationId, sessionId: 'session-root', state: 'ready' }
    },
  }
  let now = 0
  let grandRunning = true
  let waits = 0
  const rootState = { cursor: 21, active: true, running: false, queue: [], pendingInput: [] }
  const adapter = {
    async sessionWait() {
      waits += 1
      if (waits === 2) grandRunning = false
      return { reason: 'idle', state: rootState, output: { records: [], truncated: false } }
    },
    async sessionProjections({ sessionId }) {
      if (sessionId === 'session-root') {
        return {
          kind: 'sequenced',
          asOfSeq: 21,
          values: { subagentCatalog: [{ id: 'child', mode: 'one-shot', label: 'child', createdAt: 1 }] },
        }
      }
      if (sessionId === 'child') {
        return {
          kind: 'sequenced',
          asOfSeq: 5,
          values: { subagentCatalog: [{ id: 'grand', mode: 'one-shot', label: 'grand', createdAt: 2 }] },
        }
      }
      if (sessionId === 'grand') {
        return { kind: 'sequenced', asOfSeq: 2, values: { subagentCatalog: [] } }
      }
      throw new Error(`unexpected projections for ${sessionId}`)
    },
    async sessionState({ sessionId }) {
      if (sessionId === 'session-root') {
        return { ...rootState, running: false, queue: [] }
      }
      if (sessionId === 'child') {
        return { cursor: 5, active: false, running: false, queue: [], pendingInput: [] }
      }
      if (sessionId === 'grand') {
        return {
          cursor: 2,
          active: grandRunning,
          running: grandRunning,
          queue: [],
          pendingInput: [],
        }
      }
      throw new Error(`unexpected state for ${sessionId}`)
    },
    async sessionEvents({ address }) {
      if (address.kind === 'subagent') {
        assert.deepEqual(address, {
          kind: 'subagent',
          parentSessionId: 'session-root',
          childSessionId: 'child',
          mode: 'one-shot',
        })
        return historyPage(7, {
          6: {
            type: 'user/message',
            seq: 6,
            time: 1,
            data: {
              content: [{ type: 'text', text: 'grand settled' }],
              source: { kind: 'subagent-settled', senderSessionId: 'grand', stopReason: 'completed' },
            },
          },
          7: turnEndEvent(7),
        })
      }
      assert.deepEqual(address, { kind: 'session', sessionId: 'session-root' })
      return historyPage(21, {
        20: {
          type: 'user/message',
          seq: 20,
          time: 1,
          data: {
            content: [{ type: 'text', text: 'child settled' }],
            source: { kind: 'subagent-settled', senderSessionId: 'child', stopReason: 'completed' },
          },
        },
        21: turnEndEvent(21),
      })
    },
  }
  const waiter = new StateWaiter({
    registry,
    adapter,
    clock: () => now,
    sleep: async milliseconds => { now += milliseconds },
    subagentPollMs: 50,
  })
  const result = await waiter.wait({
    delegationId: 'grandchild',
    timeoutMs: 2_000,
    sliceTimeoutMs: 200,
    waitForSubagents: true,
  })
  assert.equal(result.reason, 'idle')
  assert.ok(result.slices >= 2, 'the idle direct child did not hide the live grandchild')
  assert.equal(result.subagents.catalog.length, 1, 'the direct catalog stays direct-only')
  assert.deepEqual(result.subagents.descendants.map(entry => entry.id), ['child', 'grand'])
  assert.deepEqual(result.subagents.localSubagents.map(entry => entry.id), ['child', 'grand'])
  assert.deepEqual(result.subagents.activeSubagents, [])
})

test('slice3: waitForSubagents reports external and unknown rows as indeterminate without hanging', async () => {
  const registry = {
    async get(delegationId) {
      return { delegationId, sessionId: 'session-boundary', state: 'ready' }
    },
  }
  let now = 0
  const rootState = { cursor: 8, active: true, running: false, queue: [], pendingInput: [] }
  const adapter = {
    async sessionWait() {
      return { reason: 'idle', state: rootState, output: { records: [], truncated: false } }
    },
    async sessionProjections() {
      return {
        kind: 'sequenced',
        asOfSeq: 8,
        values: {
          subagentCatalog: [
            { id: 'remote-child', mode: 'external', label: 'remote', createdAt: 1 },
            { id: 'legacy-child', mode: 'unknown', label: 'legacy', createdAt: 2 },
          ],
        },
      }
    },
    async sessionEvents() { throw new Error('events are not read without truncation') },
  }
  const waiter = new StateWaiter({
    registry,
    adapter,
    clock: () => now,
    sleep: async milliseconds => { now += milliseconds },
    subagentPollMs: 50,
  })
  const startedAt = now
  const result = await waiter.wait({
    delegationId: 'external-boundary',
    timeoutMs: 10_000,
    sliceTimeoutMs: 200,
    waitForSubagents: true,
  })
  assert.equal(result.reason, 'indeterminate')
  assert.equal(result.indeterminate, true)
  assert.equal(result.indeterminateReason, 'subagent-external-status-unavailable')
  assert.equal(result.slices, 1, 'an external row must not be retried until the deadline')
  assert.ok(now - startedAt < 1_000)
  assert.deepEqual(result.subagents.externalSubagents.map(entry => entry.id), ['remote-child'])
  assert.deepEqual(result.subagents.unknownSubagents.map(entry => entry.id), ['legacy-child'])

  const rootOnly = await waiter.wait({
    delegationId: 'root-only',
    timeoutMs: 10_000,
    sliceTimeoutMs: 200,
    waitForSubagents: false,
  })
  assert.equal(rootOnly.reason, 'idle', 'normal root-only waits ignore unprovable child rows')
  assert.equal(rootOnly.indeterminate, false)
})

test('slice3: settlement notice delayed past the old grace still prevents idle', async () => {
  const registry = {
    async get(delegationId) {
      return { delegationId, sessionId: 'session-root', state: 'ready' }
    },
  }
  let now = 0
  const childSettledAt = 50
  const noticeAt = 700
  const noticeClearedAt = 900
  const noticeEvent = {
    type: 'event',
    event: {
      type: 'user/message',
      seq: 31,
      time: noticeAt,
      data: {
        content: [{ type: 'text', text: 'child settled' }],
        source: {
          kind: 'subagent-settled',
          senderSessionId: 'child',
          stopReason: 'completed',
        },
      },
    },
  }
  const rootState = () => ({
    cursor: now >= noticeAt ? 32 : 30,
    active: true,
    running: false,
    queue: now >= noticeAt && now < noticeClearedAt
      ? [{ id: 'settlement-notice', placement: 'queued' }]
      : [],
    pendingInput: [],
  })
  const adapter = {
    async sessionWait() {
      return { reason: 'idle', state: rootState(), output: { records: [], truncated: false } }
    },
    async sessionProjections({ sessionId }) {
      if (sessionId === 'session-root') {
        return {
          kind: 'sequenced',
          asOfSeq: 30,
          values: { subagentCatalog: [{ id: 'child', mode: 'one-shot', label: 'child', createdAt: 1 }] },
        }
      }
      if (sessionId === 'child') {
        return { kind: 'sequenced', asOfSeq: 4, values: { subagentCatalog: [] } }
      }
      throw new Error(`unexpected projections for ${sessionId}`)
    },
    async sessionState({ sessionId }) {
      if (sessionId === 'session-root') return rootState()
      if (sessionId !== 'child') throw new Error(`unexpected state for ${sessionId}`)
      return {
        cursor: 4,
        active: now < childSettledAt,
        running: now < childSettledAt,
        queue: [],
        pendingInput: [],
      }
    },
    async sessionEvents({ address }) {
      assert.deepEqual(address, { kind: 'session', sessionId: 'session-root' })
      if (now >= noticeAt) {
        return historyPage(32, { 31: noticeEvent.event, 32: turnEndEvent(32) })
      }
      return historyPage(30)
    },
  }
  const waiter = new StateWaiter({
    registry,
    adapter,
    clock: () => now,
    sleep: async milliseconds => { now += milliseconds },
    subagentPollMs: 50,
  })
  const result = await waiter.wait({
    delegationId: 'notice-race',
    timeoutMs: 2_000,
    sliceTimeoutMs: 200,
    waitForSubagents: true,
  })
  assert.equal(result.reason, 'idle')
  assert.ok(result.elapsedMs >= noticeAt,
    `wait must observe the delayed durable notice (${result.elapsedMs} ms)`)
  assert.ok(result.elapsedMs >= noticeClearedAt,
    'wait must recheck root state after the notice turn clears the queue')
  assert.deepEqual(result.state.queue, [])
  assert.deepEqual(result.subagents.activeSubagents, [])
})

test('slice3: cold one-shot without a consumed notice is explicitly indeterminate', async () => {
  const registry = {
    async get(delegationId) {
      return { delegationId, sessionId: 'session-root', state: 'ready' }
    },
  }
  let now = 0
  const adapter = {
    async sessionWait() {
      return {
        reason: 'idle',
        state: { cursor: 30, active: true, running: false, queue: [], pendingInput: [] },
        output: { records: [], truncated: false },
      }
    },
    async sessionProjections({ sessionId }) {
      if (sessionId === 'session-root') {
        return {
          kind: 'sequenced',
          asOfSeq: 30,
          values: { subagentCatalog: [{ id: 'child', mode: 'one-shot', label: 'child', createdAt: 1 }] },
        }
      }
      if (sessionId === 'child') {
        return { kind: 'sequenced', asOfSeq: 4, values: { subagentCatalog: [] } }
      }
      throw new Error(`unexpected projections for ${sessionId}`)
    },
    async sessionState({ sessionId }) {
      if (sessionId === 'session-root') {
        return { cursor: 30, active: true, running: false, queue: [], pendingInput: [] }
      }
      if (sessionId !== 'child') throw new Error(`unexpected state for ${sessionId}`)
      return {
        cursor: 4,
        active: false,
        running: false,
        queue: [],
        pendingInput: [],
      }
    },
    async sessionEvents() {
      return historyPage(30)
    },
  }
  const waiter = new StateWaiter({
    registry,
    adapter,
    clock: () => now,
    sleep: async milliseconds => { now += milliseconds },
    subagentPollMs: 50,
  })
  const result = await waiter.wait({
    delegationId: 'notice-missing',
    timeoutMs: 600,
    sliceTimeoutMs: 200,
    waitForSubagents: true,
  })
  assert.equal(result.reason, 'indeterminate')
  assert.equal(result.indeterminate, true)
  assert.equal(result.indeterminateReason, 'subagent-settlement-epoch-unverifiable')
  assert.ok(result.elapsedMs < 600, 'unprovable cold settlement is not polled to the deadline')
})

test('slice3: unreadable parent notice history returns indeterminate without hanging', async () => {
  const registry = {
    async get(delegationId) {
      return { delegationId, sessionId: 'session-root', state: 'ready' }
    },
  }
  let now = 0
  const adapter = {
    async sessionWait() {
      return {
        reason: 'idle',
        state: { cursor: 30, active: true, running: false, queue: [], pendingInput: [] },
        output: { records: [], truncated: false },
      }
    },
    async sessionProjections({ sessionId }) {
      if (sessionId === 'session-root') {
        return {
          kind: 'sequenced',
          asOfSeq: 30,
          values: { subagentCatalog: [{ id: 'child', mode: 'one-shot', label: 'child', createdAt: 1 }] },
        }
      }
      if (sessionId === 'child') {
        return { kind: 'sequenced', asOfSeq: 4, values: { subagentCatalog: [] } }
      }
      throw new Error(`unexpected projections for ${sessionId}`)
    },
    async sessionState({ sessionId }) {
      if (sessionId === 'session-root') {
        return { cursor: 30, active: true, running: false, queue: [], pendingInput: [] }
      }
      if (sessionId !== 'child') throw new Error(`unexpected state for ${sessionId}`)
      return {
        cursor: 4,
        active: now < 50,
        running: now < 50,
        queue: [],
        pendingInput: [],
      }
    },
    async sessionEvents() {
      const error = new Error('history unavailable')
      error.code = 'session/agent-busy'
      throw error
    },
  }
  const waiter = new StateWaiter({
    registry,
    adapter,
    clock: () => now,
    sleep: async milliseconds => { now += milliseconds },
    subagentPollMs: 50,
  })
  const result = await waiter.wait({
    delegationId: 'notice-unavailable',
    timeoutMs: 10_000,
    sliceTimeoutMs: 200,
    waitForSubagents: true,
  })
  assert.equal(result.reason, 'indeterminate')
  assert.equal(result.indeterminate, true)
  assert.equal(result.indeterminateReason, 'subagent-settlement-notice-unavailable')
  assert.ok(result.elapsedMs < 1_000, 'unobservable notice evidence must not wait for the deadline')
})

test('slice3: child pending input is never reported as idle', async () => {
  const registry = {
    async get(delegationId) {
      return { delegationId, sessionId: 'session-root', state: 'ready' }
    },
  }
  let now = 0
  const adapter = {
    async sessionWait() {
      return {
        reason: 'idle',
        state: { cursor: 20, active: true, running: false, queue: [], pendingInput: [] },
        output: { records: [], truncated: false },
      }
    },
    async sessionProjections({ sessionId }) {
      if (sessionId === 'session-root') {
        return {
          kind: 'sequenced',
          asOfSeq: 20,
          values: { subagentCatalog: [{ id: 'child', mode: 'one-shot', label: 'child', createdAt: 1 }] },
        }
      }
      if (sessionId === 'child') {
        return { kind: 'sequenced', asOfSeq: 5, values: { subagentCatalog: [] } }
      }
      throw new Error(`unexpected projections for ${sessionId}`)
    },
    async sessionState({ sessionId }) {
      if (sessionId === 'session-root') {
        return { cursor: 20, active: true, running: false, queue: [], pendingInput: [] }
      }
      if (sessionId !== 'child') throw new Error(`unexpected state for ${sessionId}`)
      return {
        cursor: 5,
        active: true,
        running: false,
        queue: [],
        pendingInput: ['question'],
      }
    },
    async sessionEvents() { throw new Error('no settlement notice is needed for pending input') },
  }
  const waiter = new StateWaiter({
    registry,
    adapter,
    clock: () => now,
    sleep: async milliseconds => { now += milliseconds },
    subagentPollMs: 50,
  })
  const result = await waiter.wait({
    delegationId: 'child-input',
    timeoutMs: 2_000,
    sliceTimeoutMs: 200,
    waitForSubagents: true,
  })
  assert.equal(result.reason, 'indeterminate')
  assert.equal(result.indeterminate, true)
  assert.equal(result.indeterminateReason, 'subagent-input-required')
  assert.deepEqual(result.subagents.localSubagents[0].pendingInput, ['question'])
})

test('slice3: initially inactive siblings both match notices from one parent page', async () => {
  const registry = {
    async get(delegationId) {
      return { delegationId, sessionId: 'session-root', state: 'ready' }
    },
  }
  let now = 0
  const notice = (seq, childId) => ({
    type: 'user/message',
    seq,
    time: 1,
    data: {
      content: [{ type: 'text', text: `${childId} settled` }],
      source: { kind: 'subagent-settled', senderSessionId: childId, stopReason: 'completed' },
    },
  })
  const children = ['child-a', 'child-b']
  const adapter = {
    async sessionWait() {
      return {
        reason: 'idle',
        state: { cursor: 32, active: true, running: false, queue: [], pendingInput: [] },
        output: { records: [], truncated: false },
      }
    },
    async sessionProjections({ sessionId }) {
      if (sessionId === 'session-root') {
        return {
          kind: 'sequenced',
          asOfSeq: 32,
          values: {
            subagentCatalog: children.map((id, index) => ({
              id, mode: 'one-shot', label: id, createdAt: index + 1,
            })),
          },
        }
      }
      if (children.includes(sessionId)) {
        return { kind: 'sequenced', asOfSeq: 4, values: { subagentCatalog: [] } }
      }
      throw new Error(`unexpected projections for ${sessionId}`)
    },
    async sessionState({ sessionId }) {
      if (sessionId === 'session-root') {
        return { cursor: 32, active: true, running: false, queue: [], pendingInput: [] }
      }
      if (!children.includes(sessionId)) throw new Error(`unexpected state for ${sessionId}`)
      return {
        cursor: 4,
        active: false,
        running: false,
        queue: [],
        pendingInput: [],
      }
    },
    async sessionEvents({ address }) {
      assert.deepEqual(address, { kind: 'session', sessionId: 'session-root' })
      return historyPage(32, {
        30: notice(30, 'child-a'),
        31: notice(31, 'child-b'),
        32: turnEndEvent(32),
      })
    },
  }
  const waiter = new StateWaiter({
    registry,
    adapter,
    clock: () => now,
    sleep: async milliseconds => { now += milliseconds },
    subagentPollMs: 50,
  })
  const result = await waiter.wait({
    delegationId: 'siblings',
    timeoutMs: 2_000,
    sliceTimeoutMs: 200,
    waitForSubagents: true,
  })
  assert.equal(result.reason, 'idle')
  assert.ok(result.elapsedMs < 1_000, 'both sibling notices match before the deadline')
  assert.equal(result.subagents.localSubagents.length, 2)
})

test('slice3: fresh root cursor backfills completed follow-up output before idle', async () => {
  const registry = {
    async get(delegationId) {
      return { delegationId, sessionId: 'session-root', state: 'ready' }
    },
  }
  let now = 0
  const notice = {
    type: 'user/message',
    seq: 31,
    time: 1,
    data: {
      content: [{ type: 'text', text: 'child settled' }],
      source: { kind: 'subagent-settled', senderSessionId: 'child', stopReason: 'completed' },
    },
  }
  const followUp = (seq, text) => ({
    type: 'assistant/message',
    seq,
    time: 2 + seq,
    data: {
      turn: 2,
      step: 1,
      message: {
        role: 'assistant',
        content: [{ type: 'text', text }],
        source: { kind: 'model', provider: 'fixture', model: 'fixture' },
      },
    },
  })
  const adapter = {
    async sessionWait() {
      return {
        reason: 'idle',
        state: { cursor: 30, active: true, running: false, queue: [], pendingInput: [] },
        output: { records: [], truncated: false },
      }
    },
    async sessionProjections({ sessionId }) {
      if (sessionId === 'session-root') {
        return {
          kind: 'sequenced',
          asOfSeq: 30,
          values: { subagentCatalog: [{ id: 'child', mode: 'one-shot', label: 'child', createdAt: 1 }] },
        }
      }
      if (sessionId === 'child') {
        return { kind: 'sequenced', asOfSeq: 4, values: { subagentCatalog: [] } }
      }
      throw new Error(`unexpected projections for ${sessionId}`)
    },
    async sessionState({ sessionId }) {
      if (sessionId === 'session-root') {
        return { cursor: 34, active: true, running: false, queue: [], pendingInput: [] }
      }
      if (sessionId !== 'child') throw new Error(`unexpected state for ${sessionId}`)
      return {
        cursor: 4,
        active: false,
        running: false,
        queue: [],
        pendingInput: [],
      }
    },
    async sessionEvents() {
      return historyPage(34, {
        31: notice,
        32: turnEndEvent(32),
        33: followUp(33, 'FOLLOWUP_PREFIX '),
        34: followUp(34, 'FOLLOWUP_DONE'),
      })
    },
  }
  const waiter = new StateWaiter({
    registry,
    adapter,
    clock: () => now,
    sleep: async milliseconds => { now += milliseconds },
    subagentPollMs: 50,
  })
  const result = await waiter.wait({
    delegationId: 'follow-up-output',
    afterSeq: 30,
    timeoutMs: 2_000,
    sliceTimeoutMs: 200,
    waitForSubagents: true,
  })
  assert.equal(result.reason, 'idle')
  assert.equal(result.cursor, 34)
  assert.ok(result.backfilled >= 1)
  assert.ok(result.assistantMessages.some(message =>
    message.text.includes('FOLLOWUP_PREFIX') || message.text.includes('FOLLOWUP_DONE')))
})

test('slice3: fresh root pending input prevents idle and is reported as input-required', async () => {
  const registry = {
    async get(delegationId) {
      return { delegationId, sessionId: 'session-root', state: 'ready' }
    },
  }
  let now = 0
  const notice = {
    type: 'user/message',
    seq: 31,
    time: 1,
    data: {
      content: [{ type: 'text', text: 'child settled' }],
      source: { kind: 'subagent-settled', senderSessionId: 'child', stopReason: 'completed' },
    },
  }
  const adapter = {
    async sessionWait() {
      return {
        reason: 'idle',
        state: { cursor: 30, active: true, running: false, queue: [], pendingInput: [] },
        output: { records: [], truncated: false },
      }
    },
    async sessionProjections({ sessionId }) {
      if (sessionId === 'session-root') {
        return {
          kind: 'sequenced',
          asOfSeq: 30,
          values: { subagentCatalog: [{ id: 'child', mode: 'one-shot', label: 'child', createdAt: 1 }] },
        }
      }
      if (sessionId === 'child') {
        return { kind: 'sequenced', asOfSeq: 4, values: { subagentCatalog: [] } }
      }
      throw new Error(`unexpected projections for ${sessionId}`)
    },
    async sessionState({ sessionId }) {
      if (sessionId === 'session-root') {
        return {
          cursor: 32,
          active: true,
          running: false,
          queue: [],
          pendingInput: ['question'],
        }
      }
      if (sessionId !== 'child') throw new Error(`unexpected state for ${sessionId}`)
      return {
        cursor: 4,
        active: false,
        running: false,
        queue: [],
        pendingInput: [],
      }
    },
    async sessionEvents() {
      return historyPage(32, { 31: notice, 32: turnEndEvent(32) })
    },
  }
  const waiter = new StateWaiter({
    registry,
    adapter,
    clock: () => now,
    sleep: async milliseconds => { now += milliseconds },
    subagentPollMs: 50,
  })
  const result = await waiter.wait({
    delegationId: 'fresh-root-input',
    timeoutMs: 2_000,
    sliceTimeoutMs: 200,
    waitForSubagents: true,
  })
  assert.equal(result.reason, 'input-required')
  assert.deepEqual(result.pendingInput, ['question'])
  assert.equal(result.state.cursor, 32)
})

test('slice3: quiescent live child is not settled and prevents idle', async () => {
  const registry = {
    async get(delegationId) {
      return { delegationId, sessionId: 'session-root', state: 'ready' }
    },
  }
  let now = 0
  const adapter = {
    async sessionWait() {
      return {
        reason: 'idle',
        state: { cursor: 20, active: true, running: false, queue: [], pendingInput: [] },
        output: { records: [], truncated: false },
      }
    },
    async sessionProjections({ sessionId }) {
      if (sessionId === 'session-root') {
        return {
          kind: 'sequenced',
          asOfSeq: 20,
          values: { subagentCatalog: [{ id: 'child', mode: 'continuable', label: 'child', createdAt: 1 }] },
        }
      }
      if (sessionId === 'child') {
        return { kind: 'sequenced', asOfSeq: 5, values: { subagentCatalog: [] } }
      }
      throw new Error(`unexpected projections for ${sessionId}`)
    },
    async sessionState({ sessionId }) {
      if (sessionId === 'session-root') {
        return { cursor: 20, active: true, running: false, queue: [], pendingInput: [] }
      }
      if (sessionId !== 'child') throw new Error(`unexpected state for ${sessionId}`)
      return {
        cursor: 5,
        active: true,
        running: false,
        queue: [],
        pendingInput: [],
      }
    },
    async sessionEvents() { throw new Error('no settlement proof exists for a live child') },
  }
  const waiter = new StateWaiter({
    registry,
    adapter,
    clock: () => now,
    sleep: async milliseconds => { now += milliseconds },
    subagentPollMs: 50,
  })
  const result = await waiter.wait({
    delegationId: 'live-unsettled',
    timeoutMs: 2_000,
    sliceTimeoutMs: 200,
    waitForSubagents: true,
  })
  assert.equal(result.reason, 'indeterminate')
  assert.equal(result.indeterminateReason, 'subagent-live-unsettled')
  assert.equal(result.subagents.localSubagents[0].live, true)
  assert.equal(result.subagents.localSubagents[0].settled, false)
})

test('slice3: a continuable child settling twice requires a newer notice', async () => {
  const registry = {
    async get(delegationId) {
      return { delegationId, sessionId: 'session-root', state: 'ready' }
    },
  }
  let now = 0
  const firstNotice = {
    type: 'user/message',
    seq: 31,
    time: 1,
    data: {
      content: [{ type: 'text', text: 'first settlement' }],
      source: { kind: 'subagent-settled', senderSessionId: 'child', stopReason: 'completed' },
    },
  }
  const childRunning = () => now < 40 || (now >= 80 && now < 120)
  const adapter = {
    async sessionWait() {
      return {
        reason: 'idle',
        state: {
          cursor: now < 50 ? 30 : 32,
          active: true,
          running: false,
          queue: now >= 50 && now < 80
            ? [{ id: 'settlement-turn', placement: 'queued' }]
            : [],
          pendingInput: [],
        },
        output: { records: [], truncated: false },
      }
    },
    async sessionProjections({ sessionId }) {
      if (sessionId === 'session-root') {
        return {
          kind: 'sequenced',
          asOfSeq: 32,
          values: { subagentCatalog: [{ id: 'child', mode: 'continuable', label: 'child', createdAt: 1 }] },
        }
      }
      if (sessionId === 'child') {
        return { kind: 'sequenced', asOfSeq: 5, values: { subagentCatalog: [] } }
      }
      throw new Error(`unexpected projections for ${sessionId}`)
    },
    async sessionState({ sessionId }) {
      if (sessionId === 'session-root') {
        return {
          cursor: now < 50 ? 30 : 32,
          active: true,
          running: false,
          queue: now >= 50 && now < 80
            ? [{ id: 'settlement-turn', placement: 'queued' }]
            : [],
          pendingInput: [],
        }
      }
      if (sessionId !== 'child') throw new Error(`unexpected state for ${sessionId}`)
      const running = childRunning()
      return {
        cursor: 5,
        active: running,
        running,
        queue: [],
        pendingInput: [],
      }
    },
    async sessionEvents() {
      return historyPage(32, { 31: firstNotice, 32: turnEndEvent(32) })
    },
  }
  const waiter = new StateWaiter({
    registry,
    adapter,
    clock: () => now,
    sleep: async milliseconds => { now += milliseconds },
    subagentPollMs: 50,
  })
  const result = await waiter.wait({
    delegationId: 'second-settlement',
    timeoutMs: 600,
    sliceTimeoutMs: 200,
    waitForSubagents: true,
  })
  assert.equal(result.reason, 'indeterminate')
  assert.equal(result.indeterminate, true)
  assert.equal(result.indeterminateReason, 'subagent-settlement-epoch-unverifiable')
})

test('slice3: root is sampled after fresh-tree notices resolve', async () => {
  const registry = {
    async get(delegationId) {
      return { delegationId, sessionId: 'session-root', state: 'ready' }
    },
  }
  let now = 0
  let freshTreeRead = false
  let rootProjectionCalls = 0
  let rootSamples = 0
  let sawQueue = false
  const notice = (seq, childId) => ({
    type: 'user/message',
    seq,
    time: 1,
    data: {
      content: [{ type: 'text', text: `${childId} settled` }],
      source: { kind: 'subagent-settled', senderSessionId: childId, stopReason: 'completed' },
    },
  })
  const childRows = () => [
    { id: 'child-a', mode: 'one-shot', label: 'child-a', createdAt: 1 },
    ...freshTreeRead
      ? [{ id: 'child-b', mode: 'one-shot', label: 'child-b', createdAt: 2 }]
      : [],
  ]
  const adapter = {
    async sessionWait() {
      return {
        reason: 'idle',
        state: { cursor: 32, active: true, running: false, queue: [], pendingInput: [] },
        output: { records: [], truncated: false },
      }
    },
    async sessionProjections({ sessionId }) {
      if (sessionId === 'session-root') {
        rootProjectionCalls += 1
        freshTreeRead = rootProjectionCalls >= 2
        return {
          kind: 'sequenced',
          asOfSeq: 32,
          values: { subagentCatalog: childRows() },
        }
      }
      if (sessionId === 'child-a' || sessionId === 'child-b') {
        return { kind: 'sequenced', asOfSeq: 4, values: { subagentCatalog: [] } }
      }
      throw new Error(`unexpected projections for ${sessionId}`)
    },
    async sessionState({ sessionId }) {
      if (sessionId === 'session-root') {
        rootSamples += 1
        if (freshTreeRead && rootSamples === 1) {
          sawQueue = true
          return {
            cursor: 32,
            active: true,
            running: false,
            queue: [{ id: 'settlement-turn', placement: 'queued' }],
            pendingInput: [],
          }
        }
        return { cursor: 32, active: true, running: false, queue: [], pendingInput: [] }
      }
      if (sessionId === 'child-a' || sessionId === 'child-b') {
        return {
          cursor: 4,
          active: false,
          running: false,
          queue: [],
          pendingInput: [],
        }
      }
      throw new Error(`unexpected state for ${sessionId}`)
    },
    async sessionEvents() {
      return historyPage(32, {
        30: notice(30, 'child-a'),
        31: notice(31, 'child-b'),
        32: turnEndEvent(32),
      })
    },
  }
  const waiter = new StateWaiter({
    registry,
    adapter,
    clock: () => now,
    sleep: async milliseconds => { now += milliseconds },
    subagentPollMs: 50,
  })
  const result = await waiter.wait({
    delegationId: 'fresh-tree-ordering',
    timeoutMs: 2_000,
    sliceTimeoutMs: 200,
    waitForSubagents: true,
  })
  assert.equal(result.reason, 'idle')
  assert.equal(sawQueue, true, 'the root was sampled after the fresh-tree notice')
  assert.ok(rootSamples >= 2)
  assert.equal(result.subagents.localSubagents.length, 2)
})

test('slice3: cold continuable with a consumed historical notice stays indeterminate', async () => {
  const registry = {
    async get(delegationId) {
      return { delegationId, sessionId: 'session-root', state: 'ready' }
    },
  }
  let now = 0
  const notice = {
    type: 'user/message',
    seq: 31,
    time: 1,
    data: {
      content: [{ type: 'text', text: 'historical settlement' }],
      source: { kind: 'subagent-settled', senderSessionId: 'child', stopReason: 'completed' },
    },
  }
  const adapter = {
    async sessionWait() {
      return {
        reason: 'idle',
        state: { cursor: 32, active: true, running: false, queue: [], pendingInput: [] },
        output: { records: [], truncated: false },
      }
    },
    async sessionProjections({ sessionId }) {
      if (sessionId === 'session-root') {
        return {
          kind: 'sequenced',
          asOfSeq: 32,
          values: { subagentCatalog: [{ id: 'child', mode: 'continuable', label: 'child', createdAt: 1 }] },
        }
      }
      if (sessionId === 'child') {
        return { kind: 'sequenced', asOfSeq: 4, values: { subagentCatalog: [] } }
      }
      throw new Error(`unexpected projections for ${sessionId}`)
    },
    async sessionState({ sessionId }) {
      if (sessionId === 'session-root') {
        return { cursor: 32, active: true, running: false, queue: [], pendingInput: [] }
      }
      if (sessionId !== 'child') throw new Error(`unexpected state for ${sessionId}`)
      return {
        cursor: 4,
        active: false,
        running: false,
        queue: [],
        pendingInput: [],
      }
    },
    async sessionEvents() {
      return historyPage(32, { 31: notice, 32: turnEndEvent(32) })
    },
  }
  const waiter = new StateWaiter({
    registry,
    adapter,
    clock: () => now,
    sleep: async milliseconds => { now += milliseconds },
    subagentPollMs: 50,
  })
  const result = await waiter.wait({
    delegationId: 'cold-continuable',
    timeoutMs: 2_000,
    sliceTimeoutMs: 200,
    waitForSubagents: true,
  })
  assert.equal(result.reason, 'indeterminate')
  assert.equal(result.indeterminateReason, 'subagent-settlement-epoch-unverifiable')
  assert.ok(result.elapsedMs < 1_000, 'cold epoch ambiguity is not polled to the deadline')
})

test('slice3: one-shot notice without a consuming parent turn/end is indeterminate', async () => {
  const registry = {
    async get(delegationId) {
      return { delegationId, sessionId: 'session-root', state: 'ready' }
    },
  }
  let now = 0
  const notice = {
    type: 'user/message',
    seq: 31,
    time: 1,
    data: {
      content: [{ type: 'text', text: 'child settled' }],
      source: { kind: 'subagent-settled', senderSessionId: 'child', stopReason: 'completed' },
    },
  }
  const adapter = {
    async sessionWait() {
      return {
        reason: 'idle',
        state: { cursor: 31, active: true, running: false, queue: [], pendingInput: [] },
        output: { records: [], truncated: false },
      }
    },
    async sessionProjections({ sessionId }) {
      if (sessionId === 'session-root') {
        return {
          kind: 'sequenced',
          asOfSeq: 31,
          values: { subagentCatalog: [{ id: 'child', mode: 'one-shot', label: 'child', createdAt: 1 }] },
        }
      }
      if (sessionId === 'child') {
        return { kind: 'sequenced', asOfSeq: 4, values: { subagentCatalog: [] } }
      }
      throw new Error(`unexpected projections for ${sessionId}`)
    },
    async sessionState({ sessionId }) {
      if (sessionId === 'session-root') {
        return { cursor: 31, active: true, running: false, queue: [], pendingInput: [] }
      }
      if (sessionId !== 'child') throw new Error(`unexpected state for ${sessionId}`)
      return {
        cursor: 4,
        active: false,
        running: false,
        queue: [],
        pendingInput: [],
      }
    },
    async sessionEvents() {
      return historyPage(31, { 31: notice })
    },
  }
  const waiter = new StateWaiter({
    registry,
    adapter,
    clock: () => now,
    sleep: async milliseconds => { now += milliseconds },
    subagentPollMs: 50,
  })
  const result = await waiter.wait({
    delegationId: 'unconsumed-notice',
    timeoutMs: 2_000,
    sliceTimeoutMs: 200,
    waitForSubagents: true,
  })
  assert.equal(result.reason, 'indeterminate')
  assert.equal(result.indeterminateReason, 'subagent-settlement-epoch-unverifiable')
})

test('slice3: continuable first epoch with one consumed notice certifies idle', async () => {
  const registry = {
    async get(delegationId) {
      return { delegationId, sessionId: 'session-root', state: 'ready' }
    },
  }
  let now = 0
  const notice = {
    type: 'user/message',
    seq: 31,
    time: 1,
    data: {
      content: [{ type: 'text', text: 'first settlement' }],
      source: { kind: 'subagent-settled', senderSessionId: 'child', stopReason: 'completed' },
    },
  }
  const running = () => now < 50
  const adapter = {
    async sessionWait() {
      return {
        reason: 'idle',
        state: {
          cursor: now < 50 ? 30 : 32,
          active: true,
          running: false,
          queue: [],
          pendingInput: [],
        },
        output: { records: [], truncated: false },
      }
    },
    async sessionProjections({ sessionId }) {
      if (sessionId === 'session-root') {
        return {
          kind: 'sequenced',
          asOfSeq: 32,
          values: { subagentCatalog: [{ id: 'child', mode: 'continuable', label: 'child', createdAt: 1 }] },
        }
      }
      if (sessionId === 'child') {
        return { kind: 'sequenced', asOfSeq: 4, values: { subagentCatalog: [] } }
      }
      throw new Error(`unexpected projections for ${sessionId}`)
    },
    async sessionState({ sessionId }) {
      if (sessionId === 'session-root') {
        return {
          cursor: now < 50 ? 30 : 32,
          active: true,
          running: false,
          queue: [],
          pendingInput: [],
        }
      }
      if (sessionId !== 'child') throw new Error(`unexpected state for ${sessionId}`)
      return {
        cursor: 4,
        active: running(),
        running: running(),
        queue: [],
        pendingInput: [],
      }
    },
    async sessionEvents() {
      return historyPage(32, { 31: notice, 32: turnEndEvent(32) })
    },
  }
  const waiter = new StateWaiter({
    registry,
    adapter,
    clock: () => now,
    sleep: async milliseconds => { now += milliseconds },
    subagentPollMs: 50,
  })
  const result = await waiter.wait({
    delegationId: 'first-epoch',
    timeoutMs: 2_000,
    sliceTimeoutMs: 200,
    waitForSubagents: true,
  })
  assert.equal(result.reason, 'idle')
  assert.equal(result.indeterminate, false)
})

test('slice3: subagent status reports unknown, external and unreadable children honestly', async () => {
  const probed = []
  const adapter = {
    async sessionProjections({ sessionId }) {
      assert.equal(sessionId, 'session-parent')
      return {
        kind: 'sequenced',
        asOfSeq: 10,
        values: {
          subagentCatalog: [
            { id: 'legacy-child', mode: 'unknown', label: 'old', createdAt: 1 },
            { id: 'remote-child', mode: 'external', label: 'remote', name: 'remote', createdAt: 2 },
            { id: 'missing-child', mode: 'one-shot', label: 'gone', createdAt: 3 },
          ],
        },
      }
    },
    async sessionState({ sessionId }) {
      probed.push(sessionId)
      if (sessionId === 'missing-child') {
        const error = new Error('session not found')
        error.code = 'session/not-found'
        throw error
      }
      throw new Error(`unexpected state probe for ${sessionId}`)
    },
  }

  const status = await readSubagentStatus(adapter, 'session-parent')
  assert.deepEqual(probed, ['missing-child'], 'only local children may be probed')
  const byId = new Map(status.catalog.map(entry => [entry.id, entry]))

  const unknown = byId.get('legacy-child')
  assert.equal(unknown.local, false)
  assert.equal(unknown.active, null)
  assert.equal(unknown.settled, null)
  assert.equal(unknown.observation, 'unknown-mode')
  const external = byId.get('remote-child')
  assert.equal(external.local, false)
  assert.equal(external.active, null)
  assert.equal(external.observation, 'external')
  const missing = byId.get('missing-child')
  assert.equal(missing.local, true)
  assert.equal(missing.active, null)
  assert.equal(missing.observation, 'unavailable')
  assert.equal(missing.errorCode, 'session/not-found')

  assert.deepEqual(status.activeSubagents, [])
  assert.deepEqual(status.unknownSubagents.map(entry => entry.id), ['legacy-child'])
  assert.deepEqual(status.externalSubagents.map(entry => entry.id), ['remote-child'])
  assert.deepEqual(status.unavailableSubagents.map(entry => entry.id), ['missing-child'])

  const unavailable = await readSubagentStatus({
    async sessionProjections() { return null },
  }, 'session-gone')
  assert.equal(unavailable.available, false)
  assert.equal(unavailable.reason, 'projections-unavailable')
  assert.deepEqual(unavailable.catalog, [])

  const noCatalog = await readSubagentStatus({
    async sessionProjections() {
      return { kind: 'sequenced', asOfSeq: 3, values: {} }
    },
  }, 'session-no-catalog')
  assert.equal(noCatalog.available, false)
  assert.equal(noCatalog.reason, 'subagent-catalog-unavailable')
  assert.deepEqual(noCatalog.catalog, [])
})

// ─── Real Host ───────────────────────────────────────────────────────────────

test('slice3: real Host state, segmented waits, paginated contiguity and subagents', {
  timeout: 900_000,
}, async t => {
  const root = await createScratchRoot('slice3-')
  const workspaceDir = join(root, 'workspace')
  const dshHome = join(root, 'home')
  const overlayPath = join(root, 'overlay.yml')
  let mock
  let web
  let operator

  after(async () => {
    await operator?.close().catch(() => {})
    await web?.close().catch(() => {})
    await mock?.close().catch(() => {})
    await rm(root, { recursive: true, force: true })
  })

  mock = await startScriptedAgentServer()
  const upstream = await loadUpstream()
  await mkdir(workspaceDir, { recursive: true })
  await writeFile(overlayPath, [
    '- id: llm-deepseek',
    '  config:',
    `    baseURL: '${mock.baseURL}'`,
    '- id: llm-deepseek-account',
    '  disabled: true',
    '- id: session-title-llm',
    '  disabled: true',
    '- id: agent-instructions',
    '  disabled: true',
    '- id: session-log-deepseek',
    '  disabled: true',
    '- id: session-telemetry-otel',
    '  disabled: true',
    '',
  ].join('\n'))

  web = await startDshWeb({ root, cwd: workspaceDir, dshHome, overlayPath })
  operator = await connectOperator({ upstream, launchUrl: web.launchUrl })
  await operator.ready()

  const adapter = createRemoteAdapter(operator.ctx.remote)
  const catalog = await adapter.modelCatalog()
  const models = catalog.groups.flatMap(group =>
    group.models.map(model => ({ provider: group.id, model: model.id })))
  assert.ok(models.length >= 1, 'the scripted Host must advertise a model')
  const [model] = models

  const registry = new DelegationRegistry({ filePath: join(root, 'state', 'delegations.json') })
  const commands = new SessionCommands({ registry, adapter })
  const reader = new EventReader({ adapter })

  const observedWaitCalls = []
  const observedAdapter = {
    ...adapter,
    async sessionWait(request) {
      const call = { request, startedAt: Date.now(), finishedAt: undefined, reason: undefined }
      observedWaitCalls.push(call)
      const value = await adapter.sessionWait(request)
      call.finishedAt = Date.now()
      call.reason = value.reason
      return value
    },
  }
  const waiter = new StateWaiter({ registry, adapter: observedAdapter })

  async function createDelegation(label) {
    return commands.createDelegation({
      delegationId: `${label}-${randomUUID()}`,
      cwd: workspaceDir,
      title: `${label} ${randomUUID()}`,
      model,
    })
  }

  await t.test('contiguous pagination from beforeCursor and structured extraction on the real log', async () => {
    const record = await createDelegation('paging')
    const before = await adapter.sessionState({ sessionId: record.sessionId })
    assert.ok(before.cursor >= 2, `permission preset events must precede user work, got cursor ${before.cursor}`)
    const requestId = `request-${randomUUID()}`
    await commands.submitPrompt({
      delegationId: record.delegationId,
      requestId,
      content: 'WRITE_LOCAL slice3-paging.txt',
    })
    await waitForTurn(adapter, record.sessionId, requestId)
    const tail = await adapter.sessionEvents({
      address: { kind: 'session', sessionId: record.sessionId },
      maxMessages: 500,
    })
    const finalCursor = tail.cursor
    const tailEvents = eventsOf(tail)

    assert.deepEqual(
      tailEvents.slice(0, 3).map(event => event.type),
      ['permission/preset', 'sandbox/mode', 'approval/policy'],
      'the initial permission facts occupy seq 0..2',
    )
    const firstUser = tailEvents.find(event => event.type === 'user/message' && event.data.source?.kind === 'user')
    assert.ok(firstUser !== undefined && firstUser.seq >= 3, 'first user message follows the permission facts')

    const incremental = await reader.readRange({
      sessionId: record.sessionId,
      afterSeq: before.cursor,
      pageSize: 2,
    })
    assert.equal(incremental.pinnedCursor, finalCursor)
    assert.equal(incremental.cursor, finalCursor)
    assert.ok(incremental.pagesRead >= 2, 'a small message page budget must require backward pagination')
    assert.deepEqual(
      incremental.events.map(event => event.seq),
      Array.from({ length: finalCursor - before.cursor }, (_, index) => before.cursor + 1 + index),
      'incremental read must be dense with zero gaps or duplicates',
    )
    assert.ok(incremental.events.every(event => event.seq > before.cursor && event.seq <= finalCursor))
    assert.ok(
      incremental.events.some(event => event.type === 'user/message'
        && event.data.source?.rpcId === requestId),
      'the first user message after beforeCursor is captured',
    )

    const repeated = await reader.readRange({
      sessionId: record.sessionId,
      afterSeq: before.cursor,
      pageSize: 2,
    })
    assert.equal(repeated.cursor, finalCursor, 'the tail cursor stays pinned across reads')
    assert.deepEqual(repeated.events.map(event => event.seq), incremental.events.map(event => event.seq))

    const full = await reader.readRange({ sessionId: record.sessionId, afterSeq: -1, pageSize: 3 })
    assert.equal(full.cursor, finalCursor)
    assert.deepEqual(
      full.events.map(event => event.seq),
      Array.from({ length: finalCursor + 1 }, (_, index) => index),
      'full history from -1 is dense from seq 0',
    )

    const messages = extractMessages(full.events)
    const assistant = messages.filter(message => message.role === 'assistant')
    assert.ok(assistant.length >= 2, 'tool step and final step both persist assistant messages')
    assert.deepEqual(assistant.map(message => message.seq), assistant.map(message => message.seq).toSorted((a, b) => a - b))
    assert.match(assistant.at(-1).text, /SLICE2_FINAL_DONE/u)

    const activity = extractToolActivity(full.events)
    const writeCall = activity.calls.find(call => call.name === 'write')
    assert.ok(writeCall !== undefined, 'the real write tool call is extracted')
    assert.ok(writeCall.result !== undefined, 'the real tool result is paired by callId')
    assert.equal(writeCall.result.isError, false)
    assert.equal(writeCall.result.error, null)
    assert.match(JSON.stringify(writeCall.result.output), /Created file/u)
    assert.deepEqual(activity.unpairedResults, [])

    const headers = extractRequestHeaders(full.events)
    assert.ok(headers.length >= 1)
    const header = headers.at(-1)
    assert.equal(header.config.provider, model.provider)
    assert.equal(header.config.model, model.model)

    const status = await waiter.getStatus({ delegationId: record.delegationId })
    assert.equal(status.status, 'idle')
    assert.equal(status.sessionId, record.sessionId)
    assert.deepEqual(status.subagentCatalog, [])
    assert.deepEqual(status.activeSubagents, [])
  })

  await t.test('segmented wait outlives slice timeouts with lossless chronological output', async () => {
    observedWaitCalls.splice(0)
    const record = await createDelegation('segmented')
    const before = await adapter.sessionState({ sessionId: record.sessionId })
    const requestId = `request-${randomUUID()}`
    await commands.submitPrompt({
      delegationId: record.delegationId,
      requestId,
      content: 'SLOW_TURN 700',
    })

    const waitStart = Date.now()
    const result = await waiter.wait({
      delegationId: record.delegationId,
      afterSeq: before.cursor,
      timeoutMs: 15_000,
      sliceTimeoutMs: 400,
    })
    const elapsed = Date.now() - waitStart

    assert.equal(result.reason, 'idle')
    assert.ok(result.slices >= 2, `expected at least two session.wait slices, saw ${result.slices}`)
    assert.ok(elapsed >= 1_200, `the scripted turn must outlast one wait window (elapsed ${elapsed} ms)`)
    const calls = observedWaitCalls.splice(0)
    assert.equal(calls.length, result.slices)
    assert.ok(calls.every(call => call.request.timeoutMs <= 400), 'every slice stays inside sliceTimeoutMs')
    assert.equal(calls[0].request.afterSeq, before.cursor)
    assert.ok(new Set(calls.map(call => call.request.afterSeq)).size >= 2, 'the output cursor advances between slices')
    assert.ok(calls.every((call, index) => index === 0 || call.request.afterSeq >= calls[index - 1].request.afterSeq))

    const assistant = result.assistantMessages
    assert.equal(assistant.length, 2, 'both scripted assistant messages must survive segmentation')
    assert.ok(assistant[0].seq < assistant[1].seq, 'assistant output stays chronological')
    assert.match(assistant.at(-1).text, /SLICE3_SLOW_FINAL/u)

    const full = await reader.readRange({
      sessionId: record.sessionId,
      afterSeq: before.cursor,
      pageSize: 500,
    })
    const expected = extractMessages(full.events)
      .filter(message => message.role === 'assistant')
      .map(message => message.seq)
    assert.deepEqual(assistant.map(message => message.seq), expected, 'wait output loses zero assistant messages')
    assert.ok(result.cursor >= Math.max(...expected), 'the wait cursor covers the last assistant message')
    assert.deepEqual(result.subagents.activeSubagents, [])
  })

  await t.test('truncated wait output is backfilled from the contiguous event log', async () => {
    const record = await createDelegation('truncated')
    const before = await adapter.sessionState({ sessionId: record.sessionId })
    const requestId = `request-${randomUUID()}`
    await commands.submitPrompt({
      delegationId: record.delegationId,
      requestId,
      content: 'SLOW_TURN 300',
    })
    observedWaitCalls.splice(0)
    const result = await waiter.wait({
      delegationId: record.delegationId,
      afterSeq: before.cursor,
      timeoutMs: 15_000,
      sliceTimeoutMs: 5_000,
      maxMessages: 1,
    })
    assert.equal(result.reason, 'idle')
    assert.equal(result.slices, 1, 'the whole turn fits in one wait window')
    assert.equal(result.outputTruncated, true, 'maxMessages 1 truncates the two assistant messages')
    assert.ok(result.backfilled >= 1, 'the truncated window is backfilled through session.events')
    assert.equal(result.assistantMessages.length, 2)

    const full = await reader.readRange({
      sessionId: record.sessionId,
      afterSeq: before.cursor,
      pageSize: 500,
    })
    assert.deepEqual(
      result.assistantMessages.map(message => message.seq),
      extractMessages(full.events)
        .filter(message => message.role === 'assistant')
        .map(message => message.seq),
    )
    observedWaitCalls.splice(0)
  })

  await t.test('a long wait on A leaves unrelated status B responsive', async () => {
    const a = await createDelegation('long-a')
    const b = await createDelegation('unrelated-b')
    const beforeA = await adapter.sessionState({ sessionId: a.sessionId })
    const requestId = `request-${randomUUID()}`
    await commands.submitPrompt({
      delegationId: a.delegationId,
      requestId,
      content: 'HOLD 4000',
    })
    observedWaitCalls.splice(0)

    let aSettled = false
    const aWait = waiter.wait({
      delegationId: a.delegationId,
      afterSeq: beforeA.cursor,
      timeoutMs: 20_000,
      sliceTimeoutMs: 1_000,
    }).then(value => {
      aSettled = true
      return value
    }, error => {
      aSettled = true
      throw error
    })

    await sleep(300)
    assert.equal(aSettled, false, 'the long wait is still in flight')
    const callsDuringProbe = observedWaitCalls.filter(call => call.finishedAt !== undefined).length

    const probeStart = performance.now()
    const statusB = await waiter.getStatus({ delegationId: b.delegationId })
    const probeMs = performance.now() - probeStart

    assert.equal(statusB.status, 'idle')
    assert.equal(statusB.state.running, false)
    assert.equal(statusB.state.active, true)
    assert.equal(aSettled, false, 'status B must resolve while wait A is still pending')
    assert.ok(probeMs < 2_500, `unrelated status B must stay responsive (${Math.round(probeMs)} ms)`)
    assert.ok(
      callsDuringProbe < 4,
      'wait A had not completed its full slice chain when B answered',
    )

    const startedAt = performance.now()
    const result = await aWait
    const aElapsed = performance.now() - startedAt
    assert.equal(result.reason, 'idle')
    assert.ok(result.assistantMessages.some(message => /SLICE3_HOLD_FINAL/u.test(message.text)))
    assert.ok(aElapsed >= 3_000, `wait A must have held for the scripted 4 s turn (${Math.round(aElapsed)} ms)`)
    assert.ok(observedWaitCalls.length >= 2, 'wait A crossed several one-second slices')

    const statusAAfter = await waiter.getStatus({ delegationId: a.delegationId })
    assert.equal(statusAAfter.status, 'idle')
    t.diagnostic(`status B latency while wait A pending: ${probeMs.toFixed(1)} ms; A slices: ${observedWaitCalls.length}`)
    observedWaitCalls.splice(0)
  })

  await t.test('real subagent catalog and waitForSubagents observe background work', async () => {
    const record = await createDelegation('subagent')
    const before = await adapter.sessionState({ sessionId: record.sessionId })
    const requestId = `request-${randomUUID()}`
    await commands.submitPrompt({
      delegationId: record.delegationId,
      requestId,
      content: 'SUBAGENT 3000',
    })

    let observedActiveChild = false
    let entry
    let observedStatus
    const deadline = Date.now() + 30_000
    while (Date.now() < deadline) {
      const status = await waiter.getStatus({ delegationId: record.delegationId })
      if (status.subagentCatalog.length > 0) {
        observedStatus = status
        entry = status.subagentCatalog[0]
        if (status.activeSubagents.some(child => child.id === entry.id)) observedActiveChild = true
        break
      }
      await sleep(150)
    }
    assert.ok(entry !== undefined, 'the parent catalog must expose the spawned child')
    assert.ok(['one-shot', 'continuable'].includes(entry.mode), `unexpected child mode ${String(entry.mode)}`)
    assert.ok(typeof entry.name === 'string' && entry.name.length > 0)
    assert.deepEqual(observedStatus.unknownSubagents, [])
    assert.deepEqual(observedStatus.externalSubagents, [])

    observedWaitCalls.splice(0)
    const result = await waiter.wait({
      delegationId: record.delegationId,
      afterSeq: before.cursor,
      timeoutMs: 30_000,
      sliceTimeoutMs: 1_000,
      waitForSubagents: true,
    })
    assert.equal(result.reason, 'idle')
    assert.equal(result.indeterminate, false)
    assert.ok(observedActiveChild, 'the child was observed running while the parent was idle')
    assert.deepEqual(result.activeSubagents, [], 'waitForSubagents returns only after background work settles')
    const observed = result.subagentCatalog.find(child => child.id === entry.id)
    assert.ok(observed !== undefined)
    assert.equal(observed.active, false)
    assert.equal(observed.settled, true)
    assert.equal(observed.timing?.lastTurnCompleted, true)
    assert.ok(result.assistantMessages.some(message => /SLICE3_PARENT_FINAL/u.test(message.text)))

    const finalStatus = await waiter.getStatus({ delegationId: record.delegationId })
    assert.equal(finalStatus.status, 'idle')
    assert.equal(finalStatus.subagentCatalog.find(child => child.id === entry.id)?.settled, true)
    t.diagnostic(`subagent ${entry.id} mode=${entry.mode} name=${entry.name}`)
    observedWaitCalls.splice(0)
  })
})
