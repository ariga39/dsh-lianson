import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
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

test('slice3: deriveStatus and input-required waits resolve without probing subagents', async () => {
  assert.equal(deriveStatus({ pendingInput: ['question'], running: true }), 'input-required')
  assert.equal(deriveStatus({ pendingInput: [], running: true }), 'running')
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
})

// ─── Real Host ───────────────────────────────────────────────────────────────

test('slice3: real Host state, segmented waits, paginated contiguity and subagents', {
  timeout: 900_000,
}, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-slice3-'))
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
