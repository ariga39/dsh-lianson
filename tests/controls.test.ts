import { after, before, beforeEach, describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { mkdir } from 'node:fs/promises'
import { join } from 'node:path'
import { brandString } from '@deepseek-ai/dsh-brand'
import type { SessionRequestId, SessionWireEvent, SessionWaitValue } from '@deepseek-ai/dsh-api-session-controller/types'
import type { MessageId, ToolCallId } from '@deepseek-ai/dsh-llm/types'
import type { SessionId } from '@deepseek-ai/dsh-session/types'
import type { SessionEventMap } from '@deepseek-ai/dsh-session/types'
import type { AskUserQuestionAnswer } from '@deepseek-ai/dsh-user-questions/types'
import { LiaisonError } from '../src/errors.ts'
import { InteractionBroker } from '../src/interactions.ts'
import { Ledger } from '../src/ledger.ts'
import type {
  DshRemote, RemoteEventPage, RemoteEventPageRequest, RemoteModelCatalog, RemoteProjections, RemoteQueueAction,
  RemoteSessionState, RemoteWaitRequest,
} from '../src/remote.ts'
import { DelegationService } from '../src/service.ts'
import { trustPolicyOf } from '../src/trust.ts'
import { makeTestRoot, removeTestRoot } from './support.ts'

const CATALOG: RemoteModelCatalog = {
  default: { provider: 'fixture', model: 'model-a' },
  groups: [{ id: 'fixture', name: 'Fixture', models: [{ id: 'model-a', name: 'Model A' }] }],
  failures: [],
}

const idle: RemoteSessionState = { cursor: 10, active: true, running: false, queue: [], pendingInput: [] }

function output(events: readonly SessionWireEvent[]): SessionWaitValue['output'] {
  return { records: events.map(event => ({ type: 'event', event })), truncated: false }
}

function event<K extends keyof SessionEventMap>(type: K, seq: number, data: SessionEventMap[K]): SessionWireEvent {
  return { type, seq, time: seq, data: data as SessionWireEvent['data'] }
}

function userMessage(seq: number, requestId: string): SessionWireEvent {
  return event('user/message', seq, {
    id: brandString<SessionEventMap['user/message']['id']>(`message-${String(seq)}`),
    role: 'user',
    content: [],
    source: { kind: 'user', rpcId: brandString<SessionRequestId>(requestId) },
  })
}

function header(seq: number, model: string): SessionWireEvent {
  return event('request/header', seq, { header: { config: { provider: 'fixture', model } }, reason: 'initial' })
}

function assistant(seq: number, turn = 1): SessionWireEvent {
  return event('assistant/message', seq, {
    turn,
    step: 1,
    message: {
      id: brandString<SessionEventMap['assistant/message']['message']['id']>(`assistant-${String(seq)}`),
      role: 'assistant',
      content: [{ type: 'text', text: 'output' }],
      source: { kind: 'model', provider: 'fixture', model: 'model-a' },
    },
    stream: [],
  })
}

function fixture(seq: number): SessionWireEvent {
  return { type: 'fixture/event', seq, time: seq, data: { seq } }
}

function turnStart(seq: number): SessionWireEvent {
  return event('turn/start', seq, { turn: 1 })
}

function turnEnd(seq: number): SessionWireEvent {
  return event('turn/end', seq, { turn: 1, reason: { kind: 'completed' } })
}

describe('session controls and observation', () => {
  let root: string
  let project: string
  let ledger: Ledger
  let remote: ControlRemote
  let service: DelegationService
  let clock: number
  let interactions: InteractionBroker

  before(async () => {
    root = await makeTestRoot('controls-')
  })

  after(async () => {
    await removeTestRoot(root)
  })

  beforeEach(async () => {
    ledger = Ledger.open(join(root, `ledger-${String(Date.now())}-${String(Math.random())}.sqlite3`))
    remote = new ControlRemote()
    clock = 0
    interactions = new InteractionBroker({
      bindingOfSession: sessionId => ledger.findDelegationBySession(sessionId)?.delegationId,
      now: () => 100,
      newId: () => 'fixed',
    })
    service = new DelegationService({
      ledger,
      remote: {
        status: () => ({ state: 'connected', origin: remote.origin }),
        requireRemote: () => remote,
        dispose: async () => {},
      },
      trust: trustPolicyOf({
        dshHome: join(root, 'home'),
        runtimeDir: join(root, 'run'),
        socketPath: join(root, 'run', 's.sock'),
        lockPath: join(root, 'run', 's.sock.lock'),
        ledgerPath: join(root, 'data', 'ledger.sqlite3'),
      }, undefined),
      now: () => 100,
      newId: () => 'fixed',
      clock: () => clock,
      sleep: async ms => { clock += ms },
      pollIntervalMs: 1,
      interactions,
    })
    project = join(root, 'project')
    await mkdir(project, { recursive: true })
    await service.create({ delegationId: 'd1', cwd: project, title: 'Task one', model: 'fixture/model-a' })
  })

  it('advances afterSeq across multiple wait windows', async () => {
    const message = assistant(9)
    remote.waits = [
      {
        reason: 'timeout',
        state: { cursor: 7, active: true, running: true, queue: [], pendingInput: [] },
        output: output([]),
      },
      { reason: 'idle', state: { ...idle, cursor: 9 }, output: output([message]) },
    ]
    remote.projections = { kind: 'sequenced', asOfSeq: 9, values: { subagentCatalog: [] } }

    const result = await service.wait({ delegationId: 'd1', timeoutMs: 1_000, sliceTimeoutMs: 10 })
    assert.equal(result.status, 'completed')
    assert.equal(result.reason, 'idle')
    assert.equal(result.slices, 2)
    assert.equal(result.cursor, 9)
    assert.deepEqual(remote.waitCalls.map(call => call.afterSeq), [-1, 7])
    assert.deepEqual(result.output.records.map(record => record.event.seq), [9])
  })

  it('reports input-required without claiming the turn completed', async () => {
    remote.waits = [{
      reason: 'input-required',
      state: { cursor: 8, active: true, running: false, queue: [], pendingInput: ['approval'] },
      output: output([]),
    }]
    const result = await service.wait({ delegationId: 'd1', timeoutMs: 1_000 })
    assert.equal(result.status, 'input-required')
    assert.equal(result.reason, 'input-required')
    assert.equal(result.slices, 1)
  })

  it('reports indeterminate when an external child turn cannot be attributed', async () => {
    remote.waits = [{ reason: 'idle', state: { ...idle, cursor: 12 }, output: output([]) }]
    remote.projections = {
      kind: 'sequenced',
      asOfSeq: 12,
      values: { subagentCatalog: [{ id: brandString<SessionId>('external-child'), mode: 'external', createdAt: 1 }] },
    }
    const result = await service.wait({ delegationId: 'd1', timeoutMs: 1_000 })
    assert.equal(result.status, 'indeterminate')
    assert.equal(result.indeterminate, true)
    assert.equal(result.indeterminateReason, 'subagent-external-status-unavailable')
    assert.equal(result.subagentCatalog?.length, 1)
  })

  it('does not claim completion when the subagent catalog is unavailable', async () => {
    remote.waits = [{ reason: 'idle', state: { ...idle, cursor: 12 }, output: output([]) }]
    remote.projections = null
    const result = await service.wait({ delegationId: 'd1', timeoutMs: 1_000 })
    assert.equal(result.status, 'indeterminate')
    assert.equal(result.indeterminate, true)
    assert.equal(result.indeterminateReason, 'subagent-catalog-unavailable')
  })

  it('does not claim completion while a local child still has queued work', async () => {
    remote.waits = Array.from({ length: 10 }, () => ({
      reason: 'idle' as const,
      state: { ...idle, cursor: 12 },
      output: output([]),
    }))
    remote.projections = {
      kind: 'sequenced',
      asOfSeq: 12,
      values: { subagentCatalog: [{ id: brandString<SessionId>('child'), mode: 'one-shot', createdAt: 1 }] },
    }
    remote.states = [{
      ...idle,
      cursor: 4,
      queue: [{ id: brandString<MessageId>('child-queued'), placement: 'queued', content: [] }],
    }]
    const result = await service.wait({ delegationId: 'd1', timeoutMs: 5, sliceTimeoutMs: 1 })
    assert.notEqual(result.status, 'completed')
    assert.equal(result.status, 'queued')
    assert.equal(result.reason, 'timeout')
  })

  it('waits for an attributable local child before reporting completion', async () => {
    remote.waits = [
      { reason: 'idle', state: { ...idle, cursor: 12 }, output: output([]) },
      { reason: 'idle', state: { ...idle, cursor: 13 }, output: output([]) },
    ]
    remote.projections = {
      kind: 'sequenced',
      asOfSeq: 12,
      values: { subagentCatalog: [{ id: brandString<SessionId>('child'), mode: 'one-shot', createdAt: 1 }] },
    }
    remote.states = [
      { cursor: 4, active: true, running: true, queue: [], pendingInput: [] },
      { cursor: 5, active: false, running: false, queue: [], pendingInput: [] },
    ]
    const result = await service.wait({ delegationId: 'd1', timeoutMs: 1_000 })
    assert.equal(result.status, 'completed')
    assert.equal(result.slices, 2)
  })

  it('reads contiguous events and exposes the subagent catalog', async () => {
    remote.pages = [{ cursor: 2, events: [fixture(0), fixture(1), fixture(2)], hasMore: false }]
    remote.projections = {
      kind: 'sequenced',
      asOfSeq: 2,
      values: { subagentCatalog: [{ id: brandString<SessionId>('child'), mode: 'one-shot', createdAt: 1 }] },
    }
    const result = await service.events({ delegationId: 'd1' })
    assert.deepEqual(result.events.map(item => item.seq), [0, 1, 2])
    assert.equal(result.throughSeq, 2)
    assert.equal(result.subagentCatalog?.length, 1)
  })

  it('fails events explicitly on a sequence gap', async () => {
    remote.pages = [{ cursor: 2, events: [fixture(0), fixture(2)], hasMore: false }]
    await assert.rejects(
      service.events({ delegationId: 'd1' }),
      (error: unknown) => error instanceof LiaisonError && error.code === 'events/sequence-gap',
    )
  })

  it('verifies a request from beyond the old twelve-page history limit', async () => {
    const events = [
      turnStart(0), userMessage(1, 'request-early'), header(2, 'model-a'), assistant(3), turnEnd(4),
      ...Array.from({ length: 13 }, (_value, index) => fixture(index + 5)),
    ]
    remote.pages = events
      .map(item => ({ cursor: events.length - 1, events: [item], hasMore: item.seq > 0 }))
      .reverse()
    const result = await service.verifyModel({ delegationId: 'd1', requestId: 'request-early' })
    assert.equal(result.verification, 'proven')
    assert.equal(result.evidence?.userSeq, 1)
    assert.equal(result.evidence?.headerSeq, 2)
  })

  it('steers and mutates the pending queue', async () => {
    const steered = await service.steer({ delegationId: 'd1', requestId: 'steer-1', text: 'focus on tests' })
    assert.equal(steered.mode, 'steer')
    assert.deepEqual(remote.prompts, [{ requestId: 'steer-1', text: 'focus on tests', mode: 'steer' }])

    const edited = await service.queue({ delegationId: 'd1', itemId: 'item-1', action: { kind: 'edit', text: 'new' } })
    assert.equal(edited.action, 'edit')
    await service.queue({ delegationId: 'd1', itemId: 'item-2', action: { kind: 'remove' } })
    await service.queue({ delegationId: 'd1', itemId: 'item-3', action: { kind: 'steer' } })
    assert.deepEqual(remote.queueCalls.map(call => call.action), [
      { kind: 'edit', text: 'new' },
      { kind: 'remove' },
      { kind: 'steer' },
    ])
  })

  it('stops within the budget after clearing queued input and cancelling', async () => {
    const queued: RemoteSessionState = {
      cursor: 11, active: true, running: true,
      queue: [{ id: brandString<MessageId>('queued-1'), placement: 'queued', content: [] }],
      pendingInput: [],
    }
    remote.states = [queued, { ...idle, cursor: 12 }]
    const result = await service.stop({ delegationId: 'd1', timeoutMs: 100 })
    assert.equal(result.outcome, 'stopped')
    assert.equal(result.stopped, true)
    assert.equal(result.clearedQueue, 1)
    assert.equal(result.cancelled, true)
    assert.equal(result.budgetExpired, false)
    assert.deepEqual(remote.cancelled, ['session-fixed'])
  })

  it('reports a still-running stop truthfully when the budget expires', async () => {
    remote.states = [{ ...idle, cursor: 12, running: true }]
    const result = await service.stop({ delegationId: 'd1', timeoutMs: 5 })
    assert.equal(result.outcome, 'running')
    assert.equal(result.stopped, false)
    assert.equal(result.quiescent, false)
    assert.equal(result.budgetExpired, true)
    assert.deepEqual(result.residualQueue, [])
  })

  it('returns unknown promptly when the stop state read is delayed past the budget', async () => {
    remote.readState = () => new Promise<RemoteSessionState>(() => {})
    const started = performance.now()
    const result = await service.stop({ delegationId: 'd1', timeoutMs: 20 })
    assert.ok(performance.now() - started < 1_000)
    assert.equal(result.outcome, 'unknown')
    assert.equal(result.quiescent, null)
    assert.equal(result.state, null)
    assert.equal(result.budgetExpired, true)
    assert.deepEqual(remote.queueCalls, [])
    assert.deepEqual(remote.cancelled, [])
  })

  it('does not start another queue mutation after expiry during an update', async () => {
    const queued: RemoteSessionState = {
      ...idle,
      cursor: 11,
      running: true,
      queue: [
        { id: brandString<MessageId>('queued-1'), placement: 'queued', content: [] },
        { id: brandString<MessageId>('queued-2'), placement: 'queued', content: [] },
      ],
    }
    remote.states = [queued]
    let updates = 0
    remote.updateQueue = () => {
      updates += 1
      return new Promise<void>(() => {})
    }
    const result = await service.stop({ delegationId: 'd1', timeoutMs: 20 })
    assert.equal(updates, 1, 'only the in-flight mutation may start')
    assert.deepEqual(remote.cancelled, [], 'cancel must not start after expiry')
    assert.equal(result.outcome, 'unknown')
    assert.equal(result.quiescent, null)
    assert.equal(result.clearedQueue, 0)
    assert.deepEqual(result.residualQueue, ['queued-1', 'queued-2'])
  })

  it('returns unknown when cancellation is delayed past the budget', async () => {
    remote.states = [{ ...idle, cursor: 12, running: true }]
    let cancels = 0
    remote.cancel = () => {
      cancels += 1
      return new Promise<void>(() => {})
    }
    const result = await service.stop({ delegationId: 'd1', timeoutMs: 20 })
    assert.equal(cancels, 1)
    assert.equal(result.cancelled, false)
    assert.equal(result.outcome, 'unknown')
    assert.equal(result.quiescent, null)
    assert.equal(result.budgetExpired, true)
    assert.deepEqual(remote.queueCalls, [])
  })

  it('does not report stopped from a stale idle state after expiry', async () => {
    remote.states = [{ ...idle, cursor: 12 }]
    remote.cancel = async () => { clock = 100 }
    const result = await service.stop({ delegationId: 'd1', timeoutMs: 50 })
    assert.equal(result.budgetExpired, true)
    assert.equal(result.outcome, 'unknown')
    assert.equal(result.stopped, false)
    assert.equal(result.quiescent, null)
  })

  it('requires a fresh read after a queue removal raced with dispatch', async () => {
    const queued: RemoteSessionState = {
      ...idle,
      cursor: 11,
      queue: [{ id: brandString<MessageId>('raced-1'), placement: 'queued', content: [] }],
    }
    remote.states = [queued]
    remote.updateQueue = async () => {
      throw new LiaisonError('not-found', 'queued item is no longer pending', { itemId: 'raced-1' })
    }
    const result = await service.stop({ delegationId: 'd1', timeoutMs: 5 })
    assert.equal(result.stopped, false)
    assert.equal(result.outcome, 'running')
    assert.deepEqual(result.residualQueue, ['raced-1'])
    assert.equal(result.clearedQueue, 0)
  })

  it('unarchives and submits rework to the original session id', async () => {
    const result = await service.rework({ delegationId: 'd1', requestId: 'rework-1', text: 'fix the review finding' })
    assert.equal(result.sessionId, 'session-fixed')
    assert.equal(result.unarchived, true)
    assert.deepEqual(remote.unarchived, ['session-fixed'])
    assert.deepEqual(remote.prompts.map(prompt => prompt.requestId), ['rework-1'])
  })

  it('routes interaction list, approve, reject, answer, and late continued answers', async () => {
    const approval = interactions.offerApproval('session-fixed', { toolName: 'shell', reason: 'write' })
    const listed = await service.interactionList({ delegationId: 'd1' })
    assert.equal(listed.interactions.length, 1)
    const pending = listed.interactions[0]
    assert.equal(pending?.kind, 'approval')
    const approved = service.interactionApprove(pending?.id ?? '')
    assert.equal(approved.outcome, 'allowed-once')
    assert.equal(await approval, 'allowed-once')

    const rejection = interactions.offerApproval('session-fixed', { toolName: 'shell' })
    const rejectView = (await service.interactionList({ delegationId: 'd1' })).interactions[0]
    assert.equal(service.interactionReject(rejectView?.id ?? '').outcome, 'rejected')
    assert.equal(await rejection, 'rejected')

    const question = interactions.offerQuestion('session-fixed', {
      questions: [{ id: 'q1', question: 'Proceed?' }],
      wait: { callId: brandString<ToolCallId>('call-1') },
    })
    const questionView = (await service.interactionList({ delegationId: 'd1' })).interactions[0]
    const answered = await service.interactionAnswer({
      interactionId: questionView?.id ?? '',
      answers: [{ id: 'q1', selected: ['yes'] }],
    })
    assert.equal(answered.via, 'pending')
    assert.deepEqual(await question, { answers: [{ id: 'q1', selected: ['yes'] }] })

    remote.answerResult = true
    const late = await service.interactionAnswer({
      delegationId: 'd1',
      callId: 'late-call',
      answers: [{ id: 'q2', selected: ['later'] }],
    })
    assert.equal(late.via, 'continued')
    assert.deepEqual(remote.answers, [{ callId: 'late-call', answers: [{ id: 'q2', selected: ['later'] }] }])
  })
})

class ControlRemote implements DshRemote {
  readonly origin = 'http://127.0.0.1:3080'
  cursor = 5
  pages: RemoteEventPage[] = []
  waits: SessionWaitValue[] = []
  waitCalls: RemoteWaitRequest[] = []
  states: RemoteSessionState[] = []
  lastState: RemoteSessionState = { cursor: 5, active: true, running: false, queue: [], pendingInput: [] }
  projections: RemoteProjections = { kind: 'sequenced', asOfSeq: 0, values: { subagentCatalog: [] } }
  prompts: { requestId: string; text: string; mode: 'queue' | 'steer' }[] = []
  queueCalls: { itemId: string; action: RemoteQueueAction }[] = []
  cancelled: string[] = []
  unarchived: string[] = []
  answers: { callId: string; answers: AskUserQuestionAnswer['answers'] }[] = []
  answerResult = true

  async modelCatalog(): Promise<RemoteModelCatalog> {
    return CATALOG
  }
  async createWorkspace(): Promise<string> {
    return 'workspace-1'
  }
  async createSession(input: { sessionId: string }): Promise<string> {
    return input.sessionId
  }
  async renameSession(): Promise<void> {}
  async selectModel(): Promise<void> {}
  async beforeCursor(): Promise<number> {
    return this.cursor
  }
  async prompt(_sessionId: string, requestId: string, text: string, mode: 'queue' | 'steer' = 'queue'): Promise<void> {
    this.prompts.push({ requestId, text, mode })
  }
  async readEventPage(_sessionId: string, _request: RemoteEventPageRequest): Promise<RemoteEventPage> {
    const page = this.pages.shift()
    if (page === undefined) throw new Error('unscripted page')
    return page
  }
  async readState(): Promise<RemoteSessionState> {
    const next = this.states.shift()
    if (next !== undefined) this.lastState = next
    return this.lastState
  }
  async wait(_sessionId: string, request: RemoteWaitRequest): Promise<SessionWaitValue> {
    this.waitCalls.push(request)
    const next = this.waits.shift()
    if (next === undefined) throw new Error('unscripted wait')
    return next
  }
  async readProjections(): Promise<RemoteProjections> {
    return this.projections
  }
  async updateQueue(_sessionId: string, itemId: string, action: RemoteQueueAction): Promise<void> {
    this.queueCalls.push({ itemId, action })
  }
  async cancel(sessionId: string): Promise<void> {
    this.cancelled.push(sessionId)
  }
  async unarchiveSession(sessionId: string): Promise<void> {
    this.unarchived.push(sessionId)
  }
  async answerUserQuestion(_sessionId: string, callId: string, answer: AskUserQuestionAnswer): Promise<boolean> {
    this.answers.push({ callId, answers: answer.answers })
    return this.answerResult
  }
  async dispose(): Promise<void> {}
}
