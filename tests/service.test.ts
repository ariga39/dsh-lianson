import { after, before, beforeEach, describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { mkdir } from 'node:fs/promises'
import { join } from 'node:path'
import { brandString } from '@deepseek-ai/dsh-brand'
import type { SessionRequestId, SessionWireEvent } from '@deepseek-ai/dsh-api-session-controller/types'
import type { SessionEventMap } from '@deepseek-ai/dsh-session/types'
import { LiaisonError } from '../src/errors.ts'
import { Ledger } from '../src/ledger.ts'
import type { DshRemote, RemoteEventPage, RemoteModelCatalog } from '../src/remote.ts'
import { DelegationService } from '../src/service.ts'
import { trustPolicyOf } from '../src/trust.ts'
import { makeTestRoot, removeTestRoot } from './support.ts'

const CATALOG: RemoteModelCatalog = {
  default: { provider: 'fixture', model: 'model-a' },
  groups: [
    { id: 'fixture', name: 'Fixture', models: [{ id: 'model-a', name: 'Model A' }, { id: 'model-b', name: 'Model B' }] },
  ],
  failures: [],
}

describe('delegation service', () => {
  let root: string
  let project: string
  let ledger: Ledger
  let remote: FakeRemote
  let service: DelegationService
  let ids: number
  let now: number

  before(async () => {
    root = await makeTestRoot('service-')
  })

  after(async () => {
    await removeTestRoot(root)
  })

  beforeEach(() => {
    ledger = Ledger.open(join(root, `ledger-${String(Date.now())}-${String(Math.random())}.sqlite3`))
    remote = new FakeRemote()
    ids = 0
    now = 100
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
      now: () => {
        now += 1
        return now
      },
      newId: () => {
        ids += 1
        return `id-${String(ids)}`
      },
    })
    project = join(root, 'project')
  })

  it('persists the minted session id before the remote create calls', async () => {
    await mkdir(project, { recursive: true })
    let sawCreating:
      | { sessionId: string; status: string }
      | undefined
    remote.onBeforeCreateWorkspace = () => {
      const row = ledger.findDelegation('d1')
      sawCreating = row === undefined ? undefined : { sessionId: row.sessionId, status: row.status }
    }
    const result = await service.create({ delegationId: 'd1', cwd: project, title: 'Task one', model: 'fixture/model-b' })
    assert.equal(result.replayed, false)
    assert.equal(result.delegation.status, 'ready')
    assert.equal(result.delegation.sessionId, 'session-id-1')
    assert.deepEqual(sawCreating, { sessionId: 'session-id-1', status: 'creating' })
    assert.equal(remote.sessionIdArgument, 'session-id-1')
    assert.deepEqual(remote.calls, ['workspace/create', 'session/create', 'session/rename', 'session/selectModel'])
    assert.equal(ledger.findDelegation('d1')?.status, 'ready')
  })

  it('replays matching parameters without repeating remote creation and conflicts on mismatch', async () => {
    await mkdir(project, { recursive: true })
    const input = { delegationId: 'd1', cwd: project, title: 'Task one', model: 'fixture/model-a' }
    await service.create(input)
    const calls = remote.calls.length
    const replay = await service.create(input)
    assert.equal(replay.replayed, true)
    await mkdir(join(root, 'other'), { recursive: true })
    assert.equal(remote.calls.length, calls)
    await assert.rejects(
      service.create({ ...input, title: 'Different title' }),
      (error: unknown) => error instanceof LiaisonError && error.code === 'conflict',
    )
    await assert.rejects(
      service.create({ ...input, cwd: join(root, 'other') }),
      (error: unknown) => error instanceof LiaisonError && error.code === 'conflict',
    )
  })


  it('replays a ready delegation without the remote and with omitted optional parameters', async () => {
    await mkdir(project, { recursive: true })
    await service.create({ delegationId: 'd1', cwd: project, title: 'Task one', model: 'fixture/model-a' })
    const offline = new DelegationService({
      ledger,
      remote: {
        status: () => ({ state: 'unavailable' }),
        requireRemote: () => { throw new LiaisonError('dsh-unavailable', 'offline') },
        dispose: async () => {},
      },
      trust: trustPolicyOf({
        dshHome: join(root, 'home'),
        runtimeDir: join(root, 'run'),
        socketPath: join(root, 'run', 's.sock'),
        lockPath: join(root, 'run', 's.sock.lock'),
        ledgerPath: join(root, 'data', 'ledger.sqlite3'),
      }, undefined),
      now: () => 500,
    })
    const replay = await offline.create({ delegationId: 'd1', cwd: project })
    assert.equal(replay.replayed, true)
    assert.equal(replay.delegation.title, 'Task one')
    assert.equal(replay.delegation.model, 'fixture/model-a')
  })


  it('serializes same-id creates so one remote creation serves both callers', async () => {
    await mkdir(project, { recursive: true })
    const gate = Promise.withResolvers<void>()
    remote.workspaceGate = gate.promise
    const input = { delegationId: 'd1', cwd: project, title: 'Task one', model: 'fixture/model-a' }
    const first = service.create(input)
    const second = service.create(input)
    await waitFor(() => remote.workspaceEntries === 1)
    gate.resolve()
    const [left, right] = await Promise.all([first, second])
    assert.equal(remote.workspaceEntries, 1)
    assert.equal(left.delegation.sessionId, right.delegation.sessionId)
    assert.deepEqual([left.replayed, right.replayed].sort((a, b) => Number(a) - Number(b)), [false, true])
  })

  it('serializes same-id creates with different parameters into a conflict', async () => {
    await mkdir(project, { recursive: true })
    const gate = Promise.withResolvers<void>()
    remote.workspaceGate = gate.promise
    const first = service.create({ delegationId: 'd1', cwd: project, title: 'First', model: 'fixture/model-a' })
    const second = service.create({ delegationId: 'd1', cwd: project, title: 'Second', model: 'fixture/model-a' })
    await waitFor(() => remote.workspaceEntries === 1)
    gate.resolve()
    await first
    await assert.rejects(
      second,
      (error: unknown) => error instanceof LiaisonError && error.code === 'conflict',
    )
    assert.equal(remote.workspaceEntries, 1)
    assert.equal(ledger.findDelegation('d1')?.title, 'First')
  })

  it('does not block creates for different delegation ids', async () => {
    await mkdir(project, { recursive: true })
    const gate = Promise.withResolvers<void>()
    remote.workspaceGate = gate.promise
    const first = service.create({ delegationId: 'd1', cwd: project, title: 'First', model: 'fixture/model-a' })
    const second = service.create({ delegationId: 'd2', cwd: project, title: 'Second', model: 'fixture/model-a' })
    await waitFor(() => remote.workspaceEntries === 2)
    gate.resolve()
    const [left, right] = await Promise.all([first, second])
    assert.equal(left.delegation.delegationId, 'd1')
    assert.equal(right.delegation.delegationId, 'd2')
    assert.equal(left.delegation.status, 'ready')
    assert.equal(right.delegation.status, 'ready')
  })

  it('retries a partially created delegation with the persisted session id', async () => {
    await mkdir(project, { recursive: true })
    remote.failSessionOnce = true
    const input = { delegationId: 'd1', cwd: project, title: 'Task one', model: 'fixture/model-a' }
    await assert.rejects(service.create(input), /session create failed/)
    assert.equal(ledger.findDelegation('d1')?.status, 'creating')
    const sessionId = ledger.findDelegation('d1')?.sessionId
    const result = await service.create(input)
    assert.equal(result.delegation.sessionId, sessionId)
    assert.equal(result.delegation.status, 'ready')
    assert.equal(remote.sessionIds.length, 2)
    assert.deepEqual(remote.sessionIds, [sessionId, sessionId])
  })

  it('rejects a Host session id that differs from the persisted one', async () => {
    await mkdir(project, { recursive: true })
    remote.returnDifferentSessionId = true
    await assert.rejects(
      service.create({ delegationId: 'd1', cwd: project, title: 'Task one', model: 'fixture/model-a' }),
      (error: unknown) => error instanceof LiaisonError && error.code === 'conflict',
    )
  })

  it('writes a prompt receipt before the RPC and reuses its cursor on replay', async () => {
    await mkdir(project, { recursive: true })
    await service.create({ delegationId: 'd1', cwd: project, title: 'Task one', model: 'fixture/model-a' })
    remote.cursor = 7
    let receiptAtPrompt: number | undefined
    remote.onBeforePrompt = () => {
      receiptAtPrompt = ledger.findReceipt('d1', 'request-1')?.beforeCursor
    }
    const first = await service.prompt({ delegationId: 'd1', requestId: 'request-1', text: 'hello' })
    assert.equal(first.beforeCursor, 7)
    assert.equal(receiptAtPrompt, 7)
    remote.cursor = 99
    const replay = await service.prompt({ delegationId: 'd1', requestId: 'request-1', text: 'hello' })
    assert.equal(replay.beforeCursor, 7, 'replay keeps the original cursor')
    assert.equal(remote.prompts.length, 2, 'Host-native deduplication gets the retry')
    await assert.rejects(
      service.prompt({ delegationId: 'd1', requestId: 'request-1', text: 'different' }),
      (error: unknown) => error instanceof LiaisonError && error.code === 'conflict',
    )
    assert.equal(remote.prompts.length, 2)
  })

  it('leaves no receipt when reading the pre-prompt cursor fails', async () => {
    await mkdir(project, { recursive: true })
    await service.create({ delegationId: 'd1', cwd: project, title: 'Task one', model: 'fixture/model-a' })
    remote.failCursorOnce = true
    await assert.rejects(service.prompt({ delegationId: 'd1', requestId: 'request-1', text: 'hello' }), /cursor failed/)
    assert.equal(ledger.findReceipt('d1', 'request-1'), undefined)
    const retry = await service.prompt({ delegationId: 'd1', requestId: 'request-1', text: 'hello' })
    assert.equal(retry.beforeCursor, 5)
    assert.equal(remote.prompts.length, 1)
  })

  it('generates a request id when omitted', async () => {
    await mkdir(project, { recursive: true })
    await service.create({ delegationId: 'd1', cwd: project, title: 'Task one', model: 'fixture/model-a' })
    const result = await service.prompt({ delegationId: 'd1', text: 'hello' })
    assert.match(result.requestId, /^liaison-id-/)
  })

  it('attributes a header to the requested turn instead of a later turn', async () => {
    await mkdir(project, { recursive: true })
    await service.create({ delegationId: 'd1', cwd: project, title: 'Task one', model: 'fixture/model-a' })
    remote.page = { cursor: 8, events: [
      turnStart(1, 1),
      userMessage(2, 'request-1'),
      header(3, 'model-a'),
      turnEnd(4, 1),
      turnStart(5, 2),
      userMessage(6, 'request-2'),
      header(7, 'model-b'),
      turnEnd(8, 2),
    ] }
    const first = await service.verifyModel({ delegationId: 'd1', requestId: 'request-1' })
    assert.equal(first.verification, 'proven')
    assert.deepEqual(first.observed, { provider: 'fixture', model: 'model-a' })
    assert.deepEqual(first.evidence, {
      turn: 1, turnStartSeq: 1, turnEndSeq: 4, userSeq: 2, headerSeq: 3,
      headerSource: 'turn', inherited: false,
    })
    assert.equal(ledger.findDelegation('d1')?.modelVerification, 'proven')

    const second = await service.verifyModel({ delegationId: 'd1', requestId: 'request-2' })
    assert.equal(second.verification, 'mismatch')
    assert.deepEqual(second.observed, { provider: 'fixture', model: 'model-b' })
  })

  it('does not let a later turn prove an earlier turn with no header', async () => {
    await mkdir(project, { recursive: true })
    await service.create({ delegationId: 'd1', cwd: project, title: 'Task one', model: 'fixture/model-a' })
    remote.page = { cursor: 7, events: [
      turnStart(1, 1),
      userMessage(2, 'request-1'),
      turnEnd(3, 1),
      turnStart(4, 2),
      userMessage(5, 'request-2'),
      header(6, 'model-a'),
      turnEnd(7, 2),
    ] }
    const result = await service.verifyModel({ delegationId: 'd1', requestId: 'request-1' })
    assert.equal(result.verification, 'indeterminate')
    assert.equal(result.observed, null)
    assert.deepEqual(result.evidence, { turn: 1, turnStartSeq: 1, turnEndSeq: 3, userSeq: 2 })
  })

  it('uses a carried unchanged header only with execution evidence in that turn', async () => {
    await mkdir(project, { recursive: true })
    await service.create({ delegationId: 'd1', cwd: project, title: 'Task one', model: 'fixture/model-a' })
    remote.page = { cursor: 8, events: [
      turnStart(1, 1),
      userMessage(2, 'request-1'),
      header(3, 'model-a'),
      turnEnd(4, 1),
      turnStart(5, 2),
      userMessage(6, 'request-2'),
      assistant(7, 2),
      turnEnd(8, 2),
    ] }
    const proven = await service.verifyModel({ delegationId: 'd1', requestId: 'request-2' })
    assert.equal(proven.verification, 'proven')
    assert.deepEqual(proven.evidence, {
      turn: 2, turnStartSeq: 5, turnEndSeq: 8, userSeq: 6,
      headerSeq: 3, headerSource: 'carried', executionSeq: 7, inherited: false,
    })

    remote.page = { cursor: 8, events: [
      turnStart(1, 1),
      userMessage(2, 'request-1'),
      header(3, 'model-a'),
      turnEnd(4, 1),
      turnStart(5, 2),
      userMessage(6, 'request-2'),
      turnEnd(8, 2),
    ] }
    const unproven = await service.verifyModel({ delegationId: 'd1', requestId: 'request-2' })
    assert.equal(unproven.verification, 'indeterminate')
    assert.equal(unproven.observed?.model, 'model-a')
    assert.equal(unproven.evidence?.executionSeq, undefined)
  })

  it('marks a carried header from the inherited prefix while proving live execution', async () => {
    await mkdir(project, { recursive: true })
    await service.create({ delegationId: 'd1', cwd: project, title: 'Task one', model: 'fixture/model-a' })
    remote.page = { cursor: 9, events: [
      turnStart(1, 1),
      userMessage(2, 'request-1'),
      header(3, 'model-a'),
      turnEnd(4, 1),
      event('session/end-seed', 5, { inherited: true }),
      turnStart(6, 2),
      userMessage(7, 'request-2'),
      assistant(8, 2),
      turnEnd(9, 2),
    ] }
    const result = await service.verifyModel({ delegationId: 'd1', requestId: 'request-2' })
    assert.equal(result.verification, 'proven')
    assert.equal(result.evidence?.headerSource, 'carried')
    assert.equal(result.evidence?.inherited, true)
    assert.equal(result.evidence?.executionSeq, 8)
  })


  it('does not let a later step in the same turn prove an earlier prompt', async () => {
    await mkdir(project, { recursive: true })
    await service.create({ delegationId: 'd1', cwd: project, title: 'Task one', model: 'fixture/model-a' })
    remote.page = { cursor: 8, events: [
      turnStart(1, 1),
      stepStart(2, 1, 1),
      userMessage(3, 'request-1'),
      stepStart(4, 1, 2),
      userMessage(5, 'request-2'),
      header(6, 'model-a'),
      stepEnd(7, 1, 2),
      turnEnd(8, 1),
    ] }
    const result = await service.verifyModel({ delegationId: 'd1', requestId: 'request-1' })
    assert.equal(result.verification, 'indeterminate')
    assert.equal(result.observed, null)
  })

  it('reports a mismatch and indeterminate cases truthfully', async () => {
    await mkdir(project, { recursive: true })
    await service.create({ delegationId: 'd1', cwd: project, title: 'Task one', model: 'fixture/model-a' })
    remote.page = { cursor: 4, events: [
      turnStart(1, 1),
      userMessage(2, 'request-1'),
      header(3, 'model-b'),
      turnEnd(4, 1),
    ] }
    const mismatch = await service.verifyModel({ delegationId: 'd1', requestId: 'request-1' })
    assert.equal(mismatch.verification, 'mismatch')
    assert.deepEqual(mismatch.observed, { provider: 'fixture', model: 'model-b' })

    remote.page = { cursor: 4, events: [
      turnStart(1, 1),
      userMessage(2, 'request-1'),
      wireEvent('request/header', 3, { header: { config: { provider: 'fixture' } } }),
      turnEnd(4, 1),
    ] }
    const malformed = await service.verifyModel({ delegationId: 'd1', requestId: 'request-1' })
    assert.equal(malformed.verification, 'indeterminate')
    assert.equal(malformed.observed, null)

    const unknown = await service.verifyModel({ delegationId: 'd1', requestId: 'missing' })
    assert.equal(unknown.verification, 'indeterminate')
    assert.equal(unknown.evidence, null)
  })


  it('requires execution evidence for an inherited header inside an inherited turn', async () => {
    await mkdir(project, { recursive: true })
    await service.create({ delegationId: 'd1', cwd: project, title: 'Task one', model: 'fixture/model-a' })
    const inheritedTurn = [
      turnStart(0, 1),
      stepStart(1, 1, 1),
      userMessage(2, 'request-1'),
      header(3, 'model-a'),
      turnEnd(5, 1),
      event('session/end-seed', 6, { inherited: true }),
      turnStart(7, 2),
      stepStart(8, 2, 1),
      userMessage(9, 'request-2'),
      header(10, 'model-a'),
      turnEnd(11, 2),
    ]
    remote.page = { cursor: 11, events: inheritedTurn }
    const unproven = await service.verifyModel({ delegationId: 'd1', requestId: 'request-1' })
    assert.equal(unproven.verification, 'indeterminate')
    assert.equal(unproven.evidence?.inherited, true)

    remote.page = { cursor: 11, events: [
      ...inheritedTurn.slice(0, 4),
      assistant(4, 1),
      ...inheritedTurn.slice(4),
    ] }
    const proven = await service.verifyModel({ delegationId: 'd1', requestId: 'request-1' })
    assert.equal(proven.verification, 'proven')
    assert.equal(proven.evidence?.inherited, true)
    assert.equal(proven.evidence?.executionSeq, 4)
  })

  it('uses the latest turn when no request id is supplied', async () => {
    await mkdir(project, { recursive: true })
    await service.create({ delegationId: 'd1', cwd: project, title: 'Task one', model: 'fixture/model-a' })
    remote.page = { cursor: 8, events: [
      turnStart(1, 1),
      userMessage(2, 'request-1'),
      header(3, 'model-a'),
      turnEnd(4, 1),
      turnStart(5, 2),
      userMessage(6, 'request-2'),
      assistant(7, 2),
      turnEnd(8, 2),
    ] }
    const result = await service.verifyModel({ delegationId: 'd1' })
    assert.equal(result.requestId, null)
    assert.equal(result.verification, 'proven')
    assert.equal(result.evidence?.turn, 2)
    assert.equal(result.evidence?.headerSource, 'carried')
  })

  it('rejects an unsafe working directory before any remote call', async () => {
    await mkdir(join(root, 'home', 'nested'), { recursive: true })
    await assert.rejects(
      service.create({ delegationId: 'd1', cwd: join(root, 'home', 'nested'), title: 'x', model: 'fixture/model-a' }),
      (error: unknown) => error instanceof LiaisonError && error.code === 'unsafe-cwd',
    )
    assert.deepEqual(remote.calls, [])
    assert.equal(ledger.findDelegation('d1'), undefined)
  })
})

class FakeRemote implements DshRemote {
  readonly origin = 'http://127.0.0.1:3080'
  readonly calls: string[] = []
  readonly prompts: string[] = []
  readonly sessionIds: string[] = []
  cursor = 5
  page: RemoteEventPage = { cursor: 0, events: [] }
  failSessionOnce = false
  workspaceGate: Promise<void> | undefined
  workspaceEntries = 0
  returnDifferentSessionId = false
  failCursorOnce = false
  sessionIdArgument: string | undefined
  onBeforeCreateWorkspace: (() => void) | undefined
  onBeforePrompt: (() => void) | undefined

  async modelCatalog(): Promise<RemoteModelCatalog> {
    return CATALOG
  }

  async createWorkspace(): Promise<string> {
    this.calls.push('workspace/create')
    this.workspaceEntries += 1
    this.onBeforeCreateWorkspace?.()
    if (this.workspaceGate !== undefined) await this.workspaceGate
    return 'workspace-1'
  }

  async createSession(input: { workspaceId: string; sessionId: string }): Promise<string> {
    this.calls.push('session/create')
    this.sessionIds.push(input.sessionId)
    this.sessionIdArgument = input.sessionId
    if (this.failSessionOnce) {
      this.failSessionOnce = false
      throw new Error('session create failed')
    }
    return this.returnDifferentSessionId ? 'session-other' : input.sessionId
  }

  async renameSession(): Promise<void> {
    this.calls.push('session/rename')
  }

  async selectModel(): Promise<void> {
    this.calls.push('session/selectModel')
  }

  async beforeCursor(): Promise<number> {
    if (this.failCursorOnce) {
      this.failCursorOnce = false
      throw new Error('cursor failed')
    }
    return this.cursor
  }

  async prompt(_sessionId: string, requestId: string, text: string): Promise<void> {
    this.prompts.push(`${requestId}:${text}`)
    this.onBeforePrompt?.()
  }

  async readEvents(): Promise<RemoteEventPage> {
    return this.page
  }

  async dispose(): Promise<void> {}
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 500; attempt += 1) {
    if (predicate()) return
    await new Promise(resolve => setTimeout(resolve, 1))
  }
  throw new Error('condition did not settle')
}

function event<K extends keyof SessionEventMap>(type: K, seq: number, data: SessionEventMap[K]): SessionWireEvent {
  return wireEvent(type, seq, data)
}

/** Deliberately malformed wire data for indeterminate-verdict coverage. */
function wireEvent(type: string, seq: number, data: unknown): SessionWireEvent {
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

function turnStart(seq: number, turn: number): SessionWireEvent {
  return event('turn/start', seq, { turn })
}

function turnEnd(seq: number, turn: number): SessionWireEvent {
  return event('turn/end', seq, { turn, reason: { kind: 'completed' } })
}

function header(seq: number, model: string): SessionWireEvent {
  return event('request/header', seq, { header: { config: { provider: 'fixture', model } }, reason: 'initial' })
}

function assistant(seq: number, turn: number): SessionWireEvent {
  return event('assistant/attempt', seq, { turn, step: 1, stream: [] })
}

function stepStart(seq: number, turn: number, step: number): SessionWireEvent {
  return event('step/start', seq, { turn, step })
}

function stepEnd(seq: number, turn: number, step: number): SessionWireEvent {
  return event('step/end', seq, { turn, step })
}
