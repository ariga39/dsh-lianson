import { strict as assert } from 'node:assert'
import { createHash, randomUUID } from 'node:crypto'
import { mkdir, mkdtemp, readFile, rm } from 'node:fs/promises'
import { homedir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import type {
  SessionEventEntry,
  SessionRequestId,
  SessionEventsPage,
  SessionQueuedItem,
  SessionState,
  SessionSummary,
  SessionWireEvent,
} from '@deepseek-ai/dsh-api-session-controller/types'
import { brandString } from '@deepseek-ai/dsh-brand'
import type { SessionId } from '@deepseek-ai/dsh-session/types'
import type { RemoteFailure, RemoteResult } from '@deepseek-ai/dsh-typert-protocol'
import type { WorkspaceId } from '@deepseek-ai/dsh-api-workspace-controller/types'
import {
  DelegationRegistry,
  DelegationRegistryError,
  type DelegationRegistryErrorCode,
} from '../src/delegation-registry.ts'
import {
  SessionCommands,
  type CreateDelegationCommand,
  type HostOperatorAccess,
  type SessionCommandsRemote,
} from '../src/session-commands.ts'
import {
  DelegationError,
  type DelegationErrorCode,
} from '../src/workspace-guard.ts'

const TEST_PARENT = join(homedir(), '.cache', 'dsh-liaison-tests')
await mkdir(TEST_PARENT, { recursive: true, mode: 0o700 })

async function scratchRoot(label: string): Promise<string> {
  return await mkdtemp(join(TEST_PARENT, `slice2-commands-${label}-`))
}

function contentHash(content: string): string {
  return createHash('sha256').update(content).digest('hex')
}

function derivedRequestId(ipcRequestId: string): string {
  return `request-${createHash('sha256').update(ipcRequestId).digest('hex').slice(0, 32)}`
}

async function expectCode(
  promise: Promise<unknown>,
  code: DelegationErrorCode | DelegationRegistryErrorCode,
): Promise<DelegationError | DelegationRegistryError> {
  try {
    await promise
  } catch (error) {
    assert.ok(
      error instanceof DelegationError || error instanceof DelegationRegistryError,
      `expected a typed delegation failure, got ${String(error)}`,
    )
    assert.equal(error.code, code)
    return error
  }
  assert.fail(`expected ${code}`)
}

function remoteFailure(
  code: string,
  message: string,
  details: Readonly<Record<string, unknown>> = {},
): RemoteFailure {
  return { code, message, details } as unknown as RemoteFailure
}

function wireEvent(seq: number, type: string, data: SessionWireEvent['data']): SessionWireEvent {
  return { type, seq, time: seq, data }
}

function userMessage(seq: number, requestId: string): SessionWireEvent {
  return wireEvent(seq, 'user/message', {
    source: { kind: 'user', rpcId: brandString<SessionRequestId>(requestId) },
    content: [{ type: 'text', text: requestId }],
  })
}

function requestHeader(seq: number, provider: string, model: string): SessionWireEvent {
  return wireEvent(seq, 'request/header', { header: { config: { provider, model } } })
}

function eventEntry(event: SessionWireEvent): SessionEventEntry {
  return { type: 'event', event }
}

interface FakeHost {
  readonly remote: SessionCommandsRemote
  readonly requestedSessionIds: string[]
  sessionIdOverride: string | undefined
  sessionCreateFailure: ((requestedSessionId: string) => RemoteFailure) | undefined
  stateFailure: RemoteFailure | undefined
  stateThrows: Error | undefined
  eventsFailure: RemoteFailure | undefined
  eventsThrows: Error | undefined
  promptThrowsAfterCommit: boolean
  promptCalls: number
  renameCalls: number
  selectModelCalls: number
  readonly promptRequestIds: string[]
  readonly events: Map<string, SessionWireEvent[]>
  readonly queues: Map<string, readonly SessionQueuedItem[]>
  listSummaries: SessionSummary[]
}

function fakeHost(): FakeHost {
  const requestedSessionIds: string[] = []
  const promptRequestIds: string[] = []
  const events = new Map<string, SessionWireEvent[]>()
  const queues = new Map<string, readonly SessionQueuedItem[]>()
  const workspaceId = brandString<WorkspaceId>('workspace-1')
  const provider = 'provider-a'
  const models = ['model-a', 'model-b']
  const state: Omit<FakeHost, 'remote'> = {
    requestedSessionIds,
    sessionIdOverride: undefined,
    sessionCreateFailure: undefined,
    stateFailure: undefined,
    stateThrows: undefined,
    eventsFailure: undefined,
    eventsThrows: undefined,
    promptThrowsAfterCommit: false,
    promptCalls: 0,
    renameCalls: 0,
    selectModelCalls: 0,
    promptRequestIds,
    events,
    queues,
    listSummaries: [],
  }

  const sessionIdOf = (sessionId: string): SessionId => brandString<SessionId>(sessionId)
  const eventsOf = (sessionId: string): SessionWireEvent[] => {
    let current = events.get(sessionId)
    if (current === undefined) {
      current = []
      events.set(sessionId, current)
    }
    return current
  }
  const stateOf = (sessionId: string): SessionState => {
    const current = eventsOf(sessionId)
    return {
      cursor: current.at(-1)?.seq ?? 0,
      active: false,
      running: false,
      queue: queues.get(sessionId) ?? [],
      pendingInput: [],
    }
  }

  const remote: SessionCommandsRemote = {
    workspace: {
      async create(request) {
        return {
          ok: true,
          value: {
            workspace: {
              workspaceId,
              path: request.path,
              title: 'workspace',
              sessionIds: [],
              createdAt: new Date(0).toISOString(),
              updatedAt: new Date(0).toISOString(),
            },
            created: true,
          },
        }
      },
    },
    session: {
      async modelCatalog() {
        return {
          ok: true,
          value: {
            default: { provider, model: models[0] ?? 'model-a' },
            routableProviders: [provider],
            groups: [{
              id: provider,
              name: 'Provider A',
              models: models.map(id => ({ id, name: id })),
            }],
            failures: [],
          },
        }
      },
      async list() {
        return { ok: true, value: { items: state.listSummaries } }
      },
      async create(request) {
        const requested = String(request.sessionId)
        requestedSessionIds.push(requested)
        if (state.sessionIdOverride !== undefined) {
          return { ok: true, value: { sessionId: sessionIdOf(state.sessionIdOverride) } }
        }
        if (state.sessionCreateFailure !== undefined) {
          return { ok: false, error: state.sessionCreateFailure(requested) }
        }
        return { ok: true, value: { sessionId: sessionIdOf(requested) } }
      },
      async rename(request) {
        state.renameCalls += 1
        return { ok: true, value: { title: request.title, seq: 0 } }
      },
      async selectModel(request) {
        state.selectModelCalls += 1
        return {
          ok: true,
          value: {
            selected: {
              provider: request.provider,
              model: request.model,
              ...(request.reasoningEffort === undefined ? {} : { reasoningEffort: request.reasoningEffort }),
            },
          },
        }
      },
      async prompt(request) {
        state.promptCalls += 1
        promptRequestIds.push(String(request.requestId))
        const current = eventsOf(String(request.sessionId))
        current.push(userMessage((current.at(-1)?.seq ?? 0) + 1, String(request.requestId)))
        if (state.promptThrowsAfterCommit) throw new Error('operator request timed out after Host commit')
        return { ok: true, value: { accepted: true } }
      },
      async state(request) {
        if (state.stateThrows !== undefined) throw state.stateThrows
        if (state.stateFailure !== undefined) return { ok: false, error: state.stateFailure }
        return { ok: true, value: stateOf(String(request.sessionId)) }
      },
      async events(request): Promise<RemoteResult<SessionEventsPage>> {
        if (state.eventsThrows !== undefined) throw state.eventsThrows
        if (state.eventsFailure !== undefined) return { ok: false, error: state.eventsFailure }
        const address = request.address
        if (address.kind !== 'session') return { ok: false, error: remoteFailure('session/not-found', 'not a session') }
        const all = eventsOf(String(address.sessionId))
        const through = request.throughSeq ?? all.at(-1)?.seq ?? 0
        const end = Math.min(through + 1, request.beforeSeq ?? through + 1)
        const maxMessages = request.maxMessages ?? 1_000
        let count = 0
        let cut = 0
        for (let index = end - 1; index >= 0; index -= 1) {
          const type = all[index]?.type
          if (type !== 'user/message' && type !== 'assistant/message' && type !== 'tool/result'
            && type !== 'developer/message' && type !== 'system/message') continue
          count += 1
          if (count >= maxMessages) {
            cut = index
            break
          }
        }
        return {
          ok: true,
          value: {
            records: all.slice(cut, end).map(eventEntry),
            cursor: through,
            hasMore: cut > 0,
          },
        }
      },
    },
  }

  return Object.assign(state, { remote })
}

interface MutableHostContext {
  stateDir: string
  dshHome: string
  cwd: string
  hostPid: number
  origin: string
  port: number
}

function accessFor(host: FakeHost, context: MutableHostContext): HostOperatorAccess {
  return {
    async hostContext() {
      return context
    },
    async withOperator<T>(
      operation: (remote: SessionCommandsRemote, signal: AbortSignal) => Promise<RemoteResult<T>>,
    ): Promise<RemoteResult<T>> {
      return await operation(host.remote, new AbortController().signal)
    },
  }
}

interface Fixture {
  readonly root: string
  readonly work: string
  readonly ledgerFile: string
  readonly registry: DelegationRegistry
  readonly commands: SessionCommands
  readonly host: FakeHost
  readonly context: MutableHostContext
}

async function fixture(label: string, configure?: (host: FakeHost) => void): Promise<Fixture> {
  const root = await scratchRoot(label)
  const work = join(root, 'work')
  await mkdir(work, { recursive: true })
  const host = fakeHost()
  configure?.(host)
  const ledgerFile = join(root, 'ledger', 'delegations.json')
  const registry = new DelegationRegistry({ ledgerFile, lockWaitMs: 0 })
  const context: MutableHostContext = {
    stateDir: join(root, 'state'),
    dshHome: join(root, 'dsh-home'),
    cwd: root,
    hostPid: process.pid,
    origin: 'http://127.0.0.1:9',
    port: 9,
  }
  const commands = new SessionCommands({
    registry,
    access: accessFor(host, context),
    ledgerDir: join(root, 'ledger'),
  })
  return { root, work, ledgerFile, registry, commands, host, context }
}

async function cleanup(item: Fixture): Promise<void> {
  await item.registry.close()
  await rm(item.root, { recursive: true, force: true })
}

function createCommand(item: Fixture, overrides: Partial<CreateDelegationCommand> = {}): CreateDelegationCommand {
  return {
    delegationId: 'del-1',
    title: 'Alpha',
    provider: 'provider-a',
    model: 'model-a',
    cwd: item.work,
    ...overrides,
  }
}

await test('prompt replay after a committed timeout returns the original cursor without a second Host dispatch', async () => {
  const item = await fixture('prompt-replay', (host) => { host.promptThrowsAfterCommit = true })
  try {
    const created = await item.commands.create(createCommand(item))
    const requestId = 'request-1'
    const content = 'write the isolation marker'
    const first = item.commands.prompt({ delegationId: created.delegation.delegationId, requestId, content })
    await assert.rejects(first, /timed out after Host commit/u)
    assert.equal(item.host.promptCalls, 1)
    assert.equal((await item.registry.promptReceipt('del-1', requestId))?.accepted, false)

    item.host.promptThrowsAfterCommit = false
    const replay = await item.commands.prompt({ delegationId: 'del-1', requestId, content })
    assert.equal(replay.replayed, true)
    assert.equal(replay.beforeCursor, 0)
    assert.equal(item.host.promptCalls, 1, 'the retry must probe instead of dispatching a duplicate prompt')

    const acceptedReplay = await item.commands.prompt({ delegationId: 'del-1', requestId, content })
    assert.equal(acceptedReplay.replayed, true)
    assert.equal(acceptedReplay.beforeCursor, 0)
    assert.equal(item.host.promptCalls, 1)

    const promptMessages = (item.host.events.get(created.delegation.sessionId) ?? [])
      .filter(event => event.type === 'user/message')
    assert.equal(promptMessages.length, 1)
    assert.equal((await item.registry.promptReceipt('del-1', requestId))?.accepted, true)

    await expectCode(
      item.commands.prompt({ delegationId: 'del-1', requestId, content: 'different content' }),
      'DELEGATION_PROMPT_CONFLICT',
    )
    assert.equal(item.host.promptCalls, 1)
  } finally {
    await cleanup(item)
  }
})

await test('an unverifiable pending prompt fails explicitly instead of minting a new identity', async () => {
  const item = await fixture('prompt-uncertain')
  try {
    const created = await item.commands.create(createCommand(item))
    const content = 'pending content'
    await item.registry.beginPrompt('del-1', {
      requestId: 'request-pending',
      contentHash: contentHash(content),
      beforeCursor: 0,
    })

    item.host.stateFailure = remoteFailure('session/not-found', 'session expired', {
      sessionId: created.delegation.sessionId,
    })
    const expired = await expectCode(
      item.commands.prompt({ delegationId: 'del-1', requestId: 'request-pending', content }),
      'DELEGATION_REQUEST_ID_EXPIRED',
    )
    assert.match(expired.message, /expired|not found/u)

    item.host.stateFailure = undefined
    item.host.stateThrows = new Error('operator connection lost')
    await expectCode(
      item.commands.prompt({ delegationId: 'del-1', requestId: 'request-pending', content }),
      'DELEGATION_REQUEST_ID_UNCERTAIN',
    )
    assert.equal(item.host.promptCalls, 0)
    assert.equal((await item.registry.promptReceipt('del-1', 'request-pending'))?.accepted, false)
  } finally {
    await cleanup(item)
  }
})

await test('substituted session ids are recorded as conflicts and never bound', async () => {
  const item = await fixture('substitution', (host) => { host.sessionIdOverride = 'session-substitute' })
  try {
    await expectCode(item.commands.create(createCommand(item)), 'DELEGATION_SESSION_SUBSTITUTED')
    const record = await item.registry.get('del-1')
    assert.equal(record.state, 'conflict')
    assert.equal(record.sessionId, item.host.requestedSessionIds[0])
    assert.notEqual(record.sessionId, 'session-substitute')
    assert.equal(item.host.requestedSessionIds.length, 1)
  } finally {
    await cleanup(item)
  }
})

await test('workspace attach failures retain a matching session and reject a substituted one', async () => {
  const matching = await fixture('attach-matching', (host) => {
    host.sessionCreateFailure = requested => remoteFailure(
      'session/workspace-attach-failed',
      'attach failed',
      { sessionId: brandString<SessionId>(requested), workspaceId: brandString<WorkspaceId>('workspace-1') },
    )
  })
  try {
    await expectCode(matching.commands.create(createCommand(matching)), 'DELEGATION_WORKSPACE_ATTACH_FAILED')
    const record = await matching.registry.get('del-1')
    assert.equal(record.state, 'attach-failed')
    assert.equal(record.sessionId, matching.host.requestedSessionIds[0])
  } finally {
    await cleanup(matching)
  }

  const substituted = await fixture('attach-substituted', (host) => {
    host.sessionCreateFailure = () => remoteFailure(
      'session/workspace-attach-failed',
      'attach failed for another session',
      {
        sessionId: brandString<SessionId>('session-substitute'),
        workspaceId: brandString<WorkspaceId>('workspace-1'),
      },
    )
  })
  try {
    await expectCode(substituted.commands.create(createCommand(substituted)), 'DELEGATION_SESSION_SUBSTITUTED')
    const record = await substituted.registry.get('del-1')
    assert.equal(record.state, 'conflict')
    assert.equal(record.sessionId, substituted.host.requestedSessionIds[0])
  } finally {
    await cleanup(substituted)
  }
})

await test('verify-model requires turn evidence, accepts inherited headers, ignores later headers, and stays monotonic', async () => {
  const item = await fixture('verify-model')
  try {
    const created = await item.commands.create(createCommand(item))
    const sessionId = created.delegation.sessionId
    item.host.events.set(sessionId, [
      wireEvent(0, 'turn/start', { turn: 1 }),
      userMessage(1, 'request-1'),
      wireEvent(2, 'turn/end', { reason: { kind: 'cancelled' } }),
    ])
    await expectCode(
      item.commands.verifyModel({ delegationId: 'del-1', requestId: 'request-1' }),
      'DELEGATION_MODEL_EVIDENCE_MISSING',
    )

    item.host.events.set(sessionId, [
      wireEvent(0, 'turn/start', { turn: 1 }),
      requestHeader(1, 'provider-a', 'model-a'),
      userMessage(2, 'request-1'),
      wireEvent(3, 'assistant/message', { content: [{ type: 'text', text: 'first' }] }),
      wireEvent(4, 'turn/end', { reason: { kind: 'completed' } }),
      wireEvent(5, 'turn/start', { turn: 2 }),
      userMessage(6, 'request-2'),
      wireEvent(7, 'tool/call', { name: 'write', callId: 'call-1' }),
      wireEvent(8, 'turn/end', { reason: { kind: 'completed' } }),
      wireEvent(9, 'turn/start', { turn: 3 }),
      requestHeader(10, 'provider-b', 'model-b'),
      userMessage(11, 'request-3'),
      wireEvent(12, 'assistant/message', { content: [{ type: 'text', text: 'third' }] }),
      wireEvent(13, 'turn/end', { reason: { kind: 'completed' } }),
    ])

    const first = await item.commands.verifyModel({ delegationId: 'del-1', requestId: 'request-1' })
    assert.equal(first.turnSeq, 0)
    assert.deepEqual(first.observedModel, { provider: 'provider-a', model: 'model-a' })
    assert.equal((await item.registry.get('del-1')).verifiedTurnSeq, 0)

    const inherited = await item.commands.verifyModel({ delegationId: 'del-1', requestId: 'request-2' })
    assert.equal(inherited.turnSeq, 5)
    assert.deepEqual(inherited.observedModel, { provider: 'provider-a', model: 'model-a' })
    assert.equal((await item.registry.get('del-1')).verifiedTurnSeq, 5)

    await expectCode(
      item.commands.verifyModel({ delegationId: 'del-1', requestId: 'request-3' }),
      'DELEGATION_MODEL_MISMATCH',
    )

    const older = await item.commands.verifyModel({ delegationId: 'del-1', requestId: 'request-1' })
    assert.equal(older.turnSeq, 0)
    const persisted = await item.registry.get('del-1')
    assert.equal(persisted.verifiedTurnSeq, 5)
    assert.deepEqual(persisted.verifiedModel, { provider: 'provider-a', model: 'model-a' })
  } finally {
    await cleanup(item)
  }
})

await test('normalized titles collide with live Host sessions in the same workspace before any write-ahead record', async () => {
  const started = randomUUID()
  const item = await fixture('title-collision')
  try {
    item.host.listSummaries = [{
      sessionId: brandString<SessionId>(`session-live-${started}`),
      updatedAt: 0,
      running: false,
      agentAvailable: false,
      blank: false,
      cwd: item.work,
      projections: { kind: 'sequenced', asOfSeq: 0, values: { title: 'Alpha Beta' } },
    }]

    await expectCode(
      item.commands.create(createCommand(item, { title: '  Alpha\u001B[31m   Beta  ' })),
      'DELEGATION_TITLE_CONFLICT',
    )
    assert.equal(item.host.requestedSessionIds.length, 0)
    assert.equal((await item.registry.list()).length, 0)
  } finally {
    await cleanup(item)
  }
})

await test('create and prompt own the writer before replay lookup while get and list stay read-only', async () => {
  const item = await fixture('writer-order')
  try {
    const order: string[] = []
    const ensureWriter = item.registry.ensureWriter.bind(item.registry)
    const list = item.registry.list.bind(item.registry)
    const get = item.registry.get.bind(item.registry)
    const promptReceipt = item.registry.promptReceipt.bind(item.registry)
    item.registry.ensureWriter = async (options) => {
      order.push('ensureWriter')
      await ensureWriter(options)
    }
    item.registry.list = async () => {
      order.push('list')
      return await list()
    }
    item.registry.get = async (delegationId) => {
      order.push('get')
      return await get(delegationId)
    }
    item.registry.promptReceipt = async (delegationId, requestId) => {
      order.push('promptReceipt')
      return await promptReceipt(delegationId, requestId)
    }

    await item.commands.create(createCommand(item))
    assert.equal(order[0], 'ensureWriter')
    assert.ok(order.indexOf('ensureWriter') < order.indexOf('list'))

    order.length = 0
    await item.commands.prompt({ delegationId: 'del-1', requestId: 'request-order', content: 'order' })
    assert.equal(order[0], 'ensureWriter')
    assert.ok(order.indexOf('ensureWriter') < order.indexOf('get'))
    assert.ok(order.indexOf('ensureWriter') < order.indexOf('promptReceipt'))

    order.length = 0
    await item.commands.get({ delegationId: 'del-1' })
    await item.commands.list()
    assert.equal(order.includes('ensureWriter'), false)
  } finally {
    await cleanup(item)
  }
})

await test('an explicit delegation id conflicting with a replay-key binding is rejected before Host mutation', async () => {
  const item = await fixture('replay-key-conflict')
  try {
    await item.commands.create(createCommand(item, {
      delegationId: 'del-key',
      ipcRequestId: 'shared-key',
    }))
    const sessionCreates = item.host.requestedSessionIds.length
    const promptCalls = item.host.promptCalls

    await expectCode(
      item.commands.create(createCommand(item, {
        delegationId: 'del-other',
        ipcRequestId: 'shared-key',
      })),
      'DELEGATION_REPLAY_CONFLICT',
    )
    assert.equal(item.host.requestedSessionIds.length, sessionCreates)
    assert.equal(item.host.promptCalls, promptCalls)
    assert.equal((await item.registry.list()).some(record => record.delegationId === 'del-other'), false)
  } finally {
    await cleanup(item)
  }
})

await test('verify-model rejects any divergent effective configuration even when a later header matches', async () => {
  const item = await fixture('verify-every-config')
  try {
    const created = await item.commands.create(createCommand(item))
    item.host.events.set(created.delegation.sessionId, [
      wireEvent(0, 'turn/start', { turn: 1 }),
      requestHeader(1, 'provider-b', 'model-b'),
      userMessage(2, 'request-all-configs'),
      wireEvent(3, 'assistant/message', { content: [{ type: 'text', text: 'first request' }] }),
      requestHeader(4, 'provider-a', 'model-a'),
      wireEvent(5, 'assistant/message', { content: [{ type: 'text', text: 'second request' }] }),
      wireEvent(6, 'turn/end', { reason: { kind: 'completed' } }),
    ])

    await expectCode(
      item.commands.verifyModel({ delegationId: 'del-1', requestId: 'request-all-configs' }),
      'DELEGATION_MODEL_MISMATCH',
    )
  } finally {
    await cleanup(item)
  }
})


await test('ready replay and prompt dispatch revalidate the recorded cwd against current Host state', async () => {
  const item = await fixture('revalidate-recorded-cwd')
  try {
    await item.commands.create(createCommand(item))
    const sessionCreates = item.host.requestedSessionIds.length

    item.context.stateDir = join(item.work, 'state')
    await expectCode(item.commands.create(createCommand(item)), 'WORKSPACE_STATE_OVERLAP')
    assert.equal(item.host.requestedSessionIds.length, sessionCreates)
    assert.equal((await item.registry.get('del-1')).state, 'ready')

    item.context.stateDir = join(item.root, 'state')
    item.context.dshHome = join(item.work, 'dsh-home')
    await expectCode(item.commands.create(createCommand(item)), 'WORKSPACE_STATE_OVERLAP')
    assert.equal(item.host.requestedSessionIds.length, sessionCreates)

    item.context.dshHome = join(item.root, 'dsh-home')
    const ledgerInsideWork = new SessionCommands({
      registry: item.registry,
      access: accessFor(item.host, item.context),
      ledgerDir: join(item.work, 'ledger-inside'),
    })
    await expectCode(ledgerInsideWork.create(createCommand(item)), 'WORKSPACE_STATE_OVERLAP')
    assert.equal(item.host.requestedSessionIds.length, sessionCreates)

    item.context.stateDir = join(item.work, 'state')
    const promptsBefore = item.host.promptCalls
    await expectCode(
      item.commands.prompt({ delegationId: 'del-1', requestId: 'unsafe-new', content: 'unsafe new' }),
      'WORKSPACE_STATE_OVERLAP',
    )
    assert.equal(item.host.promptCalls, promptsBefore)
    assert.equal(await item.registry.promptReceipt('del-1', 'unsafe-new'), undefined)

    await item.registry.beginPrompt('del-1', {
      requestId: 'unsafe-pending',
      contentHash: contentHash('unsafe pending'),
      beforeCursor: 0,
    })
    await expectCode(
      item.commands.prompt({ delegationId: 'del-1', requestId: 'unsafe-pending', content: 'unsafe pending' }),
      'WORKSPACE_STATE_OVERLAP',
    )
    assert.equal(item.host.promptCalls, promptsBefore)
    assert.equal((await item.registry.promptReceipt('del-1', 'unsafe-pending'))?.accepted, false)

    item.context.stateDir = join(item.root, 'state')
    const replay = await item.commands.create(createCommand(item))
    assert.equal(replay.replayed, true)
  } finally {
    await cleanup(item)
  }
})

await test('verify-model checks a just-started divergent streaming header but never a later turn', async () => {
  const item = await fixture('verify-streaming-header')
  try {
    const created = await item.commands.create(createCommand(item))
    item.host.events.set(created.delegation.sessionId, [
      wireEvent(0, 'turn/start', { turn: 1 }),
      requestHeader(1, 'provider-a', 'model-a'),
      userMessage(2, 'request-streaming'),
      wireEvent(3, 'assistant/message', { content: [{ type: 'text', text: 'first request' }] }),
      wireEvent(4, 'step/start', { turn: 1, step: 2 }),
      requestHeader(5, 'provider-b', 'model-b'),
    ])
    await expectCode(
      item.commands.verifyModel({ delegationId: 'del-1', requestId: 'request-streaming' }),
      'DELEGATION_MODEL_MISMATCH',
    )

    item.host.events.set(created.delegation.sessionId, [
      wireEvent(0, 'turn/start', { turn: 1 }),
      requestHeader(1, 'provider-b', 'model-b'),
      userMessage(2, 'request-before'),
      wireEvent(3, 'assistant/message', { content: [{ type: 'text', text: 'before' }] }),
      wireEvent(4, 'turn/end', { reason: { kind: 'completed' } }),
      wireEvent(5, 'turn/start', { turn: 2 }),
      requestHeader(6, 'provider-a', 'model-a'),
      userMessage(7, 'request-selected'),
      wireEvent(8, 'assistant/message', { content: [{ type: 'text', text: 'selected' }] }),
      wireEvent(9, 'turn/end', { reason: { kind: 'completed' } }),
      wireEvent(10, 'turn/start', { turn: 3 }),
      requestHeader(11, 'provider-b', 'model-b'),
      userMessage(12, 'request-after'),
      wireEvent(13, 'assistant/message', { content: [{ type: 'text', text: 'after' }] }),
      wireEvent(14, 'turn/end', { reason: { kind: 'completed' } }),
    ])
    const selected = await item.commands.verifyModel({
      delegationId: 'del-1',
      requestId: 'request-selected',
    })
    assert.equal(selected.turnSeq, 5)
    assert.deepEqual(selected.observedModel, { provider: 'provider-a', model: 'model-a' })
  } finally {
    await cleanup(item)
  }
})

await test('verify-model does not certify early evidence from a later header when no inherited header exists', async () => {
  const item = await fixture('verify-unresolved-inherited')
  try {
    const created = await item.commands.create(createCommand(item))
    item.host.events.set(created.delegation.sessionId, [
      wireEvent(0, 'turn/start', { turn: 1 }),
      userMessage(1, 'request-early-evidence'),
      wireEvent(2, 'assistant/message', { content: [{ type: 'text', text: 'early without header' }] }),
      requestHeader(3, 'provider-a', 'model-a'),
      wireEvent(4, 'assistant/message', { content: [{ type: 'text', text: 'later matching attempt' }] }),
      wireEvent(5, 'turn/end', { reason: { kind: 'completed' } }),
    ])
    await expectCode(
      item.commands.verifyModel({ delegationId: 'del-1', requestId: 'request-early-evidence' }),
      'DELEGATION_MODEL_EVIDENCE_MISSING',
    )
  } finally {
    await cleanup(item)
  }
})

await test('unkeyed create and prompt invocations remain separate new operations', async () => {
  const item = await fixture('one-shot-unkeyed')
  try {
    const first = await item.commands.create({
      title: 'Unkeyed Alpha',
      provider: 'provider-a',
      model: 'model-a',
      cwd: item.work,
    })
    const second = await item.commands.create({
      title: 'Unkeyed Beta',
      provider: 'provider-a',
      model: 'model-a',
      cwd: item.work,
    })
    assert.equal(first.replayed, false)
    assert.equal(second.replayed, false)
    assert.notEqual(first.delegation.delegationId, second.delegation.delegationId)
    assert.notEqual(first.delegation.sessionId, second.delegation.sessionId)

    const firstPrompt = await item.commands.prompt({
      delegationId: first.delegation.delegationId,
      content: 'same unkeyed content',
    })
    const secondPrompt = await item.commands.prompt({
      delegationId: first.delegation.delegationId,
      content: 'same unkeyed content',
    })
    assert.equal(firstPrompt.replayed, false)
    assert.equal(secondPrompt.replayed, false)
    assert.notEqual(firstPrompt.requestId, secondPrompt.requestId)
    assert.equal(item.host.promptCalls, 2)
  } finally {
    await cleanup(item)
  }
})

await test('prompt key bindings persist across restart and reject changed identity or content', async () => {
  const item = await fixture('prompt-key-binding')
  let restarted: DelegationRegistry | undefined
  try {
    await item.commands.create(createCommand(item, { delegationId: 'del-1', title: 'Alpha' }))
    await item.commands.create(createCommand(item, { delegationId: 'del-2', title: 'Beta' }))
    const first = await item.commands.prompt({
      delegationId: 'del-1',
      requestId: 'request-1',
      content: 'stable content',
    })
    assert.equal(first.replayed, false)
    assert.equal(item.host.promptCalls, 1)

    const boundReplay = await item.commands.prompt({
      delegationId: 'del-1',
      requestId: 'request-1',
      content: 'stable content',
      ipcRequestId: 'caller-key',
    })
    assert.equal(boundReplay.replayed, true)
    assert.equal(boundReplay.beforeCursor, first.beforeCursor)
    assert.equal(item.host.promptCalls, 1)
    assert.deepEqual((await item.registry.get('del-1')).promptKeys?.['caller-key'], {
      delegationId: 'del-1',
      requestId: 'request-1',
      contentHash: contentHash('stable content'),
    })

    await item.registry.close()
    restarted = new DelegationRegistry({ ledgerFile: item.ledgerFile, lockWaitMs: 0 })
    const commands = new SessionCommands({
      registry: restarted,
      access: accessFor(item.host, item.context),
      ledgerDir: join(item.root, 'ledger'),
    })

    const replay = await commands.prompt({
      delegationId: 'del-1',
      requestId: 'request-1',
      content: 'stable content',
      ipcRequestId: 'caller-key',
    })
    assert.equal(replay.replayed, true)
    assert.equal(replay.beforeCursor, first.beforeCursor)
    assert.equal(item.host.promptCalls, 1)

    await expectCode(
      commands.prompt({
        delegationId: 'del-1',
        requestId: 'request-1',
        content: 'changed content',
        ipcRequestId: 'caller-key',
      }),
      'DELEGATION_PROMPT_CONFLICT',
    )
    await expectCode(
      commands.prompt({
        delegationId: 'del-1',
        requestId: 'different-request',
        content: 'stable content',
        ipcRequestId: 'caller-key',
      }),
      'DELEGATION_PROMPT_CONFLICT',
    )
    await expectCode(
      commands.prompt({
        delegationId: 'del-2',
        content: 'stable content',
        ipcRequestId: 'caller-key',
      }),
      'DELEGATION_PROMPT_CONFLICT',
    )
    assert.equal(item.host.promptCalls, 1)

    const fresh = await commands.prompt({
      delegationId: 'del-2',
      content: 'fresh keyed content',
      ipcRequestId: 'caller-key-2',
    })
    assert.equal(fresh.replayed, false)
    assert.equal(item.host.promptCalls, 2)
    assert.deepEqual((await restarted.get('del-2')).promptKeys?.['caller-key-2'], {
      delegationId: 'del-2',
      requestId: fresh.requestId,
      contentHash: contentHash('fresh keyed content'),
    })
  } finally {
    await restarted?.close()
    await cleanup(item)
  }
})

await test('prompt dictionaries treat inherited Object names as literal caller keys', async () => {
  const item = await fixture('prompt-object-names')
  let restarted: DelegationRegistry | undefined
  try {
    await item.commands.create(createCommand(item))
    const normal = await item.commands.prompt({
      delegationId: 'del-1',
      requestId: 'normal-request',
      content: 'ordinary',
    })
    assert.equal(normal.replayed, false)
    assert.equal(item.host.promptCalls, 1)

    const literal = await item.commands.prompt({
      delegationId: 'del-1',
      requestId: 'toString',
      content: 'literal toString',
    })
    assert.equal(literal.replayed, false)
    assert.equal(item.host.promptCalls, 2)

    const bound = await item.commands.prompt({
      delegationId: 'del-1',
      requestId: 'toString',
      content: 'literal toString',
      ipcRequestId: 'toString',
    })
    assert.equal(bound.replayed, true)
    assert.equal(item.host.promptCalls, 2)

    const constructorKey = await item.commands.prompt({
      delegationId: 'del-1',
      requestId: 'hasOwnProperty',
      content: 'literal constructor key',
      ipcRequestId: 'constructor',
    })
    assert.equal(constructorKey.replayed, false)
    assert.equal(item.host.promptCalls, 3)

    const text = await readFile(item.ledgerFile, 'utf8')
    const parsed = JSON.parse(text) as {
      readonly delegations: Record<string, {
        readonly prompts: Record<string, { readonly requestId: string }>
        readonly promptKeys: Record<string, { readonly requestId: string }>
      }>
    }
    const persisted = parsed.delegations['del-1']
    assert.ok(persisted !== undefined)
    assert.ok(Object.hasOwn(persisted.prompts, 'toString'))
    assert.equal(persisted.prompts['toString']?.requestId, 'toString')
    assert.ok(Object.hasOwn(persisted.prompts, 'hasOwnProperty'))
    assert.ok(Object.hasOwn(persisted.promptKeys, 'toString'))
    assert.equal(persisted.promptKeys['toString']?.requestId, 'toString')
    assert.ok(Object.hasOwn(persisted.promptKeys, 'constructor'))
    assert.equal(persisted.promptKeys['constructor']?.requestId, 'hasOwnProperty')

    await item.registry.close()
    restarted = new DelegationRegistry({ ledgerFile: item.ledgerFile, lockWaitMs: 0 })
    const commands = new SessionCommands({
      registry: restarted,
      access: accessFor(item.host, item.context),
      ledgerDir: join(item.root, 'ledger'),
    })
    const replay = await commands.prompt({
      delegationId: 'del-1',
      requestId: 'toString',
      content: 'literal toString',
      ipcRequestId: 'toString',
    })
    assert.equal(replay.replayed, true)
    assert.equal(item.host.promptCalls, 3)
    const keyedReplay = await commands.prompt({
      delegationId: 'del-1',
      requestId: 'hasOwnProperty',
      content: 'literal constructor key',
      ipcRequestId: 'constructor',
    })
    assert.equal(keyedReplay.replayed, true)
    assert.equal(item.host.promptCalls, 3)
    await expectCode(
      commands.prompt({
        delegationId: 'del-1',
        requestId: 'toString',
        content: 'changed toString',
        ipcRequestId: 'toString',
      }),
      'DELEGATION_PROMPT_CONFLICT',
    )
  } finally {
    await restarted?.close()
    await cleanup(item)
  }
})

await test('pending prompt positives can acknowledge early while negatives require full contiguous coverage', async () => {
  const item = await fixture('prompt-deep-scan')
  try {
    const created = await item.commands.create(createCommand(item))
    const fillerCount = 32 * 400 + 50
    const events: SessionWireEvent[] = [wireEvent(0, 'turn/start', { turn: 1 })]
    for (let index = 1; index <= fillerCount; index += 1) {
      events.push(wireEvent(index, 'assistant/message', { content: [{ type: 'text', text: 'filler' }] }))
    }
    events.push(userMessage(fillerCount + 1, 'request-near-head'))
    item.host.events.set(created.delegation.sessionId, events)

    await item.registry.beginPrompt('del-1', {
      requestId: 'request-near-head',
      contentHash: contentHash('near'),
      beforeCursor: 0,
    })
    const positive = await item.commands.prompt({
      delegationId: 'del-1',
      requestId: 'request-near-head',
      content: 'near',
    })
    assert.equal(positive.replayed, true)
    assert.equal(item.host.promptCalls, 0)
    assert.equal((await item.registry.promptReceipt('del-1', 'request-near-head'))?.accepted, true)

    await item.registry.beginPrompt('del-1', {
      requestId: 'request-beyond-scan',
      contentHash: contentHash('missing'),
      beforeCursor: 0,
    })
    await expectCode(
      item.commands.prompt({
        delegationId: 'del-1',
        requestId: 'request-beyond-scan',
        content: 'missing',
      }),
      'DELEGATION_REQUEST_ID_UNCERTAIN',
    )
    assert.equal(item.host.promptCalls, 0)
    assert.equal((await item.registry.promptReceipt('del-1', 'request-beyond-scan'))?.accepted, false)
  } finally {
    await cleanup(item)
  }
})

await test('an empty pending window is complete, while cursor rollback is uncertain', async () => {
  const item = await fixture('prompt-empty-window')
  try {
    await item.commands.create(createCommand(item))
    await item.registry.beginPrompt('del-1', {
      requestId: 'request-empty',
      contentHash: contentHash('empty'),
      beforeCursor: 0,
    })
    const accepted = await item.commands.prompt({
      delegationId: 'del-1',
      requestId: 'request-empty',
      content: 'empty',
    })
    assert.equal(accepted.replayed, true)
    assert.equal(item.host.promptCalls, 1)

    await item.registry.beginPrompt('del-1', {
      requestId: 'request-rollback',
      contentHash: contentHash('rollback'),
      beforeCursor: 5,
    })
    await expectCode(
      item.commands.prompt({
        delegationId: 'del-1',
        requestId: 'request-rollback',
        content: 'rollback',
      }),
      'DELEGATION_REQUEST_ID_UNCERTAIN',
    )
    assert.equal(item.host.promptCalls, 1)
  } finally {
    await cleanup(item)
  }
})

await test('verify-model refuses to certify a turn whose start was truncated from the scan', async () => {
  const item = await fixture('verify-truncated-turn')
  try {
    const created = await item.commands.create(createCommand(item))
    const fillerCount = 32 * 400 + 50
    const events: SessionWireEvent[] = [wireEvent(0, 'turn/start', { turn: 1 })]
    for (let index = 1; index <= fillerCount; index += 1) {
      events.push(wireEvent(index, 'assistant/message', { content: [{ type: 'text', text: 'filler' }] }))
    }
    const base = fillerCount + 1
    events.push(
      requestHeader(base, 'provider-a', 'model-a'),
      userMessage(base + 1, 'request-deep-turn'),
      wireEvent(base + 2, 'assistant/message', { content: [{ type: 'text', text: 'deep evidence' }] }),
      wireEvent(base + 3, 'turn/end', { reason: { kind: 'completed' } }),
    )
    item.host.events.set(created.delegation.sessionId, events)
    await expectCode(
      item.commands.verifyModel({ delegationId: 'del-1', requestId: 'request-deep-turn' }),
      'DELEGATION_MODEL_EVIDENCE_MISSING',
    )
  } finally {
    await cleanup(item)
  }
})


await test('verify-model resolves a persisted prompt key before explicit request or delegation conflicts', async () => {
  const item = await fixture('verify-key-binding')
  try {
    const first = await item.commands.create(createCommand(item, { delegationId: 'del-1', title: 'Alpha' }))
    const second = await item.commands.create(createCommand(item, { delegationId: 'del-2', title: 'Beta' }))
    await item.commands.prompt({
      delegationId: 'del-1',
      requestId: 'custom-turn',
      content: 'keyed verification',
      ipcRequestId: 'verify-key',
    })
    item.host.events.set(first.delegation.sessionId, [
      wireEvent(0, 'turn/start', { turn: 1 }),
      requestHeader(1, 'provider-a', 'model-a'),
      userMessage(2, 'custom-turn'),
      wireEvent(3, 'assistant/message', { content: [{ type: 'text', text: 'custom' }] }),
      wireEvent(4, 'turn/end', { reason: { kind: 'completed' } }),
    ])

    const byKey = await item.commands.verifyModel({
      delegationId: 'del-1',
      ipcRequestId: 'verify-key',
    })
    assert.equal(byKey.requestId, 'custom-turn')
    assert.deepEqual(byKey.observedModel, { provider: 'provider-a', model: 'model-a' })

    const byBoth = await item.commands.verifyModel({
      delegationId: 'del-1',
      requestId: 'custom-turn',
      ipcRequestId: 'verify-key',
    })
    assert.equal(byBoth.requestId, 'custom-turn')

    await expectCode(
      item.commands.verifyModel({
        delegationId: 'del-1',
        requestId: 'different-turn',
        ipcRequestId: 'verify-key',
      }),
      'DELEGATION_PROMPT_CONFLICT',
    )
    await expectCode(
      item.commands.verifyModel({
        delegationId: 'del-2',
        ipcRequestId: 'verify-key',
      }),
      'DELEGATION_PROMPT_CONFLICT',
    )

    const legacyRequestId = derivedRequestId('legacy-key')
    item.host.events.set(second.delegation.sessionId, [
      wireEvent(0, 'turn/start', { turn: 1 }),
      requestHeader(1, 'provider-a', 'model-a'),
      userMessage(2, legacyRequestId),
      wireEvent(3, 'assistant/message', { content: [{ type: 'text', text: 'legacy' }] }),
      wireEvent(4, 'turn/end', { reason: { kind: 'completed' } }),
    ])
    const legacy = await item.commands.verifyModel({
      delegationId: 'del-2',
      ipcRequestId: 'legacy-key',
    })
    assert.equal(legacy.requestId, legacyRequestId)
    assert.deepEqual(legacy.observedModel, { provider: 'provider-a', model: 'model-a' })
  } finally {
    await cleanup(item)
  }
})

await test('pending recovery rejects a within-page sequence gap instead of resending', async () => {
  const item = await fixture('prompt-page-gap')
  try {
    const created = await item.commands.create(createCommand(item))
    item.host.events.set(created.delegation.sessionId, [
      wireEvent(0, 'assistant/message', { content: [{ type: 'text', text: 'zero' }] }),
      wireEvent(2, 'assistant/message', { content: [{ type: 'text', text: 'two' }] }),
    ])
    await item.registry.beginPrompt('del-1', {
      requestId: 'request-gap',
      contentHash: contentHash('gap'),
      beforeCursor: 0,
    })
    await expectCode(
      item.commands.prompt({ delegationId: 'del-1', requestId: 'request-gap', content: 'gap' }),
      'DELEGATION_REQUEST_ID_UNCERTAIN',
    )
    assert.equal(item.host.promptCalls, 0)
    assert.equal((await item.registry.promptReceipt('del-1', 'request-gap'))?.accepted, false)
  } finally {
    await cleanup(item)
  }
})

await test('a recent pending window completes without scanning older unrelated history', async () => {
  const item = await fixture('prompt-recent-window')
  try {
    const created = await item.commands.create(createCommand(item))
    const total = 32 * 400 + 500
    const events: SessionWireEvent[] = []
    for (let index = 0; index < total; index += 1) {
      events.push(wireEvent(index, 'assistant/message', { content: [{ type: 'text', text: 'filler' }] }))
    }
    item.host.events.set(created.delegation.sessionId, events)
    const beforeCursor = total - 101
    await item.registry.beginPrompt('del-1', {
      requestId: 'request-recent',
      contentHash: contentHash('recent'),
      beforeCursor,
    })
    const accepted = await item.commands.prompt({
      delegationId: 'del-1',
      requestId: 'request-recent',
      content: 'recent',
    })
    assert.equal(accepted.replayed, true)
    assert.equal(accepted.beforeCursor, beforeCursor)
    assert.equal(item.host.promptCalls, 1)
  } finally {
    await cleanup(item)
  }
})

await test('verify-model treats an internal page gap as incomplete turn evidence', async () => {
  const item = await fixture('verify-internal-gap')
  try {
    const created = await item.commands.create(createCommand(item))
    item.host.events.set(created.delegation.sessionId, [
      wireEvent(0, 'turn/start', { turn: 1 }),
      userMessage(1, 'request-before-gap'),
      wireEvent(2, 'assistant/message', { content: [{ type: 'text', text: 'before gap' }] }),
      requestHeader(3, 'provider-a', 'model-a'),
      requestHeader(5, 'provider-b', 'model-b'),
      userMessage(6, 'request-after-gap'),
      wireEvent(7, 'assistant/message', { content: [{ type: 'text', text: 'after gap' }] }),
      wireEvent(8, 'turn/end', { reason: { kind: 'completed' } }),
    ])
    await expectCode(
      item.commands.verifyModel({ delegationId: 'del-1', requestId: 'request-after-gap' }),
      'DELEGATION_MODEL_EVIDENCE_MISSING',
    )
  } finally {
    await cleanup(item)
  }
})


await test('a failed requested-range read is uncertain and never resends', async () => {
  const item = await fixture('prompt-range-failure')
  try {
    const created = await item.commands.create(createCommand(item))
    item.host.events.set(created.delegation.sessionId, [
      wireEvent(0, 'assistant/message', { content: [{ type: 'text', text: 'zero' }] }),
      wireEvent(1, 'assistant/message', { content: [{ type: 'text', text: 'one' }] }),
    ])
    await item.registry.beginPrompt('del-1', {
      requestId: 'request-range-failure',
      contentHash: contentHash('range failure'),
      beforeCursor: 0,
    })
    item.host.eventsFailure = remoteFailure('session/not-found', 'history unavailable')
    await expectCode(
      item.commands.prompt({
        delegationId: 'del-1',
        requestId: 'request-range-failure',
        content: 'range failure',
      }),
      'DELEGATION_REQUEST_ID_UNCERTAIN',
    )
    assert.equal(item.host.promptCalls, 0)
    assert.equal((await item.registry.promptReceipt('del-1', 'request-range-failure'))?.accepted, false)
  } finally {
    await cleanup(item)
  }
})


await test('ready create replay binds one new caller key without session or model mutation', async () => {
  const item = await fixture('create-key-binding')
  let restarted: DelegationRegistry | undefined
  try {
    const created = await item.commands.create(createCommand(item, { delegationId: 'del-1', title: 'Alpha' }))
    assert.equal(item.host.requestedSessionIds.length, 1)
    assert.equal(item.host.renameCalls, 1)
    assert.equal(item.host.selectModelCalls, 1)
    const original = await item.registry.get('del-1')
    assert.equal(original.cwd, item.work)
    assert.deepEqual(original.expectedModel, { provider: 'provider-a', model: 'model-a' })

    const bound = await item.commands.create(createCommand(item, {
      delegationId: 'del-1',
      title: 'Alpha',
      ipcRequestId: 'late-create-key',
    }))
    assert.equal(bound.replayed, true)
    assert.equal(bound.delegation.sessionId, created.delegation.sessionId)
    assert.equal((await item.registry.get('del-1')).createIpcId, 'late-create-key')
    assert.equal(item.host.requestedSessionIds.length, 1)
    assert.equal(item.host.renameCalls, 1)
    assert.equal(item.host.selectModelCalls, 1)

    await expectCode(
      item.commands.create(createCommand(item, {
        delegationId: 'del-1',
        title: 'Alpha',
        ipcRequestId: 'different-create-key',
      })),
      'DELEGATION_REPLAY_CONFLICT',
    )
    assert.equal((await item.registry.get('del-1')).createIpcId, 'late-create-key')
    assert.equal(item.host.requestedSessionIds.length, 1)

    await item.registry.close()
    restarted = new DelegationRegistry({ ledgerFile: item.ledgerFile, lockWaitMs: 0 })
    const commands = new SessionCommands({
      registry: restarted,
      access: accessFor(item.host, item.context),
      ledgerDir: join(item.root, 'ledger'),
    })
    const recovered = await commands.create({
      title: 'Alpha',
      provider: 'provider-a',
      model: 'model-a',
      cwd: item.work,
      ipcRequestId: 'late-create-key',
    })
    assert.equal(recovered.replayed, true)
    assert.equal(recovered.delegation.delegationId, 'del-1')
    assert.equal(recovered.delegation.sessionId, created.delegation.sessionId)
    assert.equal(recovered.delegation.cwd, item.work)
    assert.deepEqual(recovered.delegation.expectedModel, { provider: 'provider-a', model: 'model-a' })
    assert.equal(item.host.requestedSessionIds.length, 1)
    assert.equal(item.host.renameCalls, 1)
    assert.equal(item.host.selectModelCalls, 1)
  } finally {
    await restarted?.close()
    await cleanup(item)
  }
})
