import { strict as assert } from 'node:assert'
import { createHash, randomUUID } from 'node:crypto'
import { mkdir, mkdtemp, rm } from 'node:fs/promises'
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
  type VerifiedHostContext,
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
  promptThrowsAfterCommit: boolean
  promptCalls: number
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
    promptThrowsAfterCommit: false,
    promptCalls: 0,
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
        return { ok: true, value: { title: request.title, seq: 0 } }
      },
      async selectModel(request) {
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
        const address = request.address
        if (address.kind !== 'session') return { ok: false, error: remoteFailure('session/not-found', 'not a session') }
        const all = eventsOf(String(address.sessionId))
        const through = request.throughSeq ?? all.at(-1)?.seq ?? 0
        const before = request.beforeSeq
        const selected = all.filter(event => event.seq <= through && (before === undefined || event.seq < before))
        return {
          ok: true,
          value: {
            records: selected.map(eventEntry),
            cursor: through,
            hasMore: false,
          },
        }
      },
    },
  }

  return Object.assign(state, { remote })
}

function accessFor(host: FakeHost, context: VerifiedHostContext): HostOperatorAccess {
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
}

async function fixture(label: string, configure?: (host: FakeHost) => void): Promise<Fixture> {
  const root = await scratchRoot(label)
  const work = join(root, 'work')
  await mkdir(work, { recursive: true })
  const host = fakeHost()
  configure?.(host)
  const ledgerFile = join(root, 'ledger', 'delegations.json')
  const registry = new DelegationRegistry({ ledgerFile, lockWaitMs: 0 })
  const context: VerifiedHostContext = {
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
  return { root, work, ledgerFile, registry, commands, host }
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
