import { after, before, beforeEach, describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, mkdir, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { LiaisonError } from '../src/errors.ts'
import { Ledger } from '../src/ledger.ts'
import type { DshRemote, RemoteEventPage, RemoteModelCatalog } from '../src/remote.ts'
import { DelegationService } from '../src/service.ts'
import { trustPolicyOf } from '../src/trust.ts'

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
    root = await mkdtemp(join(tmpdir(), 'dsh-liaison-service-'))
  })

  after(async () => {
    await rm(root, { recursive: true, force: true })
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
    assert.equal(remote.calls.length, calls)
    await assert.rejects(
      service.create({ ...input, title: 'Different title' }),
      (error: unknown) => error instanceof LiaisonError && error.code === 'conflict',
    )
    await assert.rejects(
      service.create({ ...input, cwd: root }),
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

  it('proves, contradicts, and honestly reports an unpersisted model', async () => {
    await mkdir(project, { recursive: true })
    await service.create({ delegationId: 'd1', cwd: project, title: 'Task one', model: 'fixture/model-a' })

    remote.page = { cursor: 5, events: [
      event('user/message', 3, { source: { rpcId: 'request-1' } }),
      event('request/header', 5, { header: { config: { provider: 'fixture', model: 'model-a' } } }),
    ] }
    const proven = await service.verifyModel({ delegationId: 'd1', requestId: 'request-1' })
    assert.equal(proven.verification, 'proven')
    assert.deepEqual(proven.observed, { provider: 'fixture', model: 'model-a' })
    assert.deepEqual(proven.evidence, { userSeq: 3, headerSeq: 5 })
    assert.equal(ledger.findDelegation('d1')?.modelVerification, 'proven')

    remote.page = { cursor: 6, events: [
      event('user/message', 4, { source: { rpcId: 'request-2' } }),
      event('request/header', 6, { header: { config: { provider: 'fixture', model: 'model-b' } } }),
    ] }
    const mismatch = await service.verifyModel({ delegationId: 'd1', requestId: 'request-2' })
    assert.equal(mismatch.verification, 'mismatch')
    assert.deepEqual(mismatch.observed, { provider: 'fixture', model: 'model-b' })

    remote.page = { cursor: 4, events: [event('user/message', 4, { source: { rpcId: 'request-2' } })] }
    const unpersisted = await service.verifyModel({ delegationId: 'd1', requestId: 'request-2' })
    assert.equal(unpersisted.verification, 'indeterminate')
    assert.equal(unpersisted.observed, null)

    const unknown = await service.verifyModel({ delegationId: 'd1', requestId: 'missing' })
    assert.equal(unknown.verification, 'indeterminate')
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
    this.onBeforeCreateWorkspace?.()
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

function event(type: string, seq: number, data: unknown) {
  return { type, seq, time: seq, data } as never
}
