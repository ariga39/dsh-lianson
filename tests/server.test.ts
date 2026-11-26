import { after, before, describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { mkdir, mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { sendCommand } from '../src/client.ts'
import { LiaisonError } from '../src/errors.ts'
import { Ledger } from '../src/ledger.ts'
import { LiaisonServer } from '../src/server.ts'
import type { DshRemote, RemoteEventPage, RemoteModelCatalog } from '../src/remote.ts'
import { DelegationService } from '../src/service.ts'
import { trustPolicyOf } from '../src/trust.ts'
import type { LiaisonPaths } from '../src/paths.ts'

const CATALOG: RemoteModelCatalog = {
  default: { provider: 'fixture', model: 'model-a' },
  groups: [{ id: 'fixture', name: 'Fixture', models: [{ id: 'model-a', name: 'Model A' }] }],
  failures: [],
}

describe('HTTP-over-UDS server', () => {
  let root: string
  let paths: LiaisonPaths
  let project: string
  let ledger: Ledger
  let server: LiaisonServer
  let remote: StubRemote

  before(async () => {
    root = await mkdtemp(join(tmpdir(), 'dsh-liaison-server-'))
    project = join(root, 'project')
    await mkdir(project, { recursive: true })
    paths = {
      dshHome: join(root, 'home'),
      runtimeDir: join(root, 'run'),
      socketPath: join(root, 'run', 'liaison.sock'),
      lockPath: join(root, 'run', 'liaison.sock.lock'),
      ledgerPath: join(root, 'data', 'ledger.sqlite3'),
    }
    ledger = Ledger.open(paths.ledgerPath)
    remote = new StubRemote()
    const service = new DelegationService({
      ledger,
      remote: {
        status: () => ({ state: 'connected', origin: remote.origin }),
        requireRemote: () => remote,
        dispose: async () => {},
      },
      trust: trustPolicyOf(paths, undefined),
      now: () => 100,
      newId: () => 'fixed',
    })
    server = new LiaisonServer({
      paths,
      ledger,
      service,
      remote: {
        status: () => ({ state: 'connected', origin: remote.origin }),
        requireRemote: () => remote,
        dispose: async () => {},
      },
      origin: () => remote.origin,
    })
    await server.start()
  })

  after(async () => {
    await server.stop()
    ledger.close()
    await rm(root, { recursive: true, force: true })
  })

  it('reports plugin health, DSH_HOME, origin, and loopback state', async () => {
    const value = await sendCommand(paths.socketPath, '/status', {}) as Record<string, unknown>
    assert.equal(value.ok, true)
    assert.deepEqual(value.plugin, { name: 'dsh-liaison', version: '0.0.0' })
    assert.equal(value.dshHome, paths.dshHome)
    assert.equal(value.origin, remote.origin)
    assert.deepEqual(value.loopback, { state: 'connected', origin: remote.origin })
    assert.equal(value.delegations, 0)
  })

  it('runs create, get, list, prompt, and verify-model through the socket', async () => {
    const created = await sendCommand(paths.socketPath, '/delegations/create', {
      delegationId: 'd1', cwd: project, title: 'Task one', model: 'fixture/model-a',
    }) as { delegation: { status: string; sessionId: string }; replayed: boolean }
    assert.equal(created.delegation.status, 'ready')
    assert.equal(created.delegation.sessionId, 'session-fixed')
    assert.equal(created.replayed, false)

    const fetched = await sendCommand(paths.socketPath, '/delegations/get', { delegationId: 'd1' }) as { delegation: { delegationId: string } }
    assert.equal(fetched.delegation.delegationId, 'd1')

    const listed = await sendCommand(paths.socketPath, '/delegations/list', {}) as { delegations: { delegationId: string }[] }
    assert.deepEqual(listed.delegations.map(item => item.delegationId), ['d1'])

    const prompted = await sendCommand(paths.socketPath, '/prompts', {
      delegationId: 'd1', requestId: 'request-1', text: 'hello',
    }) as { requestId: string; beforeCursor: number }
    assert.equal(prompted.requestId, 'request-1')
    assert.equal(prompted.beforeCursor, 5)
    assert.equal(remote.prompts.length, 1)

    remote.page = { cursor: 5, events: [
      { type: 'user/message', seq: 3, time: 3, data: { source: { rpcId: 'request-1' } } },
      { type: 'request/header', seq: 5, time: 5, data: { header: { config: { provider: 'fixture', model: 'model-a' } } } },
    ] }
    const verified = await sendCommand(paths.socketPath, '/verify-model', {
      delegationId: 'd1', requestId: 'request-1',
    }) as { verification: string }
    assert.equal(verified.verification, 'proven')
  })

  it('maps validation and unknown-route failures to structured documents', async () => {
    await assert.rejects(
      sendCommand(paths.socketPath, '/prompts', {}),
      (error: unknown) => error instanceof LiaisonError && error.code === 'usage',
    )
    await assert.rejects(
      sendCommand(paths.socketPath, '/delegations/get', { delegationId: 'missing' }),
      (error: unknown) => error instanceof LiaisonError && error.code === 'not-found',
    )
    await assert.rejects(
      sendCommand(paths.socketPath, '/nope' as never, {}),
      (error: unknown) => error instanceof LiaisonError && error.code === 'usage',
    )
  })

  it('reports plugin-offline after shutdown', async () => {
    await server.stop()
    await assert.rejects(
      sendCommand(paths.socketPath, '/status', {}),
      (error: unknown) => error instanceof LiaisonError && error.code === 'plugin-offline',
    )
  })
})

class StubRemote implements DshRemote {
  readonly origin = 'http://127.0.0.1:3080'
  readonly prompts: string[] = []
  page: RemoteEventPage = { cursor: 0, events: [] }

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
    return 5
  }
  async prompt(_sessionId: string, requestId: string): Promise<void> {
    this.prompts.push(requestId)
  }
  async readEvents(): Promise<RemoteEventPage> {
    return this.page
  }
  async dispose(): Promise<void> {}
}
