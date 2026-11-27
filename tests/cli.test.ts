import { after, before, describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { existsSync } from 'node:fs'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { sendCommand } from '../src/client.ts'
import { runCli } from '../src/cli.ts'
import type { CliDependencies } from '../src/cli.ts'
import { LiaisonError } from '../src/errors.ts'
import { Ledger } from '../src/ledger.ts'
import { ROUTES } from '../src/protocol.ts'
import type { RoutePath } from '../src/protocol.ts'

interface Captured {
  stdout: string[]
  stderr: string[]
  exitCode: number
  sends: { route: string; body: unknown }[]
}

function harness(env: NodeJS.ProcessEnv, responder: (route: string, body: unknown) => Promise<unknown>): { deps: CliDependencies; captured: Captured } {
  const captured: Captured = { stdout: [], stderr: [], exitCode: 0, sends: [] }
  return {
    captured,
    deps: {
      env,
      send: async (route, body) => {
        captured.sends.push({ route, body })
        return responder(route, body)
      },
      stdout: text => { captured.stdout.push(text) },
      stderr: text => { captured.stderr.push(text) },
      setExitCode: code => { captured.exitCode = code },
    },
  }
}

describe('CLI JSON and offline behavior', () => {
  let root: string
  let env: NodeJS.ProcessEnv
  let ledgerPath: string

  before(async () => {
    root = await mkdtemp(join(tmpdir(), 'dsh-liaison-cli-'))
    ledgerPath = join(root, 'ledger.sqlite3')
    env = {
      DSH_HOME: join(root, 'home'),
      DSH_LIAISON_LEDGER: ledgerPath,
      DSH_LIAISON_RUNTIME_DIR: join(root, 'run'),
    }
    const ledger = Ledger.open(ledgerPath)
    try {
      ledger.insertDelegation({
        delegationId: 'd1', sessionId: 'session-1', cwd: '/tmp/project', title: 'Task one',
        model: 'fixture/model-a', modelVerification: 'indeterminate', status: 'ready', createdAt: 1, updatedAt: 2,
      })
    } finally {
      ledger.close()
    }
  })

  after(async () => {
    await rm(root, { recursive: true, force: true })
  })

  it('prints one redacted JSON document', async () => {
    const { deps, captured } = harness(env, async () => ({
      ok: true,
      token: 'launch-secret',
      nested: { cookie: 'dsh=secret', authorization: 'Bearer secret' },
      message: 'token=inline-secret&x=1',
    }))
    await runCli(['status'], deps)
    assert.equal(captured.exitCode, 0)
    assert.equal(captured.stderr.length, 0)
    assert.equal(captured.stdout.length, 1)
    assert.deepEqual(JSON.parse(captured.stdout[0] ?? ''), {
      ok: true,
      token: '[redacted]',
      nested: { cookie: '[redacted]', authorization: '[redacted]' },
      message: 'token=[redacted]&x=1',
    })
  })

  it('reports redacted structured errors with exit code 1', async () => {
    const { deps, captured } = harness(env, async () => {
      throw new LiaisonError('plugin-offline', 'cannot read token=inline-secret')
    })
    await runCli(['status'], deps)
    assert.equal(captured.stdout.length, 0)
    assert.equal(captured.exitCode, 1)
    const document = JSON.parse(captured.stderr[0] ?? '') as {
      ok: boolean
      error: { code: string; message: string }
    }
    assert.equal(document.ok, false)
    assert.equal(document.error.code, 'plugin-offline')
    assert.equal(document.error.message, 'cannot read token=[redacted]')
  })

  it('falls back to the offline ledger for get and list', async () => {
    const offline = harness(env, async () => { throw new LiaisonError('plugin-offline', 'not running') })
    await runCli(['get', '--delegation-id', 'd1'], offline.deps)
    assert.equal(offline.captured.exitCode, 0)
    const fetched = JSON.parse(offline.captured.stdout[0] ?? '') as { delegation: { sessionId: string }; offline: boolean }
    assert.equal(fetched.delegation.sessionId, 'session-1')
    assert.equal(fetched.offline, true)

    const list = harness(env, async () => { throw new LiaisonError('plugin-offline', 'not running') })
    await runCli(['list'], list.deps)
    const listed = JSON.parse(list.captured.stdout[0] ?? '') as { delegations: { delegationId: string }[]; offline: boolean }
    assert.deepEqual(listed.delegations.map(item => item.delegationId), ['d1'])
    assert.equal(listed.offline, true)
  })



  it('falls back when the runtime directory is missing', async () => {
    const runtime = join(root, 'absent-runtime')
    const socketPath = join(runtime, 'dsh-liaison.sock')
    const offline = harness(env, (route, body) => sendCommand(socketPath, route as RoutePath, body))
    await runCli(['list'], offline.deps)
    assert.equal(offline.captured.exitCode, 0)
    assert.equal(existsSync(runtime), false)
    const listed = JSON.parse(offline.captured.stdout[0] ?? '') as { delegations: { delegationId: string }[]; offline: boolean }
    assert.deepEqual(listed.delegations.map(item => item.delegationId), ['d1'])
    assert.equal(listed.offline, true)
  })

  it('suppresses commander usage text on parse errors', async () => {
    const { deps, captured } = harness(env, async () => ({ accepted: true }))
    await runCli(['prompt'], deps)
    assert.equal(captured.exitCode, 1)
    assert.equal(captured.stdout.length, 0)
    assert.equal(captured.stderr.length, 1)
    assert.equal(captured.stderr[0]?.includes('Usage:'), false)
    const document = JSON.parse(captured.stderr[0] ?? '') as { error: { code: string } }
    assert.equal(document.error.code, 'usage')
  })

  it('preserves --help and --version output', async () => {
    const help = harness(env, async () => { throw new Error('help must not send') })
    await runCli(['--help'], help.deps)
    assert.equal(help.captured.exitCode, 0)
    assert.match(help.captured.stdout.join(''), /Usage: dsh-liaison/)
    assert.equal(help.captured.stderr.length, 0)

    const version = harness(env, async () => { throw new Error('version must not send') })
    await runCli(['--version'], version.deps)
    assert.equal(version.captured.exitCode, 0)
    assert.equal(version.captured.stdout.join('').trim(), '0.0.0')
    assert.equal(version.captured.stderr.length, 0)
  })
  it('reports a missing offline delegation instead of printing undefined', async () => {
    const { deps, captured } = harness(env, async () => { throw new LiaisonError('plugin-offline', 'not running') })
    await runCli(['get', '--delegation-id', 'missing'], deps)
    assert.equal(captured.exitCode, 1)
    const document = JSON.parse(captured.stderr[0] ?? '') as { error: { code: string } }
    assert.equal(document.error.code, 'not-found')
  })

  it('maps CLI flags to the wire request', async () => {
    const { deps, captured } = harness(env, async () => ({ delegation: {}, replayed: false }))
    await runCli([
      'create', '--delegation-id', 'd2', '--cwd', '/tmp/project', '--title', 'Task two', '--model', 'fixture/model-a',
    ], deps)
    assert.deepEqual(captured.sends, [{
      route: ROUTES.create,
      body: { delegationId: 'd2', cwd: '/tmp/project', title: 'Task two', model: 'fixture/model-a' },
    }])
  })

  it('reads prompt text from a file', async () => {
    const file = join(root, 'prompt.txt')
    await writeFile(file, 'from a file')
    const { deps, captured } = harness(env, async () => ({ accepted: true }))
    await runCli(['prompt', '--delegation-id', 'd1', '--file', file], deps)
    assert.deepEqual(captured.sends[0]?.body, { delegationId: 'd1', text: 'from a file' })
  })

  it('rejects conflicting prompt inputs', async () => {
    const { deps, captured } = harness(env, async () => ({ accepted: true }))
    await runCli(['prompt', '--delegation-id', 'd1', '--text', 'x', '--file', '/tmp/nope'], deps)
    assert.equal(captured.exitCode, 1)
    assert.equal(captured.sends.length, 0)
  })
})
