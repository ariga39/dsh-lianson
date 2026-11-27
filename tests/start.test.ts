import { after, before, describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { stat } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { sendCommand } from '../src/client.ts'
import { LiaisonError } from '../src/errors.ts'
import { startLiaison } from '../src/start.ts'
import type { LiaisonRuntime } from '../src/start.ts'

describe('liaison composition', () => {
  let root: string
  let env: NodeJS.ProcessEnv

  before(async () => {
    root = await mkdtemp(join(tmpdir(), 'dsh-liaison-start-'))
    env = {
      DSH_HOME: join(root, 'home'),
      DSH_LIAISON_RUNTIME_DIR: join(root, 'run'),
      DSH_LIAISON_LEDGER: join(root, 'data', 'ledger.sqlite3'),
    }
  })

  after(async () => {
    await rm(root, { recursive: true, force: true })
  })

  it('binds the socket, reports unavailable loopback, and disposes cleanly', async () => {
    const warnings: Error[] = []
    const runtime = await startLiaison({
      env,
      origin: () => undefined,
      collectEntries: () => [],
      sandboxPolicy: () => undefined,
      logger: { warn: error => warnings.push(error) },
      retry: { retryDelaysMs: [1], maxAttempts: 1, sleep: async () => {} },
    })
    try {
      assert.equal((await stat(runtime.paths.socketPath)).mode & 0o777, 0o600)
      const status = await sendCommand(runtime.paths.socketPath, '/status', {}) as {
        loopback: { state: string }
        delegations: number
      }
      assert.equal(status.loopback.state, 'unavailable')
      assert.equal(status.delegations, 0)
      assert.equal(warnings.length, 1)

      const second = await startLiaison({
        env,
        origin: () => undefined,
        collectEntries: () => [],
        sandboxPolicy: () => undefined,
        logger: { warn: () => {} },
        retry: { retryDelaysMs: [1], maxAttempts: 1, sleep: async () => {} },
      }).catch((error: unknown) => error)
      assert.ok(second instanceof LiaisonError)
      assert.equal(second.code, 'socket-in-use')
    } finally {
      await runtime.dispose()
    }
    await assert.rejects(stat(runtime.paths.socketPath), { code: 'ENOENT' })
    await assert.rejects(
      sendCommand(runtime.paths.socketPath, '/status', {}),
      (error: unknown) => error instanceof LiaisonError && error.code === 'plugin-offline',
    )
  })

  it('dispose is idempotent', async () => {
    const runtime: LiaisonRuntime = await startLiaison({
      env,
      origin: () => undefined,
      collectEntries: () => [],
      sandboxPolicy: () => undefined,
      logger: { warn: () => {} },
      retry: { retryDelaysMs: [1], maxAttempts: 1, sleep: async () => {} },
    })
    await runtime.dispose()
    await runtime.dispose()
  })
})
