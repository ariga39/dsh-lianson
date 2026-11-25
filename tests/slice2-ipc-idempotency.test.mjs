import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { startIpcServer } from '../src/ipc-server.mjs'
import { call } from '../src/ipc-client.mjs'
import { failure, success } from '../src/ipc-protocol.mjs'

test('slice2 ipc: failed calls retry and reads stay live under the same request id', { timeout: 60_000 }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-ipc-idempotency-'))
  const stateDir = join(root, 'state')
  const socketPath = join(stateDir, 'daemon.sock')
  const counts = new Map()
  const bump = command => {
    const count = (counts.get(command) ?? 0) + 1
    counts.set(command, count)
    return count
  }

  const server = await startIpcServer({
    stateDir,
    socketPath,
    dispatch: async ({ command, params, id }) => {
      const attempt = bump(command)
      if (command === 'delegation.create') {
        if (params?.scenario === 'fail-once' && attempt === 1) {
          return {
            envelope: failure(command, {
              code: 'delegation/create-failed',
              message: 'simulated first-create failure',
            }, id),
          }
        }
        return {
          envelope: success(command, {
            sessionId: 'session-same-id',
            state: 'ready',
            attempt,
          }, id),
        }
      }
      if (command === 'delegation.prompt') {
        if (params?.scenario === 'fail-once' && attempt === 1) {
          return {
            envelope: failure(command, {
              code: 'delegation/create-failed',
              message: 'simulated first-prompt failure',
            }, id),
          }
        }
        return {
          envelope: success(command, {
            accepted: true,
            completed: false,
            attempt,
          }, id),
        }
      }
      if (command === 'delegation.verifyModel' || command === 'host.status') {
        return {
          envelope: success(command, { liveRead: attempt }, id),
        }
      }
      return { envelope: failure(command, { code: 'UNKNOWN_COMMAND', message: command }, id) }
    },
  })
  t.after(async () => {
    await server.close().catch(() => {})
    await rm(root, { recursive: true, force: true })
  })

  await t.test('a failed create is not memoized and succeeds on the identical retry', async () => {
    const request = {
      command: 'delegation.create',
      params: { scenario: 'fail-once', cwd: '/tmp/example' },
      id: 'create-fail-then-succeed',
      timeoutMs: 5_000,
    }
    const first = await call(socketPath, request)
    assert.equal(first.ok, false)
    assert.equal(first.error.code, 'delegation/create-failed')
    const second = await call(socketPath, request)
    assert.equal(second.ok, true, JSON.stringify(second))
    assert.equal(second.data.sessionId, 'session-same-id')
    assert.equal(counts.get('delegation.create'), 2, 'the failed call must execute again')
  })

  await t.test('a failed prompt is not memoized and succeeds on the identical retry', async () => {
    const request = {
      command: 'delegation.prompt',
      params: { scenario: 'fail-once', delegationId: 'd', content: 'hello', requestId: 'r' },
      id: 'prompt-fail-then-succeed',
      timeoutMs: 5_000,
    }
    const first = await call(socketPath, request)
    assert.equal(first.ok, false)
    const second = await call(socketPath, request)
    assert.equal(second.ok, true, JSON.stringify(second))
    assert.equal(second.data.accepted, true)
    assert.equal(counts.get('delegation.prompt'), 2)
  })

  await t.test('successful create and prompt replies are replayed exactly once', async () => {
    const create = {
      command: 'delegation.create',
      params: { scenario: 'success', cwd: '/tmp/example' },
      id: 'create-success-replay',
      timeoutMs: 5_000,
    }
    const createFirst = await call(socketPath, create)
    const createSecond = await call(socketPath, create)
    assert.deepEqual(createSecond, createFirst)
    assert.equal(counts.get('delegation.create'), 3, 'only the failed retest executed twice')

    const prompt = {
      command: 'delegation.prompt',
      params: { scenario: 'success', delegationId: 'd', content: 'hello', requestId: 'r' },
      id: 'prompt-success-replay',
      timeoutMs: 5_000,
    }
    const promptFirst = await call(socketPath, prompt)
    const promptSecond = await call(socketPath, prompt)
    assert.deepEqual(promptSecond, promptFirst)
    assert.equal(counts.get('delegation.prompt'), 3, 'only the failed retest executed twice')
  })

  await t.test('read-only commands execute live under a reused request id', async () => {
    const verify = {
      command: 'delegation.verifyModel',
      params: { delegationId: 'd', requestId: 'r' },
      id: 'read-live-verify',
      timeoutMs: 5_000,
    }
    const verifyFirst = await call(socketPath, verify)
    const verifySecond = await call(socketPath, verify)
    assert.equal(verifyFirst.data.liveRead, 1)
    assert.equal(verifySecond.data.liveRead, 2, 'verifyModel must observe the current event log')
    assert.equal(counts.get('delegation.verifyModel'), 2)

    const status = {
      command: 'host.status',
      params: {},
      id: 'read-live-status',
      timeoutMs: 5_000,
    }
    const statusFirst = await call(socketPath, status)
    const statusSecond = await call(socketPath, status)
    assert.equal(statusFirst.data.liveRead, 1)
    assert.equal(statusSecond.data.liveRead, 2, 'host.status must not replay a stale response')
    assert.equal(counts.get('host.status'), 2)
  })
})
