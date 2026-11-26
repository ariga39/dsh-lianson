import { strict as assert } from 'node:assert'
import { existsSync } from 'node:fs'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { test } from 'node:test'
import { setTimeout as delay } from 'node:timers/promises'
import { Context } from '@deepseek-ai/cordis'
import { apply, internals } from '../../src/plugin.ts'

await test('a disposed plugin never attaches a loopback operator from late startup signals', async () => {
  const root = await mkdtemp(join(tmpdir(), 'liaison-lifecycle-'))
  const socketPath = join(root, 'liaison.sock')
  const previousSocket = process.env.DSH_LIAISON_SOCKET
  const previousConnect = internals.connectLoopback
  let calls = 0
  internals.connectLoopback = async () => {
    calls += 1
    throw new Error('late startup must not attach a loopback operator')
  }
  process.env.DSH_LIAISON_SOCKET = socketPath
  const ctx = new Context()
  let readyListener: (() => void) | undefined
  ctx.provide('appReady', {
    onReady(listener: () => void): () => void {
      readyListener = listener
      return () => {
        if (readyListener === listener) readyListener = undefined
      }
    },
  })
  try {
    await ctx.plugin({ apply })
    ctx.emit('webserver/entry-url', 'http://127.0.0.1:1/', [{
      label: 'dsh web',
      url: 'http://127.0.0.1:1/?token=lifecycle-token',
      open: true,
    }])
    await delay(0)
    const lateListener = readyListener
    assert.ok(lateListener, 'the plugin must register an appReady listener')
    await ctx.fiber.dispose()
    assert.equal(existsSync(socketPath), false)
    lateListener?.()
    await delay(50)
    assert.equal(calls, 0)
  } finally {
    await ctx.fiber.dispose().catch(() => undefined)
    internals.connectLoopback = previousConnect
    if (previousSocket === undefined) delete process.env.DSH_LIAISON_SOCKET
    else process.env.DSH_LIAISON_SOCKET = previousSocket
    await rm(root, { recursive: true, force: true })
  }
})
