import { after, before, describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { existsSync } from 'node:fs'
import { chmod, mkdir, mkdtemp, rm } from 'node:fs/promises'
import { createServer } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { sendCommand } from '../src/client.ts'
import { LiaisonError } from '../src/errors.ts'

describe('CLI socket preflight', () => {
  let root: string

  before(async () => {
    root = await mkdtemp(join(tmpdir(), 'dsh-liaison-client-'))
  })

  after(async () => {
    await rm(root, { recursive: true, force: true })
  })

  it('fails closed before delivering a prompt through an unsafe runtime directory', async () => {
    const runtime = join(root, 'shared')
    await mkdir(runtime)
    await chmod(runtime, 0o755)
    const socketPath = join(runtime, 'liaison.sock')
    let connections = 0
    let bytes = 0
    const owner = createServer((socket) => {
      connections += 1
      socket.on('data', (chunk: Buffer) => { bytes += chunk.byteLength })
    })
    await new Promise<void>((resolve, reject) => {
      owner.once('error', reject)
      owner.listen(socketPath, () => { resolve() })
    })
    try {
      await assert.rejects(
        sendCommand(socketPath, '/prompts', { delegationId: 'd1', text: 'must not ship' }),
        (error: unknown) => error instanceof LiaisonError && error.code === 'unsafe-runtime-dir',
      )
      assert.equal(connections, 0)
      assert.equal(bytes, 0)
    } finally {
      await new Promise<void>((resolve) => { owner.close(() => { resolve() }) })
    }
  })

  it('keeps a missing directory offline without creating it', async () => {
    const runtime = join(root, 'missing-runtime')
    await assert.rejects(
      sendCommand(join(runtime, 'liaison.sock'), '/status', {}),
      (error: unknown) => error instanceof LiaisonError && error.code === 'plugin-offline',
    )
    assert.equal(existsSync(runtime), false)
  })

  it('keeps a missing socket in a private directory offline', async () => {
    const runtime = join(root, 'private-runtime')
    await mkdir(runtime, { mode: 0o700 })
    await assert.rejects(
      sendCommand(join(runtime, 'liaison.sock'), '/status', {}),
      (error: unknown) => error instanceof LiaisonError && error.code === 'plugin-offline',
    )
  })
})
