import { strict as assert } from 'node:assert'
import { existsSync } from 'node:fs'
import { chmod, mkdir, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { Context } from '@deepseek-ai/cordis'
import { apply } from '../../src/plugin.ts'

async function withSocketPath(socketPath: string, run: () => Promise<void>): Promise<void> {
  const previous = process.env.DSH_LIAISON_SOCKET
  process.env.DSH_LIAISON_SOCKET = socketPath
  try {
    await run()
  } finally {
    if (previous === undefined) delete process.env.DSH_LIAISON_SOCKET
    else process.env.DSH_LIAISON_SOCKET = previous
  }
}

await test('a regular file at the socket path is preserved and rejected', async () => {
  const root = await mkdtemp(join(tmpdir(), 'liaison-socket-file-'))
  const socketPath = join(root, 'liaison.sock')
  const ctx = new Context()
  try {
    await writeFile(socketPath, 'operator data\n')
    await withSocketPath(socketPath, async () => {
      await assert.rejects(async () => {
        await ctx.plugin({ apply })
      }, /refusing to remove non-socket/u)
    })
    assert.equal(await readFile(socketPath, 'utf8'), 'operator data\n')
  } finally {
    await ctx.fiber.dispose().catch(() => undefined)
    await rm(root, { recursive: true, force: true })
  }
})

await test('the HOME fallback creates a 0700 directory and 0600 socket', async () => {
  const root = await mkdtemp(join(tmpdir(), 'liaison-socket-home-'))
  const previousHome = process.env.HOME
  const previousRuntime = process.env.XDG_RUNTIME_DIR
  const previousSocket = process.env.DSH_LIAISON_SOCKET
  const ctx = new Context()
  try {
    process.env.HOME = root
    delete process.env.XDG_RUNTIME_DIR
    delete process.env.DSH_LIAISON_SOCKET
    await ctx.plugin({ apply })
    const directory = join(root, '.local', 'state', 'dsh-liaison')
    const socketPath = join(directory, 'dsh-liaison.sock')
    assert.equal((await stat(directory)).mode & 0o777, 0o700)
    assert.equal((await stat(socketPath)).mode & 0o777, 0o600)
    await ctx.fiber.dispose()
    assert.equal(existsSync(socketPath), false)
  } finally {
    await ctx.fiber.dispose().catch(() => undefined)
    if (previousHome === undefined) delete process.env.HOME
    else process.env.HOME = previousHome
    if (previousRuntime === undefined) delete process.env.XDG_RUNTIME_DIR
    else process.env.XDG_RUNTIME_DIR = previousRuntime
    if (previousSocket === undefined) delete process.env.DSH_LIAISON_SOCKET
    else process.env.DSH_LIAISON_SOCKET = previousSocket
    await rm(root, { recursive: true, force: true })
  }
})

await test('a custom socket directory with group or world access is rejected without chmod', async () => {
  const root = await mkdtemp(join(tmpdir(), 'liaison-socket-dir-'))
  const directory = join(root, 'shared')
  const socketPath = join(directory, 'liaison.sock')
  const ctx = new Context()
  try {
    await mkdir(directory, { recursive: true })
    await chmod(directory, 0o755)
    await withSocketPath(socketPath, async () => {
      await assert.rejects(async () => {
        await ctx.plugin({ apply })
      }, /must be private \(0700\)/u)
    })
    assert.equal((await stat(directory)).mode & 0o777, 0o755)
  } finally {
    await ctx.fiber.dispose().catch(() => undefined)
    await rm(root, { recursive: true, force: true })
  }
})
