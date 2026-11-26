import { after, before, describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm, stat, writeFile } from 'node:fs/promises'
import { request } from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { LiaisonError } from '../src/errors.ts'
import { resolveLiaisonPaths } from '../src/paths.ts'
import { UdsServer } from '../src/uds-server.ts'

describe('path resolution', () => {
  it('uses XDG_RUNTIME_DIR and the DSH_HOME data category', () => {
    const root = '/tmp/liaison-paths-fixture'
    const paths = resolveLiaisonPaths({
      DSH_HOME: `${root}/home`,
      XDG_RUNTIME_DIR: `${root}/run`,
    })
    assert.equal(paths.dshHome, `${root}/home`)
    assert.equal(paths.runtimeDir, `${root}/run/dsh-liaison`)
    assert.equal(paths.socketPath, `${root}/run/dsh-liaison/dsh-liaison.sock`)
    assert.equal(paths.lockPath, `${root}/run/dsh-liaison/dsh-liaison.sock.lock`)
    assert.equal(paths.ledgerPath, `${root}/home/data/dsh-liaison/ledger.sqlite3`)
  })

  it('honors explicit runtime and ledger overrides', () => {
    const paths = resolveLiaisonPaths({
      DSH_HOME: '/tmp/liaison-paths-home',
      DSH_LIAISON_RUNTIME_DIR: '/tmp/liaison-paths-run',
      DSH_LIAISON_LEDGER: '/tmp/liaison-paths-state/ledger.sqlite3',
    })
    assert.equal(paths.socketPath, '/tmp/liaison-paths-run/dsh-liaison.sock')
    assert.equal(paths.ledgerPath, '/tmp/liaison-paths-state/ledger.sqlite3')
  })

  it('rejects a socket path beyond the kernel limit', () => {
    assert.throws(
      () => resolveLiaisonPaths({ DSH_LIAISON_RUNTIME_DIR: `/tmp/${'x'.repeat(120)}` }),
      (error: unknown) => error instanceof LiaisonError && error.code === 'usage',
    )
  })
})

describe('Unix-domain socket ownership', () => {
  let root: string

  before(async () => {
    root = await mkdtemp(join(tmpdir(), 'dsh-liaison-socket-'))
  })

  after(async () => {
    await rm(root, { recursive: true, force: true })
  })

  it('serves one owner with mode 0600 and rejects a second owner', async () => {
    const socketPath = join(root, 'liaison.sock')
    const lockPath = join(root, 'liaison.sock.lock')
    const first = new UdsServer(socketPath, lockPath, async (_request, response) => {
      response.writeHead(200, { 'content-type': 'application/json' })
      response.end(JSON.stringify({ ok: true, value: { healthy: true } }))
    })
    await first.start()
    try {
      assert.equal((await stat(socketPath)).mode & 0o777, 0o600)
      assert.deepEqual(await post(socketPath), { ok: true, value: { healthy: true } })

      const second = new UdsServer(socketPath, lockPath, async () => {})
      await assert.rejects(
        second.start(),
        (error: unknown) => error instanceof LiaisonError && error.code === 'socket-in-use',
      )
    } finally {
      await first.stop()
    }
    await assert.rejects(stat(socketPath), { code: 'ENOENT' })
    const third = new UdsServer(socketPath, lockPath, async (_request, response) => {
      response.end('{}')
    })
    await third.start()
    await third.stop()
  })

  it('refuses to replace a regular file at the socket path', async () => {
    const socketPath = join(root, 'occupied')
    const lockPath = join(root, 'occupied.lock')
    const server = new UdsServer(socketPath, lockPath, async () => {})
    await writeFile(socketPath, 'not a socket')
    await assert.rejects(
      server.start(),
      (error: unknown) => error instanceof LiaisonError && error.code === 'socket-in-use',
    )
  })
})

function post(socketPath: string): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({})
    const req = request({
      socketPath,
      path: '/status',
      method: 'POST',
      headers: { 'content-type': 'application/json', 'content-length': String(Buffer.byteLength(body)) },
    }, (response) => {
      let text = ''
      response.setEncoding('utf8')
      response.on('data', chunk => { text += chunk })
      response.on('end', () => {
        try {
          resolve(JSON.parse(text))
        } catch (error) {
          reject(error instanceof Error ? error : new Error(String(error)))
        }
      })
    })
    req.on('error', reject)
    req.end(body)
  })
}
