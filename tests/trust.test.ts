import { after, before, describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { mkdir, mkdtemp, rm, symlink } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { LiaisonError } from '../src/errors.ts'
import { trustPolicyOf, assertTrustedCwd } from '../src/trust.ts'

describe('trust boundaries', () => {
  let root: string
  let project: string
  let policy: ReturnType<typeof trustPolicyOf>

  before(async () => {
    root = await mkdtemp(join(tmpdir(), 'dsh-liaison-trust-'))
    project = join(root, 'project')
    await mkdir(project, { recursive: true })
    await mkdir(join(root, 'home', 'nested'), { recursive: true })
    await mkdir(join(root, 'run'), { recursive: true })
    await mkdir(join(root, 'data'), { recursive: true })
    policy = trustPolicyOf({
      dshHome: join(root, 'home'),
      runtimeDir: join(root, 'run'),
      socketPath: join(root, 'run', 's.sock'),
      lockPath: join(root, 'run', 's.sock.lock'),
      ledgerPath: join(root, 'data', 'ledger.sqlite3'),
    }, undefined)
  })

  after(async () => {
    await rm(root, { recursive: true, force: true })
  })

  it('accepts an ordinary directory without a sandbox policy', () => {
    assert.equal(assertTrustedCwd(project, policy), project)
  })

  it('rejects DSH_HOME, the runtime directory, and the ledger directory', () => {
    for (const cwd of [join(root, 'home'), join(root, 'home', 'nested'), join(root, 'run'), join(root, 'data')]) {
      assert.throws(
        () => assertTrustedCwd(cwd, policy),
        (error: unknown) => error instanceof LiaisonError && error.code === 'unsafe-cwd',
        cwd,
      )
    }
  })

  it('rejects paths that escape a protected root through a symlink', async () => {
    const link = join(root, 'shortcut')
    await symlink(join(root, 'home'), link)
    assert.throws(
      () => assertTrustedCwd(link, policy),
      (error: unknown) => error instanceof LiaisonError && error.code === 'unsafe-cwd',
    )
  })

  it('rejects relative and missing directories', () => {
    for (const cwd of ['relative/path', join(root, 'missing')]) {
      assert.throws(
        () => assertTrustedCwd(cwd, policy),
        (error: unknown) => error instanceof LiaisonError && error.code === 'invalid-cwd',
        cwd,
      )
    }
  })

  it('enforces configured writable roots when a sandbox policy exists', async () => {
    const paths = {
      dshHome: join(root, 'home'),
      runtimeDir: join(root, 'run'),
      socketPath: join(root, 'run', 's.sock'),
      lockPath: join(root, 'run', 's.sock.lock'),
      ledgerPath: join(root, 'data', 'ledger.sqlite3'),
    }
    const restricted = trustPolicyOf(paths, { mode: 'workspace-write', workspaceRoot: project })
    assert.equal(assertTrustedCwd(project, restricted), project)
    const readOnly = trustPolicyOf(paths, { mode: 'read-only', workspaceRoot: project })
    assert.throws(
      () => assertTrustedCwd(project, readOnly),
      (error: unknown) => error instanceof LiaisonError && error.code === 'unsafe-cwd',
    )
  })
})
