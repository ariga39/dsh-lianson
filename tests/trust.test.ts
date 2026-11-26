import { after, before, describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { mkdir, mkdtemp, rm, symlink } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { LiaisonError } from '../src/errors.ts'
import { trustPolicyOf, assertTrustedCwd } from '../src/trust.ts'
import { makeTestRoot, removeTestRoot } from './support.ts'

describe('trust boundaries', () => {
  let root: string
  let project: string
  let siblingWorktree: string
  let policy: ReturnType<typeof trustPolicyOf>

  before(async () => {
    // Ordinary fixture root outside the platform temp areas upstream treats as shared.
    root = await makeTestRoot('trust-')
    project = join(root, 'repo')
    siblingWorktree = join(root, 'repo-topic-worktree')
    await mkdir(project, { recursive: true })
    await mkdir(siblingWorktree, { recursive: true })
    await mkdir(join(root, 'home', 'nested'), { recursive: true })
    await mkdir(join(root, 'run'), { recursive: true })
    await mkdir(join(root, 'data'), { recursive: true })
    policy = trustPolicyOf({
      dshHome: join(root, 'home'),
      runtimeDir: join(root, 'run'),
      socketPath: join(root, 'run', 's.sock'),
      lockPath: join(root, 'run', 's.sock.lock'),
      ledgerPath: join(root, 'data', 'ledger.sqlite3'),
    })
  })

  after(async () => {
    await removeTestRoot(root)
  })

  it('allows an ordinary directory and a sibling worktree outside the Host cwd', () => {
    assert.equal(assertTrustedCwd(project, policy), project)
    assert.equal(assertTrustedCwd(siblingWorktree, policy), siblingWorktree)
  })

  it('keeps allowing ordinary worktrees when the deployment policy names another root', () => {
    const deployment = trustPolicyOf({
      dshHome: join(root, 'home'),
      runtimeDir: join(root, 'run'),
      socketPath: join(root, 'run', 's.sock'),
      lockPath: join(root, 'run', 's.sock.lock'),
      ledgerPath: join(root, 'data', 'ledger.sqlite3'),
    }, { mode: 'workspace-write', workspaceRoot: join(root, 'host-cwd') })
    assert.equal(assertTrustedCwd(siblingWorktree, deployment), siblingWorktree)
  })

  it('rejects protected state inside and above the protected paths', () => {
    for (const cwd of [
      join(root, 'home'),
      join(root, 'home', 'nested'),
      join(root, 'run'),
      join(root, 'data'),
      root,
      '/',
    ]) {
      assert.throws(
        () => assertTrustedCwd(cwd, policy),
        (error: unknown) => error instanceof LiaisonError && error.code === 'unsafe-cwd',
        cwd,
      )
    }
  })



  it('treats a literal ..project component as a protected descendant', async () => {
    const dotted = join(root, 'home', '..project')
    await mkdir(dotted, { recursive: true })
    assert.throws(
      () => assertTrustedCwd(dotted, policy),
      (error: unknown) => error instanceof LiaisonError && error.code === 'unsafe-cwd',
    )
  })

  it('rejects a worktree inside a root shared by every deployment Session', async () => {
    const shared = join(root, 'shared-grant')
    await mkdir(join(shared, 'worktree'), { recursive: true })
    const deployment = trustPolicyOf({
      dshHome: join(root, 'home'),
      runtimeDir: join(root, 'run'),
      socketPath: join(root, 'run', 's.sock'),
      lockPath: join(root, 'run', 's.sock.lock'),
      ledgerPath: join(root, 'data', 'ledger.sqlite3'),
    }, {
      mode: 'workspace-write',
      workspaceRoot: join(root, 'host-cwd'),
      additionalWritableRoots: [shared],
    })
    assert.throws(
      () => assertTrustedCwd(join(shared, 'worktree'), deployment),
      (error: unknown) => error instanceof LiaisonError && error.code === 'unsafe-cwd',
    )
    assert.equal(assertTrustedCwd(project, deployment), project)
  })

  it('rejects a worktree under the globally shared temp area', async () => {
    const worktree = await mkdtemp(join(tmpdir(), 'dsh-liaison-worktree-'))
    try {
      assert.throws(
        () => assertTrustedCwd(worktree, policy),
        (error: unknown) => error instanceof LiaisonError && error.code === 'unsafe-cwd',
      )
    } finally {
      await rm(worktree, { recursive: true, force: true })
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

  it('rejects the filesystem root', () => {
    assert.throws(
      () => assertTrustedCwd('/', policy),
      (error: unknown) => error instanceof LiaisonError && error.code === 'unsafe-cwd',
    )
  })
})
