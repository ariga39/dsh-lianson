import { strict as assert } from 'node:assert'
import { mkdir, mkdtemp, realpath, rm, stat, symlink, writeFile } from 'node:fs/promises'
import { homedir, tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import {
  DelegationError,
  guardWorkspace,
  type DelegationErrorCode,
  type WorkspaceGuardInput,
} from '../src/workspace-guard.ts'

const TEST_PARENT = join(homedir(), '.cache', 'dsh-liaison-tests')
await mkdir(TEST_PARENT, { recursive: true, mode: 0o700 })

async function scratchRoot(label: string): Promise<string> {
  return await mkdtemp(join(TEST_PARENT, `slice2-safety-${label}-`))
}

async function expectCode(promise: Promise<unknown>, code: DelegationErrorCode): Promise<DelegationError> {
  try {
    await promise
  } catch (error) {
    assert.ok(error instanceof DelegationError, `expected DelegationError, got ${String(error)}`)
    assert.equal(error.code, code)
    return error
  }
  assert.fail(`expected ${code}`)
}

function guardInput(root: string, overrides: Partial<WorkspaceGuardInput> = {}): WorkspaceGuardInput {
  return {
    cwd: join(root, 'work'),
    stateDir: join(root, 'state'),
    dshHome: join(root, 'dsh-home'),
    ledgerDir: join(root, 'ledger'),
    ...overrides,
  }
}

await test('temporary worktrees and trusted state roots exposed through temp roots are refused', async () => {
  const root = await scratchRoot('temp')
  const tempWorktree = await mkdtemp(join(tmpdir(), 'dsh-liaison-temp-worktree-'))
  const tempState = await mkdtemp(join(tmpdir(), 'dsh-liaison-temp-state-'))
  try {
    await mkdir(join(root, 'work'), { recursive: true })

    await expectCode(guardWorkspace(guardInput(root, { cwd: tempWorktree })), 'WORKSPACE_UNISOLATABLE')
    await expectCode(guardWorkspace(guardInput(root, { stateDir: tempState })), 'STATE_UNSAFE_TEMP_ROOT')
    await expectCode(guardWorkspace(guardInput(root, { ledgerDir: tempState })), 'STATE_UNSAFE_TEMP_ROOT')
    await expectCode(guardWorkspace(guardInput(root, { dshHome: tempState })), 'STATE_UNSAFE_TEMP_ROOT')
  } finally {
    await rm(root, { recursive: true, force: true })
    await rm(tempWorktree, { recursive: true, force: true })
    await rm(tempState, { recursive: true, force: true })
  }
})

await test('canonical directories require an existing directory and resolve symlinks before isolation checks', async () => {
  const root = await scratchRoot('canonical')
  const tempWorktree = await mkdtemp(join(tmpdir(), 'dsh-liaison-symlink-temp-'))
  try {
    const work = join(root, 'work')
    await mkdir(work, { recursive: true })
    const file = join(root, 'not-a-directory')
    await writeFile(file, 'x\n')
    await expectCode(guardWorkspace(guardInput(root, { cwd: join(root, 'missing') })), 'WORKSPACE_INVALID_CWD')
    await expectCode(guardWorkspace(guardInput(root, { cwd: file })), 'WORKSPACE_INVALID_CWD')

    const link = join(root, 'work-link')
    await symlink(work, link)
    assert.equal(await guardWorkspace(guardInput(root, { cwd: link })), await realpath(work))

    const tempLink = join(root, 'temp-link')
    await symlink(tempWorktree, tempLink)
    await expectCode(guardWorkspace(guardInput(root, { cwd: tempLink })), 'WORKSPACE_UNISOLATABLE')
    assert.equal((await stat(work)).isDirectory(), true)
  } finally {
    await rm(root, { recursive: true, force: true })
    await rm(tempWorktree, { recursive: true, force: true })
  }
})

await test('home and trusted-state overlaps are refused while ordinary sibling worktrees stay usable', async () => {
  const root = await scratchRoot('home')
  try {
    const work = join(root, 'work')
    const worktreeA = join(root, 'worktree-a')
    const worktreeB = join(root, 'worktree-b')
    const stateDir = join(root, 'state')
    const ledgerDir = join(root, 'ledger')
    await Promise.all([
      mkdir(work, { recursive: true }),
      mkdir(worktreeA, { recursive: true }),
      mkdir(worktreeB, { recursive: true }),
      mkdir(join(stateDir, 'nested'), { recursive: true }),
      mkdir(join(ledgerDir, 'nested'), { recursive: true }),
      mkdir(join(root, 'dsh-home', 'nested'), { recursive: true }),
    ])

    await expectCode(guardWorkspace(guardInput(root, { cwd: homedir() })), 'WORKSPACE_STATE_OVERLAP')
    await expectCode(
      guardWorkspace(guardInput(root, { cwd: join(stateDir, 'nested') })),
      'WORKSPACE_STATE_OVERLAP',
    )
    await expectCode(
      guardWorkspace(guardInput(root, { cwd: join(ledgerDir, 'nested') })),
      'WORKSPACE_STATE_OVERLAP',
    )
    await expectCode(
      guardWorkspace(guardInput(root, { cwd: join(root, 'dsh-home', 'nested') })),
      'WORKSPACE_STATE_OVERLAP',
    )

    assert.equal(await guardWorkspace(guardInput(root, { cwd: work })), work)
    assert.equal(await guardWorkspace(guardInput(root, { cwd: worktreeA })), worktreeA)
    assert.equal(await guardWorkspace(guardInput(root, { cwd: worktreeB })), worktreeB)
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

await test('trusted paths canonicalize a missing suffix through the nearest existing real ancestor', async () => {
  const root = await scratchRoot('missing-suffix')
  try {
    const physical = join(root, 'physical')
    const missing = join(physical, 'missing')
    const link = join(root, 'physical-link')
    await mkdir(missing, { recursive: true })
    await symlink(physical, link)

    await expectCode(
      guardWorkspace(guardInput(root, {
        cwd: missing,
        stateDir: join(link, 'missing', 'delegations'),
      })),
      'WORKSPACE_STATE_OVERLAP',
    )
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

await test('a trusted missing suffix under a symlinked temporary root stays unisolatable', async () => {
  const root = await scratchRoot('missing-temp')
  const temporary = await mkdtemp(join(tmpdir(), 'dsh-liaison-missing-temp-'))
  try {
    await mkdir(join(root, 'work'), { recursive: true })
    const link = join(root, 'temp-link')
    await symlink(temporary, link)

    await expectCode(
      guardWorkspace(guardInput(root, {
        stateDir: join(link, 'missing', 'state'),
      })),
      'STATE_UNSAFE_TEMP_ROOT',
    )
  } finally {
    await rm(root, { recursive: true, force: true })
    await rm(temporary, { recursive: true, force: true })
  }
})

await test('non-absent path errors fail closed instead of accepting a lexical spelling', async () => {
  const root = await scratchRoot('fail-closed')
  try {
    await mkdir(join(root, 'work'), { recursive: true })
    const file = join(root, 'file')
    await writeFile(file, 'not a directory\n')
    const loopA = join(root, 'loop-a')
    const loopB = join(root, 'loop-b')
    await symlink(loopB, loopA)
    await symlink(loopA, loopB)

    await expectCode(
      guardWorkspace(guardInput(root, { stateDir: join(file, 'child') })),
      'WORKSPACE_STATE_OVERLAP',
    )
    await expectCode(
      guardWorkspace(guardInput(root, { ledgerDir: join(loopA, 'child') })),
      'WORKSPACE_STATE_OVERLAP',
    )
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})
