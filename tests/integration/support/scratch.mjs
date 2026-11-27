/** Scratch roots, git worktrees, and the local pnpm shim used by `dsh plugin`. */

import { execFile } from 'node:child_process'
import { chmod, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { homedir } from 'node:os'
import { join } from 'node:path'

/**
 * Create a scratch root outside the shared temp area so workspace-write roots
 * remain isolated, plus a `pnpm` shim for `dsh plugin` (only corepack is on
 * this machine's PATH).
 * @returns {Promise<{root: string, bin: string, env: Record<string, string>, remove: () => Promise<void>}>}
 */
export async function createScratch() {
  const base = join(homedir(), '.cache')
  await mkdir(base, { recursive: true })
  const root = await mkdtemp(join(base, 'dsh-liaison-integration-'))
  const bin = join(root, 'bin')
  await mkdir(bin, { recursive: true })
  const pnpm = join(bin, 'pnpm')
  await writeFile(pnpm, '#!/bin/sh\nexec corepack pnpm "$@"\n')
  await chmod(pnpm, 0o755)
  return {
    root,
    bin,
    env: { PATH: `${bin}:${process.env.PATH ?? ''}` },
    remove: () => rm(root, { recursive: true, force: true, maxRetries: 3 }),
  }
}

/**
 * Create a base git repository and two sibling worktrees under the scratch
 * root. The roots are not inside a globally writable temp area.
 * @param {string} root
 * @returns {Promise<{repo: string, worktrees: [string, string]}>}
 */
export async function createWorktrees(root) {
  const repo = join(root, 'repo')
  const worktrees = [join(root, 'worktree-a'), join(root, 'worktree-b')]
  await mkdir(repo, { recursive: true })
  await git(repo, ['init', '--initial-branch=main'])
  await git(repo, ['config', 'user.email', 'integration@example.invalid'])
  await git(repo, ['config', 'user.name', 'dsh-liaison integration'])
  await writeFile(join(repo, 'README.md'), 'integration fixture\n')
  await git(repo, ['add', 'README.md'])
  await git(repo, ['commit', '-m', 'fixture'])
  await git(repo, ['worktree', 'add', '-b', 'worktree-a', worktrees[0]])
  await git(repo, ['worktree', 'add', '-b', 'worktree-b', worktrees[1]])
  return { repo, worktrees }
}

async function git(cwd, args) {
  await new Promise((resolve, reject) => {
    execFile('git', args, { cwd, env: { ...process.env, GIT_TERMINAL_PROMPT: '0' } }, (error, stdout, stderr) => {
      if (error === null) { resolve(stdout); return }
      reject(new Error(`git ${args.join(' ')} failed: ${stderr || error.message}`))
    })
  })
}

/** @returns whether this environment can provide a real Desktop check. */
export function desktopAvailable() {
  return process.env.DSH_DESKTOP_INTEGRATION === '1'
}
