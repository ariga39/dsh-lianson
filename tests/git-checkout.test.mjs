import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { chmod, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { checkoutDetachedWithoutHooks } from '../scripts/git-checkout.mjs'

function git(cwd, args, env) {
  const result = spawnSync('git', ['-C', cwd, ...args], { encoding: 'utf8', env })
  assert.equal(result.status, 0, `git ${args.join(' ')} failed: ${result.stderr}`)
  return result.stdout.trim()
}

test('staging checkout ignores a failing global post-checkout hook', { skip: process.platform === 'win32' }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh hook '))
  try {
    const repo = join(root, 'repo')
    const hooks = join(root, 'global hooks')
    const sentinel = join(root, 'hook-ran')
    const globalConfig = join(root, 'gitconfig')
    await mkdir(repo, { recursive: true })
    await mkdir(hooks, { recursive: true })
    const baseEnv = { ...process.env }
    git(root, ['init', repo], baseEnv)
    git(repo, ['config', 'user.name', 'Slice Zero'], baseEnv)
    git(repo, ['config', 'user.email', 'slice-zero@example.invalid'], baseEnv)
    await writeFile(join(repo, 'file.txt'), 'one\n')
    git(repo, ['add', 'file.txt'], baseEnv)
    git(repo, ['commit', '-m', 'one'], baseEnv)
    const first = git(repo, ['rev-parse', 'HEAD'], baseEnv)
    await writeFile(join(repo, 'file.txt'), 'two\n')
    git(repo, ['commit', '-am', 'two'], baseEnv)
    const second = git(repo, ['rev-parse', 'HEAD'], baseEnv)

    const hook = join(hooks, 'post-checkout')
    await writeFile(hook, '#!/bin/sh\nprintf ran > "$HOOK_SENTINEL"\nexit 1\n')
    await chmod(hook, 0o755)
    await writeFile(globalConfig, `[core]\n\thooksPath = ${hooks}\n`)
    const env = { ...baseEnv, GIT_CONFIG_GLOBAL: globalConfig, HOOK_SENTINEL: sentinel }

    const failing = spawnSync('git', ['-C', repo, 'checkout', '--detach', first], { encoding: 'utf8', env })
    assert.notEqual(failing.status, 0, 'the synthetic global post-checkout hook must fail')
    assert.equal(spawnSync('cat', [sentinel], { encoding: 'utf8' }).stdout, 'ran')
    git(repo, ['reset', '--hard', second], env)
    await rm(sentinel)

    checkoutDetachedWithoutHooks(repo, first, (command, args) => {
      const result = spawnSync(command, args, { encoding: 'utf8', env })
      assert.equal(result.status, 0, `checkout failed: ${result.stderr}`)
    })
    assert.equal(git(repo, ['rev-parse', 'HEAD'], env), first)
    assert.equal(spawnSync('test', ['-e', sentinel]).status, 1, 'disabled hook must not run')
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})
