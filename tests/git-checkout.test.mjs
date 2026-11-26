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

test('staging checkout isolates ambient Git hooks/signing and ignores a failing synthetic hook', { skip: process.platform === 'win32' }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh hook '))
  const previousGlobal = process.env.GIT_CONFIG_GLOBAL
  try {
    const repo = join(root, 'repo')
    const hostileHooks = join(root, 'hostile hooks')
    const hooks = join(root, 'synthetic hooks')
    const sentinel = join(root, 'hook-ran')
    const hostileConfig = join(root, 'hostile-gitconfig')
    const cleanConfig = join(root, 'clean-gitconfig')
    const hookConfig = join(root, 'hook-gitconfig')
    await mkdir(repo, { recursive: true })
    await mkdir(hostileHooks, { recursive: true })
    await mkdir(hooks, { recursive: true })
    await writeFile(hostileConfig, [
      '[core]',
      `\thooksPath = ${hostileHooks}`,
      '[commit]',
      '\tgpgsign = true',
      '[gpg]',
      '\tprogram = /bin/false',
      '',
    ].join('\n'))
    await writeFile(join(hostileHooks, 'pre-commit'), '#!/bin/sh\nexit 1\n')
    await chmod(join(hostileHooks, 'pre-commit'), 0o755)
    await writeFile(cleanConfig, '')
    process.env.GIT_CONFIG_GLOBAL = hostileConfig

    const setupEnv = {
      ...process.env,
      GIT_CONFIG_GLOBAL: cleanConfig,
      GIT_CONFIG_NOSYSTEM: '1',
      GIT_AUTHOR_NAME: 'Slice Zero',
      GIT_AUTHOR_EMAIL: 'slice-zero@example.invalid',
      GIT_COMMITTER_NAME: 'Slice Zero',
      GIT_COMMITTER_EMAIL: 'slice-zero@example.invalid',
      GIT_TERMINAL_PROMPT: '0',
    }
    git(root, ['init', repo], setupEnv)
    git(repo, ['config', 'user.name', 'Slice Zero'], setupEnv)
    git(repo, ['config', 'user.email', 'slice-zero@example.invalid'], setupEnv)
    await writeFile(join(repo, 'file.txt'), 'one\n')
    git(repo, ['add', 'file.txt'], setupEnv)
    git(repo, ['commit', '-m', 'one'], setupEnv)
    const first = git(repo, ['rev-parse', 'HEAD'], setupEnv)
    await writeFile(join(repo, 'file.txt'), 'two\n')
    git(repo, ['commit', '-am', 'two'], setupEnv)
    const second = git(repo, ['rev-parse', 'HEAD'], setupEnv)

    const hook = join(hooks, 'post-checkout')
    await writeFile(hook, '#!/bin/sh\nprintf ran > "$HOOK_SENTINEL"\nexit 1\n')
    await chmod(hook, 0o755)
    await writeFile(hookConfig, `[core]\n\thooksPath = ${hooks}\n`)
    const checkoutEnv = { ...setupEnv, GIT_CONFIG_GLOBAL: hookConfig, HOOK_SENTINEL: sentinel }

    const failing = spawnSync('git', ['-C', repo, 'checkout', '--detach', first], { encoding: 'utf8', env: checkoutEnv })
    assert.notEqual(failing.status, 0, 'the synthetic post-checkout hook must fail')
    assert.equal(spawnSync('cat', [sentinel], { encoding: 'utf8' }).stdout, 'ran')
    git(repo, ['reset', '--hard', second], setupEnv)
    await rm(sentinel)

    checkoutDetachedWithoutHooks(repo, first, (command, args) => {
      const result = spawnSync(command, args, { encoding: 'utf8', env: checkoutEnv })
      assert.equal(result.status, 0, `checkout failed: ${result.stderr}`)
    })
    assert.equal(git(repo, ['rev-parse', 'HEAD'], setupEnv), first)
    assert.equal(spawnSync('test', ['-e', sentinel]).status, 1, 'disabled hook must not run')
  } finally {
    if (previousGlobal === undefined) delete process.env.GIT_CONFIG_GLOBAL
    else process.env.GIT_CONFIG_GLOBAL = previousGlobal
    await rm(root, { recursive: true, force: true })
  }
})
