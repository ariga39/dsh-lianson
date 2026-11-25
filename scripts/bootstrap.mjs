#!/usr/bin/env node
/** Idempotent clean-checkout bootstrap for the pinned DeepSeek Harness upstream. */

import { spawnSync } from 'node:child_process'
import { existsSync, mkdtempSync, renameSync, rmSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { ensureUpstreamReady } from './upstream-readiness.mjs'

const ROOT = resolve(import.meta.dirname, '..')
const UPSTREAM = join(ROOT, 'deepseek-harness')
const REMOTE = 'https://github.com/deepseek-ai/deepseek-harness.git'
const COMMIT = '4b399c39a6d1479cd84ca591b4d7c88e24d38a96'
const NODE = '24.22.3'
const PNPM = '11.28.5'

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { cwd: ROOT, stdio: 'inherit', ...options })
  if (result.error !== undefined) throw result.error
  if (result.status !== 0) throw new Error(`${command} ${args.join(' ')} exited ${String(result.status)}`)
}

function output(command, args, options = {}) {
  const result = spawnSync(command, args, { cwd: ROOT, encoding: 'utf8', ...options })
  if (result.error !== undefined) throw result.error
  if (result.status !== 0) throw new Error(`${command} ${args.join(' ')} exited ${String(result.status)}: ${result.stderr}`)
  return result.stdout.trim()
}

function pnpm(args, options = {}) {
  run('corepack', ['pnpm', ...args], { cwd: UPSTREAM, ...options })
}

function stageUpstream() {
  const staging = mkdtempSync(join(ROOT, '.upstream-staging-'))
  try {
    run('git', ['init', staging])
    run('git', ['-C', staging, 'remote', 'add', 'origin', REMOTE])
    run('git', ['-C', staging, 'fetch', '--depth', '1', 'origin', COMMIT])
    run('git', ['-C', staging, 'checkout', '--detach', 'FETCH_HEAD'])
    renameSync(staging, UPSTREAM)
  } catch (error) {
    rmSync(staging, { recursive: true, force: true })
    throw error
  }
}

if (process.versions.node !== NODE) {
  throw new Error(`node ${NODE} is required; this process is ${process.versions.node}`)
}
const pnpmVersion = output('corepack', ['pnpm', '--version'], { cwd: ROOT })
if (pnpmVersion !== PNPM) throw new Error(`pnpm ${PNPM} is required; corepack resolved ${pnpmVersion}`)

if (existsSync(UPSTREAM) && !existsSync(join(UPSTREAM, '.git'))) {
  throw new Error(`${UPSTREAM} exists without .git; refusing to modify it. Move it aside so bootstrap can create the pinned checkout`)
}
if (existsSync(join(UPSTREAM, '.git'))) {
  const head = spawnSync('git', ['-C', UPSTREAM, 'rev-parse', '--verify', 'HEAD'], { encoding: 'utf8' })
  if (head.status !== 0) {
    throw new Error(`${UPSTREAM} has an unborn or invalid git HEAD; refusing to remove it. Move it aside so bootstrap can create the pinned checkout`)
  }
}
if (!existsSync(join(UPSTREAM, '.git'))) stageUpstream()

const head = output('git', ['-C', UPSTREAM, 'rev-parse', 'HEAD'])
if (head !== COMMIT) throw new Error(`upstream HEAD is ${head}, expected ${COMMIT}`)
const dirty = output('git', ['-C', UPSTREAM, 'status', '--porcelain'])
if (dirty !== '') throw new Error(`upstream checkout is dirty:\n${dirty}`)

pnpm(['install', '--frozen-lockfile'], { env: { ...process.env, CI: 'true' } })

await ensureUpstreamReady({
  root: UPSTREAM,
  build: async () => { pnpm(['run', 'build:artifacts']) },
})

const playwright = await import('node:module').then(({ createRequire }) =>
  createRequire(join(UPSTREAM, 'apps/web/package.json'))('playwright'))
if (!existsSync(playwright.chromium.executablePath())) {
  pnpm(['--filter', '@deepseek-ai/dsh-web-frontend', 'exec', 'playwright', 'install', 'chromium'])
}

console.log(`bootstrap: ready at ${COMMIT}`)
