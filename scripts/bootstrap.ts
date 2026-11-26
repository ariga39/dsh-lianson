import { existsSync, mkdtempSync, renameSync, rmSync } from 'node:fs'
import { join } from 'node:path'
import { spawnSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { PINNED_NODE, PINNED_PNPM, PINNED_UPSTREAM_COMMIT, PINNED_UPSTREAM_URL } from '../src/constants.ts'
import { missingUpstreamArtifacts } from './artifacts.ts'

const projectRoot = fileURLToPath(new URL('..', import.meta.url))
const upstreamRoot = join(projectRoot, 'deepseek-harness')

/** Prepare the pinned upstream checkout, frozen dependencies, artifacts, and browser. */
function main(): void {
  assertToolchain()
  ensureUpstream()
  assertUpstreamIdentity()
  run('corepack', ['pnpm', 'install', '--frozen-lockfile'], upstreamRoot, { CI: 'true' })
  ensureUpstreamBuild()
  run('corepack', ['pnpm', 'install', '--frozen-lockfile'], projectRoot)
  run('corepack', ['pnpm', 'exec', 'playwright', 'install', 'chromium'], projectRoot)
  assertUpstreamIdentity()
  console.log(`[bootstrap] ready: Node ${PINNED_NODE}, pnpm ${PINNED_PNPM}, upstream ${PINNED_UPSTREAM_COMMIT.slice(0, 12)}`)
}

function assertToolchain(): void {
  if (process.version !== `v${PINNED_NODE}`) {
    throw new Error(`This project pins Node ${PINNED_NODE}; current is ${process.version}`)
  }
  const pnpm = capture('corepack', ['pnpm', '--version'], projectRoot).trim()
  if (pnpm !== PINNED_PNPM) {
    throw new Error(`This project pins pnpm ${PINNED_PNPM}; corepack resolved ${pnpm}`)
  }
}

function ensureUpstream(): void {
  if (existsSync(join(upstreamRoot, 'package.json'))) return
  if (existsSync(upstreamRoot)) {
    throw new Error(`${upstreamRoot} exists but is not a DeepSeek Harness checkout; move it aside`)
  }
  console.log('[bootstrap] staging a shallow fetch of the pinned upstream commit')
  const staging = mkdtempSync(join(projectRoot, '.deepseek-harness.staging-'))
  try {
    run('git', ['init', '--quiet'], staging)
    run('git', ['remote', 'add', 'origin', PINNED_UPSTREAM_URL], staging)
    run('git', [
      '-c', 'core.hooksPath=/dev/null',
      'fetch', '--depth', '1', '--no-tags', 'origin', PINNED_UPSTREAM_COMMIT,
    ], staging)
    run('git', ['-c', 'core.hooksPath=/dev/null', 'checkout', '--detach', 'FETCH_HEAD'], staging)
    if (existsSync(upstreamRoot)) throw new Error(`${upstreamRoot} appeared during staging; refusing to replace it`)
    renameSync(staging, upstreamRoot)
  } finally {
    // A failed partial staging must not poison the next attempt.
    rmSync(staging, { recursive: true, force: true })
  }
}

function assertUpstreamIdentity(): void {
  const head = capture('git', ['-C', upstreamRoot, 'rev-parse', 'HEAD'], projectRoot).trim()
  if (head !== PINNED_UPSTREAM_COMMIT) {
    throw new Error(`upstream deepseek-harness is at ${head}, expected ${PINNED_UPSTREAM_COMMIT}`)
  }
  const status = capture('git', ['-C', upstreamRoot, 'status', '--porcelain', '--untracked-files=normal'], projectRoot).trim()
  if (status !== '') {
    throw new Error(`upstream deepseek-harness is not clean; refusing to modify it:\n${status}`)
  }
}

function ensureUpstreamBuild(): void {
  const missing = missingUpstreamArtifacts(upstreamRoot)
  if (missing.length === 0) return
  console.log(`[bootstrap] upstream artifacts missing:\n${missing.join('\n')}`)
  // The scoped artifact graph skips aggregate type checks; it still emits the
  // CLI/Host/Client libraries, their declarations, and the Web dist.
  run('corepack', ['pnpm', 'run', 'build:artifacts'], upstreamRoot)
  const remaining = missingUpstreamArtifacts(upstreamRoot)
  if (remaining.length > 0) throw new Error(`upstream build did not produce:\n${remaining.join('\n')}`)
}

function run(
  command: string,
  args: readonly string[],
  cwd: string,
  env: Readonly<Record<string, string>> = {},
): void {
  console.log(`[bootstrap] ${command} ${args.join(' ')}`)
  const result = spawnSync(command, [...args], {
    cwd,
    env: {
      ...process.env,
      ...env,
      GIT_CONFIG_NOSYSTEM: '1',
    },
    stdio: 'inherit',
  })
  if (result.error !== undefined) throw result.error
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(' ')} exited with ${String(result.status)}`)
  }
}

function capture(command: string, args: readonly string[], cwd: string): string {
  const result = spawnSync(command, [...args], {
    cwd,
    env: {
      ...process.env,
      GIT_CONFIG_NOSYSTEM: '1',
    },
    encoding: 'utf8',
  })
  if (result.error !== undefined) throw result.error
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(' ')} exited with ${String(result.status)}: ${result.stderr}`)
  }
  return result.stdout
}

main()
