import { existsSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { PINNED_NODE, PINNED_PNPM, PINNED_UPSTREAM_COMMIT, PINNED_UPSTREAM_URL } from './pinned.ts'
import { missingUpstreamArtifacts } from './artifacts.ts'
import { captureCommand, runCommand } from './commands.ts'
import { assertUpstreamIdentity, stageUpstream } from './upstream.ts'

const projectRoot = fileURLToPath(new URL('..', import.meta.url))
const upstreamRoot = join(projectRoot, 'deepseek-harness')

/** Prepare the pinned upstream checkout, frozen dependencies, artifacts, and browser. */
function main(): void {
  assertToolchain()
  ensureUpstream()
  assertUpstreamIdentity(upstreamRoot, projectRoot, PINNED_UPSTREAM_COMMIT)
  runCommand('corepack', ['pnpm', 'install', '--frozen-lockfile'], upstreamRoot, { CI: 'true' })
  ensureUpstreamBuild()
  runCommand('corepack', ['pnpm', 'install', '--frozen-lockfile'], projectRoot)
  runCommand('corepack', ['pnpm', 'exec', 'playwright', 'install', 'chromium'], projectRoot)
  assertUpstreamIdentity(upstreamRoot, projectRoot, PINNED_UPSTREAM_COMMIT)
  console.log(`[bootstrap] ready: Node ${PINNED_NODE}, pnpm ${PINNED_PNPM}, upstream ${PINNED_UPSTREAM_COMMIT.slice(0, 12)}`)
}

function assertToolchain(): void {
  if (process.version !== `v${PINNED_NODE}`) {
    throw new Error(`This project pins Node ${PINNED_NODE}; current is ${process.version}`)
  }
  const pnpm = captureCommand('corepack', ['pnpm', '--version'], projectRoot).trim()
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
  stageUpstream({
    url: PINNED_UPSTREAM_URL,
    commit: PINNED_UPSTREAM_COMMIT,
    destination: upstreamRoot,
    stagingParent: projectRoot,
  })
}

function ensureUpstreamBuild(): void {
  const missing = missingUpstreamArtifacts(upstreamRoot)
  if (missing.length === 0) return
  console.log(`[bootstrap] upstream artifacts missing:\n${missing.join('\n')}`)
  // The scoped artifact graph skips aggregate type checks; it still emits the
  // CLI/Host/Client libraries, their declarations, and the Web dist.
  runCommand('corepack', ['pnpm', 'run', 'build:artifacts'], upstreamRoot)
  const remaining = missingUpstreamArtifacts(upstreamRoot)
  if (remaining.length > 0) throw new Error(`upstream build did not produce:\n${remaining.join('\n')}`)
}

main()
