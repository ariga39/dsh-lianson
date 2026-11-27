#!/usr/bin/env node
/**
 * Prepare the pinned DeepSeek Harness checkout and this project's links.
 *
 * The upstream checkout is never modified in place: only ignored `lib/`
 * build outputs are produced when they are missing. The tracked commit is
 * asserted before and after every step.
 */

import { execFileSync } from 'node:child_process'
import { existsSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

const PINNED_UPSTREAM = '4b399c39a6d1479cd84ca591b4d7c88e24d38a96'
const projectRoot = fileURLToPath(new URL('..', import.meta.url))
const upstreamRoot = join(projectRoot, 'deepseek-harness')

const REQUIRED = [
  ['cordis', 'vendor/cordis/lib/index.js'],
  ['api gateway client', 'packages/api/gateway/lib/client.node.js'],
  ['api remotes client', 'packages/api/remotes/lib/client.js'],
  ['session controller', 'packages/api/session-controller/lib/typert.remote-client.js'],
  ['workspace controller', 'packages/api/workspace-controller/lib/typert.remote-client.js'],
  ['home paths', 'packages/util/home-paths/lib/index.js'],
  ['host webserver', 'packages/host/webserver/lib/index.js'],
  ['llm', 'packages/llm/llm/lib/index.js'],
  ['sandbox', 'packages/sandbox/sandbox/lib/index.js'],
  ['sandbox policy', 'packages/sandbox/sandbox-policy/lib/index.js'],
  ['session', 'packages/core/session/lib/index.js'],
  ['session title', 'packages/session/session-title/lib/index.js'],
  ['typert protocol', 'packages/typert/protocol/lib/index.js'],
  ['typert registry', 'packages/typert/registry/lib/index.js'],
  ['user approval', 'packages/interaction/user-approval/lib/index.js'],
  ['user questions', 'packages/interaction/user-questions/lib/index.js'],
  ['user questions remote', 'packages/interaction/user-questions/lib/typert.remote-client.js'],
  ['flock entry', 'native/system/packages/entry/lib/flock.js'],
]

function main() {
  const major = Number(process.versions.node.split('.')[0])
  if (major < 24) throw new Error(`dsh-liaison requires Node 24 or newer; current is ${process.version}`)
  if (!existsSync(join(upstreamRoot, 'package.json'))) {
    throw new Error(`${upstreamRoot} is missing; check out the pinned DeepSeek Harness commit there first`)
  }
  assertUpstream()
  const missing = missingArtifacts()
  if (missing.length > 0) {
    console.log(`[bootstrap] building missing upstream artifacts:\n${missing.join('\n')}`)
    run('corepack', ['pnpm', 'run', 'build:artifacts'], upstreamRoot)
    const remaining = missingArtifacts()
    if (remaining.length > 0) throw new Error(`upstream build did not produce:\n${remaining.join('\n')}`)
  }
  assertUpstream()
  run('corepack', ['pnpm', 'install', '--frozen-lockfile'], projectRoot)
  assertUpstream()
  console.log(`[bootstrap] ready: Node ${process.version}, upstream ${PINNED_UPSTREAM.slice(0, 12)}`)
}

function assertUpstream() {
  const head = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: upstreamRoot, encoding: 'utf8' }).trim()
  if (head !== PINNED_UPSTREAM) {
    throw new Error(`deepseek-harness is at ${head}; expected pinned ${PINNED_UPSTREAM}`)
  }
}

function missingArtifacts() {
  return REQUIRED.filter(([, path]) => !existsSync(join(upstreamRoot, path))).map(([label, path]) => `${label}: ${path}`)
}

function run(command, args, cwd) {
  execFileSync(command, args, { cwd, stdio: 'inherit', env: { ...process.env, CI: 'true' } })
}

main()
