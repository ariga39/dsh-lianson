import assert from 'node:assert/strict'
import { readFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { test } from 'node:test'
import { startScriptedAgentServer } from './support/scripted-agent-server.mjs'
import { createKeylessEnvironment } from './support/keyless-environment.mjs'
import { createScratchRoot } from './support/scratch.mjs'
import { runCli, waitFor } from './support/cli-harness.mjs'

test('slice2 ownership race: the daemon that wins the socket owns the ledger writer', {
  timeout: 300_000,
}, async t => {
  const root = await createScratchRoot('slice2-ownership-race-')
  const mock = await startScriptedAgentServer()
  const fixture = await createKeylessEnvironment({ root, mockBaseURL: mock.baseURL })
  const stateDir = join(root, 'state')
  const env = { ...fixture.env, DSH_LIAISON_STATE_DIR: stateDir }
  let started

  t.after(async () => {
    await runCli(['host', 'stop'], { cwd: fixture.cwd, env, timeoutMs: 60_000 }).catch(() => {})
    await mock.close().catch(() => {})
    await rm(root, { recursive: true, force: true })
  })

  const args = [
    'host', 'start',
    '--workspace', fixture.cwd,
    '--dsh-home', fixture.dshHome,
    '--patch', fixture.overlayPath,
  ]
  const [left, right] = await Promise.all([
    runCli(args, { cwd: fixture.cwd, env }),
    runCli(args, { cwd: fixture.cwd, env }),
  ])
  started = left.envelope?.ok === true ? left : right
  assert.equal(left.code, 0, left.stderr)
  assert.equal(right.code, 0, right.stderr)
  assert.equal(left.envelope?.ok, true, left.stdout)
  assert.equal(right.envelope?.ok, true, right.stdout)
  const daemonPid = left.envelope.data.daemonPid
  assert.equal(right.envelope.data.daemonPid, daemonPid, 'one daemon wins the runtime socket')

  const lockPath = join(stateDir, 'delegations.json.lock')
  const lock = await waitFor(async () => {
    try {
      const parsed = JSON.parse(await readFile(lockPath, 'utf8'))
      return parsed.pid === daemonPid ? parsed : false
    } catch {
      return false
    }
  }, { timeoutMs: 15_000, label: 'winning daemon to own the ledger writer lock' })
  assert.equal(lock.pid, daemonPid, 'the socket winner must be the ledger writer')

  const created = await runCli([
    'create',
    '--delegation-id', 'race-owner',
    '--id', 'race-owner-create',
    '--cwd', fixture.cwd,
    '--title', 'race owner',
    '--provider', 'deepseek-official',
    '--model', 'deepseek-flash',
  ], { cwd: fixture.cwd, env })
  assert.equal(created.code, 0, created.stderr)
  assert.equal(created.envelope?.ok, true, created.stdout)
  assert.equal(created.envelope.data.delegationId, 'race-owner')
})
