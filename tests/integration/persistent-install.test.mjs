/**
 * Persistent installation through the real `dsh plugin` command and the
 * home-level `$DSH_HOME/cordis.patch.yml` layer, then one real Host boot.
 */

import assert from 'node:assert/strict'
import { execFile } from 'node:child_process'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { test } from 'node:test'
import { startDshWeb } from './support/dsh-web.mjs'
import { runLiaison, waitForPlugin } from './support/liaison.mjs'
import { createScratch, desktopAvailable } from './support/scratch.mjs'
import { startScriptedModel } from './support/scripted-model.mjs'
import { REPO_ROOT } from './support/upstream.mjs'

const DSH = join(REPO_ROOT, 'deepseek-harness/apps/cli/lib/bin.js')

function runDsh(args, cwd, env, timeout) {
  return new Promise((resolve, reject) => {
    execFile(process.execPath, [DSH, ...args], { cwd, env, timeout, maxBuffer: 16 * 1024 * 1024 }, (error, stdout, stderr) => {
      if (error !== null) {
        reject(new Error(`dsh ${args.join(' ')} failed (${String(error.code)}):\n${stdout}\n${stderr}`))
        return
      }
      resolve({ stdout, stderr })
    })
  })
}

test('persistent plugin installation and home patch boot', { timeout: 300_000 }, async (t) => {
  const scratch = await createScratch()
  const model = await startScriptedModel()
  const root = scratch.root
  const dshHome = join(root, 'dsh-home')
  const runtimeDir = join(root, 'runtime')
  const cwd = join(root, 'workspace')
  const overlay = join(root, 'model.overlay.yml')
  const env = { ...process.env, DSH_HOME: dshHome, ...scratch.env }
  await mkdir(cwd, { recursive: true })
  await writeFile(overlay, [
    '- id: llm-deepseek',
    '  config:',
    `    baseURL: '${model.baseURL}'`,
    '- id: llm-deepseek-account',
    '  disabled: true',
    '- id: session-title-llm',
    '  disabled: true',
    '- id: agent-instructions',
    '  disabled: true',
    '- id: session-log-deepseek',
    '  disabled: true',
    '- id: session-telemetry-otel',
    '  disabled: true',
    '',
  ].join('\n'))

  let host
  t.after(async () => {
    await host?.close().catch(() => {})
    await model.close().catch(() => {})
    await scratch.remove().catch(() => {})
  })

  await t.test('dsh plugin installs the checkout as a profile dependency', async () => {
    const result = await runDsh([
      'plugin', '--profile', 'web', 'add', `link:${REPO_ROOT}`, '--offline',
    ], cwd, env, 180_000)
    assert.match(result.stderr, /initialized profile web/u)
    const manifest = JSON.parse(await readFile(join(dshHome, 'profiles/web/package.json'), 'utf8'))
    assert.equal(manifest.dependencies['dsh-liaison'], `link:${REPO_ROOT}`)
    assert.match(result.stderr, /declares no dsh\.bundle/u)
  })

  await t.test('the home patch boots the installed plugin in a real Host', async () => {
    await writeFile(join(dshHome, 'cordis.patch.yml'), [
      '- insert:',
      '    - id: dsh-liaison',
      '      name: dsh-liaison',
      '',
    ].join('\n'))
    model.setHandler(async () => ({ kind: 'text', text: 'INSTALLED_SENTINEL' }))
    host = await startDshWeb({ root, cwd, dshHome, runtimeDir, patches: [overlay], env: scratch.env })
    const status = await waitForPlugin({
      env: { DSH_HOME: dshHome, DSH_LIAISON_RUNTIME_DIR: runtimeDir, ...scratch.env },
    })
    assert.equal(status.plugin.name, 'dsh-liaison')
    assert.equal(status.dshHome, dshHome)
    await runLiaison({
      args: ['create', '--delegation-id', 'installed', '--cwd', cwd, '--title', 'installed plugin'],
      env: { DSH_HOME: dshHome, DSH_LIAISON_RUNTIME_DIR: runtimeDir, ...scratch.env },
    })
    await runLiaison({
      args: ['prompt', '--delegation-id', 'installed', '--request-id', 'installed-1', '--text', 'INSTALLED_PROMPT'],
      env: { DSH_HOME: dshHome, DSH_LIAISON_RUNTIME_DIR: runtimeDir, ...scratch.env },
    })
    const waited = (await runLiaison({
      args: ['wait', '--delegation-id', 'installed', '--timeout-ms', '30000'],
      env: { DSH_HOME: dshHome, DSH_LIAISON_RUNTIME_DIR: runtimeDir, ...scratch.env },
    })).value
    assert.equal(waited.status, 'completed')
    assert.ok(JSON.stringify(waited.output).includes('INSTALLED_SENTINEL'))
  })

  await t.test('Desktop is reported only when a carrier is observable', (subtest) => {
    if (!desktopAvailable()) {
      subtest.diagnostic('Desktop not exercised: no Desktop carrier or display is available in this environment.')
      return
    }
    subtest.diagnostic('Desktop carrier flag is set; shared-profile behavior still requires a manual Desktop launch.')
  })
})
