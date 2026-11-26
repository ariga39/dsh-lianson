import { strict as assert } from 'node:assert'
import { existsSync } from 'node:fs'
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { fileURLToPath } from 'node:url'
import { createLaunchLineCollector } from '../src/launch-lines.ts'
import { startAnnouncedProcess } from '../src/host.ts'
import { missingRequirements, missingWebAssets, upstreamArtifactRequirements } from '../scripts/artifacts.ts'
import { captureCommand, runCommand } from '../scripts/commands.ts'
import { stageUpstream } from '../scripts/upstream.ts'

const projectRoot = fileURLToPath(new URL('..', import.meta.url))
const upstreamRoot = join(projectRoot, 'deepseek-harness')
const announceFixture = fileURLToPath(new URL('./fixtures/announce.ts', import.meta.url))

await test('announcement survives chunk and CRLF splits', async () => {
  const collector = createLaunchLineCollector()
  assert.deepEqual(collector.push('first\r'), [])
  assert.deepEqual(collector.push('\nsecond\r'), ['first'])
  assert.deepEqual(collector.finish(), ['second'])

  const running = await startAnnouncedProcess({
    command: process.execPath,
    args: [announceFixture, 'ok'],
    cwd: projectRoot,
    env: process.env,
    timeoutMs: 10_000,
    label: 'announce fixture',
  })
  try {
    assert.equal(running.launchUrl, 'http://127.0.0.1:9/?token=framing-token')
    assert.match(running.stdout(), /noise line\r?\n/u)
  } finally {
    await running.stop()
  }
})

await test('a closed announcement returns full stderr with the token redacted', async () => {
  await assert.rejects(
    startAnnouncedProcess({
      command: process.execPath,
      args: [announceFixture, 'fail'],
      cwd: projectRoot,
      env: process.env,
      timeoutMs: 10_000,
      label: 'announce fixture',
    }),
    (error: unknown) => {
      assert.ok(error instanceof Error)
      assert.match(error.message, /failure-start/u)
      assert.match(error.message, /failure-end/u)
      assert.match(error.message, /<redacted>/u)
      assert.ok(!error.message.includes('supersecret'))
      return true
    },
  )
})

await test('missing Web assets and a consumed upstream declaration are detected', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-artifact-check-'))
  try {
    const assets = join(root, 'dist', 'assets')
    await mkdir(assets, { recursive: true })
    await writeFile(join(assets, 'present.js'), '')
    const indexPath = join(root, 'dist', 'index.html')
    await writeFile(indexPath, [
      '<script type="module" src="./assets/present.js"></script>',
      '<link rel="stylesheet" href="./assets/missing.css">',
    ].join('\n'))
    const missingAssets = missingWebAssets(indexPath)
    assert.equal(missingAssets.length, 1)
    assert.match(missingAssets[0] ?? '', /^Web asset \.\/assets\/missing\.css:/u)
  } finally {
    await rm(root, { recursive: true, force: true })
  }

  const requirements = upstreamArtifactRequirements(upstreamRoot)
  const consumed = requirements.find(requirement =>
    requirement.label === '@deepseek-ai/dsh-api-session-controller/remote types')
  assert.ok(consumed, 'the consumed Remote declaration must be a tracked requirement')
  const missing = missingRequirements(requirements, path => path !== consumed.path && existsSync(path))
  assert.ok(missing.includes(`${consumed.label}: ${consumed.path}`))
})

await test('staging ignores a failing user hook', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-hook-isolation-'))
  const previousConfig = process.env.GIT_CONFIG_GLOBAL
  try {
    const source = join(root, 'source')
    await mkdir(source)
    runCommand('git', ['init', '--quiet'], source)
    runCommand('git', ['config', 'user.email', 'fixture@example.invalid'], source)
    runCommand('git', ['config', 'user.name', 'Fixture'], source)
    await writeFile(join(source, 'README.md'), 'fixture\n')
    runCommand('git', ['add', 'README.md'], source)
    runCommand('git', ['commit', '--quiet', '-m', 'fixture'], source)
    const commit = captureCommand('git', ['rev-parse', 'HEAD'], source).trim()
    const bare = join(root, 'upstream.git')
    runCommand('git', ['clone', '--quiet', '--bare', source, bare], root)

    const hooks = join(root, 'hostile-hooks')
    await mkdir(hooks)
    const marker = join(root, 'hook-ran')
    await writeFile(join(hooks, 'post-checkout'), `#!/bin/sh\necho ran > ${marker}\nexit 1\n`, { mode: 0o755 })
    const gitConfig = join(root, 'gitconfig')
    await writeFile(gitConfig, `[core]\n\thooksPath = ${hooks}\n`)
    process.env.GIT_CONFIG_GLOBAL = gitConfig

    const destination = join(root, 'staged')
    stageUpstream({
      url: `file://${bare}`,
      commit,
      destination,
      stagingParent: root,
    })
    assert.equal(captureCommand('git', ['-C', destination, 'rev-parse', 'HEAD'], root).trim(), commit)
    assert.equal(existsSync(marker), false, 'the failing user hook must not run')
  } finally {
    if (previousConfig === undefined) delete process.env.GIT_CONFIG_GLOBAL
    else process.env.GIT_CONFIG_GLOBAL = previousConfig
    await rm(root, { recursive: true, force: true })
  }
})
