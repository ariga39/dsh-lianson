import assert from 'node:assert/strict'
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { test } from 'node:test'
import { UPSTREAM_ENTRIES } from '../src/upstream-inventory.mjs'
import { pnpmInvocation } from '../scripts/pnpm-invocation.mjs'
import {
  ensureUpstreamReady, missingArtifacts, requiresNativeSystem, verifyUpstreamImports,
} from '../scripts/upstream-readiness.mjs'

function splitSpecifier(specifier) {
  const parts = specifier.split('/')
  if (specifier.startsWith('@')) {
    return { name: parts.slice(0, 2).join('/'), subpath: parts.slice(2).join('/') || undefined }
  }
  return { name: parts[0], subpath: parts.slice(1).join('/') || undefined }
}

async function createFakeUpstream() {
  const root = await mkdtemp(join(tmpdir(), 'dsh-readiness-'))
  const files = new Map()
  for (const entry of UPSTREAM_ENTRIES) {
    const { name, subpath } = splitSpecifier(entry.specifier)
    const packageDir = join(root, 'node_modules', name)
    const target = subpath === undefined ? 'index.js' : `lib/${subpath.replaceAll('/', '-')}.js`
    await mkdir(join(packageDir, dirname(target)), { recursive: true })
    await writeFile(join(packageDir, target), 'export const fake = true\n')
    await writeFile(join(packageDir, 'package.json'), JSON.stringify({
      name,
      type: 'module',
      exports: subpath === undefined ? { '.': `./${target}` } : { [`./${subpath}`]: `./${target}` },
    }))
    const anchor = join(root, entry.anchor)
    await mkdir(dirname(anchor), { recursive: true })
    await writeFile(anchor, '{"name":"fake-anchor","type":"module"}\n')
    files.set(entry.key, join(packageDir, target))
  }
  await mkdir(join(root, 'apps/cli/lib'), { recursive: true })
  await writeFile(join(root, 'apps/cli/lib/bin.js'), '#!/usr/bin/env node\n')
  await mkdir(join(root, 'apps/web/dist'), { recursive: true })
  await writeFile(join(root, 'apps/web/dist/index.html'), '<!doctype html>\n')
  await mkdir(join(root, 'native/system/packages', `${process.platform}-${process.arch}/bin`), { recursive: true })
  await writeFile(join(root, 'native/system/packages', `${process.platform}-${process.arch}/bin/system.node`), '')
  return { root, files }
}

test('bootstrap readiness detects a missing entry and rebuilds once', async () => {
  const fixture = await createFakeUpstream()
  try {
    assert.deepEqual(missingArtifacts(fixture.root), [])
    assert.equal(verifyUpstreamImports(fixture.root).ok, true)

    const missing = fixture.files.get('session')
    await rm(missing)
    assert.ok(missingArtifacts(fixture.root).some(item => item.includes('session')))
    assert.equal(verifyUpstreamImports(fixture.root).ok, false)

    let builds = 0
    await ensureUpstreamReady({
      root: fixture.root,
      log: () => {},
      build: async () => {
        builds += 1
        await writeFile(missing, 'export const repaired = true\n')
      },
    })
    assert.equal(builds, 1)
    assert.equal(verifyUpstreamImports(fixture.root).ok, true)
  } finally {
    await rm(fixture.root, { recursive: true, force: true })
  }
})

test('bootstrap readiness rebuilds partial output and rechecks in a fresh process', async () => {
  const fixture = await createFakeUpstream()
  try {
    const broken = fixture.files.get('gateway')
    await writeFile(broken, 'export const = broken\n')
    assert.deepEqual(missingArtifacts(fixture.root), [], 'present but unimportable output is not a static miss')
    assert.equal(verifyUpstreamImports(fixture.root).ok, false)

    let builds = 0
    await ensureUpstreamReady({
      root: fixture.root,
      log: () => {},
      build: async () => {
        builds += 1
        await writeFile(broken, 'export const repaired = true\n')
      },
    })
    assert.equal(builds, 1)
    assert.equal(verifyUpstreamImports(fixture.root).ok, true)
  } finally {
    await rm(fixture.root, { recursive: true, force: true })
  }
})

test('bootstrap readiness rejects missing Web output after its single rebuild', async () => {
  const fixture = await createFakeUpstream()
  try {
    await rm(join(fixture.root, 'apps/web/dist/index.html'))
    let builds = 0
    await assert.rejects(
      ensureUpstreamReady({
        root: fixture.root,
        log: () => {},
        build: async () => { builds += 1 },
      }),
      /apps\/web\/dist\/index\.html/u,
    )
    assert.equal(builds, 1, 'readiness must not double-build')
  } finally {
    await rm(fixture.root, { recursive: true, force: true })
  }
})

test('pnpm invocation is shell-free and preserves spaced JavaScript paths', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh pnpm invocation-'))
  try {
    const jsEntry = join(root, 'tool path', 'pnpm.cjs')
    assert.deepEqual(
      pnpmInvocation(['--version'], {
        env: { npm_execpath: jsEntry },
        platform: 'linux',
        execPath: '/usr/bin/node',
      }),
      { command: '/usr/bin/node', args: [jsEntry, '--version'] },
    )

    const packageBin = join(root, 'pnpm', 'bin')
    await mkdir(packageBin, { recursive: true })
    const fallback = join(packageBin, 'pnpm.cjs')
    await writeFile(fallback, '')
    assert.deepEqual(
      pnpmInvocation(['install'], {
        env: {
          npm_execpath: 'C:\\tools\\pnpm.cmd',
          PNPM_HOME: join(root, 'home with space'),
        },
        platform: 'win32',
        execPath: 'C:\\node.exe',
      }),
      { command: 'C:\\node.exe', args: [fallback, 'install'] },
    )

    assert.throws(
      () => pnpmInvocation([], { env: { npm_execpath: 'C:\\tools\\pnpm.cmd' }, platform: 'win32' }),
      /refusing to spawn a \.cmd shim/u,
    )
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

test('npm lifecycle entrypoints fall back to pinned corepack or PNPM_HOME', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh npm invocation-'))
  try {
    assert.deepEqual(
      pnpmInvocation(['run', 'smoke:slice0'], {
        env: { npm_execpath: '/usr/lib/node_modules/npm/bin/npm-cli.js' },
        platform: 'linux',
      }),
      { command: 'corepack', args: ['pnpm', 'run', 'smoke:slice0'] },
    )
    assert.deepEqual(
      pnpmInvocation(['run', 'smoke:slice0'], {
        env: { npm_execpath: 'C:\\Program Files\\npm\\node_modules\\npm\\bin\\npm-cli.js' },
        platform: 'linux',
      }),
      { command: 'corepack', args: ['pnpm', 'run', 'smoke:slice0'] },
    )

    const packageBin = join(root, 'pnpm', 'bin')
    await mkdir(packageBin, { recursive: true })
    const fallback = join(packageBin, 'pnpm.mjs')
    await writeFile(fallback, '')
    assert.deepEqual(
      pnpmInvocation(['run', 'smoke:slice0'], {
        env: {
          npm_execpath: 'C:\\Program Files\\npm\\node_modules\\npm\\bin\\npm-cli.js',
          PNPM_HOME: join(root, 'home with space'),
        },
        platform: 'win32',
        execPath: 'C:\\node.exe',
      }),
      { command: 'C:\\node.exe', args: [fallback, 'run', 'smoke:slice0'] },
    )
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

test('native system.node is required only on documented upstream platforms', async () => {
  assert.equal(requiresNativeSystem('linux', 'x64'), true)
  assert.equal(requiresNativeSystem('linux', 'arm64'), true)
  assert.equal(requiresNativeSystem('darwin', 'x64'), true)
  assert.equal(requiresNativeSystem('darwin', 'arm64'), true)
  assert.equal(requiresNativeSystem('win32', 'x64'), false)
  assert.equal(requiresNativeSystem('freebsd', 'x64'), false)

  const fixture = await createFakeUpstream()
  try {
    await rm(join(fixture.root, 'native'), { recursive: true, force: true })
    assert.deepEqual(
      missingArtifacts(fixture.root, { platform: 'win32', arch: 'x64' })
        .filter(item => item.includes('system.node')),
      [],
      'Windows uses the Harness locking path, not system.node',
    )
    assert.ok(
      missingArtifacts(fixture.root, { platform: 'linux', arch: 'x64' })
        .some(item => item.includes('system.node')),
      'documented native platforms still require system.node',
    )
  } finally {
    await rm(fixture.root, { recursive: true, force: true })
  }
})
