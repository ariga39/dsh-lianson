import { strict as assert } from 'node:assert'
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { homedir } from 'node:os'
import { join, resolve } from 'node:path'
import { test } from 'node:test'
import { fileURLToPath } from 'node:url'
import { LiaisonError } from '../src/errors.ts'
import {
  HostManager,
  resolveHostDshHome,
  resolveRecordedDshHome,
} from '../src/host-manager.ts'

const projectRoot = fileURLToPath(new URL('..', import.meta.url))
const TEST_PARENT = join(homedir(), '.cache', 'dsh-liaison-tests')
await mkdir(TEST_PARENT, { recursive: true, mode: 0o700 })

await test('child DSH_HOME resolution follows relative cwd and pinned tilde rules', () => {
  assert.equal(
    resolveHostDshHome({
      HOME: '/home/child',
      DSH_HOME: 'relative-dsh',
    }, '/launch/work'),
    resolve('/launch/work/relative-dsh'),
  )
  assert.equal(
    resolveHostDshHome({
      HOME: '/home/child',
      DSH_HOME: '~/tilde-dsh',
    }, '/launch/work'),
    resolve('/home/child/tilde-dsh'),
  )
  assert.equal(
    resolveHostDshHome({
      HOME: '/home/child',
    }, '/launch/work'),
    resolve('/home/child/.dsh'),
  )
  assert.equal(
    resolveHostDshHome({
      HOME: '/home/child',
      DSH_HOME: '/absolute/dsh',
    }, '/launch/work'),
    resolve('/absolute/dsh'),
  )

  assert.equal(resolveRecordedDshHome('legacy-relative', '/launch/work'), resolve('/launch/work/legacy-relative'))
  assert.equal(resolveRecordedDshHome('/absolute/legacy', '/launch/work'), resolve('/absolute/legacy'))
  assert.throws(
    () => resolveRecordedDshHome('~/legacy', '/launch/work'),
    (error: unknown) => error instanceof LiaisonError && error.code === 'HOST_RECORD_INVALID',
  )
  assert.throws(
    () => resolveRecordedDshHome('legacy-relative', 'relative-cwd'),
    (error: unknown) => error instanceof LiaisonError && error.code === 'HOST_RECORD_INVALID',
  )
})

await test('relative and tilde DSH_HOME reattach from a different cwd and HOME and keep stop operable', { timeout: 300_000 }, async () => {
  const root = await mkdtemp(join(TEST_PARENT, 'slice2-host-home-'))
  let first: HostManager | undefined
  let second: HostManager | undefined
  try {
    for (const [index, scenario] of [
      { label: 'relative', dshHome: 'relative-dsh' },
      { label: 'tilde', dshHome: '~/tilde-dsh' },
    ].entries()) {
      const workspaceA = join(root, `${scenario.label}-work-a`)
      const workspaceB = join(root, `${scenario.label}-work-b`)
      const homeOne = join(root, `${scenario.label}-home-one`)
      const homeTwo = join(root, `${scenario.label}-home-two`)
      const stateDir = join(root, `${scenario.label}-state-${String(index)}`)
      await Promise.all([
        mkdir(workspaceA, { recursive: true }),
        mkdir(workspaceB, { recursive: true }),
        mkdir(homeOne, { recursive: true }),
        mkdir(homeTwo, { recursive: true }),
      ])
      const envOne: NodeJS.ProcessEnv = {
        ...process.env,
        HOME: homeOne,
        DSH_HOME: scenario.dshHome,
        DSH_AGENTS_HOME: join(homeOne, 'agents'),
        DSH_TELEMETRY_DISABLED: '1',
      }
      const expected = scenario.dshHome.startsWith('~/')
        ? join(homeOne, scenario.dshHome.slice(2))
        : join(workspaceA, scenario.dshHome)

      first = new HostManager({
        stateDir,
        projectRoot,
        cwd: workspaceA,
        env: envOne,
        port: 0,
        startTimeoutMs: 120_000,
        stopGraceMs: 10_000,
        killGraceMs: 2_000,
      })
      const started = await first.start()
      const record = JSON.parse(await readFile(join(stateDir, 'host.json'), 'utf8')) as Record<string, unknown>
      assert.equal(record.dshHome, expected)
      assert.equal((await first.hostContext()).dshHome, expected)
      const childEnvironment = await readFile(`/proc/${String(started.hostPid)}/environ`, 'utf8')
      const childDshHome = childEnvironment.split('\0').find(entry => entry.startsWith('DSH_HOME='))
      assert.equal(childDshHome, `DSH_HOME=${expected}`)
      await first.close()

      const envTwo: NodeJS.ProcessEnv = {
        ...process.env,
        HOME: homeTwo,
        DSH_HOME: '~/different-home',
        DSH_AGENTS_HOME: join(homeTwo, 'agents'),
        DSH_TELEMETRY_DISABLED: '1',
      }
      second = new HostManager({
        stateDir,
        projectRoot,
        cwd: workspaceB,
        env: envTwo,
        port: 0,
        startTimeoutMs: 120_000,
        stopGraceMs: 10_000,
        killGraceMs: 2_000,
      })
      const reattached = await second.start()
      assert.equal(reattached.hostPid, started.hostPid)
      assert.equal((await second.hostContext()).dshHome, expected)

      await writeFile(join(stateDir, 'host.json'), JSON.stringify({ ...record, dshHome: 'legacy-relative' }))
      assert.equal((await second.hostContext()).dshHome, resolve(workspaceA, 'legacy-relative'))
      await writeFile(join(stateDir, 'host.json'), JSON.stringify({ ...record, dshHome: '~/legacy-tilde' }))
      await assert.rejects(second.hostContext(), (error: unknown) =>
        error instanceof LiaisonError && error.code === 'HOST_RECORD_INVALID')

      const stopped = await second.stop()
      assert.equal(stopped.stoppedHost, true)
      second = undefined
      first = undefined
    }
  } finally {
    await second?.stop().catch(() => undefined)
    await first?.stop().catch(() => undefined)
    await second?.close().catch(() => undefined)
    await first?.close().catch(() => undefined)
    await rm(root, { recursive: true, force: true })
  }
})
