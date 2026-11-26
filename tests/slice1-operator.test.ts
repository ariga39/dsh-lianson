import { strict as assert } from 'node:assert'
import { mkdtemp, readFile, rm, mkdir } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { fileURLToPath } from 'node:url'
import WebSocket from 'ws'
import { LiaisonError } from '../src/errors.ts'
import { HostManager } from '../src/host-manager.ts'
import { OperatorConnection, type OperatorTarget } from '../src/operator-connection.ts'

const projectRoot = fileURLToPath(new URL('..', import.meta.url))

await test('operator recovery: WS drop re-exchanges /connect, rejects the old cookie, and retries past five outages', { timeout: 180_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-operator-'))
  const workspace = join(root, 'workspace')
  const dshHome = join(root, 'dsh-home')
  const stateDir = join(root, 'state')
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    HOME: root,
    DSH_HOME: dshHome,
    DSH_AGENTS_HOME: join(dshHome, 'agents'),
    DSH_TELEMETRY_DISABLED: '1',
  }
  const manager = new HostManager({
    stateDir,
    projectRoot,
    cwd: workspace,
    env,
    port: 0,
    startTimeoutMs: 120_000,
    stopGraceMs: 10_000,
    killGraceMs: 2_000,
  })
  let operator: OperatorConnection | undefined
  try {
    await mkdir(workspace, { recursive: true })
    await manager.start()
    await manager.close()
    const record = JSON.parse(await readFile(join(stateDir, 'host.json'), 'utf8')) as Record<string, unknown>
    const target: OperatorTarget = {
      origin: stringField(record, 'origin'),
      launchToken: stringField(record, 'launchToken'),
    }

    const realFetch = globalThis.fetch.bind(globalThis)
    let connectFailures = 0
    let connectAttempts = 0
    const cookies: string[] = []
    const upgradeStatuses: number[] = []
    const sockets: Array<{ readonly socket: WebSocket; readonly url: string }> = []
    const fetchImpl: typeof fetch = async (input, init) => {
      const url = input instanceof URL ? input : new URL(input instanceof Request ? input.url : input)
      if (url.pathname === '/connect') {
        connectAttempts += 1
        if (connectFailures > 0) {
          connectFailures -= 1
          return new Response('temporary outage', { status: 503 })
        }
      }
      return await realFetch(input, init)
    }
    operator = new OperatorConnection({
      fetch: fetchImpl,
      reconnectBaseMs: 20,
      reconnectMaxMs: 60,
      generationTimeoutMs: 10_000,
      onCookie: (cookie) => { cookies.push(cookie) },
      onWebSocket: (socket, url) => {
        const webSocket = socket as unknown as WebSocket
        sockets.push({ socket: webSocket, url })
        webSocket.on('unexpected-response', (_request, response) => {
          upgradeStatuses.push(response.statusCode ?? 0)
        })
      },
    })

    await operator.setHost(target, 20_000)
    const firstCookie = cookies.at(-1)
    assert.ok(firstCookie !== undefined && firstCookie !== '')
    const first = await operator.withClient(remote => remote.session.list({}), 10_000)
    assert.equal(first.ok, true)

    // An unsolicited transport loss disposes the old generation, revokes its
    // v2 cookie, and must perform a fresh /connect exchange. Six consecutive
    // /connect failures prove the background loop does not stop at five
    // attempts even though the caller never initiated recovery.
    const firstAttempts = connectAttempts
    connectFailures = 6
    terminateAll(sockets)
    await waitFor(
      () => cookies.length >= 2 && connectAttempts >= firstAttempts + 7,
      20_000,
      'fresh reconnect after transport loss',
    )
    await operator.waitReady(20_000)
    const secondCookie = cookies.at(-1)
    assert.ok(secondCookie !== undefined && secondCookie !== '')
    assert.notEqual(secondCookie, firstCookie, 'an unsolicited loss must not reuse the revoked cookie')
    const afterRecovery = await operator.withClient(remote => remote.session.list({}), 10_000)
    assert.equal(afterRecovery.ok, true, 'the public RPC surface must recover after unsolicited transport loss')

    const oldCookieResponse = await fetch(new URL('/', target.origin), {
      redirect: 'manual',
      headers: { cookie: firstCookie },
    })
    assert.equal(oldCookieResponse.status, 401, 'the old v2 cookie must be revoked after the transport closes')

    // Foreground waits stay bounded while the background loop keeps retrying.
    const secondAttempts = connectAttempts
    connectFailures = 1_000
    terminateAll(sockets)
    await waitFor(() => connectAttempts > secondAttempts, 5_000, 'background retry during outage')
    const started = Date.now()
    await assert.rejects(operator.withClient(remote => remote.session.list({}), 150), (error: unknown) => {
      assert.ok(error instanceof LiaisonError)
      assert.equal(error.code, 'OPERATOR_UNAVAILABLE')
      return true
    })
    assert.ok(Date.now() - started < 2_000, 'a foreground call must respect its deadline during an outage')
    connectFailures = 0
    await operator.waitReady(20_000)

    // setHost detaches the failing loop before waiting on the replacement.
    const thirdAttempts = connectAttempts
    connectFailures = 1_000
    terminateAll(sockets)
    await waitFor(() => connectAttempts > thirdAttempts, 5_000, 'background retry before replacement')
    connectFailures = 0
    const detachedAt = Date.now()
    await operator.setHost(target, 10_000)
    assert.ok(Date.now() - detachedAt < 5_000, 'setHost must detach the prior loop promptly')

    const followUp = await operator.withClient(remote => remote.session.list({}), 10_000)
    assert.equal(followUp.ok, true)
    assert.deepEqual(upgradeStatuses.filter(status => status === 409), [], 'reconnects must not collide with the old mux')
  } finally {
    await operator?.close()
    await manager.stop()
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})

await test('first launch: a temporary operator outage leaves the ready Host and recovers in the background', { timeout: 180_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-first-launch-'))
  const workspace = join(root, 'workspace')
  const dshHome = join(root, 'dsh-home')
  const stateDir = join(root, 'state')
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    HOME: root,
    DSH_HOME: dshHome,
    DSH_AGENTS_HOME: join(dshHome, 'agents'),
    DSH_TELEMETRY_DISABLED: '1',
  }
  const realFetch = globalThis.fetch.bind(globalThis)
  let outage = true
  const fetchImpl: typeof fetch = async (input, init) => {
    const url = input instanceof URL ? input : new URL(input instanceof Request ? input.url : input)
    if (outage && url.pathname === '/connect') return new Response('temporary outage', { status: 503 })
    return await realFetch(input, init)
  }
  const manager = new HostManager({
    stateDir,
    projectRoot,
    cwd: workspace,
    env,
    port: 0,
    startTimeoutMs: 120_000,
    stopGraceMs: 10_000,
    killGraceMs: 2_000,
    operatorReadyTimeoutMs: 300,
    operator: {
      fetch: fetchImpl,
      reconnectBaseMs: 20,
      reconnectMaxMs: 60,
      generationTimeoutMs: 10_000,
    },
  })
  try {
    await mkdir(workspace, { recursive: true })
    await assert.rejects(manager.start(), (error: unknown) => {
      assert.ok(error instanceof LiaisonError)
      assert.equal(error.code, 'OPERATOR_UNAVAILABLE')
      return true
    })
    const ready = JSON.parse(await readFile(join(stateDir, 'host.json'), 'utf8')) as Record<string, unknown>
    assert.equal(ready.state, 'ready', 'the ready record must survive a foreground operator timeout')
    const hostPid = ready.hostPid
    assert.equal(typeof hostPid, 'number')
    assert.doesNotThrow(() => { process.kill(hostPid as number, 0) }, 'the ready Host must still be alive')

    outage = false
    const ping = await manager.ping()
    assert.equal(ping.ok, true)
    assert.equal(ping.hostPid, hostPid, 'background recovery must reach the same Host, not a relaunch')
    const after = JSON.parse(await readFile(join(stateDir, 'host.json'), 'utf8')) as Record<string, unknown>
    assert.equal(after.hostPid, hostPid)
  } finally {
    await manager.stop().catch(() => undefined)
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})

function stringField(record: Record<string, unknown>, key: string): string {
  const value = record[key]
  assert.equal(typeof value, 'string')
  return value as string
}

function terminateAll(sockets: Array<{ readonly socket: WebSocket; readonly url: string }>): void {
  const index = sockets.findIndex(({ url }) => new URL(url).pathname === '/api/remote.mux')
  const terminated = index === -1 ? sockets.splice(0) : sockets.splice(index, 1)
  if (terminated.length === 0) throw new Error('no live transport socket was captured')
  for (const { socket } of terminated) socket.terminate()
}

async function waitFor(predicate: () => boolean, timeoutMs: number, label: string): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error(`timed out waiting for ${label}`)
    await new Promise(resolve => setTimeout(resolve, 20))
  }
}
