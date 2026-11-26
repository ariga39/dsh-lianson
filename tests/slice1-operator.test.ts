import { strict as assert } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { mkdtemp, readFile, rm, mkdir } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { fileURLToPath } from 'node:url'
import WebSocket from 'ws'
import { createDeadline } from '../src/deadline.ts'
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
    assert.equal(
      await authenticatedApiStatus(target.origin, firstCookie),
      200,
      'the original v2 cookie must authenticate a supported /api request while the peer is live',
    )

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

    assert.equal(
      await authenticatedApiStatus(target.origin, firstCookie),
      401,
      'the old v2 cookie must be rejected by the same /api request after the transport closes',
    )
    assert.equal(
      await authenticatedApiStatus(target.origin, secondCookie),
      200,
      'the freshly exchanged cookie must authenticate the same /api request',
    )

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

await test('concurrent setHost calls keep one tracked loop and close stops it', { timeout: 180_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-single-flight-'))
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
    let connects = 0
    const fetchImpl: typeof fetch = async (input, init) => {
      const url = input instanceof URL ? input : new URL(input instanceof Request ? input.url : input)
      if (url.pathname === '/connect') {
        connects += 1
        if (connects === 1) await sleep(100)
      }
      return await realFetch(input, init)
    }
    operator = new OperatorConnection({
      fetch: fetchImpl,
      reconnectBaseMs: 20,
      reconnectMaxMs: 60,
      generationTimeoutMs: 10_000,
    })
    await Promise.all([
      operator.setHost(target, 10_000),
      operator.setHost(target, 10_000),
    ])
    assert.ok(connects >= 2, 'each serialized replacement establishes one generation')
    await operator.close()
    const afterClose = connects
    await sleep(300)
    assert.equal(connects, afterClose, 'no reconnect loop may outlive close')
  } finally {
    await operator?.close()
    await manager.stop()
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})

await test('a never-answering first /connect times out, retries without a foreground caller, and closes', { timeout: 180_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-connect-timeout-'))
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
    let connects = 0
    const fetchImpl: typeof fetch = async (input, init) => {
      const url = input instanceof URL ? input : new URL(input instanceof Request ? input.url : input)
      if (url.pathname === '/connect') {
        connects += 1
        if (connects === 1) return await new Promise<Response>(() => {})
      }
      return await realFetch(input, init)
    }
    operator = new OperatorConnection({
      fetch: fetchImpl,
      connectTimeoutMs: 100,
      reconnectBaseMs: 20,
      reconnectMaxMs: 60,
      generationTimeoutMs: 10_000,
    })
    await operator.setHost(target)
    await operator.waitReady(5_000)
    assert.ok(connects >= 2, 'a stalled handshake must be abandoned and retried in the background')
    const closeStarted = Date.now()
    await operator.close()
    assert.ok(Date.now() - closeStarted < 2_000, 'close must not wait on a stalled handshake')
    const afterClose = connects
    await sleep(300)
    assert.equal(connects, afterClose, 'retries must stop after close')
  } finally {
    await operator?.close()
    await manager.stop()
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})

await test('close promptly aborts a stalled foreground RPC and post-close waits', { timeout: 180_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-close-rpc-'))
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
    operator = new OperatorConnection({
      reconnectBaseMs: 20,
      reconnectMaxMs: 60,
      generationTimeoutMs: 10_000,
    })
    await operator.setHost(target, 10_000)
    const started = Promise.withResolvers<void>()
    let callError: unknown
    const call = operator.withClient<never>(() => {
      started.resolve()
      return new Promise<never>(() => {})
    }, 30_000).catch((error: unknown) => { callError = error })
    await started.promise
    const closeStarted = Date.now()
    await operator.close()
    assert.ok(Date.now() - closeStarted < 2_000, 'close must not wait for a stalled RPC')
    await call
    assert.ok(callError instanceof LiaisonError)
    assert.equal(callError.code, 'OPERATOR_UNAVAILABLE')
    const waitStarted = Date.now()
    await assert.rejects(operator.waitReady(30_000), (error: unknown) => {
      assert.ok(error instanceof LiaisonError)
      assert.equal(error.code, 'OPERATOR_UNAVAILABLE')
      return true
    })
    assert.ok(Date.now() - waitStarted < 1_000, 'a post-close wait must reject immediately')
  } finally {
    await operator?.close()
    await manager.stop()
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})

await test('an aborted initial upgrade re-exchanges without waiting for the generation timeout', { timeout: 180_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-upgrade-abort-'))
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
    let connects = 0
    let sockets = 0
    const fetchImpl: typeof fetch = async (input, init) => {
      const url = input instanceof URL ? input : new URL(input instanceof Request ? input.url : input)
      if (url.pathname === '/connect') connects += 1
      return await realFetch(input, init)
    }
    operator = new OperatorConnection({
      fetch: fetchImpl,
      connectTimeoutMs: 5_000,
      reconnectBaseMs: 20,
      reconnectMaxMs: 60,
      generationTimeoutMs: 5_000,
      onWebSocket: (socket) => {
        sockets += 1
        if (sockets === 1) (socket as unknown as WebSocket).terminate()
      },
    })
    await operator.setHost(target, 2_500)
    assert.ok(connects >= 2, 'the aborted upgrade must trigger a fresh /connect')
    assert.ok(sockets >= 2, 'the retry must open a new transport')
    const rpc = await operator.withClient(remote => remote.session.list({}), 5_000)
    assert.equal(rpc.ok, true)
  } finally {
    await operator?.close()
    await manager.stop()
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})

await test('an authenticated 401 starts a fresh exchange without replaying the mutation', { timeout: 180_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-401-'))
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
    let connects = 0
    let rejectRpc = true
    let mutationAttempts = 0
    const fetchImpl: typeof fetch = async (input, init) => {
      const url = input instanceof URL ? input : new URL(input instanceof Request ? input.url : input)
      if (url.pathname === '/connect') {
        connects += 1
        return await realFetch(input, init)
      }
      const method = requestMethodOf(init)
      if (url.pathname.startsWith('/api/') && method?.includes('workspace') === true) {
        mutationAttempts += 1
        if (rejectRpc) {
          rejectRpc = false
          return new Response('unauthorized', { status: 401 })
        }
      }
      return await realFetch(input, init)
    }
    operator = new OperatorConnection({
      fetch: fetchImpl,
      connectTimeoutMs: 5_000,
      reconnectBaseMs: 20,
      reconnectMaxMs: 60,
      generationTimeoutMs: 10_000,
    })
    await operator.setHost(target, 10_000)
    const mutation = operator
      .withClient(remote => remote.workspace.create({ path: join(root, 'mutation') }), 5_000)
      .then(
        value => ({ kind: 'value' as const, value }),
        (error: unknown) => ({ kind: 'error' as const, error }),
      )
    await waitFor(() => mutationAttempts >= 1, 5_000, 'authenticated mutation 401')
    await waitFor(() => connects >= 2, 5_000, 'fresh exchange after authenticated 401')
    const outcome = await mutation
    if (outcome.kind === 'value') assert.equal(outcome.value.ok, false)
    else assert.ok(outcome.error instanceof Error)
    const read = await operator.withClient(remote => remote.session.list({}), 5_000)
    assert.equal(read.ok, true)
    assert.equal(mutationAttempts, 1, 'the rejected mutation must not be replayed')
  } finally {
    await operator?.close()
    await manager.stop()
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})

function requestMethodOf(init: RequestInit | undefined): string | undefined {
  if (init === undefined) return undefined
  if (typeof init.body !== 'string') return undefined
  try {
    const parsed: unknown = JSON.parse(init.body)
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) return undefined
    const method = (parsed as Record<string, unknown>).method
    return typeof method === 'string' ? method : undefined
  } catch {
    return undefined
  }
}

await test('close settles promptly when the transport is already lost at mount handoff', { timeout: 180_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-handoff-close-'))
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
    const mounted = Promise.withResolvers<void>()
    let closeTask: Promise<void> | undefined
    operator = new OperatorConnection({
      connectTimeoutMs: 5_000,
      reconnectBaseMs: 20,
      reconnectMaxMs: 60,
      generationTimeoutMs: 5_000,
      onWebSocket: (socket) => {
        ;(socket as unknown as WebSocket).terminate()
        closeTask ??= operator?.close()
        mounted.resolve()
      },
    })
    const started = Date.now()
    const mounting = operator.setHost(target, 5_000)
    await mounted.promise
    assert.ok(closeTask !== undefined)
    await closeTask
    await Promise.allSettled([mounting])
    assert.ok(Date.now() - started < 2_000, 'close must not wait for the lost handoff generation timeout')
    await assert.rejects(operator.waitReady(30_000), (error: unknown) => {
      assert.ok(error instanceof LiaisonError)
      assert.equal(error.code, 'OPERATOR_UNAVAILABLE')
      return true
    })
  } finally {
    await operator?.close()
    await manager.stop()
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})

await test('host ping uses one deadline across probe, attach, readiness, and RPC', { timeout: 180_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-ping-deadline-'))
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
  const realFetch = globalThis.fetch.bind(globalThis)
  let connects = 0
  let rpcCalls = 0
  const fetchImpl: typeof fetch = async (input, init) => {
    const url = input instanceof URL ? input : new URL(input instanceof Request ? input.url : input)
    if (url.pathname === '/connect') {
      connects += 1
      if (connects === 1) await sleep(500)
      return await realFetch(input, init)
    }
    if (url.pathname.startsWith('/api/')) {
      rpcCalls += 1
      return await new Promise<Response>(() => {})
    }
    return await realFetch(input, init)
  }
  const pingManager = new HostManager({
    stateDir,
    projectRoot,
    cwd: workspace,
    env,
    pingTimeoutMs: 1_500,
    stopGraceMs: 10_000,
    killGraceMs: 2_000,
    operator: {
      fetch: fetchImpl,
      connectTimeoutMs: 5_000,
      reconnectBaseMs: 20,
      reconnectMaxMs: 60,
      generationTimeoutMs: 5_000,
    },
  })
  try {
    await mkdir(workspace, { recursive: true })
    await manager.start()
    const started = Date.now()
    await assert.rejects(pingManager.ping(), (error: unknown) => {
      assert.ok(error instanceof LiaisonError)
      assert.equal(error.code, 'OPERATOR_UNAVAILABLE')
      return true
    })
    assert.ok(Date.now() - started < 3_000, 'ping must honor one complete foreground deadline')
    assert.equal(rpcCalls, 1, 'the stalled RPC must not be retried')
  } finally {
    await pingManager.close().catch(() => undefined)
    await manager.stop()
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})

await test('one deadline covers a queued setHost, readiness, and a stalled operation', { timeout: 180_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-liaison-queued-deadline-'))
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
    let connects = 0
    let rpcCalls = 0
    const fetchImpl: typeof fetch = async (input, init) => {
      const url = input instanceof URL ? input : new URL(input instanceof Request ? input.url : input)
      if (url.pathname === '/connect') {
        connects += 1
        if (connects === 1) await sleep(500)
        return await realFetch(input, init)
      }
      if (url.pathname.startsWith('/api/')) {
        rpcCalls += 1
        return await new Promise<Response>(() => {})
      }
      return await realFetch(input, init)
    }
    operator = new OperatorConnection({
      fetch: fetchImpl,
      connectTimeoutMs: 5_000,
      reconnectBaseMs: 20,
      reconnectMaxMs: 60,
      generationTimeoutMs: 5_000,
    })
    const deadline = createDeadline(2_000)
    const started = Date.now()
    const first = operator.setHostUntil(target, deadline)
    const second = operator.setHostUntil(target, deadline)
    const client = operator.withClientUntil(remote => remote.session.list({}), deadline)
    const outcomes = await Promise.allSettled([first, second, client])
    assert.ok(Date.now() - started < 3_500, 'the queued replacement and operation share one deadline')
    assert.equal(outcomes[0]?.status, 'fulfilled')
    assert.equal(outcomes[1]?.status, 'fulfilled')
    assert.equal(outcomes[2]?.status, 'rejected')
    if (outcomes[2]?.status === 'rejected') {
      assert.ok(outcomes[2].reason instanceof LiaisonError)
      assert.equal(outcomes[2].reason.code, 'OPERATOR_UNAVAILABLE')
    }
    assert.equal(rpcCalls, 1, 'the timed-out operation must not be retried')
    await sleep(300)
    assert.equal(rpcCalls, 1, 'background reconnect must not replay the operation')
    assert.equal(operator.matches(target), true, 'background reconnect keeps the replacement target')
  } finally {
    await operator?.close()
    await manager.stop()
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})

async function sleep(ms: number): Promise<void> {
  await new Promise(resolve => setTimeout(resolve, ms))
}

function stringField(record: Record<string, unknown>, key: string): string {
  const value = record[key]
  assert.equal(typeof value, 'string')
  return value as string
}

async function authenticatedApiStatus(origin: string, cookie: string): Promise<number> {
  const body = JSON.stringify({
    type: 'client-request',
    rpcId: randomUUID(),
    method: 'settings/describe',
    payload: { args: {} },
  })
  const response = await fetch(new URL('/api/settings/describe', origin), {
    method: 'POST',
    redirect: 'manual',
    headers: { 'content-type': 'application/json', cookie },
    body,
  })
  return response.status
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
