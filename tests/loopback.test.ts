import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { LiaisonError } from '../src/errors.ts'
import { LoopbackClient } from '../src/loopback.ts'
import { exchangeLoopbackCookie, selectAuthenticatedEntry } from '../src/dsh-remote.ts'
import type { DshRemote, LoopbackStatus } from '../src/remote.ts'

describe('authenticated entry selection', () => {
  it('accepts exactly one token-bearing entry', () => {
    assert.equal(
      selectAuthenticatedEntry([
        { label: 'docs', url: 'http://127.0.0.1:3080/docs', open: false },
        { label: 'dsh web', url: 'http://127.0.0.1:3080/?token=launch', open: true },
      ]),
      'http://127.0.0.1:3080/?token=launch',
    )
  })

  it('rejects entries without exactly one non-empty token', () => {
    assert.equal(selectAuthenticatedEntry([{ label: 'dsh web', url: 'http://127.0.0.1:3080/', open: true }]), undefined)
    assert.equal(selectAuthenticatedEntry([{ label: 'dsh web', url: 'http://127.0.0.1:3080/?token=', open: true }]), undefined)
    assert.equal(selectAuthenticatedEntry([{ label: 'dsh web', url: 'http://127.0.0.1:3080/?token=a&token=b', open: true }]), undefined)
    assert.equal(selectAuthenticatedEntry([{ label: 'broken', url: 'not a url', open: true }]), undefined)
  })
})

describe('loopback cookie exchange', () => {
  it('exchanges the token without following the redirect', async () => {
    let seen: URL | undefined
    let init: RequestInit | undefined
    const fetchImpl: typeof fetch = async (input, requestInit) => {
      seen = new URL(typeof input === 'string' ? input : input instanceof URL ? input.href : input.url)
      init = requestInit
      return new Response(null, {
        status: 302,
        headers: { location: './api/remote.mux', 'set-cookie': 'dsh_host=secret; Path=/; HttpOnly' },
      })
    }
    assert.deepEqual(
      await exchangeLoopbackCookie('http://127.0.0.1:3080/?token=launch', fetchImpl),
      { origin: 'http://127.0.0.1:3080', cookie: 'dsh_host=secret' },
    )
    assert.equal(seen?.pathname, '/connect')
    assert.equal(seen?.searchParams.get('token'), 'launch')
    assert.equal(seen?.searchParams.get('interactions'), 'answer')
    assert.equal(init?.redirect, 'manual')
  })

  it('reports refusal instead of retrying with a bad credential', async () => {
    await assert.rejects(
      exchangeLoopbackCookie('http://127.0.0.1:3080/?token=launch', async () => new Response(null, { status: 401 })),
      (error: unknown) => error instanceof LiaisonError && error.code === 'dsh-unavailable',
    )
    await assert.rejects(
      exchangeLoopbackCookie('http://127.0.0.1:3080/?token=launch', async () => new Response(null, { status: 302 })),
      (error: unknown) => error instanceof LiaisonError && error.code === 'dsh-unavailable',
    )
  })
})

describe('loopback lifecycle', () => {
  it('retries until an entry and connection are available', async () => {
    const states: LoopbackStatus[] = []
    const disposed = createRemote()
    let entriesCalls = 0
    let connects = 0
    const client = new LoopbackClient({
      collectEntries: () => {
        entriesCalls += 1
        return entriesCalls < 2 ? [] : [{ label: 'dsh web', url: 'http://127.0.0.1:3080/?token=launch', open: true }]
      },
      exchange: async () => ({ origin: 'http://127.0.0.1:3080', cookie: 'dsh=secret' }),
      connect: async () => {
        connects += 1
        if (connects < 2) throw new Error('not ready yet')
        return disposed.remote
      },
      retryDelaysMs: [1],
      maxAttempts: 4,
      sleep: async () => {},
      onStatus: status => states.push(status),
    })
    client.start()
    await connected(client)
    assert.equal(connects, 2)
    assert.equal(states.at(-1)?.state, 'connected')
    assert.equal(states.at(-1)?.origin, 'http://127.0.0.1:3080')
    assert.equal(client.requireRemote(), disposed.remote)
    await client.dispose()
    assert.equal(disposed.disposeCalls(), 1)
    assert.equal(client.status().state, 'disposed')
    assert.throws(() => client.requireRemote(), (error: unknown) => error instanceof LiaisonError && error.code === 'dsh-unavailable')
  })

  it('stops retrying after the configured attempts', async () => {
    const client = new LoopbackClient({
      collectEntries: () => [],
      exchange: async () => { throw new Error('unused') },
      connect: async () => { throw new Error('unused') },
      retryDelaysMs: [1],
      maxAttempts: 2,
      sleep: async () => {},
    })
    await settled(client)
    assert.equal(client.status().state, 'unavailable')
    assert.match(client.status().lastError ?? '', /no authenticated entry URL/)
  })


  it('starts a fresh lazy attempt when a command arrives after failure', async () => {
    let entries = 0
    const client = new LoopbackClient({
      collectEntries: () => { entries += 1; return [] },
      exchange: async () => { throw new Error('unused') },
      connect: async () => { throw new Error('unused') },
      retryDelaysMs: [1],
      maxAttempts: 1,
      sleep: async () => {},
    })
    await settled(client)
    assert.equal(entries, 1)
    assert.throws(() => client.requireRemote())
    await waitFor(() => entries > 1)
    assert.throws(() => client.requireRemote())
  })

  it('aborts an in-flight connect and releases its resources on dispose', async () => {
    let release: (() => void) | undefined
    const client = new LoopbackClient({
      collectEntries: () => [{ label: 'dsh web', url: 'http://127.0.0.1:3080/?token=launch', open: true }],
      exchange: async () => ({ origin: 'http://127.0.0.1:3080', cookie: 'dsh=secret' }),
      connect: async (_origin, _cookie, signal) => {
        await new Promise<void>((_resolve, reject) => {
          release = () => { reject(new Error('disposed')) }
          signal.addEventListener('abort', () => { release?.() }, { once: true })
        })
        throw new Error('unreachable')
      },
      retryDelaysMs: [1],
      maxAttempts: 1,
      sleep: async () => {},
    })
    client.start()
    await waitFor(() => release !== undefined)
    await client.dispose()
    assert.equal(client.status().state, 'disposed')
    assert.throws(() => client.requireRemote())
  })
})

function createRemote(): { remote: DshRemote; disposeCalls: () => number } {
  let calls = 0
  const unused = (): never => { throw new Error('not used by this fixture') }
  const remote: DshRemote = {
    origin: 'http://127.0.0.1:3080',
    modelCatalog: async () => ({ default: { provider: 'fixture', model: 'm' }, groups: [], failures: [] }),
    createWorkspace: unused,
    createSession: unused,
    renameSession: unused,
    selectModel: unused,
    beforeCursor: unused,
    prompt: unused,
    readEventPage: unused,
    readState: unused,
    wait: unused,
    readProjections: unused,
    updateQueue: unused,
    cancel: unused,
    unarchiveSession: unused,
    answerUserQuestion: unused,
    dispose: async () => { calls += 1 },
  }
  return { remote, disposeCalls: () => calls }
}

async function connected(client: LoopbackClient): Promise<void> {
  await waitFor(() => client.status().state === 'connected')
  assert.equal(client.status().state, 'connected')
}

async function settled(client: LoopbackClient): Promise<void> {
  client.start()
  await waitFor(() => client.status().state === 'unavailable')
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    if (predicate()) return
    await new Promise(resolve => setTimeout(resolve, 1))
  }
  throw new Error('condition did not settle')
}
