/** In-process loopback answering operator mounted from the liaison plugin. */
import { setTimeout as delay } from 'node:timers/promises'
import { Context } from '@deepseek-ai/cordis'
import { apply as gatewayApply, inject as gatewayInject, type ClientRemote } from '@deepseek-ai/dsh-api-gateway/client'
import type {} from '@deepseek-ai/dsh-api-remotes/types'
import sessionRemote from '@deepseek-ai/dsh-api-session-controller/remote'
import workspaceRemote from '@deepseek-ai/dsh-api-workspace-controller/remote'
import { installConnection, type ConnectionHandle } from '@deepseek-ai/dsh-client-connection/client'
import commandsRemote from '@deepseek-ai/dsh-commands/remote'
import permissionPresetsRemote from '@deepseek-ai/dsh-permission-presets/remote'
import TypertRegistry from '@deepseek-ai/dsh-typert-registry'
import userQuestionsRemote from '@deepseek-ai/dsh-user-questions/remote'
import WebSocket from 'ws'

/** One maintained loopback operator and its reconnect evidence. */
export interface LoopbackOperator {
  /** Advertised Host origin, never carrying the launch token. */
  readonly origin: string
  /** Mounted typed Remote surface. */
  readonly remote: ClientRemote
  /** Whether a Host generation is currently ready. */
  isConnected(): boolean
  /** Current Host generation id, or null before readiness and during reconnect. */
  generation(): number | null
  /** Number of successful `/connect` re-exchanges after a mux loss. */
  reauthentications(): number
  /** Call one authenticated Remote method and return the catalog model count. */
  modelCount(): Promise<number>
  /** Dispose the loopback Context and its WebSocket carrier. */
  close(): Promise<void>
}

interface Credential {
  readonly cookie: string
  readonly baseUrl: string
}

const LOOPBACK_HOSTS = new Set(['127.0.0.1', '::1', '[::1]', 'localhost'])

/**
 * Exchange the Host launch token for one independent answering credential.
 * @param launchUrl - authenticated entry URL printed by `dsh web`.
 * @returns the v2 cookie and Host base URL.
 */
async function exchangeCredential(launchUrl: string): Promise<Credential> {
  const application = new URL(launchUrl)
  if (!LOOPBACK_HOSTS.has(application.hostname)) {
    throw new Error(`dsh-liaison: refusing a non-loopback Host entry ${application.hostname}`)
  }
  const token = application.searchParams.get('token')
  if (token === null || token === '') throw new Error('dsh-liaison: Host entry carries no launch token')
  const connect = new URL('/connect', application.origin)
  connect.searchParams.set('token', token)
  connect.searchParams.set('interactions', 'answer')
  const response = await fetch(connect, { redirect: 'manual', signal: AbortSignal.timeout(10_000) })
  await response.body?.cancel()
  if (response.status !== 302) {
    throw new Error(`dsh-liaison: /connect returned HTTP ${String(response.status)}, expected 302`)
  }
  const setCookie = response.headers.get('set-cookie')
  if (setCookie === null) throw new Error('dsh-liaison: /connect omitted Set-Cookie')
  const cookie = setCookie.split(';', 1)[0]
  if (cookie === undefined || cookie === '') throw new Error('dsh-liaison: /connect returned an empty cookie')
  return { cookie, baseUrl: `${application.origin}/` }
}

/**
 * Mount the public upstream Node client packages against one authenticated Host.
 * A lost mux revokes its cookie, so the operator exchanges `/connect` again and
 * reconnects with the replacement credential.
 * @param launchUrl - authenticated loopback entry URL held only in memory.
 * @returns the maintained operator.
 */
export async function connectLoopback(launchUrl: string): Promise<LoopbackOperator> {
  const origin = new URL(launchUrl).origin
  let credential = await exchangeCredential(launchUrl)
  let reauthCount = 0

  const ctx = new Context()
  try {
    installConnection(ctx, {
      interactions: 'answer',
      transport: {
        streamBaseUrl: credential.baseUrl,
        fetch: async (input, init) => {
          const headers = new Headers(init.headers)
          headers.set('cookie', credential.cookie)
          return await fetch(new URL(input, credential.baseUrl), { ...init, headers })
        },
        createWebSocket: (url) => new WebSocket(url, { headers: { cookie: credential.cookie } }),
      },
    })
    const connection = ctx.get('connection') as ConnectionHandle
    let connectedGeneration: number | undefined
    let reauthing: Promise<void> | undefined
    let retryTimer: NodeJS.Timeout | undefined

    const reauthenticate = (): Promise<void> => {
      reauthing ??= (async () => {
        const next = await exchangeCredential(launchUrl)
        credential = next
        reauthCount += 1
        connection.reconnect()
      })().catch((error: unknown) => {
        retryTimer = setTimeout(() => {
          retryTimer = undefined
          if (reauthing !== undefined) return
          void reauthenticate().catch(() => undefined)
        }, 500)
        retryTimer.unref()
        throw error
      }).finally(() => {
        reauthing = undefined
      })
      return reauthing
    }

    const unsubscribe = connection.generation.subscribe(() => {
      const snapshot = connection.generation.getSnapshot()
      if (snapshot !== undefined) {
        connectedGeneration = snapshot.id
        return
      }
      if (connectedGeneration === undefined || reauthing !== undefined) return
      connectedGeneration = undefined
      void reauthenticate().catch(() => undefined)
    })

    await ctx.plugin(TypertRegistry)
    await ctx.plugin({ apply: gatewayApply, inject: gatewayInject })
    for (const contribution of [
      sessionRemote,
      workspaceRemote,
      commandsRemote,
      permissionPresetsRemote,
      userQuestionsRemote,
    ]) {
      await ctx.remote.$mount(contribution)
    }
    await waitFor(() => connection.generation.getSnapshot() !== undefined, 20_000, 'loopback generation')
    connectedGeneration = connection.generation.getSnapshot()?.id

    const remote = ctx.remote
    return {
      origin,
      remote,
      isConnected: () => connection.generation.getSnapshot() !== undefined,
      generation: () => connection.generation.getSnapshot()?.id ?? null,
      reauthentications: () => reauthCount,
      async modelCount(): Promise<number> {
        const result = await remote.session.modelCatalog()
        if (!result.ok) throw new Error(`${result.error.code}: ${result.error.message}`)
        return result.value.groups.length
      },
      async close(): Promise<void> {
        unsubscribe()
        if (retryTimer !== undefined) {
          clearTimeout(retryTimer)
          retryTimer = undefined
        }
        const disposing = ctx.fiber.dispose().catch(() => undefined)
        await Promise.race([disposing, delay(5_000)])
      },
    }
  } catch (error) {
    await ctx.fiber.dispose().catch(() => undefined)
    throw error
  }
}

async function waitFor(predicate: () => boolean, timeoutMs: number, label: string): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error(`dsh-liaison: timed out waiting for ${label}`)
    await delay(50)
  }
}
