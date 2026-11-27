/** Plugin composition: paths, ledger, loopback client, and the UDS server. */

import type { SandboxExecutionPolicy } from '@deepseek-ai/dsh-sandbox'
import { createLoopbackRemote, exchangeLoopbackCookie } from './dsh-remote.ts'
import type { EntryUrl } from './dsh-remote.ts'
import { InteractionBroker } from './interactions.ts'
import { Ledger } from './ledger.ts'
import { LoopbackClient } from './loopback.ts'
import { resolveLiaisonPaths } from './paths.ts'
import type { LiaisonPaths } from './paths.ts'
import { LiaisonServer } from './server.ts'
import { DelegationService } from './service.ts'
import { trustPolicyOf } from './trust.ts'

/** Host services the plugin borrows from the surrounding dsh process. */
export interface LiaisonHost {
  /** Environment used to resolve `DSH_HOME` and runtime paths. */
  readonly env: NodeJS.ProcessEnv
  /** Advertised Host base URL, or undefined before the listener binds. */
  readonly origin: () => string | undefined
  /** Collect `webserver/entry-url` contributions for one base URL. */
  readonly collectEntries: (baseUrl: string) => readonly EntryUrl[]
  /** Resolved deployment sandbox policy, when available. */
  readonly sandboxPolicy: () => SandboxExecutionPolicy | undefined
  /** Runs the connect loop and UDS server outside a live Host in tests. */
  readonly logger: { warn: (error: Error) => void }
  /** Retry timing override for focused tests. */
  readonly retry?: {
    readonly retryDelaysMs: readonly number[]
    readonly maxAttempts: number
    readonly sleep?: (ms: number, signal: AbortSignal) => Promise<void>
  }
}

/** One started liaison composition. */
export interface LiaisonRuntime {
  readonly paths: LiaisonPaths
  readonly ledger: Ledger
  readonly loopback: LoopbackClient
  readonly server: LiaisonServer
  dispose(): Promise<void>
}

/**
 * Start the ledger, socket, and loopback lifecycle.
 * @param host - borrowed Host reads.
 * @returns the started composition.
 */
export async function startLiaison(host: LiaisonHost): Promise<LiaisonRuntime> {
  const paths = resolveLiaisonPaths(host.env)
  const ledger = Ledger.open(paths.ledgerPath)
  const bindingOfSession = (sessionId: string): string | undefined =>
    ledger.findDelegationBySession(sessionId)?.delegationId
  const interactions = new InteractionBroker({ bindingOfSession })
  const loopback = new LoopbackClient({
    collectEntries: () => {
      const origin = host.origin()
      return origin === undefined ? [] : host.collectEntries(`${origin}/`)
    },
    exchange: (entryUrl, signal) => exchangeLoopbackCookie(entryUrl, fetchWithSignal(signal)),
    connect: (origin, cookie, signal) => createLoopbackRemote(origin, cookie, signal, {
      interactions: { broker: interactions, bindingOfSession },
    }),
    onStatus: (status) => {
      if (status.lastError !== undefined && status.state === 'unavailable') {
        host.logger.warn(new Error(`dsh-liaison loopback: ${status.lastError}`))
      }
    },
    ...(host.retry === undefined ? {} : {
      retryDelaysMs: host.retry.retryDelaysMs,
      maxAttempts: host.retry.maxAttempts,
      ...(host.retry.sleep === undefined ? {} : { sleep: host.retry.sleep }),
    }),
  })
  const trust = trustPolicyOf(paths, host.sandboxPolicy())
  const service = new DelegationService({ ledger, remote: loopback, trust, interactions })
  const server = new LiaisonServer({
    paths,
    ledger,
    service,
    remote: loopback,
    origin: host.origin,
  })
  try {
    await server.start()
  } catch (error) {
    ledger.close()
    throw error
  }
  loopback.start()
  let disposed: Promise<void> | undefined
  return {
    paths,
    ledger,
    loopback,
    server,
    dispose: () => {
      disposed ??= disposeAll(loopback, server, ledger, interactions)
      return disposed
    },
  }
}

async function disposeAll(
  loopback: LoopbackClient,
  server: LiaisonServer,
  ledger: Ledger,
  interactions: InteractionBroker,
): Promise<void> {
  interactions.clear()
  await loopback.dispose()
  await server.stop()
  ledger.close()
}

/** Small adapter so `exchangeLoopbackCookie` still honors the connect lifetime. */
function fetchWithSignal(signal: AbortSignal): typeof fetch {
  return (input, init) => fetch(input, { ...init, signal })
}
