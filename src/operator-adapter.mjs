/**
 * Small seam over the Host Remote namespace.
 *
 * Issue #2 owns the transport and reconnect lifecycle. This adapter only names
 * the unary operations slice 2 needs and unwraps `RemoteResult`, so the daemon
 * can supply either a mounted `ctx.remote` or `OperatorConnection.withClient`.
 */

import { LiaisonError } from './errors.mjs'

/**
 * Unwrap one generated Remote result.
 * @param {unknown} result - `{ ok, value }` or `{ ok: false, error }`.
 * @returns the successful value.
 * @throws the reconstructed Remote failure.
 */
export function unwrapRemote(result) {
  if (result === null || typeof result !== 'object' || !('ok' in result)) return result
  if (result.ok === true) return result.value
  const failure = result.error ?? {}
  const error = new LiaisonError(
    failure.code ?? 'gateway/internal',
    failure.message ?? 'Remote call failed',
    failure.details ?? {},
  )
  error.isDSHRemoteError = true
  throw error
}

async function invoke(remote, namespace, method, request) {
  const target = remote?.[namespace]
  if (target === null || typeof target !== 'object' || typeof target[method] !== 'function') {
    throw new LiaisonError(
      'delegation/adapter-unavailable',
      `Host Remote namespace "${namespace}.${method}" is not mounted`,
      { namespace, method },
    )
  }
  const result = request === undefined
    ? await target[method]()
    : await target[method](request)
  return unwrapRemote(result)
}

function operations(call) {
  return {
    modelCatalog: () => call('session', 'modelCatalog'),
    listSessions: () => call('session', 'list', {}),
    ensureWorkspace: async path => (await call('workspace', 'create', { path })).workspace,
    createSession: request => call('session', 'create', request),
    renameSession: request => call('session', 'rename', request),
    selectModel: request => call('session', 'selectModel', request),
    sessionState: request => call('session', 'state', request),
    sessionWait: request => call('session', 'wait', request),
    sessionEvents: request => call('session', 'events', request),
    prompt: request => call('session', 'prompt', request),
  }
}

/**
 * Build the slice-2 adapter from a mounted `ctx.remote`.
 * @param {object} remote - mounted Remote namespace object.
 * @returns the named operation adapter.
 */
export function createRemoteAdapter(remote) {
  return {
    raw: remote,
    ...operations((namespace, method, request) => invoke(remote, namespace, method, request)),
  }
}

/**
 * Build the slice-2 adapter from Issue #2's `OperatorConnection.withClient`.
 * @param {{ withClient: (fn: (remote: object) => unknown) => Promise<unknown> }} connection - operator lifecycle owner.
 * @returns the named operation adapter.
 */
export function createOperatorAdapter(connection) {
  if (typeof connection?.withClient !== 'function') {
    throw new TypeError('createOperatorAdapter requires OperatorConnection.withClient')
  }
  return operations((namespace, method, request) =>
    connection.withClient(remote => invoke(remote, namespace, method, request)))
}
