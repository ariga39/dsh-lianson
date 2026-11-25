/** The one JSON envelope used by the local Unix-socket protocol. */

export const IPC_COMMANDS = new Set([
  'host.start',
  'host.status',
  'host.ping',
  'host.stop',
  'delegation.create',
  'delegation.get',
  'delegation.list',
  'delegation.prompt',
  'delegation.verifyModel',
])

export function success(command, data, id) {
  return { ok: true, command, ...id === undefined ? {} : { id }, data }
}

export function failure(command, error, id) {
  return { ok: false, command, ...id === undefined ? {} : { id }, error }
}

function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`
  if (value !== null && typeof value === 'object') {
    return `{${Object.keys(value).sort().map(key =>
      `${JSON.stringify(key)}:${canonical(value[key])}`).join(',')}}`
  }
  return JSON.stringify(value) ?? 'null'
}

/**
 * Stable identity of one IPC request for same-id response replay.
 * Object key order does not change the fingerprint.
 * @param {string} command
 * @param {unknown} params
 */
export function requestFingerprint(command, params) {
  return canonical({ command, params: params ?? null })
}
