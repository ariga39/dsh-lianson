/** The one JSON envelope used by the local Unix-socket protocol. */

export const IPC_COMMANDS = new Set(['host.start', 'host.status', 'host.ping', 'host.stop'])

export function success(command, data, id) {
  return { ok: true, command, ...id === undefined ? {} : { id }, data }
}

export function failure(command, error, id) {
  return { ok: false, command, ...id === undefined ? {} : { id }, error }
}
