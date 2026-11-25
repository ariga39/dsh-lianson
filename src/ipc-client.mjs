/** NDJSON client for the user-only Unix-socket daemon. */

import { createConnection } from 'node:net'
import { LiaisonError } from './errors.mjs'

/**
 * Send one command and resolve its one response envelope.
 * @param {string} socketPath
 * @param {{command: string, params?: unknown, id?: string, timeoutMs?: number}} request
 */
export function call(socketPath, { command, params, id, timeoutMs = 30_000 }) {
  return new Promise((resolve, reject) => {
    const socket = createConnection(socketPath)
    let buffer = ''
    let settled = false
    const finish = (error, value) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      if (error !== undefined) reject(error)
      else resolve(value)
    }
    const timer = setTimeout(() => {
      socket.destroy()
      finish(new LiaisonError('DAEMON_UNAVAILABLE', 'Timed out waiting for the local daemon'))
    }, timeoutMs)
    timer.unref?.()

    socket.setEncoding('utf8')
    socket.once('connect', () => {
      socket.write(`${JSON.stringify({ command, params, id })}\n`)
    })
    socket.on('data', chunk => {
      buffer += chunk
      const newline = buffer.indexOf('\n')
      if (newline === -1) return
      const line = buffer.slice(0, newline)
      socket.end()
      let envelope
      try { envelope = JSON.parse(line) } catch {
        finish(new LiaisonError('IPC_PROTOCOL_ERROR', 'Daemon returned invalid JSON'))
        return
      }
      if (typeof envelope !== 'object' || envelope === null
        || typeof envelope.ok !== 'boolean'
        || typeof envelope.command !== 'string'
        || (envelope.ok === true ? !('data' in envelope) : typeof envelope.error !== 'object')) {
        finish(new LiaisonError('IPC_PROTOCOL_ERROR', 'Daemon returned an invalid envelope'))
        return
      }
      finish(undefined, envelope)
    })
    socket.once('error', error => {
      finish(new LiaisonError('DAEMON_UNAVAILABLE',
        'Local daemon is not reachable', { cause: error?.code ?? 'socket-error' }))
    })
    socket.once('end', () => {
      if (buffer.trim() === '') finish(new LiaisonError('DAEMON_UNAVAILABLE', 'Local daemon closed without a response'))
    })
  })
}

/** True when a live daemon answers on this socket path. */
export async function probe(socketPath, timeoutMs = 1_000) {
  try {
    const envelope = await call(socketPath, { command: 'host.status', timeoutMs })
    return envelope.ok === true || envelope.ok === false
  } catch {
    return false
  }
}
