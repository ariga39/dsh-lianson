/** Test-only IPC proxy that observes one real daemon response and drops it. */

import { createConnection, createServer } from 'node:net'
import { rm } from 'node:fs/promises'

/**
 * Forward one request to the real daemon, wait for its completed response, then
 * destroy the client connection without delivering that response.
 * @param {{socketPath: string, proxyPath: string, request: object, timeoutMs?: number}} options
 * @returns {Promise<object>} the response the client never received
 */
export function dropIpcResponse({ socketPath, proxyPath, request, timeoutMs = 60_000 }) {
  return new Promise((resolvePromise, reject) => {
    let settled = false
    let upstream
    let clientSide
    let incomingSide
    const finish = (error, value) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      upstream?.destroy()
      clientSide?.destroy()
      incomingSide?.destroy()
      server.close()
      void rm(proxyPath, { force: true })
      if (error !== undefined) reject(error)
      else resolvePromise(value)
    }
    const timer = setTimeout(() => {
      finish(new Error('timed out waiting for the real daemon response'))
    }, timeoutMs)
    timer.unref?.()

    const server = createServer(incoming => {
      incomingSide = incoming
      incoming.on('error', () => {})
      upstream = createConnection(socketPath)
      upstream.on('error', error => finish(error))
      upstream.setEncoding('utf8')
      incoming.on('data', chunk => { upstream.write(chunk) })
      let buffer = ''
      upstream.on('data', chunk => {
        buffer += chunk
        const newline = buffer.indexOf('\n')
        if (newline === -1) return
        try {
          finish(undefined, JSON.parse(buffer.slice(0, newline)))
        } catch (error) {
          finish(error)
        }
      })
    })
    server.on('error', error => finish(error))
    server.listen(proxyPath, () => {
      clientSide = createConnection(proxyPath)
      clientSide.on('error', error => finish(error))
      clientSide.once('connect', () => {
        clientSide.write(`${JSON.stringify(request)}\n`)
      })
    })
  })
}
