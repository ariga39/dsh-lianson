/**
 * Test transport fixture for the real Node WebSocket carrier.
 *
 * This is deliberately only a transport seam: it records the physical sockets
 * the operator opens and can terminate the real `/api/remote.mux` socket. It
 * adds no test-only RPC or daemon command.
 */
export function createTransportFixture(WebSocketClass) {
  const sockets = new Set()
  const cookies = []
  const unexpectedStatuses = []

  function createWebSocket(url, options = {}) {
    const cookie = options?.headers?.cookie
    if (typeof cookie === 'string') cookies.push(cookie)
    const socket = new WebSocketClass(url, options)
    sockets.add(socket)
    socket.addEventListener('close', () => { sockets.delete(socket) }, { once: true })
    if (typeof socket.on === 'function') {
      socket.on('unexpected-response', (_request, response) => {
        unexpectedStatuses.push(response.statusCode)
        response.resume?.()
      })
    }
    return socket
  }

  return {
    createWebSocket,
    cookies,
    unexpectedStatuses,
    sockets,
    get muxSockets() {
      return [...sockets].filter(socket => String(socket.url ?? '').includes('/api/remote.mux'))
    },
    forceDropMux() {
      const mux = this.muxSockets
      for (const socket of mux) {
        if (typeof socket.terminate === 'function') socket.terminate()
        else socket.close(4001, 'test transport drop')
      }
      return mux.length
    },
  }
}
