import { chmod, unlink } from 'node:fs/promises'
import { createServer, type Server, type Socket } from 'node:net'
import { LiaisonError, wireErrorOf } from './errors.ts'
import { ensurePrivateDirectory, isMissing } from './fs-safe.ts'
import { parseIpcRequest, type IpcRequest } from './ipc-protocol.ts'

/** Daemon-facing server options. */
export interface IpcServerOptions {
  readonly socketPath: string
  readonly stateDir: string
  readonly handle: (request: IpcRequest) => Promise<unknown>
}

/** One running user-only Unix socket server. */
export interface IpcServer {
  /** Stop accepting new connections and remove the socket path while letting replies finish. */
  withdraw(): Promise<void>
  close(): Promise<void>
}

/**
 * Bind the single Unix-domain socket with `0700` directory and `0600` socket permissions.
 * @param options - socket path, state directory, and request handler.
 * @returns the running server.
 */
export async function startIpcServer(options: IpcServerOptions): Promise<IpcServer> {
  await ensurePrivateDirectory(options.stateDir)
  try {
    await unlink(options.socketPath)
  } catch (error) {
    if (!isMissing(error)) throw error
  }
  const sockets = new Set<Socket>()
  const server: Server = createServer((socket) => {
    sockets.add(socket)
    socket.setEncoding('utf8')
    let buffer = ''
    socket.on('data', (chunk: string) => {
      buffer += chunk
      while (true) {
        const newline = buffer.indexOf('\n')
        if (newline === -1) break
        const line = buffer.slice(0, newline)
        buffer = buffer.slice(newline + 1)
        if (line.trim() === '') continue
        void respond(socket, line, options.handle)
      }
    })
    socket.on('error', () => { sockets.delete(socket) })
    socket.on('close', () => { sockets.delete(socket) })
  })
  await new Promise<void>((resolve, reject) => {
    const onError = (error: Error): void => {
      server.off('listening', onListening)
      reject(error)
    }
    const onListening = (): void => {
      server.off('error', onError)
      resolve()
    }
    server.once('error', onError)
    server.once('listening', onListening)
    server.listen(options.socketPath)
  })
  await chmod(options.socketPath, 0o600)
  let closed = false
  let withdrawn = false
  let serverClosed: Promise<void> | undefined
  const closeServer = (): Promise<void> => {
    serverClosed ??= new Promise<void>((resolve) => {
      server.close(() => { resolve() })
    })
    return serverClosed
  }
  const removeSocket = async (): Promise<void> => {
    try {
      await unlink(options.socketPath)
    } catch (error) {
      if (!isMissing(error)) throw error
    }
  }
  return {
    async withdraw(): Promise<void> {
      if (withdrawn) return
      withdrawn = true
      await removeSocket()
      void closeServer()
    },
    async close(): Promise<void> {
      if (closed) return
      closed = true
      if (!withdrawn) {
        withdrawn = true
        void closeServer()
      }
      for (const socket of sockets) socket.destroy()
      sockets.clear()
      await closeServer()
      await removeSocket()
    },
  }
}

async function respond(socket: Socket, line: string, handle: (request: IpcRequest) => Promise<unknown>): Promise<void> {
  let id = ''
  try {
    const request = parseIpcRequest(line)
    id = request.id
    const result = await handle(request)
    writeLine(socket, JSON.stringify({ id, ok: true, result }))
  } catch (error) {
    const failure = error instanceof LiaisonError ? error : undefined
    const wire = failure === undefined
      ? wireErrorOf(new LiaisonError('IPC_PROTOCOL', 'the daemon could not handle the request', { cause: error }))
      : wireErrorOf(failure)
    writeLine(socket, JSON.stringify({ id, ok: false, error: wire }))
  }
}

function writeLine(socket: Socket, line: string): void {
  if (socket.destroyed) return
  // Keep the NDJSON connection open: a client may pipeline several requests
  // (for example a queued start followed by stop) before closing it.
  socket.write(`${line}\n`)
}
