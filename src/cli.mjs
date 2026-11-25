/** Thin CLI: JSON in/out over the user-only daemon socket. */

import { spawn } from 'node:child_process'
import { join, resolve } from 'node:path'
import { parseArgs } from 'node:util'
import { resolveStateDir } from './host-manager.mjs'
import { call, probe } from './ipc-client.mjs'
import { failure } from './ipc-protocol.mjs'
import { errorEnvelope, LiaisonError } from './errors.mjs'
import { redactSecrets } from './redaction.mjs'

const DAEMON_PATH = join(import.meta.dirname, 'daemon.mjs')
const COMMANDS = new Set(['start', 'status', 'ping', 'stop'])

const HELP = `Usage: dsh-liaison host <start|status|ping|stop> [options]

Commands:
  host start    attach to or spawn one managed dsh web Host
  host status   print daemon and Host status (use --ensure to start the Host)
  host ping     run one authenticated unary RPC
  host stop     stop the daemon; stop its managed Host unless --keep-host

Options:
  --dsh-home <path>   DSH_HOME used when spawning a Host
  --workspace <path>  working directory used when spawning a Host
  --patch <path>      extra dsh web profile patch (repeatable)
  --port <number>     Host listen port, 0 for an OS-assigned loopback port
  --ensure            with status, attach or spawn the Host first
  --keep-host         with stop, leave dsh web running
  -h, --help          show this help
`

function sleep(ms) {
  return new Promise(resolvePromise => setTimeout(resolvePromise, ms))
}

async function waitForDaemon(socketPath, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await probe(socketPath, 750)) return
    await sleep(50)
  }
  throw new LiaisonError('DAEMON_UNAVAILABLE', 'Local daemon did not become reachable')
}

async function ensureDaemon(socketPath) {
  if (await probe(socketPath, 1_000)) return
  const child = spawn(process.execPath, [DAEMON_PATH], {
    cwd: process.cwd(),
    env: process.env,
    detached: true,
    stdio: 'ignore',
  })
  child.unref()
  await waitForDaemon(socketPath)
}

function readPort(value) {
  if (value === undefined) return undefined
  const port = Number(value)
  if (!Number.isInteger(port) || port < 0 || port > 65_535) {
    throw new LiaisonError('HOST_PORT_INVALID', 'Port must be an integer from 0 to 65535')
  }
  return port
}

function hostParams(values) {
  const params = {}
  if (values['dsh-home'] !== undefined) params.dshHome = resolve(values['dsh-home'])
  if (values.workspace !== undefined) params.cwd = resolve(values.workspace)
  if (values.patch !== undefined && values.patch.length > 0) {
    params.patches = values.patch.map(path => resolve(path))
  }
  const port = readPort(values.port)
  if (port !== undefined) params.port = port
  return params
}

async function execute(argv, io) {
  const { values, positionals } = parseArgs({
    args: argv,
    allowPositionals: true,
    options: {
      'dsh-home': { type: 'string' },
      workspace: { type: 'string' },
      patch: { type: 'string', multiple: true },
      port: { type: 'string' },
      ensure: { type: 'boolean', default: false },
      'keep-host': { type: 'boolean', default: false },
      help: { type: 'boolean', short: 'h', default: false },
    },
  })
  if (values.help === true) {
    io.stdout.write(HELP)
    return 0
  }
  const [group, action, ...extra] = positionals
  if (group !== 'host' || action === undefined || extra.length > 0 || !COMMANDS.has(action)) {
    throw new LiaisonError('USAGE', 'Usage: dsh-liaison host <start|status|ping|stop> [options]')
  }

  const stateDir = resolveStateDir()
  const socketPath = join(stateDir, 'daemon.sock')
  const command = `host ${action}`
  const params = hostParams(values)

  let envelope
  try {
    if (action === 'stop' && !(await probe(socketPath, 500))) {
      envelope = {
        ok: true,
        command,
        data: { daemonPid: null, hostPid: null, stoppedHost: false, stopReason: 'daemon-not-running' },
      }
    } else {
      await ensureDaemon(socketPath)
      if (action === 'status') params.ensure = values.ensure === true
      if (action === 'stop') params.stopHost = values['keep-host'] !== true
      envelope = await call(socketPath, {
        command,
        params,
        timeoutMs: action === 'stop' ? 30_000 : 60_000,
      })
    }
  } catch (error) {
    envelope = failure(command, errorEnvelope(error, redactSecrets))
  }

  io.stdout.write(`${redactSecrets(JSON.stringify(envelope))}\n`)
  return envelope.ok === true ? 0 : 1
}

/**
 * Parse one CLI invocation, print exactly one JSON envelope, and return an exit code.
 * @param {string[]} argv
 * @param {{stdout: NodeJS.WritableStream, stderr: NodeJS.WritableStream}} io
 */
export async function run(argv, io = { stdout: process.stdout, stderr: process.stderr }) {
  try {
    return await execute(argv, io)
  } catch (error) {
    const envelope = failure(null, errorEnvelope(error, redactSecrets))
    io.stdout.write(`${redactSecrets(JSON.stringify(envelope))}\n`)
    return error?.code === 'USAGE' ? 2 : 1
  }
}
