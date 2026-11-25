/** Thin CLI: JSON in/out over the user-only daemon socket. */

import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { stat } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import { parseArgs } from 'node:util'
import { HostManager, createLock, hostStopSucceeded, resolveStateDir } from './host-manager.mjs'
import { call, probe } from './ipc-client.mjs'
import { failure, stablePromptRequestId } from './ipc-protocol.mjs'
import { errorEnvelope, LiaisonError } from './errors.mjs'
import { redactSecrets } from './redaction.mjs'

const DAEMON_PATH = join(import.meta.dirname, 'daemon.mjs')
const HOST_COMMANDS = new Set(['start', 'status', 'ping', 'stop'])
const DELEGATION_COMMANDS = new Set(['create', 'get', 'list', 'prompt', 'verify-model'])
// Host spawn permits 120s and stop permits 10s SIGTERM + 2s SIGKILL, on top of
// the Host lock wait. The IPC deadline must cover the documented operation,
// not just ordinary replies.
const START_TIMEOUT_MS = 180_000
const STOP_TIMEOUT_MS = 180_000

const HELP = `Usage:
  dsh-liaison host <start|status|ping|stop> [options]
  dsh-liaison create --title <title> --provider <id> --model <id> [options]
  dsh-liaison get (--delegation-id <id> | --session-id <session-id>)
  dsh-liaison list
  dsh-liaison prompt --delegation-id <id> --content <text> [options]
  dsh-liaison verify-model --delegation-id <id> [--request-id <id>]

Host commands:
  host start    attach to or spawn one managed dsh web Host
  host status   print daemon and Host status (use --ensure to start the Host)
  host ping     run one authenticated unary RPC
  host stop     stop the daemon; stop its managed Host unless --keep-host

Delegation commands:
  create        create or resume one write-ahead delegation session
  get           read one delegation by delegation id or bound session id
  list          list delegation records
  prompt        submit one idempotent prompt
  verify-model  verify the persisted request-header model for a prompt

Options:
  --id <id>                 IPC request id; retrying the same id replays the response
  --delegation-id <id>      stable delegation identity (default: --id)
  --session-id <id>         Host session identity for get
  --cwd <path>              delegation working directory (default: current directory)
  --title <title>           unique session title
  --provider <id>           advertised model provider
  --model <id>              advertised model id
  --reasoning-effort <id>   advertised reasoning effort
  --agent-preset <name>     optional Host agent preset
  --content <text>          prompt text
  --request-id <id>         stable prompt request id (default: derived from --id)
  --dsh-home <path>         DSH_HOME used when spawning a Host
  --workspace <path>        working directory used when spawning a Host
  --patch <path>            extra dsh web profile patch (repeatable)
  --port <number>           Host listen port, 0 for an OS-assigned loopback port
  --ensure                  with host status, attach or spawn the Host first
  --keep-host               with host stop, leave dsh web running
  -h, --help                show this help

Runtime:
  Managed Host lifecycle currently requires Linux with readable procfs process
  identity. Other hosts fail before spawning; the upstream bootstrap artifact
  matrix remains independent of this runtime boundary.
`

function sleep(ms) {
  return new Promise(resolvePromise => setTimeout(resolvePromise, ms))
}

async function waitForDaemon(socketPath, timeoutMs = 60_000) {
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

function required(values, name, usage) {
  const value = values[name]
  if (typeof value !== 'string' || value.trim() === '') {
    throw new LiaisonError('USAGE', usage)
  }
  return value
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

async function stopWithoutDaemon({ stateDir, keepHost }) {
  const manager = new HostManager({ stateDir })
  if (keepHost) {
    const host = await manager.inspectHost({ probe: false }).catch(() => null)
    return {
      daemonPid: null,
      hostPid: host?.pid ?? null,
      stoppedHost: false,
      stopReason: 'kept',
    }
  }
  const stopped = await manager.stopHost()
  if (!hostStopSucceeded(stopped)) {
    throw new LiaisonError('HOST_STOP_INCOMPLETE', 'Host stop did not complete', {
      pid: stopped.pid ?? null,
      reason: stopped.reason,
      retained: stopped.retained === true,
    })
  }
  return {
    daemonPid: null,
    hostPid: stopped.pid ?? null,
    stoppedHost: stopped.stopped,
    stopReason: stopped.reason,
    stopRetained: stopped.retained === true,
  }
}

async function stopWithOwnership({ socketPath, stateDir, keepHost }) {
  const stateExists = await stat(stateDir).then(info => info.isDirectory()).catch(error => {
    if (error?.code === 'ENOENT') return false
    throw error
  })
  if (!stateExists) {
    return {
      ok: true,
      command: 'host stop',
      data: {
        daemonPid: null,
        hostPid: null,
        stoppedHost: false,
        stopReason: 'missing',
        stopRetained: false,
      },
    }
  }
  const lock = await createLock(join(stateDir, 'daemon.lock'))
  const deadline = Date.now() + 15_000
  while (true) {
    if (await probe(socketPath, 500)) {
      return await call(socketPath, {
        command: 'host stop',
        params: { stopHost: !keepHost },
        timeoutMs: STOP_TIMEOUT_MS,
      })
    }
    let release
    try {
      release = await lock.acquire({ timeoutMs: 0 })
    } catch (error) {
      if (error?.code !== 'HOST_LOCK_TIMEOUT') throw error
      if (Date.now() >= deadline) {
        throw new LiaisonError('DAEMON_STARTING',
          'A daemon is starting but did not expose its socket; refusing to race it')
      }
      await sleep(100)
      continue
    }
    try {
      return {
        ok: true,
        command: 'host stop',
        data: await stopWithoutDaemon({ stateDir, keepHost }),
      }
    } finally {
      await release()
    }
  }
}

function delegationHostParams(values, cwd) {
  const params = hostParams(values)
  if (params.cwd === undefined) params.cwd = cwd
  return params
}

function delegationCwd(values) {
  return resolve(values.cwd ?? values.workspace ?? process.cwd())
}

function delegationRequest(action, values) {
  if (action === 'create') {
    const cwd = delegationCwd(values)
    const title = required(values, 'title', 'create requires --title <title>')
    const provider = required(values, 'provider', 'create requires --provider <id>')
    const model = required(values, 'model', 'create requires --model <id>')
    return {
      command: 'delegation.create',
      params: {
        delegationId: values['delegation-id'] ?? values.id ?? `delegation-${randomUUID()}`,
        cwd,
        title,
        model: {
          provider,
          model,
          ...(values['reasoning-effort'] === undefined
            ? {}
            : { reasoningEffort: values['reasoning-effort'] }),
        },
        ...(values['agent-preset'] === undefined ? {} : { agentPreset: values['agent-preset'] }),
        host: delegationHostParams(values, cwd),
      },
    }
  }
  if (action === 'get') {
    if (values['delegation-id'] === undefined && values['session-id'] === undefined) {
      throw new LiaisonError('USAGE', 'get requires --delegation-id <id> or --session-id <id>')
    }
    return {
      command: 'delegation.get',
      params: {
        ...(values['delegation-id'] === undefined ? {} : { delegationId: values['delegation-id'] }),
        ...(values['session-id'] === undefined ? {} : { sessionId: values['session-id'] }),
      },
    }
  }
  if (action === 'list') return { command: 'delegation.list', params: {} }
  if (action === 'prompt') {
    const delegationId = required(values, 'delegation-id', 'prompt requires --delegation-id <id>')
    const content = required(values, 'content', 'prompt requires --content <text>')
    const requestId = stablePromptRequestId(values['request-id'], values.id)
    return {
      command: 'delegation.prompt',
      params: {
        delegationId,
        content,
        ...(requestId === undefined ? {} : { requestId }),
      },
    }
  }
  if (action === 'verify-model') {
    const delegationId = required(values, 'delegation-id', 'verify-model requires --delegation-id <id>')
    const requestId = stablePromptRequestId(values['request-id'], values.id)
    return {
      command: 'delegation.verifyModel',
      params: {
        delegationId,
        ...(requestId === undefined ? {} : { requestId }),
      },
    }
  }
  throw new LiaisonError('USAGE', `Unknown delegation command: ${String(action)}`)
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
      id: { type: 'string' },
      'delegation-id': { type: 'string' },
      'session-id': { type: 'string' },
      cwd: { type: 'string' },
      title: { type: 'string' },
      provider: { type: 'string' },
      model: { type: 'string' },
      'reasoning-effort': { type: 'string' },
      'agent-preset': { type: 'string' },
      content: { type: 'string' },
      'request-id': { type: 'string' },
      help: { type: 'boolean', short: 'h', default: false },
    },
  })
  if (values.help === true) {
    io.stdout.write(HELP)
    return 0
  }
  const [group, action, ...extra] = positionals
  if (group === 'host') {
    if (action === undefined || extra.length > 0 || !HOST_COMMANDS.has(action)) {
      throw new LiaisonError('USAGE', 'Usage: dsh-liaison host <start|status|ping|stop> [options]')
    }
  } else if (!DELEGATION_COMMANDS.has(group) || action !== undefined || extra.length > 0) {
    throw new LiaisonError('USAGE', 'Usage: dsh-liaison <host|create|get|list|prompt|verify-model> [options]')
  }

  const stateDir = resolveStateDir()
  const socketPath = join(stateDir, 'daemon.sock')
  const request = group === 'host'
    ? { command: `host ${action}`, params: hostParams(values) }
    : delegationRequest(group, values)
  const command = request.command
  const params = request.params

  let envelope
  try {
    if (group === 'host' && action === 'stop') {
      envelope = await stopWithOwnership({
        socketPath,
        stateDir,
        keepHost: values['keep-host'] === true,
      })
    } else {
      await ensureDaemon(socketPath)
      if (group === 'host' && action === 'status') params.ensure = values.ensure === true
      envelope = await call(socketPath, {
        command,
        params,
        id: values.id,
        timeoutMs: group === 'host'
          ? START_TIMEOUT_MS
          : (group === 'create' ? START_TIMEOUT_MS : 60_000),
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
