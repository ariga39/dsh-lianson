/** Detached bounded-output drainer for one managed Host. */

import { spawn } from 'node:child_process'

const args = JSON.parse(process.env.DSH_LIAISON_DRAIN_ARGS ?? '[]')
const cwd = process.env.DSH_LIAISON_DRAIN_CWD
const hostMarker = process.env.DSH_LIAISON_DRAIN_HOST_MARKER
const env = { ...process.env }
delete env.DSH_LIAISON_DRAIN_ARGS
delete env.DSH_LIAISON_DRAIN_CWD
delete env.DSH_LIAISON_DRAIN_MARKER
delete env.DSH_LIAISON_DRAIN_HOST_MARKER
if (typeof hostMarker === 'string' && hostMarker !== '') {
  env.DSH_LIAISON_HOST_MARKER = hostMarker
} else {
  delete env.DSH_LIAISON_HOST_MARKER
}

let relayOut = true
let relayErr = true
process.stdout.on('error', () => { relayOut = false })
process.stderr.on('error', () => { relayErr = false })

const child = spawn(process.execPath, args, {
  cwd,
  env,
  detached: true,
  stdio: ['ignore', 'pipe', 'pipe'],
})
child.once('error', error => {
  try { process.stderr.write(`host-drain: ${error.message}\n`) } catch {}
  process.exit(1)
})

function forward(stream, target, relay) {
  stream.setEncoding('utf8')
  stream.on('data', chunk => {
    if (!relay()) return
    try { target.write(chunk) } catch { /* the target error handler stops relaying */ }
  })
  stream.on('error', () => {})
}
if (child.stdout !== null) forward(child.stdout, process.stdout, () => relayOut)
if (child.stderr !== null) forward(child.stderr, process.stderr, () => relayErr)

if (child.pid !== undefined) {
  try { process.stdout.write(`DSH_DRAIN_PID ${String(child.pid)}\n`) } catch { relayOut = false }
}
child.once('exit', (code, signal) => {
  process.exit(code ?? (signal === null ? 0 : 1))
})
