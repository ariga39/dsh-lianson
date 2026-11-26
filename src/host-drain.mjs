/** Detached bounded-output drainer for one managed Host. */

import { spawn } from 'node:child_process'

const args = JSON.parse(process.env.DSH_LIAISON_DRAIN_ARGS ?? '[]')
const cwd = process.env.DSH_LIAISON_DRAIN_CWD
const env = { ...process.env }
delete env.DSH_LIAISON_DRAIN_ARGS
delete env.DSH_LIAISON_DRAIN_CWD

let relay = true
process.stdout.on('error', () => { relay = false })
process.stderr.on('error', () => { relay = false })

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

function forward(stream) {
  stream.setEncoding('utf8')
  stream.on('data', chunk => {
    if (!relay) return
    try { process.stdout.write(chunk) } catch { relay = false }
  })
  stream.on('error', () => {})
}
if (child.stdout !== null) forward(child.stdout)
if (child.stderr !== null) forward(child.stderr)

if (child.pid !== undefined) {
  try { process.stdout.write(`DSH_DRAIN_PID ${String(child.pid)}\n`) } catch { relay = false }
}
child.once('exit', (code, signal) => {
  process.exit(code ?? (signal === null ? 0 : 1))
})
