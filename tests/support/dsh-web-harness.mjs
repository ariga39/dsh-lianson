import { spawn } from 'node:child_process'
import { mkdir } from 'node:fs/promises'
import { join } from 'node:path'
import { scrubTokenDiagnostics, tokenOfUrl } from './scrub.mjs'

const BIN = join(import.meta.dirname, '../../deepseek-harness/apps/cli/lib/bin.js')

export async function startDshWeb({ root, cwd, dshHome, overlayPath, bin = BIN }) {
  await mkdir(dshHome, { recursive: true })
  const env = Object.fromEntries(Object.entries(process.env).filter(([name]) =>
    !/(?:KEY|SECRET|TOKEN|PASSWORD)/iu.test(name)))
  Object.assign(env, {
    DSH_HOME: dshHome,
    DSH_AGENTS_HOME: join(root, 'agents'),
    DSH_TELEMETRY_DISABLED: '1',
    DEEPSEEK_API_KEY: 'dsh-mock-key',
    NODE_NO_WARNINGS: '1',
    SSH_CONNECTION: '',
    SSH_TTY: '',
  })

  const started = performance.now()
  const child = spawn(process.execPath, [
    bin,
    '--profile', 'web',
    '--patch', overlayPath,
    '--no-open',
    '--host', '127.0.0.1',
    '--port', '0',
  ], { cwd, env, stdio: ['ignore', 'pipe', 'pipe'] })

  let output = ''
  let token
  const pending = { stdout: '', stderr: '' }
  let launchResolve
  let timer
  const diagnostics = () => scrubTokenDiagnostics(output, token)

  const observeLine = line => {
    const match = /dsh web: (http:\/\/\S+)/u.exec(line)
    if (match?.[1] !== undefined) {
      token = tokenOfUrl(match[1]) ?? token
      return match[1]
    }
    const rawToken = /[?&]token=([^&#\s)"'`]+)/u.exec(line)
    if (rawToken?.[1] !== undefined) token = rawToken[1]
    return undefined
  }

  const consume = (stream, chunk) => {
    output = `${output}${String(chunk)}`.slice(-100_000)
    let text = `${pending[stream]}${String(chunk)}`
    while (true) {
      // Frame only complete lines; a trailing \r may be the first half of CRLF.
      const lineEnd = /\r\n|\n|\r(?!$)/u.exec(text)
      if (lineEnd === null) break
      const line = text.slice(0, lineEnd.index)
      text = text.slice(lineEnd.index + lineEnd[0].length)
      const launchUrl = observeLine(line)
      if (launchUrl === undefined || launchResolve === undefined) continue
      const resolve = launchResolve
      launchResolve = undefined
      clearTimeout(timer)
      resolve(launchUrl)
    }
    pending[stream] = text
  }

  let launchUrl
  try {
    launchUrl = await new Promise((resolve, reject) => {
      launchResolve = resolve
      timer = setTimeout(() => reject(new Error(`dsh web did not become ready:\n${diagnostics()}`)), 120_000)
      child.stdout.on('data', chunk => { consume('stdout', chunk) })
      child.stderr.on('data', chunk => { consume('stderr', chunk) })
      child.once('error', error => {
        clearTimeout(timer)
        reject(new Error(scrubTokenDiagnostics(error.message, token)))
      })
      child.once('exit', code => {
        clearTimeout(timer)
        reject(new Error(`dsh web exited before readiness (${String(code)}):\n${diagnostics()}`))
      })
    })
  } catch (error) {
    child.kill('SIGKILL')
    throw error
  }

  return {
    child,
    launchUrl,
    startupMs: Math.round(performance.now() - started),
    diagnostics,
    close: async () => {
      if (child.exitCode !== null || child.signalCode !== null) return
      const exited = new Promise(resolve => child.once('exit', resolve))
      child.kill('SIGTERM')
      const forced = setTimeout(() => child.kill('SIGKILL'), 10_000)
      forced.unref()
      await exited
      clearTimeout(forced)
    },
  }
}
