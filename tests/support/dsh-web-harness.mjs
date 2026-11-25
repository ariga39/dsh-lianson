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
  const append = chunk => {
    output = `${output}${String(chunk)}`.slice(-100_000)
    const match = /dsh web: (http:\/\/[^\s]+)/u.exec(output)
    if (match?.[1] !== undefined) token = tokenOfUrl(match[1]) ?? token
    const rawToken = /[?&]token=([^&#\s)"'`]+)/u.exec(output)
    if (rawToken?.[1] !== undefined) token = rawToken[1]
  }
  const diagnostics = () => scrubTokenDiagnostics(output, token)

  let launchUrl
  try {
    launchUrl = await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`dsh web did not become ready:\n${diagnostics()}`)), 120_000)
      const onData = chunk => {
        append(chunk)
        const match = /dsh web: (http:\/\/[^\s]+)/u.exec(output)
        if (match?.[1] === undefined) return
        clearTimeout(timer)
        resolve(match[1])
      }
      child.stdout.on('data', onData)
      child.stderr.on('data', onData)
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
