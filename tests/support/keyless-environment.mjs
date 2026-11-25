import { mkdir, writeFile } from 'node:fs/promises'
import { join } from 'node:path'

export async function createKeylessEnvironment({ root, mockBaseURL }) {
  const cwd = join(root, 'workspace')
  const dshHome = join(root, 'home')
  const agentsHome = join(root, 'agents')
  const overlayPath = join(root, 'overlay.yml')
  await mkdir(cwd, { recursive: true })
  await mkdir(dshHome, { recursive: true })
  await writeFile(overlayPath, [
    '- id: llm-deepseek',
    '  config:',
    `    baseURL: '${mockBaseURL}'`,
    '- id: llm-deepseek-account',
    '  disabled: true',
    '- id: session-title-llm',
    '  disabled: true',
    '- id: agent-instructions',
    '  disabled: true',
    '- id: session-log-deepseek',
    '  disabled: true',
    '- id: session-telemetry-otel',
    '  disabled: true',
    '',
  ].join('\n'), { mode: 0o600 })

  const env = Object.fromEntries(Object.entries(process.env).filter(([name]) =>
    !/(?:KEY|SECRET|TOKEN|PASSWORD)/iu.test(name)))
  Object.assign(env, {
    DSH_HOME: dshHome,
    DSH_AGENTS_HOME: agentsHome,
    DSH_TELEMETRY_DISABLED: '1',
    DEEPSEEK_API_KEY: 'dsh-mock-key',
    NODE_NO_WARNINGS: '1',
    SSH_CONNECTION: '',
    SSH_TTY: '',
  })

  return { cwd, dshHome, agentsHome, overlayPath, env }
}
