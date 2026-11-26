import { spawnSync } from 'node:child_process'

/**
 * Run one child command with inherited stdio.
 * @param command - executable to spawn.
 * @param args - exact argument vector.
 * @param cwd - working directory.
 * @param env - environment overlay; the child also ignores system Git config.
 */
export function runCommand(
  command: string,
  args: readonly string[],
  cwd: string,
  env: Readonly<Record<string, string>> = {},
): void {
  console.log(`[bootstrap] ${command} ${args.join(' ')}`)
  const result = spawnSync(command, [...args], {
    cwd,
    env: childEnvironment(env),
    stdio: 'inherit',
  })
  if (result.error !== undefined) throw result.error
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(' ')} exited with ${String(result.status)}`)
  }
}

/**
 * Run one child command and capture stdout.
 * @param command - executable to spawn.
 * @param args - exact argument vector.
 * @param cwd - working directory.
 * @returns complete stdout.
 */
export function captureCommand(command: string, args: readonly string[], cwd: string): string {
  const result = spawnSync(command, [...args], {
    cwd,
    env: childEnvironment(),
    encoding: 'utf8',
  })
  if (result.error !== undefined) throw result.error
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(' ')} exited with ${String(result.status)}: ${result.stderr}`)
  }
  return result.stdout
}

function childEnvironment(overlay: Readonly<Record<string, string>> = {}): NodeJS.ProcessEnv {
  return {
    ...process.env,
    ...overlay,
    GIT_CONFIG_NOSYSTEM: '1',
  }
}
