/** Shell-free pnpm invocation, mirroring upstream's lifecycle npm_execpath pattern. */

import { existsSync } from 'node:fs'
import { extname, join, resolve } from 'node:path'

function basenameOf(path) {
  return path.split(/[\\/]/u).at(-1)?.toLowerCase() ?? ''
}

function isPnpmEntrypoint(path) {
  const basename = basenameOf(path)
  return basename === 'pnpm'
    || basename === 'pnpm.exe'
    || basename === 'pnpm.cjs'
    || basename === 'pnpm.js'
    || basename === 'pnpm.mjs'
}

export function pnpmInvocation(args, {
  env = process.env,
  platform = process.platform,
  execPath = process.execPath,
  exists = existsSync,
} = {}) {
  const entrypoint = env.npm_execpath?.trim()
  if (entrypoint !== undefined && entrypoint !== '' && isPnpmEntrypoint(entrypoint)) {
    const extension = extname(entrypoint).toLowerCase()
    if (extension === '.js' || extension === '.cjs' || extension === '.mjs') {
      return { command: execPath, args: [entrypoint, ...args] }
    }
    if (extension !== '.cmd') return { command: entrypoint, args: [...args] }
  }

  const home = env.PNPM_HOME?.trim()
  if (home !== undefined && home !== '') {
    const packageBin = resolve(home, '..', 'pnpm', 'bin')
    for (const filename of ['pnpm.mjs', 'pnpm.cjs']) {
      const candidate = join(packageBin, filename)
      if (exists(candidate)) return { command: execPath, args: [candidate, ...args] }
    }
  }

  if (platform === 'win32') {
    throw new Error('bootstrap: pnpm must expose a JavaScript entrypoint through npm_execpath or PNPM_HOME on Windows; refusing to spawn a .cmd shim')
  }
  return { command: 'corepack', args: ['pnpm', ...args] }
}
