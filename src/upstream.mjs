import { createRequire } from 'node:module'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { join } from 'node:path'
import { UPSTREAM_ENTRIES } from './upstream-inventory.mjs'

export const REPO_ROOT = fileURLToPath(new URL('..', import.meta.url))
export const UPSTREAM_ROOT = join(REPO_ROOT, 'deepseek-harness')

async function importFrom(anchor, specifier) {
  const resolved = createRequire(anchor).resolve(specifier)
  return import(pathToFileURL(resolved).href)
}

export async function loadUpstream() {
  const modules = Object.fromEntries(await Promise.all(UPSTREAM_ENTRIES.map(async entry => [
    entry.key,
    await importFrom(join(UPSTREAM_ROOT, entry.anchor), entry.specifier),
  ])))
  return {
    Context: modules.cordis.Context,
    installConnection: modules.connection.installConnection,
    TypertRegistry: modules.registry.default,
    gateway: modules.gateway,
    remotes: [
      modules.session.default,
      modules.workspace.default,
      modules.commands.default,
      modules.permissions.default,
      modules.questions.default,
    ],
    chromium: modules.playwright.default.chromium,
    WebSocket: modules.ws.default,
  }
}
