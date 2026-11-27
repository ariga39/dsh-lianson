/**
 * Load the pinned upstream modules and Playwright through the packages that
 * already depend on them. Integration tests stay outside the fast unit suite.
 */

import { createRequire } from 'node:module'
import { join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

export const REPO_ROOT = fileURLToPath(new URL('../../..', import.meta.url)).replace(/\/$/u, '')
export const UPSTREAM_ROOT = join(REPO_ROOT, 'deepseek-harness')

const ANCHORS = {
  gateway: join(UPSTREAM_ROOT, 'packages/api/gateway/package.json'),
  remotes: join(UPSTREAM_ROOT, 'packages/api/remotes/package.json'),
  web: join(UPSTREAM_ROOT, 'apps/web/package.json'),
}

async function importFrom(anchor, specifier) {
  const resolved = createRequire(anchor).resolve(specifier)
  return import(pathToFileURL(resolved).href)
}

export async function loadUpstream() {
  const [cordis, connection, registry, gateway, session, workspace, playwright, ws] = await Promise.all([
    importFrom(ANCHORS.gateway, '@deepseek-ai/cordis'),
    importFrom(ANCHORS.gateway, '@deepseek-ai/dsh-client-connection/client'),
    importFrom(ANCHORS.gateway, '@deepseek-ai/dsh-typert-registry'),
    importFrom(ANCHORS.remotes, '@deepseek-ai/dsh-api-gateway/client'),
    importFrom(ANCHORS.remotes, '@deepseek-ai/dsh-api-session-controller/remote'),
    importFrom(ANCHORS.remotes, '@deepseek-ai/dsh-api-workspace-controller/remote'),
    importFrom(ANCHORS.web, 'playwright'),
    importFrom(ANCHORS.gateway, 'ws'),
  ])
  return {
    Context: cordis.Context,
    installConnection: connection.installConnection,
    TypertRegistry: registry.default,
    gateway,
    remotes: [session.default, workspace.default],
    chromium: playwright.default.chromium,
    WebSocket: ws.default,
  }
}
