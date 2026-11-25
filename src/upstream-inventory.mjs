/** Public upstream entries consumed by the keyless E2E, shared by loader and bootstrap. */
export const UPSTREAM_ENTRIES = [
  { key: 'cordis', specifier: '@deepseek-ai/cordis', anchor: 'packages/api/gateway/package.json' },
  { key: 'connection', specifier: '@deepseek-ai/dsh-client-connection/client', anchor: 'packages/api/gateway/package.json' },
  { key: 'registry', specifier: '@deepseek-ai/dsh-typert-registry', anchor: 'packages/api/gateway/package.json' },
  { key: 'gateway', specifier: '@deepseek-ai/dsh-api-gateway/client', anchor: 'packages/api/remotes/package.json' },
  { key: 'session', specifier: '@deepseek-ai/dsh-api-session-controller/remote', anchor: 'packages/api/remotes/package.json' },
  { key: 'workspace', specifier: '@deepseek-ai/dsh-api-workspace-controller/remote', anchor: 'packages/api/remotes/package.json' },
  { key: 'commands', specifier: '@deepseek-ai/dsh-commands/remote', anchor: 'packages/api/remotes/package.json' },
  { key: 'permissions', specifier: '@deepseek-ai/dsh-permission-presets/remote', anchor: 'packages/api/remotes/package.json' },
  { key: 'questions', specifier: '@deepseek-ai/dsh-user-questions/remote', anchor: 'packages/api/remotes/package.json' },
  { key: 'playwright', specifier: 'playwright', anchor: 'apps/web/package.json' },
  { key: 'ws', specifier: 'ws', anchor: 'packages/api/gateway/package.json' },
]
