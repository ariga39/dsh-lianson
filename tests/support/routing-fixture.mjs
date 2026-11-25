import { execFile } from 'node:child_process';
import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';

const execFileAsync = promisify(execFile);
const ASSET_DIRECTORY = fileURLToPath(new URL('../fixtures/routing-exercise/', import.meta.url));
const ASSET_FILES = ['server.mjs', 'router.test.mjs', 'verify-138.mjs', 'README.md'];

/**
 * Live source mutations used by the negative-control tests. Each mutation
 * touches only the authenticated GET response of /api/session, so exactly one
 * of the 138 observations should change.
 */
export const MUTATIONS = {
  status: {
    find: "json(200, { session: 'fixture-token', user: 1 })",
    replace: "json(500, { session: 'fixture-token', user: 1 })",
  },
  body: {
    find: "{ session: 'fixture-token', user: 1 }",
    replace: "{ session: 'fixture-token', user: 2 }",
  },
  cacheControl: {
    find: "json(200, { session: 'fixture-token', user: 1 })",
    replace: "json(200, { session: 'fixture-token', user: 1 }, 'private, max-age=60')",
  },
  contentType: {
    find: "json(200, { session: 'fixture-token', user: 1 })",
    replace:
      "json(200, { session: 'fixture-token', user: 1 }, 'no-store', 'text/plain; charset=utf-8')",
  },
  /**
   * Real transport failure: commit a Content-Length larger than the body, flush
   * a partial JSON payload, then destroy the socket. The verifier must reject
   * (not crash) and still reap its child server.
   */
  truncatedBody: {
    find: `export function handleRequest(request, response) {
  const { pathname } = new URL(request.url, 'http://127.0.0.1');
`,
    replace: `export function handleRequest(request, response) {
  const { pathname } = new URL(request.url, 'http://127.0.0.1');
  if (pathname === '/health') {
    response.writeHead(200, {
      'cache-control': 'no-store',
      'content-type': 'application/json; charset=utf-8',
      'content-length': '64',
    });
    response.write('{"status":', () => {
      response.destroy();
    });
    return;
  }
`,
  },
};

export function applyServerMutation(source, mutation) {
  if (!source.includes(mutation.find)) {
    throw new Error(`mutation target not found: ${mutation.find}`);
  }
  return source.replace(mutation.find, mutation.replace);
}

async function git(cwd, args) {
  const { stdout } = await execFileAsync('git', args, {
    cwd,
    encoding: 'utf8',
    env: { ...process.env, GIT_TERMINAL_PROMPT: '0', GIT_CONFIG_NOSYSTEM: '1' },
  });
  return stdout;
}

/**
 * Initialize a fresh temporary git repository containing the representative
 * router fixture. Nothing is cached between runs.
 */
export async function createRoutingFixture(options = {}) {
  const baseDirectory = options.baseDirectory ?? os.tmpdir();
  await mkdir(baseDirectory, { recursive: true });
  const root = await mkdtemp(path.join(baseDirectory, 'routing-fixture-'));
  const directory = path.join(root, 'repo');
  await mkdir(directory, { recursive: true });

  for (const file of ASSET_FILES) {
    await copyFile(path.join(ASSET_DIRECTORY, file), path.join(directory, file));
  }
  await writeFile(
    path.join(directory, 'package.json'),
    `${JSON.stringify(
      {
        name: 'routing-exercise-fixture',
        private: true,
        type: 'module',
        scripts: {
          test: 'node --test',
          verify: 'node verify-138.mjs --dir .',
        },
      },
      null,
      2,
    )}\n`,
  );
  await writeFile(path.join(directory, '.gitignore'), 'observations.json\n');

  await git(directory, ['init', '-q']);
  await git(directory, ['config', 'user.name', 'Routing Fixture']);
  await git(directory, ['config', 'user.email', 'routing-fixture@example.invalid']);
  await git(directory, ['config', 'commit.gpgsign', 'false']);
  await git(directory, ['add', '-A']);
  await git(directory, ['commit', '-q', '-m', 'baseline routing fixture']);
  const baselineRev = (await git(directory, ['rev-parse', 'HEAD'])).trim();

  return {
    root,
    directory,
    baselineRev,

    /**
     * Create an isolated git worktree branched from a revision. This is the
     * same shape the later delegation rehearsal uses for wt-a / wt-b.
     */
    async addWorktree(label, { from = 'HEAD', branch } = {}) {
      if (!/^[A-Za-z0-9._-]+$/.test(label)) {
        throw new Error(`invalid worktree label: ${label}`);
      }
      const worktree = path.join(root, `wt-${label}`);
      if (branch) {
        await git(directory, ['worktree', 'add', '-b', branch, worktree, from]);
      } else {
        await git(directory, ['worktree', 'add', '--detach', worktree, from]);
      }
      return worktree;
    },

    async mutateServer(worktree, mutation) {
      const serverPath = path.join(worktree, 'server.mjs');
      const source = await readFile(serverPath, 'utf8');
      await writeFile(serverPath, applyServerMutation(source, mutation));
    },

    async commitAll(worktree, message) {
      await git(worktree, ['add', '-A']);
      await git(worktree, ['commit', '-q', '-m', message]);
      return (await git(worktree, ['rev-parse', 'HEAD'])).trim();
    },

    async cleanup() {
      await rm(root, { recursive: true, force: true });
    },
  };
}
