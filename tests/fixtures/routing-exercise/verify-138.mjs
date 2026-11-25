#!/usr/bin/env node
/**
 * Standalone live 138-observation routing verifier.
 *
 * It starts <dir>/server.mjs on 127.0.0.1:0, performs every probe, captures the
 * four contract fields, and shuts the child server down again. Nothing is
 * pre-recorded: both baseline and candidate observations are produced by a
 * live HTTP run against the directory being checked.
 *
 * CLI:
 *   node verify-138.mjs --dir <worktree> [--out observations.json]
 *   node verify-138.mjs --baseline-dir <worktree> --dir <worktree> [--out candidate.json]
 *
 * This file is copied verbatim into every generated fixture repository, so it
 * depends only on Node built-ins.
 */
import http from 'node:http';
import { spawn } from 'node:child_process';
import { writeFile } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

export const METHODS = ['GET', 'POST', 'DELETE'];
export const AUTH_STATES = ['anonymous', 'session'];
export const SESSION_TOKEN = 'fixture-session-token';

export const PATHS = [
  '/',
  '/health',
  '/version',
  '/api/users',
  '/api/users/me',
  '/api/users/42',
  '/api/users/42/avatar',
  '/api/admin/users',
  '/api/admin/metrics',
  '/api/items',
  '/api/items/7',
  '/api/items/7/stock',
  '/api/orders',
  '/api/orders/1001',
  '/api/orders/1001/status',
  '/api/orders/1001/cancel',
  '/api/session',
  '/api/session/refresh',
  '/api/search',
  '/api/files/readme.txt',
  '/api/files/missing.txt',
  '/webhooks/github',
  '/metrics',
];

export const OBSERVATION_FIELDS = ['status', 'body', 'cacheControl', 'contentType'];
export const EXPECTED_OBSERVATIONS = PATHS.length * METHODS.length * AUTH_STATES.length;

const LISTEN_TIMEOUT_MS = 15_000;
const REQUEST_TIMEOUT_MS = 10_000;
const SHUTDOWN_TIMEOUT_MS = 5_000;

export function observationKey(observation) {
  return `${observation.method} ${observation.path} ${observation.auth}`;
}

function request({ port, path: requestPath, method, headers }) {
  return new Promise((resolve, reject) => {
    const label = `${method} ${requestPath}`;
    let settled = false;

    const fail = (error) => {
      if (settled) return;
      settled = true;
      req.destroy();
      reject(error instanceof Error ? error : new Error(String(error)));
    };

    const req = http.request(
      {
        host: '127.0.0.1',
        port,
        path: requestPath,
        method,
        headers,
        agent: false,
      },
      (res) => {
        const chunks = [];
        res.on('data', (chunk) => chunks.push(chunk));
        res.on('aborted', () => {
          fail(
            new Error(
              `truncated response for ${label}: connection aborted before the body completed`,
            ),
          );
        });
        res.on('error', (error) => {
          fail(new Error(`response stream failed for ${label}: ${error.message}`));
        });
        res.on('end', () => {
          if (settled) return;
          if (!res.complete) {
            fail(new Error(`truncated response for ${label}: incomplete body`));
            return;
          }
          settled = true;
          resolve({
            status: res.statusCode,
            headers: res.headers,
            body: Buffer.concat(chunks).toString('utf8'),
          });
        });
        res.on('close', () => {
          if (!settled && !res.complete) {
            fail(
              new Error(
                `truncated response for ${label}: connection closed before the body completed`,
              ),
            );
          }
        });
      },
    );

    req.setTimeout(REQUEST_TIMEOUT_MS, () => {
      req.destroy(new Error(`request timed out: ${label}`));
    });
    req.on('error', (error) => {
      fail(new Error(`request failed for ${label}: ${error.message}`));
    });
    req.end();
  });
}

function startServer(directory) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [path.join(directory, 'server.mjs')], {
      cwd: directory,
      env: { ...process.env, PORT: '0' },
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    let stdout = '';
    let stderr = '';
    let settled = false;

    const fail = (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(error);
    };

    const timer = setTimeout(() => {
      child.kill('SIGKILL');
      fail(
        new Error(
          `fixture server did not listen within ${LISTEN_TIMEOUT_MS}ms\nstdout:\n${stdout}\nstderr:\n${stderr}`,
        ),
      );
    }, LISTEN_TIMEOUT_MS);

    child.stdout.setEncoding('utf8');
    child.stdout.on('data', (chunk) => {
      stdout += chunk;
      const match = stdout.match(/^LISTENING (\d+)$/m);
      if (!match || settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({ child, port: Number(match[1]) });
    });

    child.stderr.setEncoding('utf8');
    child.stderr.on('data', (chunk) => {
      stderr += chunk;
    });

    child.on('error', (error) => fail(error));
    child.on('exit', (code, signal) => {
      if (settled) return;
      fail(
        new Error(
          `fixture server exited before listening (code=${code}, signal=${signal})\nstdout:\n${stdout}\nstderr:\n${stderr}`,
        ),
      );
    });
  });
}

function waitForExit(child, timeoutMs) {
  return new Promise((resolve) => {
    if (child.exitCode !== null || child.signalCode !== null) {
      resolve(true);
      return;
    }

    const onExit = () => {
      clearTimeout(timer);
      resolve(true);
    };
    const timer = setTimeout(() => {
      child.off('exit', onExit);
      resolve(false);
    }, timeoutMs);
    child.once('exit', onExit);
  });
}

async function stopServer(child) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  child.kill('SIGTERM');
  if (await waitForExit(child, SHUTDOWN_TIMEOUT_MS)) return;
  child.kill('SIGKILL');
  await waitForExit(child, SHUTDOWN_TIMEOUT_MS);
}

/**
 * Capture all 138 observations from a fixture directory by running its server
 * over real HTTP.
 */
export async function captureObservations(directory, options = {}) {
  const { child, port } = await startServer(directory);
  const observations = [];
  try {
    options.onServerStart?.({ pid: child.pid, port });
    for (const requestPath of PATHS) {
      for (const method of METHODS) {
        for (const auth of AUTH_STATES) {
          const headers =
            auth === 'session'
              ? { authorization: `Bearer ${SESSION_TOKEN}` }
              : {};
          const response = await request({ port, path: requestPath, method, headers });
          observations.push({
            path: requestPath,
            method,
            auth,
            status: response.status,
            body: response.body,
            cacheControl: response.headers['cache-control'] ?? null,
            contentType: response.headers['content-type'] ?? null,
          });
        }
      }
    }
  } finally {
    await stopServer(child);
  }
  return observations;
}

function indexObservations(observations, label) {
  const indexed = new Map();
  for (const observation of observations) {
    const key = observationKey(observation);
    if (indexed.has(key)) {
      throw new Error(`duplicate ${label} observation: ${key}`);
    }
    indexed.set(key, observation);
  }
  return indexed;
}

/**
 * Strictly compare status, body, Cache-Control and Content-Type for every
 * observation tuple. Returns every mismatch with its exact (path, method, auth,
 * field) address so negative controls can name the broken field.
 */
export function diffObservations(baseline, candidate) {
  const baselineIndex = indexObservations(baseline, 'baseline');
  const candidateIndex = indexObservations(candidate, 'candidate');
  const mismatches = [];
  let matched = 0;

  for (const [key, before] of baselineIndex) {
    const after = candidateIndex.get(key);
    if (!after) {
      mismatches.push({
        path: before.path,
        method: before.method,
        auth: before.auth,
        field: 'observation',
        baseline: 'present',
        candidate: 'missing',
      });
      continue;
    }

    let differs = false;
    for (const field of OBSERVATION_FIELDS) {
      if (before[field] === after[field]) continue;
      differs = true;
      mismatches.push({
        path: before.path,
        method: before.method,
        auth: before.auth,
        field,
        baseline: before[field],
        candidate: after[field],
      });
    }
    if (!differs) matched += 1;
  }

  for (const [key, after] of candidateIndex) {
    if (baselineIndex.has(key)) continue;
    mismatches.push({
      path: after.path,
      method: after.method,
      auth: after.auth,
      field: 'observation',
      baseline: 'missing',
      candidate: 'present',
    });
  }

  return {
    ok: mismatches.length === 0,
    expected: baselineIndex.size,
    actual: candidateIndex.size,
    matched,
    mismatches,
  };
}

export function formatMismatch(mismatch) {
  const tuple = `${mismatch.method} ${mismatch.path} [auth=${mismatch.auth}]`;
  if (mismatch.field === 'observation') {
    return `${tuple} observation: baseline=${JSON.stringify(mismatch.baseline)} candidate=${JSON.stringify(mismatch.candidate)}`;
  }
  return `${tuple} ${mismatch.field}: baseline=${JSON.stringify(mismatch.baseline)} candidate=${JSON.stringify(mismatch.candidate)}`;
}

export function assertNoMismatches(result) {
  if (result.ok) return;
  const error = new Error(
    `Routing contract failed: ${result.mismatches.length} mismatch(es) across ${result.expected} baseline observations\n` +
      result.mismatches.map((mismatch) => `  - ${formatMismatch(mismatch)}`).join('\n'),
  );
  error.result = result;
  throw error;
}

async function main() {
  const args = process.argv.slice(2);
  const valueOf = (flag) => {
    const index = args.indexOf(flag);
    return index === -1 ? undefined : args[index + 1];
  };

  const directory = path.resolve(valueOf('--dir') ?? process.cwd());
  const baselineDirectory = valueOf('--baseline-dir');
  const outFile = valueOf('--out');

  const observations = await captureObservations(directory);
  if (outFile) {
    await writeFile(path.resolve(outFile), `${JSON.stringify(observations, null, 2)}\n`);
  }

  if (!baselineDirectory) {
    process.stdout.write(`${JSON.stringify(observations, null, 2)}\n`);
    return;
  }

  const baseline = await captureObservations(path.resolve(baselineDirectory));
  const result = diffObservations(baseline, observations);
  if (result.ok) {
    process.stdout.write(
      `routing contract: ${result.matched}/${result.expected} observations matched\n`,
    );
    return;
  }

  process.stderr.write(
    `routing contract: ${result.matched}/${result.expected} matched, ${result.mismatches.length} mismatch(es)\n`,
  );
  for (const mismatch of result.mismatches) {
    process.stderr.write(`  - ${formatMismatch(mismatch)}\n`);
  }
  process.exitCode = 1;
}

const isMain =
  process.argv[1] !== undefined &&
  import.meta.url === pathToFileURL(process.argv[1]).href;

if (isMain) {
  main().catch((error) => {
    console.error(error?.stack ?? error);
    process.exitCode = 1;
  });
}
