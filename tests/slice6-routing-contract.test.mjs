import test from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { promisify } from 'node:util';

// Issue #8 phase A only: prove the self-contained routing fixture and live
// 138-observation verifier before the later Web/unattended delegation flow.
// This file deliberately has no dsh-liaison CLI or dsh web dependency.

import { createRoutingFixture, MUTATIONS } from './support/routing-fixture.mjs';
import {
  AUTH_STATES,
  EXPECTED_OBSERVATIONS,
  METHODS,
  PATHS,
  assertNoMismatches,
  captureObservations,
  diffObservations,
  formatMismatch,
  observationKey,
} from './fixtures/routing-exercise/verify-138.mjs';

const execFileAsync = promisify(execFile);

function childTestEnvironment() {
  const environment = { ...process.env, NO_COLOR: '1' };
  for (const key of Object.keys(environment)) {
    if (key.startsWith('NODE_TEST')) delete environment[key];
  }
  return environment;
}

async function withFixture(t) {
  const fixture = await createRoutingFixture();
  t.after(() => fixture.cleanup());
  return fixture;
}

test(
  'baseline fixture yields 138/138 stable live observations',
  { timeout: 30_000 },
  async (t) => {
    const fixture = await withFixture(t);

    const baseline = await captureObservations(fixture.directory);
    assert.equal(baseline.length, EXPECTED_OBSERVATIONS);
    assert.equal(baseline.length, 138);

    const keys = new Set(baseline.map(observationKey));
    assert.equal(keys.size, EXPECTED_OBSERVATIONS);
    for (const requestPath of PATHS) {
      for (const method of METHODS) {
        for (const auth of AUTH_STATES) {
          assert.ok(
            keys.has(`${method} ${requestPath} ${auth}`),
            `missing observation: ${method} ${requestPath} [auth=${auth}]`,
          );
        }
      }
    }

    const statuses = new Set(baseline.map((observation) => observation.status));
    for (const status of [200, 201, 204, 401, 403, 404, 405]) {
      assert.ok(statuses.has(status), `fixture is missing status ${status}`);
    }

    const cachePolicies = new Set(baseline.map((observation) => observation.cacheControl));
    for (const policy of ['no-store', 'private, max-age=60', 'public, max-age=300']) {
      assert.ok(cachePolicies.has(policy), `fixture is missing Cache-Control ${policy}`);
    }

    const contentTypes = new Set(baseline.map((observation) => observation.contentType));
    assert.ok(contentTypes.has('application/json; charset=utf-8'));
    assert.ok(contentTypes.has('text/plain; charset=utf-8'));
    assert.ok(contentTypes.has(null), 'fixture must have a 204 response without Content-Type');

    for (const observation of baseline.filter((item) => item.status === 204)) {
      assert.equal(observation.body, '', `${observationKey(observation)} must have an empty body`);
      assert.equal(observation.contentType, null, `${observationKey(observation)} must omit Content-Type`);
    }

    // A second live capture from the same baseline must match on all four
    // contract fields. No observation is loaded from disk.
    const replay = await captureObservations(fixture.directory);
    const result = diffObservations(baseline, replay);
    assertNoMismatches(result);
    assert.equal(result.matched, 138);
    assert.equal(result.expected, 138);
    assert.equal(result.actual, 138);
  },
);

test('baseline legacy router tests pass', { timeout: 30_000 }, async (t) => {
  const fixture = await withFixture(t);
  const { stdout, stderr } = await execFileAsync(process.execPath, ['--test', 'router.test.mjs'], {
    cwd: fixture.directory,
    encoding: 'utf8',
    env: childTestEnvironment(),
  });
  const output = `${stdout}${stderr}`;
  assert.match(output, /pass 5\b/);
  assert.match(output, /fail 0\b/);
});

for (const field of ['status', 'body', 'cacheControl', 'contentType']) {
  test(
    `negative control: live ${field} mutation is reported at the exact tuple`,
    { timeout: 30_000 },
    async (t) => {
      const fixture = await withFixture(t);
      const baseline = await captureObservations(fixture.directory);

      const worktree = await fixture.addWorktree(`mutation-${field}`);
      await fixture.mutateServer(worktree, MUTATIONS[field]);
      const candidate = await captureObservations(worktree);

      const result = diffObservations(baseline, candidate);
      assert.equal(result.ok, false);
      assert.equal(
        result.mismatches.length,
        1,
        result.mismatches.map(formatMismatch).join('\n'),
      );

      const [mismatch] = result.mismatches;
      assert.deepEqual(
        {
          path: mismatch.path,
          method: mismatch.method,
          auth: mismatch.auth,
          field: mismatch.field,
        },
        {
          path: '/api/session',
          method: 'GET',
          auth: 'session',
          field,
        },
      );
      assert.equal(result.matched, 137);

      assert.throws(
        () => assertNoMismatches(result),
        (error) =>
          error.message.includes('Routing contract failed') &&
          error.message.includes(`GET /api/session [auth=session] ${field}`),
      );
    },
  );
}

test(
  'truncated response body rejects promptly and reaps the fixture server',
  { timeout: 20_000 },
  async (t) => {
    const fixture = await withFixture(t);
    const worktree = await fixture.addWorktree('truncated-body');
    await fixture.mutateServer(worktree, MUTATIONS.truncatedBody);

    let serverPid;
    const startedAt = Date.now();
    await assert.rejects(
      captureObservations(worktree, {
        onServerStart: ({ pid }) => {
          serverPid = pid;
        },
      }),
      (error) =>
        error.message.includes('truncated response') &&
        error.message.includes('GET /health'),
    );
    const captureElapsed = Date.now() - startedAt;
    assert.ok(captureElapsed < 4_000, `truncated capture took ${captureElapsed}ms`);
    assert.ok(Number.isInteger(serverPid) && serverPid > 0, 'onServerStart must expose a pid');
    assert.throws(() => process.kill(serverPid, 0), { code: 'ESRCH' });

    // The standalone CLI must fail fast, after the same finally-based cleanup.
    const cliStartedAt = Date.now();
    let cliError;
    try {
      await execFileAsync(
        process.execPath,
        [path.join(fixture.directory, 'verify-138.mjs'), '--dir', worktree],
        { encoding: 'utf8', env: childTestEnvironment() },
      );
    } catch (error) {
      cliError = error;
    }
    const cliElapsed = Date.now() - cliStartedAt;
    assert.ok(cliError, 'verifier CLI must exit nonzero for a truncated response');
    assert.equal(cliError.code, 1);
    assert.match(`${cliError.stdout}${cliError.stderr}`, /truncated response for GET \/health/);
    assert.ok(cliElapsed < 4_000, `truncated CLI took ${cliElapsed}ms`);
  },
);

test(
  'independent modular refactor and legacy test migration preserve 138/138',
  { timeout: 30_000 },
  async (t) => {
    const fixture = await withFixture(t);
    const baseline = await captureObservations(fixture.directory);

    const worktree = await fixture.addWorktree('refactor', { branch: 'refactor' });

    // Perform a real structural refactor in the worktree: extract the routing
    // core into router-core.mjs, leave a thin HTTP adapter in server.mjs, and
    // migrate the legacy unit-test imports.
    const server = await readFile(path.join(worktree, 'server.mjs'), 'utf8');
    const coreStart = server.indexOf('// --- routing core');
    const adapterStart = server.indexOf('// --- HTTP adapter');
    assert.ok(coreStart > -1 && adapterStart > coreStart, 'fixture must keep the refactor seam');

    const headerEnd = server.indexOf('\n', server.indexOf("import { pathToFileURL } from 'node:url';"));
    const core = server.slice(coreStart, adapterStart).trimEnd();
    const adapter = server.slice(adapterStart);
    const exports = ['makeResponse', 'json', 'text', 'empty', 'error'];

    await writeFile(
      path.join(worktree, 'router-core.mjs'),
      `${core}\n\nexport { ${exports.join(', ')} };\n`,
    );
    await writeFile(
      path.join(worktree, 'server.mjs'),
      `${server.slice(0, headerEnd + 1)}import { renderResponse } from './router-core.mjs';\n\n${adapter}`,
    );

    const legacyTests = await readFile(path.join(worktree, 'router.test.mjs'), 'utf8');
    assert.ok(legacyTests.includes("from './server.mjs'"));
    await writeFile(
      path.join(worktree, 'router.test.mjs'),
      legacyTests.replace("from './server.mjs'", "from './router-core.mjs'"),
    );

    const { stdout, stderr } = await execFileAsync(process.execPath, ['--test', 'router.test.mjs'], {
      cwd: worktree,
      encoding: 'utf8',
      env: childTestEnvironment(),
    });
    const output = `${stdout}${stderr}`;
    assert.match(output, /pass 5\b/);
    assert.match(output, /fail 0\b/);

    const candidate = await captureObservations(worktree);
    const result = diffObservations(baseline, candidate);
    assertNoMismatches(result);
    assert.equal(result.matched, 138);
  },
);
