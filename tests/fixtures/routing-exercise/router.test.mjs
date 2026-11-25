import test from 'node:test';
import assert from 'node:assert/strict';

// Legacy callers: these import internal routing helpers directly from
// server.mjs. A production route refactor that moves handlers into modules
// must migrate these imports (and the renderResponse call shapes) with it.
import { ROUTES, matchRoute, checkAuth, renderResponse } from './server.mjs';

const SESSION = 'Bearer fixture-session-token';

test('route table exposes 23 distinct paths', () => {
  assert.equal(ROUTES.length, 23);
  assert.equal(new Set(ROUTES.map((route) => route.path)).size, 23);
});

test('matchRoute resolves known paths and rejects unknown paths', () => {
  assert.equal(matchRoute('/health')?.path, '/health');
  assert.equal(matchRoute('/api/orders/1001/status')?.methods.join(','), 'GET');
  assert.equal(matchRoute('/api/files/missing.txt')?.auth, 'public');
  assert.equal(matchRoute('/does-not-exist'), null);
});

test('checkAuth distinguishes public, session and admin routes', () => {
  assert.deepEqual(checkAuth({ auth: 'public' }, undefined), { ok: true });
  assert.equal(checkAuth({ auth: 'session' }, undefined).status, 401);
  assert.deepEqual(checkAuth({ auth: 'session' }, SESSION), { ok: true });
  assert.equal(checkAuth({ auth: 'admin' }, SESSION).status, 403);
});

test('renderResponse preserves JSON, text and empty responses', () => {
  const health = renderResponse('/health', 'GET', undefined);
  assert.equal(health.status, 200);
  assert.equal(health.headers['content-type'], 'application/json; charset=utf-8');
  assert.equal(health.headers['cache-control'], 'no-store');
  assert.equal(health.body, '{"status":"ok"}');

  const readme = renderResponse('/api/files/readme.txt', 'GET', undefined);
  assert.equal(readme.headers['content-type'], 'text/plain; charset=utf-8');
  assert.equal(readme.headers['cache-control'], 'public, max-age=300');
  assert.equal(readme.body, 'routing fixture readme\n');

  const deleted = renderResponse('/api/users/42', 'DELETE', SESSION);
  assert.equal(deleted.status, 204);
  assert.equal(deleted.headers['content-type'], undefined);
  assert.equal(deleted.body, '');
});

test('renderResponse reports auth, missing-resource and method failures', () => {
  const anonymous = renderResponse('/api/users', 'GET', undefined);
  assert.equal(anonymous.status, 401);
  assert.equal(anonymous.body, '{"error":"unauthorized"}');

  const forbidden = renderResponse('/api/admin/users', 'GET', SESSION);
  assert.equal(forbidden.status, 403);
  assert.equal(forbidden.body, '{"error":"forbidden"}');

  const missing = renderResponse('/api/files/missing.txt', 'GET', undefined);
  assert.equal(missing.status, 404);
  assert.equal(missing.headers['cache-control'], 'no-store');

  const wrongMethod = renderResponse('/health', 'POST', undefined);
  assert.equal(wrongMethod.status, 405);
  assert.equal(wrongMethod.headers.allow, 'GET');
});
