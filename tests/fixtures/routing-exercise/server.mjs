#!/usr/bin/env node
/**
 * Baseline routing fixture.
 *
 * A deliberately monolithic switch/table router used by the routing rehearsal.
 * It serves 23 paths. The verifier probes every path with GET, POST and DELETE
 * under both an anonymous and a valid-session auth state (23 x 3 x 2 = 138
 * observations).
 *
 * The routing core between the "routing core" and "HTTP adapter" markers is a
 * real refactor seam: a production change may move it into modules, provided
 * the legacy callers in router.test.mjs are migrated with it.
 */
import http from 'node:http';
import { pathToFileURL } from 'node:url';

// --- routing core -----------------------------------------------------------

const JSON_TYPE = 'application/json; charset=utf-8';
const TEXT_TYPE = 'text/plain; charset=utf-8';
const SESSION_TOKEN = 'fixture-session-token';

function makeResponse(status, body, cacheControl, contentType) {
  const headers = { 'cache-control': cacheControl };
  if (contentType !== null) {
    headers['content-type'] = contentType;
  }
  return { status, headers, body };
}

function json(status, body, cacheControl = 'no-store', contentType = JSON_TYPE) {
  return makeResponse(status, JSON.stringify(body), cacheControl, contentType);
}

function text(status, body, cacheControl = 'public, max-age=300') {
  return makeResponse(status, body, cacheControl, TEXT_TYPE);
}

function empty(status, cacheControl = 'no-store') {
  return makeResponse(status, '', cacheControl, null);
}

function error(status, code) {
  return json(status, { error: code });
}

export const ROUTES = [
  {
    path: '/',
    methods: ['GET'],
    auth: 'public',
    handle: () => text(200, 'routing fixture\n', 'public, max-age=300'),
  },
  {
    path: '/health',
    methods: ['GET'],
    auth: 'public',
    handle: () => json(200, { status: 'ok' }),
  },
  {
    path: '/version',
    methods: ['GET'],
    auth: 'public',
    handle: () => json(200, { version: '1.0.0' }, 'public, max-age=300'),
  },
  {
    path: '/api/users',
    methods: ['GET', 'POST'],
    auth: 'session',
    handle: ({ method }) =>
      method === 'GET'
        ? json(200, { users: [{ id: 1, name: 'Ada' }, { id: 2, name: 'Grace' }] }, 'private, max-age=60')
        : json(201, { id: 3, name: 'New User' }),
  },
  {
    path: '/api/users/me',
    methods: ['GET'],
    auth: 'session',
    handle: () => json(200, { id: 1, name: 'Ada' }, 'private, max-age=60'),
  },
  {
    path: '/api/users/42',
    methods: ['GET', 'DELETE'],
    auth: 'session',
    handle: ({ method }) =>
      method === 'GET'
        ? json(200, { id: 42, name: 'Turing' }, 'private, max-age=60')
        : empty(204),
  },
  {
    path: '/api/users/42/avatar',
    methods: ['GET'],
    auth: 'session',
    handle: () => text(200, 'avatar-bytes', 'private, max-age=60'),
  },
  {
    path: '/api/admin/users',
    methods: ['GET', 'DELETE'],
    auth: 'admin',
    handle: () => json(200, { users: [] }),
  },
  {
    path: '/api/admin/metrics',
    methods: ['GET'],
    auth: 'admin',
    handle: () => json(200, { activeSessions: 2 }),
  },
  {
    path: '/api/items',
    methods: ['GET', 'POST'],
    auth: 'public',
    handle: ({ method }) =>
      method === 'GET'
        ? json(200, { items: [{ id: 7, name: 'Widget' }] }, 'public, max-age=300')
        : json(201, { id: 8, name: 'Created' }),
  },
  {
    path: '/api/items/7',
    methods: ['GET', 'DELETE'],
    auth: 'public',
    handle: ({ method }) =>
      method === 'GET'
        ? json(200, { id: 7, name: 'Widget' }, 'public, max-age=300')
        : empty(204),
  },
  {
    path: '/api/items/7/stock',
    methods: ['POST'],
    auth: 'public',
    handle: () => json(200, { id: 7, stock: 12 }),
  },
  {
    path: '/api/orders',
    methods: ['GET', 'POST'],
    auth: 'session',
    handle: ({ method }) =>
      method === 'GET'
        ? json(200, { orders: [{ id: 1001, status: 'paid' }] }, 'private, max-age=60')
        : json(201, { id: 1002, status: 'created' }),
  },
  {
    path: '/api/orders/1001',
    methods: ['GET', 'DELETE'],
    auth: 'session',
    handle: ({ method }) =>
      method === 'GET'
        ? json(200, { id: 1001, status: 'paid' }, 'private, max-age=60')
        : empty(204),
  },
  {
    path: '/api/orders/1001/status',
    methods: ['GET'],
    auth: 'session',
    handle: () => text(200, 'paid\n', 'private, max-age=60'),
  },
  {
    path: '/api/orders/1001/cancel',
    methods: ['POST'],
    auth: 'session',
    handle: () => json(200, { id: 1001, status: 'cancelled' }),
  },
  {
    path: '/api/session',
    methods: ['GET', 'DELETE'],
    auth: 'session',
    handle: ({ method }) =>
      method === 'GET'
        ? json(200, { session: 'fixture-token', user: 1 })
        : empty(204),
  },
  {
    path: '/api/session/refresh',
    methods: ['POST'],
    auth: 'session',
    handle: () => json(200, { session: 'fixture-token', expiresIn: 3600 }),
  },
  {
    path: '/api/search',
    methods: ['GET'],
    auth: 'public',
    handle: () => json(200, { query: '', results: [] }, 'public, max-age=300'),
  },
  {
    path: '/api/files/readme.txt',
    methods: ['GET'],
    auth: 'public',
    handle: () => text(200, 'routing fixture readme\n', 'public, max-age=300'),
  },
  {
    path: '/api/files/missing.txt',
    methods: ['GET'],
    auth: 'public',
    handle: () => error(404, 'not_found'),
  },
  {
    path: '/webhooks/github',
    methods: ['POST'],
    auth: 'public',
    handle: () => json(200, { received: true }),
  },
  {
    path: '/metrics',
    methods: ['GET'],
    auth: 'public',
    handle: () => text(200, 'requests_total 138\n', 'no-store'),
  },
];

export function matchRoute(pathname) {
  return ROUTES.find((route) => route.path === pathname) ?? null;
}

export function checkAuth(route, authorization) {
  if (route.auth === 'public') {
    return { ok: true };
  }
  if (authorization !== `Bearer ${SESSION_TOKEN}`) {
    return { ok: false, status: 401, code: 'unauthorized' };
  }
  if (route.auth === 'admin') {
    return { ok: false, status: 403, code: 'forbidden' };
  }
  return { ok: true };
}

export function renderResponse(pathname, method, authorization) {
  const route = matchRoute(pathname);
  if (!route) {
    return error(404, 'not_found');
  }
  if (!route.methods.includes(method)) {
    const response = error(405, 'method_not_allowed');
    response.headers.allow = route.methods.join(', ');
    return response;
  }
  const auth = checkAuth(route, authorization);
  if (!auth.ok) {
    return error(auth.status, auth.code);
  }
  return route.handle({ method, path: pathname, authorization });
}

// --- HTTP adapter -----------------------------------------------------------

export function handleRequest(request, response) {
  const { pathname } = new URL(request.url, 'http://127.0.0.1');
  const result = renderResponse(pathname, request.method, request.headers.authorization);
  response.writeHead(result.status, result.headers);
  response.end(result.body === '' ? undefined : result.body);
}

export function createServer() {
  return http.createServer(handleRequest);
}

const isMain =
  process.argv[1] !== undefined &&
  import.meta.url === pathToFileURL(process.argv[1]).href;

if (isMain) {
  const port = Number(process.env.PORT ?? 0);
  const server = createServer();
  server.on('error', (error) => {
    console.error(error);
    process.exitCode = 1;
  });
  server.listen(port, '127.0.0.1', () => {
    const address = server.address();
    process.stdout.write(`LISTENING ${address.port}\n`);
  });
  const shutdown = () => {
    server.close(() => process.exit(0));
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}
