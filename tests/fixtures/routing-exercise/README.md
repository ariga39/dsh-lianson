# Routing exercise fixture

Self-contained reference app for the issue #8 routing rehearsal. The generated
temporary git repository is built from these files.

- `server.mjs` — monolithic baseline router serving 23 paths.
- `router.test.mjs` — legacy unit tests that import internal routing helpers,
  so a modular route refactor must migrate these callers too.
- `verify-138.mjs` — standalone live verifier. Probes 23 paths x 3 methods
  (`GET`, `POST`, `DELETE`) x 2 auth states (`anonymous`, `session`) and compares
  HTTP status, response body, `Cache-Control` and `Content-Type`.

```sh
node verify-138.mjs --dir .
node verify-138.mjs --baseline-dir ../baseline --dir .
node --test router.test.mjs
```

The fixture deliberately mixes status codes (`200`, `201`, `204`, `401`, `403`,
`404`, `405`), JSON/text/empty bodies, cache policies (`no-store`,
`private, max-age=60`, `public, max-age=300`) and content types (JSON, text,
absent on `204`).
