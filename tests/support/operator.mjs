import { scrubTokenDiagnostics } from './scrub.mjs'

export async function connectOperator({ upstream, launchUrl }) {
  const url = new URL(launchUrl)
  const token = url.searchParams.get('token')
  if (token === null) throw new Error('launch URL has no token')
  const scrub = error => new Error(scrubTokenDiagnostics(error instanceof Error ? error.message : String(error), token))

  const connectUrl = new URL('/connect', url.origin)
  connectUrl.searchParams.set('token', token)
  connectUrl.searchParams.set('interactions', 'answer')
  let response
  try {
    response = await fetch(connectUrl, { redirect: 'manual', headers: { accept: 'application/json' } })
  } catch (error) {
    throw scrub(error)
  }
  if (response.status !== 302) throw new Error(`/connect expected 302, got ${response.status}`)
  const setCookie = response.headers.getSetCookie()[0]
  if (setCookie === undefined) throw new Error('/connect returned no Set-Cookie')
  const cookie = setCookie.split(';', 1)[0]

  const ctx = new upstream.Context()
  const base = `${url.origin}/`
  try {
    upstream.installConnection(ctx, {
      interactions: 'answer',
      transport: {
        streamBaseUrl: base,
        fetch: (input, init = {}) => {
          const headers = new Headers(init.headers)
          headers.set('cookie', cookie)
          return fetch(new URL(input, base), { ...init, headers })
        },
        createWebSocket: target => new upstream.WebSocket(target, { headers: { cookie } }),
      },
    })
    await ctx.plugin(upstream.TypertRegistry)
    await ctx.plugin({ apply: upstream.gateway.apply, inject: upstream.gateway.inject })
    for (const contribution of upstream.remotes) await ctx.remote.$mount(contribution)
  } catch (error) {
    await ctx.fiber.dispose().catch(() => {})
    throw scrub(error)
  }

  const ready = async () => {
    const deadline = Date.now() + 30_000
    while (ctx.get('connection')?.generation.getSnapshot() === undefined) {
      if (Date.now() > deadline) throw new Error('operator connection did not become ready')
      await new Promise(resolve => setTimeout(resolve, 50))
    }
  }

  return {
    ctx,
    ready,
    close: () => ctx.fiber.dispose(),
  }
}
