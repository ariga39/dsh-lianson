export async function connectOperator({ upstream, launchUrl }) {
  const url = new URL(launchUrl)
  const token = url.searchParams.get('token')
  if (token === null) throw new Error('launch URL has no token')

  const connectUrl = new URL('/connect', url.origin)
  connectUrl.searchParams.set('token', token)
  connectUrl.searchParams.set('interactions', 'answer')
  const response = await fetch(connectUrl, { redirect: 'manual', headers: { accept: 'application/json' } })
  if (response.status !== 302) throw new Error(`/connect expected 302, got ${response.status}`)
  const setCookie = response.headers.getSetCookie()[0]
  if (setCookie === undefined) throw new Error('/connect returned no Set-Cookie')
  const cookie = setCookie.split(';', 1)[0]

  const ctx = new upstream.Context()
  const base = `${url.origin}/`
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
