import { anchorHeaders, streamUrl } from "@/lib/anchor";

/**
 * Proxies anchor-server's SSE stream to the browser.
 *
 * A Route Handler, which runs on the SERVER. The browser cannot call
 * anchor-server directly without being given the API key, and a key in browser
 * JavaScript is a key published to every visitor. The proxy keeps it here.
 *
 * The upstream body is piped through untouched, so this adds a hop but no
 * buffering -- the events reach the browser as they arrive.
 */
export async function GET(
  request: Request,
  { params }: { params: Promise<{ id: string }> },
) {
  const { id } = await params;
  const after = Number(new URL(request.url).searchParams.get("after") ?? 0);

  const upstream = await fetch(streamUrl(id, Number.isFinite(after) ? after : 0), {
    headers: anchorHeaders(),
    cache: "no-store",
    // When the browser disconnects, this signal aborts the upstream request
    // too, so a closed tab does not leave a stream open on the server.
    signal: request.signal,
  });

  if (!upstream.ok || !upstream.body) {
    return new Response(`event: error\ndata: {"error":"upstream ${upstream.status}"}\n\n`, {
      status: upstream.status === 404 ? 404 : 502,
      headers: { "content-type": "text/event-stream" },
    });
  }

  return new Response(upstream.body, {
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
      "x-accel-buffering": "no",
    },
  });
}
