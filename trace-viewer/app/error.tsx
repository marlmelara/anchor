"use client";

/**
 * An error boundary. Next.js renders this when a route throws.
 *
 * It must be a Client Component: it holds a reset handler, which is
 * interactivity, and interactivity only exists in the browser.
 */
export default function Error({ error, reset }: { error: Error; reset: () => void }) {
  return (
    <div className="rounded border border-red-900 bg-red-950/40 p-6">
      <h2 className="font-semibold text-red-300">Could not load this page</h2>
      <p className="mt-2 font-mono text-sm text-red-200/80">{error.message}</p>
      <p className="mt-3 text-sm text-neutral-400">
        Is <code className="font-mono">anchor-server</code> running, and is{" "}
        <code className="font-mono">ANCHOR_URL</code> pointing at it?
      </p>
      <button
        onClick={reset}
        className="mt-4 rounded bg-neutral-800 px-3 py-1.5 text-sm hover:bg-neutral-700"
      >
        Try again
      </button>
    </div>
  );
}
