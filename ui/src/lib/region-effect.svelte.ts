import { currentRegion } from "$lib/region.svelte";

/**
 * Runs `callback` immediately and again every time the active region
 * changes.
 *
 * Because a Svelte `$effect` always runs once when it is created (in
 * addition to whenever its dependencies change), this is a one-line,
 * top-level replacement for `onMount(loadData)`: call
 * `onRegionChange(loadData)` directly in the component script instead of
 * wrapping `loadData` in `onMount`. Do not use both — combining them will
 * run `loadData` twice on mount.
 */
export function onRegionChange(callback: () => void): void {
  $effect(() => {
    currentRegion();
    callback();
  });
}

/**
 * Wraps an AWS SDK client factory (e.g. `getDynamoDBClient` from
 * `$lib/aws-client`) so the returned accessor always hands back a client
 * instance signed for the CURRENTLY selected region — including after a
 * mid-session region switch, with no page reload.
 *
 * ## Why a client needs to be rebuilt at all
 *
 * `clientConfig()` passes `region: region ?? regionProvider`, a
 * `Provider<string>` that the AWS SDK re-invokes on every request — so in
 * principle a single long-lived client should be "live-reactive" to region
 * changes for free. In practice it is not, because of how the vendored SDK
 * resolves the SigV4 signing region. In
 * `@aws-sdk/core/dist-es/submodules/httpAuthSchemes/aws_sdk/resolveAwsSdkSigV4Config.js`,
 * the `signer` closure does:
 *
 * ```js
 * signingRegion: await normalizeProvider(config.region)(),  // re-evaluated every call, correctly
 * ...
 * config.signingRegion = config.signingRegion || signingRegion;  // written once, onto the shared config object
 * ...
 * region: config.signingRegion,  // <- the signer's actual params always read the FROZEN value, not the fresh one above
 * ```
 *
 * `config.signingRegion` is `undefined` only on a client's first signed
 * request; every later request short-circuits on `config.signingRegion ||
 * ...` and keeps signing for whatever region was active at that first
 * request, forever. (The client's *endpoint* has no analogous freeze — this
 * codebase always passes an explicit static `endpoint` override anyway, so
 * endpoint resolution was never dynamic to begin with; the freeze is purely
 * a signing-region problem.) The sibling `regionInfoProvider` branch a few
 * lines up (`config.signingRegion = config.signingRegion || signingRegion ||
 * region;`) has the identical bug shape, but no client factory in
 * `aws-client.ts` configures `regionInfoProvider` (that's a legacy v2-style
 * path), so it's dead code for this app — confirmed by grepping every
 * vendored `@aws-sdk/client-*` package for it.
 *
 * The only fix available from application code is: don't keep signing on
 * the same client past a region change. Construct a new one.
 *
 * ## Lazy (on next access), not eager (in an effect)
 *
 * `regionalClient` exposes the rebuilt client through a getter backed by
 * `$derived`, not through a separate `$effect` that reconstructs the client
 * as a side effect whenever the region changes. `$derived` is pull-based:
 * it only recomputes when actually read, and recomputation happens
 * synchronously at that read, not on the next microtask/effect-flush tick.
 * That has two consequences that matter here:
 *
 *   - A `loadData` already in flight when the region changes keeps using
 *     the client instance it already captured — it finishes against the
 *     region it was called with, which is the only sane behavior for a
 *     request that already left the browser. Nothing needs to reach in and
 *     cancel or redirect it.
 *   - The NEXT time anything calls `client()` — whether that's a manual
 *     "Refresh" button or the callback registered via `onRegionChange`
 *     below — it gets a freshly constructed, correctly-regioned client,
 *     because reading a `$derived` re-evaluates it if a dependency changed
 *     since the last read.
 *
 * An eager design (rebuild inside its own `$effect` as soon as the region
 * changes) would need that effect to run and complete *before* the
 * `onRegionChange(loadData)` effect's callback fires, on every region
 * change, forever. Svelte does not document or guarantee effect-to-effect
 * ordering across two independently-created effects strongly enough to
 * lean on — and getting that ordering wrong is exactly how this bug class
 * reintroduces itself. `$derived`'s pull semantics sidestep the ordering
 * question entirely: there is nothing to order, because the rebuild happens
 * inside the read, not before it.
 *
 * ## Composing with `onRegionChange`
 *
 * A page combines the two like this:
 *
 * ```ts
 * const client = regionalClient(getDetectiveClient);
 *
 * async function loadData() {
 *   const res = await client().send(new ListGraphsCommand({}));
 *   ...
 * }
 *
 * onRegionChange(loadData);
 * ```
 *
 * When the region changes, `onRegionChange`'s effect re-runs `loadData`,
 * which calls `client()` — and because `$derived` recomputes on that very
 * read, `loadData` always sends its request through a client already
 * rebuilt for the new region. No merged primitive is needed: composing the
 * two is correct by construction, not by convention.
 *
 * ## `regionProvider` in `clientConfig()`: kept, not dropped
 *
 * `regionalClient` calls `factory(currentRegion())`, passing the resolved
 * region as a concrete string rather than leaving it undefined (which would
 * fall back to `regionProvider`). That sidesteps the frozen-Provider issue
 * entirely for clients built this way: `clientConfig()` bakes in a literal
 * string, not a function the SDK could ever re-invoke, frozen or not.
 *
 * `regionProvider` itself stays in `clientConfig()` as defense in depth,
 * for two reasons:
 *
 *   1. Most of the ~150 `getXClient()` factories are not yet wrapped in
 *      `regionalClient` (this migration covers five pages; ~156 remain).
 *      Those pages call `getXClient()` directly with no `region` argument,
 *      and rely on `regionProvider` to get the region right the first time
 *      the page mounts after a region switch — dropping it would revert
 *      those pages to being hardcoded to whatever region was active at
 *      first import.
 *   2. Even for a `regionalClient`-wrapped client, `regionProvider` is what
 *      makes a page's very first render (before `regionalClient` has ever
 *      rebuilt anything) correct if that first render itself races a region
 *      change — a narrow case, but a free one to keep covered.
 */
export function regionalClient<T>(factory: (region?: string) => T): () => T {
  const client = $derived(factory(currentRegion()));
  return () => client;
}
