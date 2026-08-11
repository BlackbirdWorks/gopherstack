import { currentRegion, isAllRegions } from "$lib/region.svelte";
import { regionsForFanout } from "$lib/region-data";

export type RegionedItem<T> = { region: string; item: T };

export type MultiRegionListResult<T> = {
  items: RegionedItem<T>[];
  errors: { region: string; error: unknown }[];
};

/**
 * Fans a list-style SDK call out across every region with data when "All"
 * is selected, tagging each result row with the region it came from. In
 * single-region mode this collapses to exactly one call against the
 * currently selected region -- identical behavior to calling `regionCall`
 * directly.
 *
 * `regionCall` takes the region and returns the SDK response, e.g.
 * `(region) => getDAXClient(region).send(new DescribeClustersCommand({}))`
 * -- a NEW client is built for every region this way, never reused across
 * regions: `@aws-sdk/core` freezes a client's SigV4 signing region on its
 * first request (`config.signingRegion = config.signingRegion ||
 * signingRegion` in `resolveAwsSdkSigV4Config.js`), so a client built once
 * and sent requests for two different regions would sign the second
 * region's request as if it were still the first. See
 * region-effect.svelte.ts for the full trace through the vendored SDK.
 *
 * Letting the caller write the `.send(command)` call itself (rather than
 * this helper taking a client factory and a command separately) sidesteps
 * a real TypeScript limitation: the AWS SDK v3 `send()` method is generic
 * per call, and passing it through an extra layer of structural typing
 * here loses that per-call inference, widening every command down to the
 * client's broadest `ServiceInputTypes`/`ServiceOutputTypes` union. A
 * direct call like the one in the example above type-checks normally.
 */
export async function multiRegionList<TResponse, TItem>(
  regionCall: (region: string) => Promise<TResponse>,
  extractItems: (response: TResponse) => TItem[],
): Promise<MultiRegionListResult<TItem>> {
  const regions = isAllRegions() ? await regionsForFanout() : [currentRegion()];

  const settled = await Promise.allSettled(
    regions.map(async (region) => ({ region, items: extractItems(await regionCall(region)) })),
  );

  // Single-region mode must be behaviorally identical to calling
  // `regionCall(region)` directly -- including a rejection propagating to
  // the caller's own try/catch, with its original error intact, instead of
  // being swallowed into `errors` below. That swallowing is only correct
  // once there is more than one region in flight, where one region's
  // failure shouldn't hide the others' results.
  if (regions.length === 1 && settled[0].status === "rejected") {
    throw settled[0].reason;
  }

  const items: RegionedItem<TItem>[] = [];
  const errors: { region: string; error: unknown }[] = [];

  settled.forEach((result, i) => {
    if (result.status === "fulfilled") {
      for (const item of result.value.items) {
        items.push({ region: result.value.region, item });
      }
    } else {
      errors.push({ region: regions[i], error: result.reason });
    }
  });

  return { items, errors };
}
