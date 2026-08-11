// "Regions with data", for multi-region fan-out -- distinct from
// `region-catalog.ts`'s full AWS region list (used by the region picker's
// autocomplete). Backed by GET /dashboard/api/system/regions, contract:
// `{ "regions": ["us-east-1", "eu-west-1"] }`, sorted, deduplicated,
// possibly empty.
import { DEFAULT_REGION } from "$lib/region.svelte";

type RegionsWithDataResponse = { regions?: unknown };

async function fetchRegionsWithData(): Promise<string[]> {
  try {
    const res = await fetch("/dashboard/api/system/regions");
    // A 404 (older server build without this endpoint) is handled the
    // same as an empty list, not an error -- this endpoint is new and
    // callers must not be blocked by it being absent.
    if (!res.ok) return [];
    const body = (await res.json()) as RegionsWithDataResponse;
    if (!Array.isArray(body.regions)) return [];
    return body.regions.filter((r): r is string => typeof r === "string");
  } catch {
    return [];
  }
}

/**
 * Regions to fan a multi-region call out across. Falls back to just
 * `DEFAULT_REGION` when the backend reports no regions with data yet.
 */
export async function regionsForFanout(): Promise<string[]> {
  const regions = await fetchRegionsWithData();
  return regions.length > 0 ? regions : [DEFAULT_REGION];
}
