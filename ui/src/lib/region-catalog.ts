// The full AWS region catalog (~36 regions), for the region picker's
// autocomplete. Distinct from `region-data.ts`'s "regions with data" list:
// this is every region the emulator knows about via EC2 DescribeRegions,
// not just the ones holding resources. Module-level cache: the catalog
// doesn't change within a session, and every page could otherwise trigger
// its own DescribeRegions call.
import { DescribeRegionsCommand } from "@aws-sdk/client-ec2";
import { getEC2Client } from "$lib/aws-client";

let cache: string[] | null = null;
let inflight: Promise<string[]> | null = null;

export function fetchRegionCatalog(): Promise<string[]> {
  if (cache) return Promise.resolve(cache);
  if (!inflight) {
    inflight = getEC2Client()
      .send(new DescribeRegionsCommand({}))
      .then((res) => {
        const names = (res.Regions ?? [])
          .map((r) => r.RegionName)
          .filter((n): n is string => Boolean(n))
          .toSorted((a, b) => a.localeCompare(b));
        cache = names;
        return names;
      })
      .catch(() => [])
      .finally(() => {
        inflight = null;
      });
  }
  return inflight;
}

/** Test-only: clears the module-level cache between test cases. */
export function resetRegionCatalogCache(): void {
  cache = null;
  inflight = null;
}
