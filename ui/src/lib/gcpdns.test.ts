import { describe, expect, it, vi } from "vitest";

import {
  DNS_PROXY_BASE,
  deleteRecordSet,
  listChanges,
  listRecordSets,
  listZones,
  parseChange,
  parseZone,
  summarizeChange,
} from "./gcpdns";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

function reply(f: (url: string) => Response) {
  return vi.fn((...a: unknown[]) => Promise.resolve(f(...(a as [string]))));
}

describe("parsing", () => {
  it("defaults zone visibility to public", () => {
    expect(parseZone({ name: "z", dnsName: "example.com.", nameServers: ["ns1."] })).toMatchObject({
      visibility: "public",
      nameServers: ["ns1."],
    });
  });

  it("summarises changes as removals then additions", () => {
    const c = parseChange({
      id: "1",
      status: "done",
      deletions: [{ name: "a.example.com.", type: "A", rrdatas: ["1.1.1.1"] }],
      additions: [{ name: "a.example.com.", type: "A", ttl: 60, rrdatas: ["2.2.2.2"] }],
    });
    expect(summarizeChange(c)).toEqual([
      "- a.example.com. A 1.1.1.1",
      "+ a.example.com. A 2.2.2.2",
    ]);
  });
});

describe("requests", () => {
  it("follows page tokens for zones", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce(json({ managedZones: [{ name: "a" }], nextPageToken: "t" }))
      .mockResolvedValueOnce(json({ managedZones: [{ name: "b" }] }));
    const zones = await listZones("p", fetchFn as unknown as typeof fetch);
    expect(zones.map((z) => z.name)).toEqual(["a", "b"]);
    expect(String(fetchFn.mock.calls[1]?.[0])).toContain("pageToken=t");
  });

  it("lists record sets and changes newest first", async () => {
    const fetchFn = reply((url: string) =>
      url.includes("/changes")
        ? json({ changes: [{ id: "2" }] })
        : json({
            rrsets: [{ name: "www.example.com.", type: "A", ttl: 300, rrdatas: ["192.0.2.1"] }],
          }),
    );
    expect((await listRecordSets("p", "z", fetchFn as unknown as typeof fetch))[0]?.ttl).toBe(300);
    await listChanges("p", "z", fetchFn as unknown as typeof fetch);
    expect(String(fetchFn.mock.calls[1]?.[0])).toContain("sortOrder=descending");
  });

  it("encodes the record name and type when deleting", async () => {
    const fetchFn = reply(() => json({}));
    await deleteRecordSet(
      "p",
      "z",
      { name: "a.example.com.", type: "A", ttl: 1, rrdatas: [] },
      fetchFn as unknown as typeof fetch,
    );
    expect(fetchFn).toHaveBeenCalledWith(
      `${DNS_PROXY_BASE}/projects/p/managedZones/z/rrsets/a.example.com./A`,
      { method: "DELETE" },
    );
  });
});
