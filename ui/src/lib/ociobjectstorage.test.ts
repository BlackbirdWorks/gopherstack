import { describe, expect, it, vi } from "vitest";

import {
  OS_PROXY_BASE,
  buildEntries,
  deleteBucket,
  deleteObject,
  getLifecycle,
  listBuckets,
  listObjects,
  listPars,
  listRetention,
  objectDownloadUrl,
  parseBucket,
  parseObject,
  prefixCrumbs,
} from "./ociobjectstorage";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

function reply(f: (url: string) => Response) {
  return vi.fn((...a: unknown[]) => Promise.resolve(f(...(a as [string]))));
}

describe("parsing", () => {
  it("parses a bucket with approximate totals", () => {
    const b = parseBucket({
      name: "bk",
      namespace: "ns",
      approximateCount: 3,
      approximateSize: 11,
      freeformTags: { env: "dev", bad: 1 },
    });
    expect(b).toMatchObject({
      name: "bk",
      objectCount: 3,
      sizeBytes: 11,
      freeformTags: { env: "dev" },
    });
  });

  it("defaults a sparse object", () => {
    expect(parseObject({ name: "a" })).toMatchObject({ name: "a", size: 0, metadata: {} });
  });
});

describe("browsing", () => {
  it("builds folder and object entries relative to the prefix", () => {
    const entries = buildEntries(
      "docs/",
      [parseObject({ name: "docs/a.txt", size: 5 })],
      ["docs/sub/"],
    );
    expect(entries.map((e) => [e.kind, e.name])).toEqual([
      ["folder", "sub"],
      ["object", "a.txt"],
    ]);
  });

  it("builds breadcrumbs", () => {
    expect(prefixCrumbs("a/b/")).toEqual([
      { label: "a", prefix: "a/" },
      { label: "b", prefix: "a/b/" },
    ]);
    expect(prefixCrumbs("")).toEqual([]);
  });

  it("lists with prefix and delimiter", async () => {
    const fetchFn = reply(() =>
      json({
        objects: [{ name: "docs/a.txt", size: 5 }],
        prefixes: ["docs/x/"],
        nextStartWith: "n",
      }),
    );
    const got = await listObjects("bk", { prefix: "docs/" }, fetchFn as unknown as typeof fetch);
    expect(fetchFn.mock.calls[0]?.[0]).toBe(
      `${OS_PROXY_BASE}/buckets/bk/objects?limit=200&prefix=docs%2F&delimiter=%2F`,
    );
    expect(got.nextStartWith).toBe("n");
    expect(got.entries).toHaveLength(2);
  });

  it("omits the delimiter for a flat listing", async () => {
    const fetchFn = reply(() => json({ objects: [], prefixes: [] }));
    await listObjects("bk", { delimiter: "" }, fetchFn as unknown as typeof fetch);
    expect(String(fetchFn.mock.calls[0]?.[0])).not.toContain("delimiter");
  });

  it("encodes names in download URLs", () => {
    expect(objectDownloadUrl("b k", "a/b c.txt")).toBe(
      `${OS_PROXY_BASE}/buckets/b%20k/object/content?name=a%2Fb%20c.txt`,
    );
  });
});

describe("requests", () => {
  it("lists buckets, pars, lifecycle and retention", async () => {
    const fetchFn = reply((url: string) => {
      if (url.endsWith("/buckets")) return json({ buckets: [{ name: "bk" }] });
      if (url.endsWith("/pars"))
        return json({ pars: [{ id: "1", name: "p", accessType: "ObjectRead" }] });
      if (url.endsWith("/lifecycle"))
        return json({
          configured: true,
          rules: [
            { name: "r", action: "DELETE", timeAmount: 7, timeUnit: "DAYS", isEnabled: true },
          ],
        });
      return json({
        rules: [{ id: "x", displayName: "keep", duration: { timeAmount: 1, timeUnit: "DAYS" } }],
      });
    }) as unknown as typeof fetch;
    expect((await listBuckets(fetchFn))[0]?.name).toBe("bk");
    expect((await listPars("bk", fetchFn))[0]?.accessType).toBe("ObjectRead");
    expect(await getLifecycle("bk", fetchFn)).toMatchObject({
      configured: true,
      rules: [{ amount: 7, enabled: true }],
    });
    expect((await listRetention("bk", fetchFn))[0]?.duration).toBe("1 days");
  });

  it("deletes via DELETE and surfaces API errors", async () => {
    const ok = reply(() => new Response(null, { status: 204 }));
    await deleteObject("bk", "a b", ok as unknown as typeof fetch);
    expect(ok).toHaveBeenCalledWith(`${OS_PROXY_BASE}/buckets/bk/object?name=a%20b`, {
      method: "DELETE",
    });
    const fail = reply(() =>
      json({ error: { code: 409, status: "BucketNotEmpty", message: "not empty" } }, 409),
    );
    await expect(deleteBucket("bk", fail as unknown as typeof fetch)).rejects.toMatchObject({
      code: "BucketNotEmpty",
      httpStatus: 409,
    });
  });
});
