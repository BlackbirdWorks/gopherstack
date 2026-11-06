import { describe, expect, it, vi } from "vitest";

import {
  SEARCH_ARM_BASE,
  SEARCH_DATA_BASE,
  SEARCH_KEYS_BASE,
  buildQueryBody,
  createQueryKey,
  deleteQueryKey,
  emptyQuery,
  executionTone,
  fieldFlags,
  formatBytes,
  getIndexStats,
  getKeys,
  getIndexerStatus,
  getServiceStats,
  listIndexers,
  listIndexes,
  listObjects,
  listServices,
  parseFacetResults,
  parseFacets,
  parseField,
  parseIndex,
  parseIndexerStatus,
  parseService,
  resetIndexer,
  revealAdminKeys,
  revealQueryKey,
  runIndexer,
  runSearch,
  searchUnits,
  validateKeyName,
  validateQuery,
} from "./azuresearch";
import { GcpRestError } from "./gcprest";

const sub = "00000000-0000-0000-0000-000000000000";
const target = { subscription: sub, group: "demo", service: "hotels" };
const base = `/${sub}/demo/hotels`;
const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
const asFetch = (f: unknown) => f as typeof fetch;
const answer = (make: () => Response) => vi.fn(() => Promise.resolve(make()));
const callOf = (f: { mock: { calls: unknown[] } }, i = 0): [string, RequestInit] =>
  f.mock.calls[i] as [string, RequestInit];

describe("service parsing", () => {
  it("parses an ARM search service", () => {
    const s = parseService({
      id: "/subscriptions/s/resourceGroups/demo/providers/Microsoft.Search/searchServices/hotels",
      name: "hotels",
      location: "westus",
      sku: { name: "standard" },
      properties: {
        replicaCount: 3,
        partitionCount: 2,
        hostingMode: "default",
        status: "running",
        provisioningState: "succeeded",
        endpoint: "https://hotels.search.windows.net",
        publicNetworkAccess: "enabled",
      },
    });
    expect(s).toMatchObject({ name: "hotels", sku: "standard", replicas: 3, partitions: 2 });
    expect(searchUnits(s)).toBe(6);
    expect(parseService({}).sku).toBe("");
  });

  it("lists services sorted through the ARM proxy with the api version", async () => {
    const f = answer(() =>
      json({
        value: [
          { name: "b", properties: {} },
          { name: "a", properties: {} },
        ],
      }),
    );
    const out = await listServices(sub, "demo", asFetch(f));
    expect(out.map((s) => s.name)).toEqual(["a", "b"]);
    expect(callOf(f)[0]).toBe(
      `${SEARCH_ARM_BASE}/subscriptions/${sub}/resourceGroups/demo/providers/Microsoft.Search/searchServices?api-version=2025-05-01`,
    );
  });
});

describe("keys", () => {
  it("reads masked keys only", async () => {
    const f = answer(() =>
      json({
        admin: { primary: "••••••••abcd", secondary: "••••••••wxyz" },
        query: [{ name: "web", fp: "0a1b2c3d", masked: "••••••••1234" }],
      }),
    );
    const k = await getKeys(target, asFetch(f));
    expect(k.primary).toBe("••••••••abcd");
    expect(k.query[0]).toEqual({ name: "web", fingerprint: "0a1b2c3d", masked: "••••••••1234" });
    expect(callOf(f)[0]).toBe(`${SEARCH_KEYS_BASE}${base}`);
  });

  it("fetches clear keys only through the reveal calls", async () => {
    const admin = answer(() => json({ primaryKey: "P", secondaryKey: "S" }));
    expect(await revealAdminKeys(target, asFetch(admin))).toEqual({ primary: "P", secondary: "S" });
    expect(callOf(admin)[0]).toBe(`${SEARCH_KEYS_BASE}${base}/admin/reveal`);
    expect(callOf(admin)[1].method).toBe("POST");

    const query = answer(() => json({ name: "web", key: "K" }));
    expect(await revealQueryKey(target, "0a1b2c3d", asFetch(query))).toBe("K");
    expect(callOf(query)[0]).toBe(`${SEARCH_KEYS_BASE}${base}/query/0a1b2c3d/reveal`);
  });

  it("creates and deletes query keys by name and fingerprint", async () => {
    const make = answer(() => json({ name: "web", fp: "11223344", masked: "••••" }, 201));
    const made = await createQueryKey(target, "web", asFetch(make));
    expect(made.fingerprint).toBe("11223344");
    expect(JSON.parse(callOf(make)[1].body as string)).toEqual({ name: "web" });

    const del = answer(() => new Response(null, { status: 204 }));
    await deleteQueryKey(target, "11223344", asFetch(del));
    expect(callOf(del)[0]).toBe(`${SEARCH_KEYS_BASE}${base}/query/11223344`);
    expect(callOf(del)[1].method).toBe("DELETE");
  });

  it("validates query key names", () => {
    expect(validateKeyName("web front-end_1")).toBe("");
    expect(validateKeyName("")).not.toBe("");
    expect(validateKeyName("bad/name")).not.toBe("");
    expect(validateKeyName("x".repeat(65))).not.toBe("");
  });

  it("surfaces a missing service as a typed error", async () => {
    const f = answer(() => json({ error: { code: "ResourceNotFound", message: "gone" } }, 404));
    await expect(getKeys(target, asFetch(f))).rejects.toBeInstanceOf(GcpRestError);
  });
});

describe("indexes", () => {
  const raw = {
    name: "hotels",
    fields: [
      { name: "id", type: "Edm.String", key: true, retrievable: true },
      { name: "name", type: "Edm.String", searchable: true, analyzer: "en.lucene" },
      { name: "rating", type: "Edm.Double", filterable: true, sortable: true, facetable: true },
      { name: "secret", type: "Edm.String", retrievable: false },
      { name: "vec", type: "Collection(Edm.Single)", dimensions: 3 },
    ],
    suggesters: [{ name: "sg" }],
    scoringProfiles: [{ name: "boost" }],
  };

  it("parses fields and flags", () => {
    const idx = parseIndex(raw);
    expect(idx.fields).toHaveLength(5);
    expect(idx.vector).toBe(true);
    expect(idx.suggesters).toEqual(["sg"]);
    expect(fieldFlags(parseField(raw.fields[2]!))).toEqual(["filterable", "sortable", "facetable"]);
    expect(fieldFlags(parseField(raw.fields[3]!))).toEqual(["not retrievable"]);
    expect(fieldFlags(parseField(raw.fields[0]!))).toEqual(["key"]);
  });

  it("lists indexes and statistics", async () => {
    const list = answer(() => json({ value: [raw, { name: "a", fields: [] }] }));
    const out = await listIndexes(target, asFetch(list));
    expect(out.map((i) => i.name)).toEqual(["a", "hotels"]);
    expect(callOf(list)[0]).toBe(`${SEARCH_DATA_BASE}${base}/indexes`);

    const stats = answer(() => json({ documentCount: 7, storageSize: 2048, vectorIndexSize: 0 }));
    expect(await getIndexStats(target, "hotels", asFetch(stats))).toEqual({
      documents: 7,
      storage: 2048,
      vector: 0,
    });
    expect(callOf(stats)[0]).toBe(`${SEARCH_DATA_BASE}${base}/indexes/hotels/search.stats`);
  });

  it("reads service counters", async () => {
    const f = answer(() =>
      json({
        counters: {
          documentCount: { usage: 12 },
          storageSize: { usage: 4096 },
          indexesCount: { usage: 2, quota: 50 },
          indexersCount: { usage: 1 },
          dataSourcesCount: { usage: 1 },
          skillsetCount: { usage: 0 },
          synonymMaps: { usage: 3 },
        },
      }),
    );
    expect(await getServiceStats(target, asFetch(f))).toMatchObject({
      documents: 12,
      indexes: 2,
      indexesQuota: 50,
      synonymMaps: 3,
    });
  });

  it("formats bytes", () => {
    expect(formatBytes(0)).toBe("0 B");
    expect(formatBytes(1536)).toBe("1.50 KB");
    expect(formatBytes(5 * 1024 * 1024)).toBe("5.00 MB");
  });
});

describe("search explorer", () => {
  it("builds a body from fixed fields and leaves empty ones out", () => {
    expect(buildQueryBody({ ...emptyQuery, search: "" })).toEqual({
      search: "*",
      count: true,
      top: 10,
    });
    const body = buildQueryBody({
      ...emptyQuery,
      search: "luxury",
      filter: " rating ge 4 ",
      orderby: "rating desc",
      select: "name,rating",
      facets: "category\nrating,interval:1\n\n",
      top: "5",
      skip: "10",
      queryType: "full",
      searchMode: "all",
      count: false,
    });
    expect(body).toEqual({
      search: "luxury",
      count: false,
      filter: "rating ge 4",
      orderby: "rating desc",
      select: "name,rating",
      queryType: "full",
      searchMode: "all",
      facets: ["category", "rating,interval:1"],
      top: 5,
      skip: 10,
    });
  });

  it("keeps commas inside one facet", () => {
    expect(parseFacets("a\n b,count:5 \n")).toEqual(["a", "b,count:5"]);
  });

  it("validates numbers and facet count", () => {
    expect(validateQuery(emptyQuery)).toBe("");
    expect(validateQuery({ ...emptyQuery, top: "-1" })).toContain("Top");
    expect(validateQuery({ ...emptyQuery, top: "1001" })).toContain("Top");
    expect(validateQuery({ ...emptyQuery, skip: "abc" })).toContain("Skip");
    expect(validateQuery({ ...emptyQuery, facets: "f\n".repeat(33) })).toContain("facets");
  });

  it("parses value and range facets", () => {
    const groups = parseFacetResults({
      category: [
        { value: "hotel", count: 3 },
        { value: "motel", count: 1 },
      ],
      rating: [
        { from: 0, to: 4, count: 2 },
        { from: 4, count: 2 },
      ],
    });
    expect(groups[0]).toEqual({
      field: "category",
      values: [
        { label: "hotel", count: 3 },
        { label: "motel", count: 1 },
      ],
    });
    expect(groups[1]!.values.map((v) => v.label)).toEqual(["0 – 4", "4 – …"]);
    expect(parseFacetResults(null)).toEqual([]);
  });

  it("posts the query and reads count, rows and facets", async () => {
    const f = answer(() =>
      json({
        "@odata.count": 2,
        "@search.facets": { category: [{ value: "hotel", count: 2 }] },
        value: [{ id: "1" }, { id: "2" }],
      }),
    );
    let t = 100;
    const res = await runSearch(
      target,
      "hotels",
      { ...emptyQuery, search: "x" },
      asFetch(f),
      () => (t += 25),
    );
    expect(res.count).toBe(2);
    expect(res.rows).toHaveLength(2);
    expect(res.facets[0]!.values[0]!.count).toBe(2);
    expect(res.elapsedMs).toBe(25);
    const [url, init] = callOf(f);
    expect(url).toBe(`${SEARCH_DATA_BASE}${base}/indexes/hotels/docs/search.post.search`);
    expect(init.method).toBe("POST");
    expect(JSON.parse(init.body as string)).toMatchObject({ search: "x" });
  });

  it("reports an absent count", async () => {
    const f = answer(() => json({ value: [] }));
    expect((await runSearch(target, "i", emptyQuery, asFetch(f))).count).toBeUndefined();
  });
});

describe("indexers", () => {
  it("parses status and history", () => {
    const st = parseIndexerStatus({
      status: "running",
      lastResult: {
        status: "success",
        itemsProcessed: 5,
        itemsFailed: 1,
        startTime: "a",
        endTime: "b",
      },
      executionHistory: [
        { status: "success", itemsProcessed: 5 },
        { status: "transientFailure", errorMessage: "boom" },
      ],
    });
    expect(st.last).toMatchObject({ status: "success", processed: 5, failed: 1 });
    expect(st.history).toHaveLength(2);
    expect(st.history[1]!.error).toBe("boom");
    expect(parseIndexerStatus({ status: "unknown", lastResult: null }).last).toBeUndefined();
  });

  it("colors execution statuses", () => {
    expect(executionTone("success")).toBe("good");
    expect(executionTone("transientFailure")).toBe("bad");
    expect(executionTone("inProgress")).toBe("busy");
    expect(executionTone("reset")).toBe("muted");
  });

  it("lists, reads status, runs and resets", async () => {
    const list = answer(() =>
      json({
        value: [
          {
            name: "ix",
            dataSourceName: "ds",
            targetIndexName: "hotels",
            schedule: { interval: "PT5M" },
          },
        ],
      }),
    );
    const out = await listIndexers(target, asFetch(list));
    expect(out[0]).toMatchObject({
      name: "ix",
      dataSource: "ds",
      target: "hotels",
      schedule: "PT5M",
    });

    const status = answer(() => json({ status: "unknown", executionHistory: [] }));
    expect((await getIndexerStatus(target, "ix", asFetch(status))).status).toBe("unknown");
    expect(callOf(status)[0]).toBe(`${SEARCH_DATA_BASE}${base}/indexers/ix/search.status`);

    const run = answer(() => new Response(null, { status: 202 }));
    await runIndexer(target, "ix", asFetch(run));
    expect(callOf(run)[0]).toBe(`${SEARCH_DATA_BASE}${base}/indexers/ix/search.run`);
    expect(callOf(run)[1].method).toBe("POST");

    const reset = answer(() => new Response(null, { status: 204 }));
    await resetIndexer(target, "ix", asFetch(reset));
    expect(callOf(reset)[0]).toBe(`${SEARCH_DATA_BASE}${base}/indexers/ix/search.reset`);
  });
});

describe("read-only objects", () => {
  it("summarizes each kind", async () => {
    const ds = answer(() =>
      json({ value: [{ name: "d", type: "azureblob", container: { name: "docs" } }] }),
    );
    expect((await listObjects(target, "datasources", asFetch(ds)))[0]!.summary).toBe(
      "azureblob · docs",
    );
    expect(callOf(ds)[0]).toBe(`${SEARCH_DATA_BASE}${base}/datasources`);

    const sk = answer(() => json({ value: [{ name: "s", skills: [{}, {}] }] }));
    expect((await listObjects(target, "skillsets", asFetch(sk)))[0]!.summary).toBe("2 skills");

    const sm = answer(() => json({ value: [{ name: "m", synonyms: "a, b\nc, d\n" }] }));
    expect((await listObjects(target, "synonymmaps", asFetch(sm)))[0]!.summary).toBe("2 rules");
  });
});
