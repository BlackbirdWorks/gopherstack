import { describe, expect, it, vi } from "vitest";

import {
  DNS_PROXY_BASE,
  addOp,
  createZone,
  createZoneBody,
  deleteZone,
  editOps,
  getDataPlane,
  getZoneRecords,
  groupRecords,
  isConflict,
  listEndpoints,
  listResolvers,
  listSteeringPolicies,
  listTsigKeys,
  listViews,
  listZones,
  parseRecord,
  parseZone,
  patchRecords,
  removeOp,
  tryQuery,
  validateQuery,
  validateRecord,
  validateZone,
} from "./ocidns";
import { GcpRestError } from "./gcprest";

const json = (body: unknown, status = 200, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
const asFetch = (f: unknown) => f as typeof fetch;
const comp = "ocid1.compartment.oc1..aaaa";

const zoneRaw = {
  id: "z1",
  name: "example.com",
  zoneType: "PRIMARY",
  scope: "GLOBAL",
  serial: 7,
  timeCreated: "2026-11-01T00:00:00Z",
  lifecycleState: "ACTIVE",
  nameservers: [{ hostname: "ns1.example.net" }],
};

const rec = (domain: string, rtype: string, rdata: string, ttl = 300) => ({
  domain,
  rtype,
  rdata,
  ttl,
  isProtected: false,
});

describe("parsing", () => {
  it("parses a zone", () => {
    expect(parseZone(zoneRaw)).toEqual({
      id: "z1",
      name: "example.com",
      type: "PRIMARY",
      scope: "GLOBAL",
      serial: 7,
      created: "2026-11-01T00:00:00Z",
      state: "ACTIVE",
      viewId: "",
      nameservers: ["ns1.example.net"],
    });
  });

  it("parses a record and groups by rrset", () => {
    expect(
      parseRecord({ ...rec("a.example.com", "A", "1.1.1.1"), isProtected: true }).protected,
    ).toBe(true);
    const sets = groupRecords([
      parseRecord(rec("b.example.com", "A", "2.2.2.2")),
      parseRecord(rec("a.example.com", "TXT", "hi")),
      parseRecord(rec("a.example.com", "A", "1.1.1.2")),
      parseRecord(rec("a.example.com", "A", "1.1.1.1")),
    ]);
    expect(sets.map((s) => `${s.domain} ${s.rtype} ${s.records.length}`)).toEqual([
      "a.example.com A 2",
      "a.example.com TXT 1",
      "b.example.com A 1",
    ]);
  });
});

describe("record operations", () => {
  const draft = { domain: " www.example.com. ", rtype: "A", ttl: 60, rdata: " 192.0.2.1 " };

  it("builds ADD and REMOVE from fixed fields", () => {
    const want = { domain: "www.example.com", rtype: "A", ttl: 60, rdata: "192.0.2.1" };
    expect(addOp(draft)).toEqual({ operation: "ADD", ...want });
    expect(removeOp(draft)).toEqual({ operation: "REMOVE", ...want });
  });

  it("edits as remove then add", () => {
    const ops = editOps(
      { domain: "www.example.com", rtype: "A", ttl: 60, rdata: "192.0.2.1" },
      { domain: "www.example.com", rtype: "A", ttl: 120, rdata: "192.0.2.2" },
    );
    expect(ops.map((o) => [o.operation, o.rdata, o.ttl])).toEqual([
      ["REMOVE", "192.0.2.1", 60],
      ["ADD", "192.0.2.2", 120],
    ]);
  });

  it("validates a record against its zone", () => {
    const ok = { domain: "www.example.com", rtype: "A", ttl: 300, rdata: "192.0.2.1" };
    expect(validateRecord("example.com", ok)).toBe("");
    expect(validateRecord("example.com", { ...ok, domain: "example.com" })).toBe("");
    expect(validateRecord("example.com", { ...ok, domain: "" })).toMatch(/required/);
    expect(validateRecord("example.com", { ...ok, domain: "evil.org" })).toMatch(/must be/);
    expect(validateRecord("example.com", { ...ok, domain: "notexample.com" })).toMatch(/must be/);
    expect(validateRecord("example.com", { ...ok, domain: "a b.example.com" })).toMatch(/invalid/);
    expect(validateRecord("example.com", { ...ok, rtype: "SOA" })).toMatch(/type/);
    expect(validateRecord("example.com", { ...ok, rdata: " " })).toMatch(/data/);
    expect(validateRecord("example.com", { ...ok, ttl: -1 })).toMatch(/TTL/);
    expect(validateRecord("example.com", { ...ok, ttl: 1.5 })).toMatch(/TTL/);
  });

  it("recognises precondition failures as conflicts", () => {
    expect(isConflict(new GcpRestError(412, "PreconditionFailed", "x"))).toBe(true);
    expect(isConflict(new GcpRestError(409, "Conflict", "x"))).toBe(true);
    expect(isConflict(new GcpRestError(404, "NotFound", "x"))).toBe(false);
    expect(isConflict(new Error("x"))).toBe(false);
  });
});

describe("zones", () => {
  it("lists both scopes sorted by name and hides deleted zones", async () => {
    const fetchMock = vi.fn((url: string) =>
      Promise.resolve(
        url.includes("scope=PRIVATE")
          ? json([{ ...zoneRaw, id: "z2", name: "a.internal", scope: "PRIVATE", viewId: "v1" }])
          : json([zoneRaw, { ...zoneRaw, id: "z3", name: "gone.com", lifecycleState: "DELETED" }]),
      ),
    );
    const zones = await listZones(comp, asFetch(fetchMock));
    expect(zones.map((z) => z.name)).toEqual(["a.internal", "example.com"]);
    const urls = fetchMock.mock.calls.map((c) => c[0] as string);
    expect(urls).toContain(
      `${DNS_PROXY_BASE}/api/zones?compartmentId=${encodeURIComponent(comp)}&scope=GLOBAL`,
    );
    expect(urls.some((u) => u.endsWith("&scope=PRIVATE"))).toBe(true);
  });

  it("builds the create body and validates it", () => {
    const base = {
      name: " example.com ",
      compartmentId: comp,
      scope: "GLOBAL" as const,
      viewId: "v1",
    };
    expect(createZoneBody(base)).toEqual({
      name: "example.com",
      compartmentId: comp,
      zoneType: "PRIMARY",
      scope: "GLOBAL",
    });
    expect(createZoneBody({ ...base, scope: "PRIVATE" })).toMatchObject({
      scope: "PRIVATE",
      viewId: "v1",
    });
    expect(validateZone(base)).toBe("");
    expect(validateZone({ ...base, name: "" })).toMatch(/required/);
    expect(validateZone({ ...base, name: "nodots" })).toMatch(/domain/);
    expect(validateZone({ ...base, scope: "PRIVATE", viewId: "" })).toMatch(/view/);
  });

  it("creates and deletes zones through the proxy", async () => {
    const fetchMock = vi.fn(() => Promise.resolve(json({})));
    await createZone(
      { name: "example.com", compartmentId: comp, scope: "GLOBAL", viewId: "" },
      asFetch(fetchMock),
    );
    expect(fetchMock).toHaveBeenLastCalledWith(
      `${DNS_PROXY_BASE}/api/zones`,
      expect.objectContaining({ method: "POST" }),
    );
    await deleteZone({ id: "z2", scope: "PRIVATE", viewId: "v 1" }, asFetch(fetchMock));
    expect(fetchMock).toHaveBeenLastCalledWith(
      `${DNS_PROXY_BASE}/api/zones/z2?scope=PRIVATE&viewId=v%201`,
      expect.objectContaining({ method: "DELETE" }),
    );
  });
});

describe("records", () => {
  it("collects records over pages and keeps the first etag", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        json({ items: [rec("a.example.com", "A", "1.1.1.1")] }, 200, {
          etag: "e1",
          "opc-next-page": "p2",
        }),
      )
      .mockResolvedValueOnce(
        json({ items: [rec("b.example.com", "A", "2.2.2.2")] }, 200, { etag: "e2" }),
      );
    const got = await getZoneRecords({ id: "z1", scope: "GLOBAL", viewId: "" }, asFetch(fetchMock));
    expect(got.etag).toBe("e1");
    expect(got.records.map((r) => r.domain)).toEqual(["a.example.com", "b.example.com"]);
    expect(fetchMock.mock.calls[1][0]).toBe(`${DNS_PROXY_BASE}/api/zones/z1/records?page=p2`);
  });

  it("sends the etag as If-Match with the operations", async () => {
    const fetchMock = vi.fn(() => Promise.resolve(json({})));
    const ops = [addOp({ domain: "www.example.com", rtype: "A", ttl: 60, rdata: "192.0.2.1" })];
    await patchRecords({ id: "z1", scope: "GLOBAL", viewId: "" }, ops, "e1", asFetch(fetchMock));
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe(`${DNS_PROXY_BASE}/api/zones/z1/records`);
    expect(init.method).toBe("PATCH");
    expect((init.headers as Record<string, string>)["If-Match"]).toBe("e1");
    expect(JSON.parse(init.body as string)).toEqual({ items: ops });
  });

  it("surfaces a precondition failure as a conflict", async () => {
    const fetchMock = vi.fn(() =>
      Promise.resolve(json({ code: "PreconditionFailed", message: "etag mismatch" }, 412)),
    );
    const err = await patchRecords(
      { id: "z1", scope: "GLOBAL", viewId: "" },
      [],
      "stale",
      asFetch(fetchMock),
    ).catch((e: unknown) => e);
    expect(isConflict(err)).toBe(true);
    expect((err as Error).message).toBe("etag mismatch");
  });
});

const reply = (body: unknown) => asFetch(vi.fn(() => Promise.resolve(json(body))));

describe("read-only lists", () => {
  it("parses views, resolvers, endpoints, TSIG keys and policies", async () => {
    expect(
      await listViews(
        comp,
        reply([{ id: "v1", displayName: "main", isProtected: true, lifecycleState: "ACTIVE" }]),
      ),
    ).toEqual([{ id: "v1", name: "main", created: "", protected: true, state: "ACTIVE" }]);
    expect(
      (
        await listResolvers(
          comp,
          reply([{ id: "r1", displayName: "res", attachedVcnId: "vcn", defaultViewId: "v1" }]),
        )
      )[0],
    ).toMatchObject({
      id: "r1",
      name: "res",
      vcnId: "vcn",
    });
    const eps = await listEndpoints(
      "r1",
      reply([
        { name: "fwd", isForwarding: true, forwardingAddress: "10.0.0.5", subnetId: "s1" },
        { name: "lst", isListening: true, listeningAddress: "10.0.0.6" },
      ]),
    );
    expect(eps.map((e) => [e.name, e.kind, e.address])).toEqual([
      ["fwd", "Forwarding", "10.0.0.5"],
      ["lst", "Listening", "10.0.0.6"],
    ]);
    const keys = await listTsigKeys(
      comp,
      reply([{ id: "k1", name: "key", algorithm: "hmac-sha256", secret: "LEAK" }]),
    );
    expect(keys).toEqual([
      { id: "k1", name: "key", algorithm: "hmac-sha256", state: "", created: "" },
    ]);
    expect(JSON.stringify(keys)).not.toContain("LEAK");
    expect(
      (
        await listSteeringPolicies(
          comp,
          reply([{ id: "p1", displayName: "pol", template: "FAILOVER", ttl: 30 }]),
        )
      )[0],
    ).toMatchObject({
      name: "pol",
      template: "FAILOVER",
      ttl: 30,
    });
  });
});

describe("try it", () => {
  it("validates names and types", () => {
    expect(validateQuery("www.example.com", "A")).toBe("");
    expect(validateQuery("www.example.com.", "SOA")).toBe("");
    expect(validateQuery("", "A")).toMatch(/Enter/);
    expect(validateQuery("bad name.com", "A")).toMatch(/valid/);
    expect(validateQuery("a..com", "A")).toMatch(/valid/);
    expect(validateQuery(`${"a".repeat(64)}.com`, "A")).toMatch(/valid/);
    expect(validateQuery("example.com", "AXFR")).toMatch(/type/);
  });

  it("reports the data plane state", async () => {
    const f = asFetch(vi.fn(() => Promise.resolve(json({ enabled: true, port: 8169 }))));
    expect(await getDataPlane(f)).toEqual({ enabled: true, port: 8169 });
  });

  it("posts the query and parses the answer section", async () => {
    const fetchMock = vi.fn(() =>
      Promise.resolve(
        json({
          rcode: "NOERROR",
          authoritative: true,
          name: "www.example.com.",
          type: "A",
          server: "127.0.0.1:8169",
          answer: [{ name: "www.example.com.", type: "A", ttl: 60, data: "192.0.2.1" }],
          authority: [],
        }),
      ),
    );
    const got = await tryQuery(" www.example.com ", "A", asFetch(fetchMock));
    expect(
      JSON.parse((fetchMock.mock.calls[0] as unknown as [string, RequestInit])[1].body as string),
    ).toEqual({
      name: "www.example.com",
      type: "A",
    });
    expect(got).toMatchObject({ rcode: "NOERROR", authoritative: true });
    expect(got.answer).toEqual([
      { name: "www.example.com.", type: "A", ttl: 60, data: "192.0.2.1" },
    ]);
  });
});
