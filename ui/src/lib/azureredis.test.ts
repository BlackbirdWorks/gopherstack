import { describe, expect, it, vi } from "vitest";

import {
  MAX_PATTERN_LEN,
  REDIS_PROXY_BASE,
  getValue,
  listCaches,
  revealKeys,
  scanKeys,
  ttlLabel,
  validatePattern,
} from "./azureredis";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const ok = (body: unknown) => vi.fn().mockImplementation(() => Promise.resolve(json(body)));

describe("helpers", () => {
  it.each([
    ["*", ""],
    ["user:*", ""],
    ["x".repeat(MAX_PATTERN_LEN + 1), `Pattern is at most ${MAX_PATTERN_LEN} characters`],
  ])("pattern %j", (p, want) => expect(validatePattern(p)).toBe(want));

  it.each([
    [-1, "no expiry"],
    [30, "30s"],
  ])("ttl %j", (s, want) => expect(ttlLabel(s)).toBe(want));
});

describe("client", () => {
  it("lists caches with their ports", async () => {
    const f = ok({
      caches: [
        { id: "/s/c1", name: "c1", sku: "Standard", sslPort: 6380, enableNonSslPort: false },
      ],
    });
    const out = await listCaches(f);
    expect(f.mock.calls[0][0]).toBe(`${REDIS_PROXY_BASE}/caches`);
    expect(out[0]).toMatchObject({
      name: "c1",
      sku: "Standard",
      sslPort: 6380,
      enableNonSslPort: false,
    });
  });

  it("scans with pattern and cursor", async () => {
    const f = ok({ keys: [{ key: "a", type: "string", ttlSeconds: -1 }], next: "a" });
    const out = await scanKeys("/s/c1", 2, "u*", "prev", f);
    const url = new URL(f.mock.calls[0][0], "http://x");
    expect(url.pathname).toBe(`${REDIS_PROXY_BASE}/keys`);
    expect(Object.fromEntries(url.searchParams)).toEqual({
      cache: "/s/c1",
      db: "2",
      match: "u*",
      cursor: "prev",
    });
    expect(out).toEqual({ keys: [{ key: "a", type: "string", ttlSeconds: -1 }], next: "a" });
  });

  it("omits empty pattern and cursor", async () => {
    const f = ok({ keys: [] });
    await scanKeys("c", 0, "", "", f);
    expect(f.mock.calls[0][0]).toBe(`${REDIS_PROXY_BASE}/keys?cache=c&db=0`);
  });

  it("reads a capped value", async () => {
    const f = ok({
      key: "k",
      type: "list",
      elements: ["1", "2"],
      total: 200,
      truncated: true,
      ttlSeconds: 5,
    });
    const v = await getValue("c", 0, "k", f);
    expect(v).toMatchObject({ type: "list", elements: ["1", "2"], total: 200, truncated: true });
  });

  it("reveals keys only by POST", async () => {
    const f = ok({ primary: "p", secondary: "s" });
    expect(await revealKeys("c", f)).toEqual({ primary: "p", secondary: "s" });
    expect(f.mock.calls[0][1].method).toBe("POST");
    const g = ok({ caches: [{ id: "c", name: "c" }] });
    expect(JSON.stringify(await listCaches(g))).not.toContain("primary");
  });
});
