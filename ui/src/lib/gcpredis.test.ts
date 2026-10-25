import { describe, expect, it, vi } from "vitest";

import {
  REDIS_PROXY_BASE,
  deleteInstance,
  endpointOf,
  getAuthString,
  listInstances,
  parseInstance,
} from "./gcpredis";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

describe("instances", () => {
  it("parses an instance", () => {
    const i = parseInstance({
      name: "projects/p/locations/l/instances/cache1",
      state: "READY",
      tier: "BASIC",
      memorySizeGb: 2,
      host: "10.0.0.3",
      port: 6379,
      authEnabled: true,
      transitEncryptionMode: "SERVER_AUTHENTICATION",
      serverCaCerts: [{}, {}],
      labels: { env: "dev", bad: 3 },
    });
    expect(i).toMatchObject({
      id: "cache1",
      state: "READY",
      memorySizeGb: 2,
      authEnabled: true,
      caCerts: 2,
      labels: { env: "dev" },
    });
    expect(endpointOf(i.host, i.port)).toBe("10.0.0.3:6379");
  });

  it("defaults sparse instances", () => {
    const i = parseInstance({ name: "x/y" });
    expect(i.state).toBe("STATE_UNSPECIFIED");
    expect(i.transitEncryption).toBe("DISABLED");
    expect(i.authEnabled).toBe(false);
    expect(endpointOf("", 0)).toBe("");
  });
});

describe("requests", () => {
  it("lists instances of a scope", async () => {
    const f = vi.fn((..._a: unknown[]) =>
      Promise.resolve(json({ instances: [{ name: "projects/p/locations/l/instances/a" }] })),
    );
    expect((await listInstances({ project: "p", location: "l" }, f)).map((i) => i.id)).toEqual([
      "a",
    ]);
    expect(f.mock.calls[0][0]).toBe(`${REDIS_PROXY_BASE}/projects/p/locations/l/instances`);
  });

  it("fetches the AUTH string only on request", async () => {
    const f = vi.fn((..._a: unknown[]) => Promise.resolve(json({ authString: "s3cret" })));
    expect(await getAuthString("projects/p/locations/l/instances/a", f)).toBe("s3cret");
    expect(f.mock.calls[0][0]).toBe(
      `${REDIS_PROXY_BASE}/projects/p/locations/l/instances/a/authString`,
    );
  });

  it("deletes and surfaces errors", async () => {
    const f = vi.fn((..._a: unknown[]) =>
      Promise.resolve(json({ error: { message: "busy", status: "FAILED_PRECONDITION" } }, 400)),
    );
    await expect(deleteInstance("a/b", f)).rejects.toThrow("busy");
  });
});
