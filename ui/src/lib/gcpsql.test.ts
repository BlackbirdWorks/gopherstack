import { describe, expect, it, vi } from "vitest";

import {
  SQL_PROXY_BASE,
  listDatabases,
  listInstances,
  listUsers,
  parseInstance,
  stateTone,
} from "./gcpsql";

const json = (body: unknown) =>
  new Response(JSON.stringify(body), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });

describe("parseInstance", () => {
  it("flattens tier and addresses", () => {
    expect(
      parseInstance({
        name: "db",
        state: "RUNNABLE",
        settings: { tier: "db-f1-micro" },
        ipAddresses: [{ ipAddress: "127.0.0.1" }, {}],
      }),
    ).toMatchObject({ name: "db", tier: "db-f1-micro", ipAddresses: ["127.0.0.1"] });
  });
});

describe("client", () => {
  it("lists instances, databases and users", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(json({ items: [{ name: "db" }] }))
      .mockResolvedValueOnce(json({ items: [{ name: "app", charset: "UTF8" }] }))
      .mockResolvedValueOnce(json({}));
    expect((await listInstances("p", f))[0].name).toBe("db");
    expect((await listDatabases("p", "db", f))[0].charset).toBe("UTF8");
    expect(await listUsers("p", "db", f)).toEqual([]);
    expect(f.mock.calls.map((c) => c[0])).toEqual([
      `${SQL_PROXY_BASE}/projects/p/instances`,
      `${SQL_PROXY_BASE}/projects/p/instances/db/databases`,
      `${SQL_PROXY_BASE}/projects/p/instances/db/users`,
    ]);
  });

  it.each([
    ["RUNNABLE", "green"],
    ["PENDING_CREATE", "amber"],
    ["STOPPED", "gray"],
  ])("tone %s", (state, color) => expect(stateTone(state)).toContain(color));
});
