import { describe, expect, it, vi } from "vitest";

import {
  SM_PROXY_BASE,
  accessVersion,
  addVersion,
  aliasesToRows,
  describeReplication,
  parseSecret,
  rowsToAliases,
  secretBody,
  updateSecret,
  validateAliasKey,
  validateSecretId,
  versionAction,
} from "./gcpsecretmanager";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });

describe("validation", () => {
  it.each([
    ["", "name is required"],
    ["bad name", "use letters, digits, - and _ (max 255)"],
    ["db_password-1", null],
  ])("secret id %s", (id, want) => {
    expect(validateSecretId(id)).toBe(want);
  });
  it.each([
    ["prod", null],
    ["1prod", "start with a letter; use letters, digits, - and _"],
  ])("alias %s", (a, want) => {
    expect(validateAliasKey(a)).toBe(want);
  });
});

describe("secretBody", () => {
  it("automatic with ttl and labels", () => {
    expect(
      secretBody({
        replication: { kind: "automatic" },
        labels: { a: "b" },
        ttlSeconds: "86400",
        expireTime: "",
      }),
    ).toEqual({ replication: { automatic: {} }, labels: { a: "b" }, ttl: "86400s" });
  });
  it("user-managed replicas", () => {
    expect(
      secretBody({
        replication: { kind: "user", locations: ["us-east1", "us-west1"] },
        labels: {},
        ttlSeconds: "",
        expireTime: "",
      }),
    ).toEqual({
      replication: {
        userManaged: { replicas: [{ location: "us-east1" }, { location: "us-west1" }] },
      },
    });
  });
});

describe("aliases", () => {
  it("round-trips rows", () => {
    expect(aliasesToRows({ b: "2", a: "1" })).toEqual([
      { key: "a", value: "1" },
      { key: "b", value: "2" },
    ]);
    expect(rowsToAliases([{ key: "a", value: "3" }])).toEqual({ aliases: { a: 3 } });
  });
  it("rejects non-numeric", () => {
    expect(rowsToAliases([{ key: "a", value: "x" }])).toEqual({
      error: "a: value must be a version number",
    });
  });
});

describe("requests", () => {
  it("adds a text version as base64", async () => {
    const f = vi
      .fn()
      .mockResolvedValue(json({ name: "projects/p/secrets/s/versions/1", state: "ENABLED" }));
    const v = await addVersion("p", "s", { mode: "text", value: "hi" }, f);
    expect(v.id).toBe("1");
    expect(f).toHaveBeenCalledWith(
      `${SM_PROXY_BASE}/projects/p/secrets/s:addVersion`,
      expect.objectContaining({ body: JSON.stringify({ payload: { data: "aGk=" } }) }),
    );
  });

  it("accesses and decodes", async () => {
    const f = vi.fn().mockResolvedValue(json({ payload: { data: "aGk=" } }));
    expect(await accessVersion("p", "s", "latest", f)).toEqual({ text: "hi", base64: "aGk=" });
  });

  it("posts version actions", async () => {
    const f = vi
      .fn()
      .mockResolvedValue(json({ name: "projects/p/secrets/s/versions/2", state: "DISABLED" }));
    expect((await versionAction("p", "s", "2", "disable", f)).state).toBe("DISABLED");
    expect(f.mock.calls[0]?.[0]).toBe(`${SM_PROXY_BASE}/projects/p/secrets/s/versions/2:disable`);
  });

  it("patches aliases with a mask", async () => {
    const f = vi
      .fn()
      .mockResolvedValue(json({ name: "projects/p/secrets/s", versionAliases: { prod: "2" } }));
    const s = await updateSecret("p", "s", { aliases: { prod: 2 } }, f);
    expect(s.aliases).toEqual({ prod: "2" });
    expect(f.mock.calls[0]?.[0]).toBe(
      `${SM_PROXY_BASE}/projects/p/secrets/s?updateMask=versionAliases`,
    );
  });
});

describe("parsing", () => {
  it("describes replication", () => {
    expect(describeReplication({ automatic: {} })).toBe("automatic");
    expect(describeReplication({ userManaged: { replicas: [{ location: "a" }] } })).toBe(
      "user-managed: a",
    );
  });
  it("parses a secret", () => {
    const s = parseSecret({ name: "projects/p/secrets/s", ttl: "60s", versionAliases: { x: "1" } });
    expect(s).toMatchObject({ id: "s", expiry: "ttl 60s", aliases: { x: "1" } });
  });
});
