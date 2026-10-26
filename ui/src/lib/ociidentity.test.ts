import { describe, expect, it, vi } from "vitest";

import {
  buildCompartmentTree,
  flattenTree,
  listApiKeys,
  listPolicies,
  parseApiKey,
  parseCompartment,
  parsePolicy,
  parseUser,
} from "./ociidentity";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

const comp = (id: string, parentId: string, name: string) =>
  parseCompartment({ id, compartmentId: parentId, name });

function reply(f: (url: string) => Response) {
  return vi.fn((...a: unknown[]) => Promise.resolve(f(...(a as [string]))));
}

describe("compartment tree", () => {
  it("nests children under parents, sorted by name", () => {
    const tree = buildCompartmentTree([
      comp("c-b", "root", "beta"),
      comp("c-a", "root", "alpha"),
      comp("c-a1", "c-a", "child"),
      comp("root", "", "tenancy"),
    ]);
    expect(tree).toHaveLength(1);
    expect(flattenTree(tree).map((n) => [n.name, n.depth])).toEqual([
      ["tenancy", 0],
      ["alpha", 1],
      ["child", 2],
      ["beta", 1],
    ]);
  });

  it("keeps orphans visible as roots", () => {
    const tree = buildCompartmentTree([comp("x", "missing", "lonely")]);
    expect(tree.map((n) => n.name)).toEqual(["lonely"]);
  });

  it("does not loop on a self-parented node", () => {
    expect(flattenTree(buildCompartmentTree([comp("x", "x", "self")]))).toHaveLength(1);
  });
});

describe("parsing", () => {
  it("parses users, policies and API keys", () => {
    expect(parseUser({ id: "u", name: "alice", isMfaActivated: true })).toMatchObject({
      name: "alice",
      mfa: true,
    });
    expect(parsePolicy({ name: "p", statements: ["Allow a", 3] }).statements).toEqual([
      "Allow a",
      "3",
    ]);
    expect(
      parseApiKey({ fingerprint: "aa:bb", keyId: "t/u/aa:bb", lifecycleState: "ACTIVE" }),
    ).toEqual({
      fingerprint: "aa:bb",
      keyId: "t/u/aa:bb",
      state: "ACTIVE",
      timeCreated: "",
    });
  });

  it("never carries key material", () => {
    const k = parseApiKey({ fingerprint: "aa", keyValue: "-----BEGIN PUBLIC KEY-----" });
    expect(JSON.stringify(k)).not.toContain("BEGIN");
  });
});

describe("requests", () => {
  it("lists policies and API keys", async () => {
    const fetchFn = reply((url: string) =>
      url.endsWith("/policies")
        ? json({ policies: [{ id: "p1", name: "pol" }] })
        : json({ keys: [{ fingerprint: "aa:bb" }] }),
    ) as unknown as typeof fetch;
    expect((await listPolicies(fetchFn))[0]?.name).toBe("pol");
    expect((await listApiKeys("ocid1.user.oc1..u", fetchFn))[0]?.fingerprint).toBe("aa:bb");
  });
});
