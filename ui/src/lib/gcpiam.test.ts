import { describe, expect, it, vi } from "vitest";

import {
  IAM_PROXY_BASE,
  createAccount,
  createKey,
  keyStatus,
  listAccounts,
  matchesRole,
  parseKey,
  parsePermissions,
  parseRole,
  signBlob,
  undeleteAccount,
  updateRole,
  validateAccountId,
  validatePermissions,
  validateRoleId,
} from "./gcpiam";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });

describe("validation", () => {
  it.each([
    ["", "account ID is required"],
    ["abc", "6-30 chars: lowercase letters, digits and -, starting with a letter"],
    ["Bad_Name1", "6-30 chars: lowercase letters, digits and -, starting with a letter"],
    ["my-service", null],
  ])("account id %j", (id, want) => expect(validateAccountId(id)).toBe(want));

  it.each([
    ["", "role ID is required"],
    ["ab", "3-64 chars: letters, digits, _ and ."],
    ["my.Role_1", null],
  ])("role id %j", (id, want) => expect(validateRoleId(id)).toBe(want));

  it("permissions", () => {
    expect(parsePermissions("b.c.d, a.b.c\na.b.c")).toEqual(["a.b.c", "b.c.d"]);
    expect(validatePermissions(["storage.objects.get"])).toBeNull();
    expect(validatePermissions(["nope"])).toContain('"nope"');
  });
});

describe("parsing", () => {
  it("reads keys and their status", () => {
    const k = parseKey({
      name: "projects/p/serviceAccounts/a@p.iam.gserviceaccount.com/keys/k1",
      keyType: "USER_MANAGED",
      keyAlgorithm: "KEY_ALG_RSA_2048",
      validBeforeTime: "2030-01-01T00:00:00Z",
    });
    expect(k).toMatchObject({ id: "k1", algorithm: "RSA_2048", disabled: false });
    expect(keyStatus(k, Date.parse("2026-01-01"))).toBe("active");
    expect(keyStatus(k, Date.parse("2031-01-01"))).toBe("expired");
    expect(keyStatus({ ...k, disabled: true })).toBe("disabled");
  });

  it("reads predefined and custom roles", () => {
    const p = parseRole({ name: "roles/viewer", includedPermissions: ["a.b.c"], stage: "GA" });
    expect(p).toMatchObject({ id: "viewer", predefined: true });
    const c = parseRole({ name: "projects/p/roles/mine", deleted: true, stage: "BETA" });
    expect(c).toMatchObject({ id: "mine", predefined: false, deleted: true, stage: "BETA" });
  });

  it("searches roles by id, title and permission", () => {
    const r = parseRole({
      name: "roles/viewer",
      title: "Viewer",
      includedPermissions: ["a.b.get"],
    });
    expect(matchesRole(r, "")).toBe(true);
    expect(matchesRole(r, "VIEW")).toBe(true);
    expect(matchesRole(r, "b.get")).toBe(true);
    expect(matchesRole(r, "zzz")).toBe(false);
  });
});

describe("calls", () => {
  const sa = "projects/p/serviceAccounts/a@p.iam.gserviceaccount.com";

  it("lists accounts across pages", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce(json({ accounts: [{ email: "a@p" }], nextPageToken: "t" }))
      .mockResolvedValueOnce(json({ accounts: [{ email: "b@p" }] }));
    const out = await listAccounts("p", fetchFn);
    expect(out.map((a) => a.email)).toEqual(["a@p", "b@p"]);
    expect(fetchFn.mock.calls[1][0]).toContain("pageToken=t");
  });

  it("creates accounts with an accountId body", async () => {
    const fetchFn = vi.fn().mockResolvedValue(json({ email: "my-sa@p", uniqueId: "1" }));
    await createAccount("p", { id: " my-sa ", displayName: "D", description: "" }, fetchFn);
    const [url, init] = fetchFn.mock.calls[0];
    expect(url).toBe(`${IAM_PROXY_BASE}/projects/p/serviceAccounts`);
    expect(JSON.parse(init.body)).toEqual({
      accountId: "my-sa",
      serviceAccount: { displayName: "D" },
    });
  });

  it("decodes the one-time key file", async () => {
    const file = '{"type":"service_account"}';
    const fetchFn = vi
      .fn()
      .mockResolvedValue(json({ name: `${sa}/keys/abcdef0123456789`, privateKeyData: btoa(file) }));
    const made = await createKey("p", "a@p.iam.gserviceaccount.com", fetchFn);
    expect(made.file).toBe(file);
    expect(made.filename).toBe("p-abcdef012345.json");
  });

  it("undeletes by unique id and updates roles with a mask", async () => {
    const fetchFn = vi.fn().mockResolvedValue(json({ restoredAccount: { email: "a@p" } }));
    expect((await undeleteAccount("123", fetchFn)).email).toBe("a@p");
    expect(fetchFn.mock.calls[0][0]).toBe(
      `${IAM_PROXY_BASE}/projects/-/serviceAccounts/123:undelete`,
    );

    const roleFetch = vi.fn().mockResolvedValue(json({ name: "projects/p/roles/r" }));
    await updateRole(
      "p",
      "r",
      { title: "T", description: "", stage: "BETA", permissions: ["a.b.c"] },
      roleFetch,
    );
    expect(roleFetch.mock.calls[0][0]).toContain(
      "updateMask=title,description,stage,includedPermissions",
    );
    expect(roleFetch.mock.calls[0][1].method).toBe("PATCH");
  });

  it("signs base64-encoded text", async () => {
    const fetchFn = vi.fn().mockResolvedValue(json({ keyId: "k", signature: "c2ln" }));
    expect(await signBlob("p", "a@p", "hi", fetchFn)).toEqual({ keyId: "k", signature: "c2ln" });
    expect(JSON.parse(fetchFn.mock.calls[0][1].body)).toEqual({ bytesToSign: "aGk=" });
  });
});
