import { describe, expect, it, vi } from "vitest";

import {
  IDP_PROXY_BASE,
  createUser,
  deleteUser,
  filterUsers,
  formatTime,
  getConfig,
  listOobCodes,
  listProjects,
  listTenants,
  listUsers,
  parseClaims,
  parseUser,
  providerLabel,
  setClaims,
  setSignIn,
  userAction,
  userProviders,
  validEmail,
  validPassword,
  validateClaims,
} from "./gcpidentitytoolkit";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const asFetch = (f: unknown) => f as typeof fetch;
const initOf = (f: { mock: { calls: unknown[][] } }, i: number): RequestInit =>
  (f.mock.calls[i] ?? [])[1] as RequestInit;
const urlOf = (f: { mock: { calls: unknown[][] } }, i: number): string =>
  String((f.mock.calls[i] ?? [])[0]);

const ada = {
  localId: "uid-ada",
  email: "ada@example.com",
  displayName: "Ada",
  emailVerified: true,
  hasPassword: true,
  createdAt: "1700000000000",
  lastLoginAt: "1700000100000",
  customAttributes: '{"role":"admin"}',
  providers: [{ providerId: "password", rawId: "ada@example.com", email: "ada@example.com" }],
};

describe("parsing", () => {
  it("parses a user including claims and times", () => {
    expect(parseUser(ada)).toMatchObject({
      uid: "uid-ada",
      email: "ada@example.com",
      emailVerified: true,
      disabled: false,
      hasPassword: true,
      claims: { role: "admin" },
      createdAt: 1700000000000,
      lastLoginAt: 1700000100000,
    });
  });

  it.each([
    ["", {}],
    ["not json", {}],
    ["[1]", {}],
    ['{"a":1}', { a: 1 }],
  ])("parses claims %j", (raw, want) => {
    expect(parseClaims(raw)).toEqual(want);
  });

  it("never exposes hash fields even if the wire carried them", () => {
    const u = parseUser({ ...ada, passwordHash: "abc", salt: "def" });
    expect(JSON.stringify(u)).not.toMatch(/passwordHash|salt|abc|def/);
  });

  it("derives providers", () => {
    expect(userProviders(parseUser(ada))).toEqual(["password"]);
    expect(userProviders(parseUser({ localId: "x" }))).toEqual(["anonymous"]);
    expect(userProviders(parseUser({ localId: "x", email: "a@b.co", hasPassword: true }))).toEqual([
      "password",
    ]);
    expect(providerLabel("password")).toBe("Email/Password");
    expect(providerLabel("google.com")).toBe("google.com");
  });

  it("formats times", () => {
    expect(formatTime(0)).toBe("—");
    expect(formatTime(1700000000000)).toBe("2023-11-14 22:13:20Z");
  });
});

describe("validation", () => {
  it.each([
    ["a@b.co", true],
    ["nope", false],
    ["a b@c.de", false],
  ])("email %s", (v, want) => expect(validEmail(v)).toBe(want));

  it.each([
    ["12345", false],
    ["123456", true],
  ])("password %s", (v, want) => expect(validPassword(v)).toBe(want));

  it("accepts object claims and treats empty as {}", () => {
    expect(validateClaims('{"a":1}')).toEqual({ ok: true, claims: { a: 1 } });
    expect(validateClaims("  ")).toEqual({ ok: true, claims: {} });
  });

  it.each(["[1]", "3", "null", "{bad"])("rejects claims %s", (text) => {
    expect(validateClaims(text).ok).toBe(false);
  });

  it("rejects oversized claims", () => {
    const res = validateClaims(JSON.stringify({ big: "x".repeat(1100) }));
    expect(res).toMatchObject({ ok: false });
  });
});

describe("filterUsers", () => {
  const users = [parseUser(ada), parseUser({ localId: "uid-bob", email: "bob@x.io" })];

  it.each([
    ["", 2],
    ["ADA@", 1],
    ["uid-bob", 1],
    ["ada", 1],
    ["zzz", 0],
  ])("query %j", (q, n) => expect(filterUsers(users, q)).toHaveLength(n));
});

describe("requests", () => {
  it("lists projects and users", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(json({ projects: ["p1", "p2"] }))
      .mockResolvedValueOnce(json({ users: [ada], total: 7 }));
    expect(await listProjects(asFetch(f))).toEqual(["p1", "p2"]);
    const res = await listUsers("p1", "t-1", asFetch(f));
    expect(res.total).toBe(7);
    expect(res.users[0].uid).toBe("uid-ada");
    expect(urlOf(f, 1)).toBe(`${IDP_PROXY_BASE}/projects/p1/users?limit=500&tenantId=t-1`);
  });

  it("creates a user sending the password once and returning only the uid", async () => {
    const f = vi.fn().mockResolvedValue(json({ localId: "new-uid" }));
    const uid = await createUser("p1", "", { email: "a@b.co", password: "hunter22" }, asFetch(f));
    expect(uid).toBe("new-uid");
    expect(initOf(f, 0).method).toBe("POST");
    expect(JSON.parse(String(initOf(f, 0).body))).toEqual({
      email: "a@b.co",
      password: "hunter22",
      displayName: "",
    });
    expect(urlOf(f, 0)).toBe(`${IDP_PROXY_BASE}/projects/p1/users`);
  });

  it.each(["disable", "enable", "revoke"] as const)("runs %s", async (action) => {
    const f = vi.fn().mockImplementation(() => Promise.resolve(json({ localId: "u" })));
    await userAction("p1", "t1", "u/1", action, asFetch(f));
    expect(urlOf(f, 0)).toBe(`${IDP_PROXY_BASE}/projects/p1/users/u%2F1/${action}?tenantId=t1`);
    expect(initOf(f, 0).method).toBe("POST");
  });

  it("sets claims, deletes and reads codes, tenants and config", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(json({ localId: "u" }))
      .mockResolvedValueOnce(json({}))
      .mockResolvedValueOnce(
        json({
          oobCodes: [
            { email: "a@b.co", oobCode: "c", oobLink: "http://x/y", requestType: "PASSWORD_RESET" },
          ],
        }),
      )
      .mockResolvedValueOnce(
        json({ tenants: [{ id: "t-1", displayName: "Acme", allowPasswordSignup: true }] }),
      )
      .mockResolvedValueOnce(
        json({ emailEnabled: true, anonymousEnabled: false, authorizedDomains: ["localhost"] }),
      )
      .mockResolvedValueOnce(
        json({ emailEnabled: false, anonymousEnabled: false, authorizedDomains: [] }),
      );
    await setClaims("p1", "", "u", { role: "x" }, asFetch(f));
    expect(JSON.parse(String(initOf(f, 0).body))).toEqual({ claims: { role: "x" } });
    await deleteUser("p1", "", "u", asFetch(f));
    expect(initOf(f, 1).method).toBe("DELETE");
    expect((await listOobCodes("p1", "", asFetch(f)))[0]).toMatchObject({
      requestType: "PASSWORD_RESET",
    });
    expect((await listTenants("p1", asFetch(f)))[0]).toMatchObject({
      id: "t-1",
      allowPasswordSignup: true,
    });
    expect(await getConfig("p1", asFetch(f))).toMatchObject({
      emailEnabled: true,
      anonymousEnabled: false,
    });
    expect((await setSignIn("p1", { emailEnabled: false }, asFetch(f))).emailEnabled).toBe(false);
    expect(initOf(f, 5).method).toBe("PATCH");
    expect(JSON.parse(String(initOf(f, 5).body))).toEqual({ emailEnabled: false });
  });

  it("surfaces proxy errors", async () => {
    const f = vi
      .fn()
      .mockResolvedValue(
        json({ error: { message: "EMAIL_EXISTS", status: "INVALID_ARGUMENT" } }, 400),
      );
    await expect(
      createUser("p1", "", { email: "a@b.co", password: "123456" }, asFetch(f)),
    ).rejects.toThrow("EMAIL_EXISTS");
  });
});
