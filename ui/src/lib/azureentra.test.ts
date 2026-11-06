import { describe, expect, it, vi } from "vitest";

import {
  AUTHZ_PROXY_BASE,
  GRAPH_PROXY_BASE,
  addMember,
  addSecret,
  createApplication,
  createLock,
  createRoleAssignment,
  createUser,
  deleteLock,
  deleteRoleAssignment,
  groupScope,
  isGuid,
  listApplications,
  listGroups,
  listLocks,
  listPolicyAssignments,
  listRoleAssignments,
  listRoleDefinitions,
  mailNicknameFrom,
  parseApplication,
  parseApplicationDetail,
  parseDirObject,
  parseRoleAssignment,
  removeMember,
  removeSecret,
  resolvePrincipals,
  roleNameOf,
  secretExpiry,
  subscriptionScope,
  validLockName,
  validPassword,
  validUpn,
} from "./azureentra";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const asFetch = (f: unknown) => f as typeof fetch;
const initOf = (f: { mock: { calls: unknown[][] } }, i: number): RequestInit =>
  (f.mock.calls[i] ?? [])[1] as RequestInit;
const urlOf = (f: { mock: { calls: unknown[][] } }, i: number): string =>
  String((f.mock.calls[i] ?? [])[0]);
const bodyOf = (f: { mock: { calls: unknown[][] } }, i: number) =>
  JSON.parse(String(initOf(f, i).body));

const GUID = "11111111-1111-1111-1111-111111111111";
const ROLE = `/subscriptions/s/providers/Microsoft.Authorization/roleDefinitions/${GUID}`;

describe("parsing", () => {
  it("keeps only the hint and expiry of secrets and flags expired ones", () => {
    const app = parseApplication(
      {
        id: "o1",
        appId: "a1",
        displayName: "web",
        createdDateTime: "2026-01-02T00:00:00Z",
        passwordCredentials: [
          { keyId: "k1", hint: "abc", endDateTime: "2026-02-01T00:00:00Z", secretText: null },
          { keyId: "k2", hint: "def", endDateTime: "2030-01-01T00:00:00Z" },
        ],
      },
      new Date("2026-06-01T00:00:00Z"),
    );
    expect(app.secrets.map((s) => [s.keyId, s.hint, s.expired])).toEqual([
      ["k1", "abc", true],
      ["k2", "def", false],
    ]);
    expect(JSON.stringify(app)).not.toContain("secretText");
  });

  it("collects redirect URIs, API permissions, roles and owners", () => {
    const d = parseApplicationDetail(
      {
        id: "o",
        web: { redirectUris: ["https://a"] },
        spa: { redirectUris: ["https://b"] },
        publicClient: { redirectUris: null },
        requiredResourceAccess: [
          {
            resourceAppId: "r",
            resourceAccess: [{ type: "Scope" }, { type: "Role" }, { type: "Role" }],
          },
        ],
        appRoles: [{ id: "i", displayName: "Reader", value: "r.read", isEnabled: false }],
      },
      [
        {
          id: "u",
          "@odata.type": "#microsoft.graph.user",
          displayName: "Ann",
          userPrincipalName: "ann@x",
        },
      ],
    );
    expect(d.redirectUris).toEqual(["https://a", "https://b"]);
    expect(d.resourceAccess).toEqual([{ resourceAppId: "r", scopes: 1, roles: 2 }]);
    expect(d.appRoles[0]).toMatchObject({ value: "r.read", enabled: false });
    expect(d.owners[0]).toMatchObject({ type: "User", name: "Ann", detail: "ann@x" });
  });

  it("maps odata types and role assignments", () => {
    expect(parseDirObject({ "@odata.type": "#microsoft.graph.group", id: "g" }).type).toBe("Group");
    const a = parseRoleAssignment({
      name: "n",
      properties: { scope: "/s", roleDefinitionId: ROLE, principalId: GUID, principalType: "User" },
    });
    expect(a).toMatchObject({ name: "n", scope: "/s", principalId: GUID });
  });

  it("resolves role names from definitions", () => {
    const defs = [
      { id: ROLE, name: GUID.toUpperCase(), roleName: "Reader", roleType: "BuiltInRole" },
    ];
    expect(roleNameOf(defs, ROLE)).toBe("Reader");
    expect(roleNameOf([], ROLE)).toBe(GUID);
  });
});

describe("graph calls", () => {
  it("creates an application with only a display name", async () => {
    const f = vi.fn().mockResolvedValue(json({ id: "o", appId: "a", displayName: "web" }));
    const app = await createApplication("  web ", asFetch(f));
    expect(app.displayName).toBe("web");
    expect(urlOf(f, 0)).toBe(`${GRAPH_PROXY_BASE}/applications`);
    expect(bodyOf(f, 0)).toEqual({ displayName: "web" });
  });

  it("returns the secret value only from addSecret", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(
        json({ keyId: "k", secretText: "s3cr3t", endDateTime: "2027-01-01T00:00:00Z" }),
      )
      .mockResolvedValueOnce(
        json({
          value: [
            { id: "o", displayName: "web", passwordCredentials: [{ keyId: "k", hint: "s3c" }] },
          ],
        }),
      );
    const created = await addSecret("o", " ci ", "2027-01-01T00:00:00Z", asFetch(f));
    expect(created).toMatchObject({ keyId: "k", value: "s3cr3t" });
    expect(bodyOf(f, 0)).toEqual({ displayName: "ci", endDateTime: "2027-01-01T00:00:00Z" });
    expect(urlOf(f, 0)).toContain("/applications/o/addPassword");

    const apps = await listApplications(asFetch(f));
    expect(JSON.stringify(apps)).not.toContain("s3cr3t");
    expect(apps[0].secrets[0].hint).toBe("s3c");
  });

  it("removes a secret by key id", async () => {
    const f = vi.fn().mockResolvedValue(new Response(null, { status: 204 }));
    await removeSecret("o", "k", asFetch(f));
    expect(urlOf(f, 0)).toContain("/applications/o/removePassword");
    expect(bodyOf(f, 0)).toEqual({ keyId: "k" });
  });

  it("sends the user password once and returns no password", async () => {
    const f = vi
      .fn()
      .mockResolvedValue(json({ id: "u", userPrincipalName: "a@x", displayName: "A" }));
    const user = await createUser(
      {
        displayName: "A",
        mailNickname: "a",
        userPrincipalName: "a@x",
        password: "Pw1!pw1!pw",
        forceChangePasswordNextSignIn: true,
      },
      asFetch(f),
    );
    expect(bodyOf(f, 0).password).toBe("Pw1!pw1!pw");
    expect(JSON.stringify(user)).not.toContain("Pw1!");
  });

  it("lists groups with their members", async () => {
    const f = vi.fn((url: string) =>
      Promise.resolve(
        String(url).endsWith("/members")
          ? json({
              value: [{ id: "u", displayName: "Ann", "@odata.type": "#microsoft.graph.user" }],
            })
          : json({ value: [{ id: "g", displayName: "ops" }] }),
      ),
    );
    const groups = await listGroups(asFetch(f));
    expect(groups).toEqual([
      {
        id: "g",
        displayName: "ops",
        members: [expect.objectContaining({ id: "u", type: "User" })],
      },
    ]);
  });

  it("adds and removes members through $ref", async () => {
    const f = vi.fn().mockResolvedValue(new Response(null, { status: 204 }));
    await addMember("g", "m", asFetch(f));
    await removeMember("g", "m", asFetch(f));
    expect(urlOf(f, 0)).toBe(`${GRAPH_PROXY_BASE}/groups/g/members/$ref`);
    expect(bodyOf(f, 0)).toEqual({ id: "m" });
    expect(initOf(f, 1).method).toBe("DELETE");
    expect(urlOf(f, 1)).toBe(`${GRAPH_PROXY_BASE}/groups/g/members/m/$ref`);
  });

  it("resolves only valid unique ids", async () => {
    const f = vi
      .fn()
      .mockResolvedValue(
        json({ value: [{ id: GUID, displayName: "Ann", "@odata.type": "#microsoft.graph.user" }] }),
      );
    const out = await resolvePrincipals([GUID, GUID, "nope"], asFetch(f));
    expect(bodyOf(f, 0)).toEqual({ ids: [GUID] });
    expect(out.get(GUID)?.name).toBe("Ann");
    const none = vi.fn();
    expect((await resolvePrincipals(["x"], asFetch(none))).size).toBe(0);
    expect(none).not.toHaveBeenCalled();
  });
});

describe("authorization calls", () => {
  const sub = subscriptionScope("s");
  const rg = groupScope("s", "my rg");

  it("builds scopes", () => {
    expect(sub).toBe("/subscriptions/s");
    expect(rg).toBe("/subscriptions/s/resourceGroups/my%20rg");
  });

  it("lists and sorts role definitions", async () => {
    const f = vi.fn().mockResolvedValue(
      json({
        value: [
          { id: "2", name: "b", properties: { roleName: "Reader" } },
          { id: "1", name: "a", properties: { roleName: "Owner" } },
        ],
      }),
    );
    const defs = await listRoleDefinitions(sub, asFetch(f));
    expect(defs.map((d) => d.roleName)).toEqual(["Owner", "Reader"]);
    expect(urlOf(f, 0)).toBe(
      `${AUTHZ_PROXY_BASE}/subscriptions/s/providers/Microsoft.Authorization/roleDefinitions?api-version=2022-04-01`,
    );
  });

  it("creates an assignment with fixed fields and a GUID name", async () => {
    const f = vi.fn().mockResolvedValue(json({}));
    await createRoleAssignment(
      rg,
      { roleDefinitionId: ROLE, principalId: ` ${GUID} `, principalType: "User" },
      GUID,
      asFetch(f),
    );
    expect(initOf(f, 0).method).toBe("PUT");
    expect(urlOf(f, 0)).toContain(`/roleAssignments/${GUID}?api-version=`);
    expect(bodyOf(f, 0)).toEqual({
      properties: { roleDefinitionId: ROLE, principalId: GUID, principalType: "User" },
    });
  });

  it("omits the principal type for raw GUIDs", async () => {
    const f = vi.fn().mockResolvedValue(json({}));
    await createRoleAssignment(
      sub,
      { roleDefinitionId: ROLE, principalId: GUID },
      GUID,
      asFetch(f),
    );
    expect(bodyOf(f, 0).properties).not.toHaveProperty("principalType");
  });

  it("lists and deletes assignments", async () => {
    const f = vi
      .fn()
      .mockResolvedValue(json({ value: [{ name: "n", properties: { principalId: GUID } }] }));
    expect((await listRoleAssignments(sub, asFetch(f)))[0].principalId).toBe(GUID);
    const d = vi.fn().mockResolvedValue(new Response(null, { status: 204 }));
    await deleteRoleAssignment(sub, "n", asFetch(d));
    expect(initOf(d, 0).method).toBe("DELETE");
  });

  it("creates, lists and deletes locks", async () => {
    const f = vi.fn(() =>
      Promise.resolve(
        json({
          value: [{ name: "keep", id: "/x", properties: { level: "CanNotDelete", notes: "n" } }],
        }),
      ),
    );
    await createLock(rg, "keep", "CanNotDelete", " why ", asFetch(f));
    expect(bodyOf(f, 0)).toEqual({ properties: { level: "CanNotDelete", notes: "why" } });
    expect((await listLocks(rg, asFetch(f)))[0]).toMatchObject({
      name: "keep",
      level: "CanNotDelete",
    });
    await deleteLock(rg, "keep", asFetch(f));
    expect(initOf(f, 2).method).toBe("DELETE");
    await createLock(rg, "k2", "ReadOnly", "", asFetch(f));
    expect(bodyOf(f, 3)).toEqual({ properties: { level: "ReadOnly" } });
  });

  it("reduces a policy assignment to its definition name", async () => {
    const f = vi.fn().mockResolvedValue(
      json({
        value: [
          {
            name: "pa",
            properties: {
              displayName: "Audit",
              scope: "/s",
              policyDefinitionId: "/x/policyDefinitions/audit-vms",
            },
          },
        ],
      }),
    );
    expect((await listPolicyAssignments(sub, asFetch(f)))[0].definition).toBe("audit-vms");
  });

  it("surfaces ScopeLocked errors", async () => {
    const f = vi
      .fn()
      .mockResolvedValue(json({ error: { code: "ScopeLocked", message: "locked" } }, 409));
    await expect(deleteLock(rg, "k", asFetch(f))).rejects.toMatchObject({ httpStatus: 409 });
  });
});

describe("validation helpers", () => {
  it("validates input", () => {
    expect(isGuid(GUID)).toBe(true);
    expect(isGuid("x")).toBe(false);
    expect(validLockName("keep-1")).toBe(true);
    expect(validLockName("-bad")).toBe(false);
    expect(validUpn("a@gopherstack.onmicrosoft.com")).toBe(true);
    expect(validUpn("a b@x")).toBe(false);
    expect(validPassword("")).toBe(false);
    expect(validPassword("x")).toBe(true);
    expect(mailNicknameFrom("Ann  O'Neil!")).toBe("annoneil");
  });

  it("computes an expiry in whole seconds", () => {
    expect(secretExpiry(12, new Date("2026-03-15T10:20:30.123Z"))).toBe("2027-03-15T10:20:30Z");
  });
});
