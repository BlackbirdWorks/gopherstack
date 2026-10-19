import { describe, expect, it, vi } from "vitest";

import {
  ArmError,
  armRequest,
  deleteResourceGroup,
  formatTags,
  getSubscriptionId,
  inGroup,
  listNamespaceKeys,
  listResourceGroups,
  listStorageKeys,
  parseArmId,
  parseTags,
  putStorageAccount,
  resourcePath,
  resourcesInGroup,
  shortType,
  validateNamespaceName,
  validateResourceGroupName,
  validateStorageAccountName,
} from "./azurearm";

const SUB = "00000000-0000-0000-0000-000000000000";
const ACCT_ID = `/subscriptions/${SUB}/resourceGroups/Demo/providers/Microsoft.Storage/storageAccounts/acct1`;

function res(body: unknown, status = 200): Response {
  const text = body === undefined ? "" : typeof body === "string" ? body : JSON.stringify(body);
  return new Response(text, { status });
}

function mockFetch(body?: unknown, status = 200) {
  return vi.fn<typeof fetch>(() => Promise.resolve(res(body, status)));
}

describe("parseArmId", () => {
  it.each([
    [ACCT_ID, { subscription: SUB, resourceGroup: "Demo", provider: "Microsoft.Storage" }],
    [
      `/subscriptions/${SUB}/resourcegroups/rg1`,
      { subscription: SUB, resourceGroup: "rg1", provider: "" },
    ],
    ["", { subscription: "", resourceGroup: "", provider: "" }],
  ])("%s", (id, want) => {
    expect(parseArmId(id)).toMatchObject(want);
  });

  it("captures type and name segments", () => {
    expect(parseArmId(ACCT_ID).segments).toEqual(["storageAccounts", "acct1"]);
  });
});

describe("group helpers", () => {
  const a = { id: ACCT_ID, name: "acct1", type: "Microsoft.Storage/storageAccounts" };
  const b = { id: ACCT_ID.replace("Demo", "Other"), name: "acct2", type: "x/y" };

  it("matches groups case-insensitively", () => {
    expect(inGroup(a, "demo")).toBe(true);
    expect(inGroup(b, "demo")).toBe(false);
    expect(resourcesInGroup([a, b], "DEMO")).toEqual([a]);
  });

  it("shortType drops the namespace", () => {
    expect(shortType("Microsoft.Storage/storageAccounts")).toBe("storageAccounts");
    expect(shortType("plain")).toBe("plain");
  });

  it("resourcePath rebuilds the scoped path", () => {
    expect(resourcePath(SUB, a)).toBe(
      `/subscriptions/${SUB}/resourceGroups/Demo/providers/Microsoft.Storage/storageAccounts/acct1`,
    );
  });
});

describe("validation", () => {
  it.each([
    ["rg", "demo", null],
    ["rg", "", "Name is required"],
    ["rg", "bad name", "not-null"],
    ["rg", "trailing.", "not-null"],
    ["storage", "abc123", null],
    ["storage", "ab", "not-null"],
    ["storage", "Upper1", "not-null"],
    ["sb", "my-bus-1", null],
    ["sb", "1bus-name", "not-null"],
    ["sb", "short", "not-null"],
    ["sb", "ends-with-", "not-null"],
  ])("%s %s", (kind, name, want) => {
    const fn =
      kind === "rg"
        ? validateResourceGroupName
        : kind === "storage"
          ? validateStorageAccountName
          : validateNamespaceName;
    const got = fn(name);
    if (want === null) expect(got).toBeNull();
    else if (want === "not-null") expect(got).not.toBeNull();
    else expect(got).toBe(want);
  });
});

describe("tags", () => {
  it("round-trips", () => {
    const tags = parseTags("env=dev, team=core\nowner=a=b");
    expect(tags).toEqual({ env: "dev", team: "core", owner: "a=b" });
    expect(formatTags({ env: "dev" })).toBe("env=dev");
    expect(formatTags()).toBe("");
  });

  it.each(["novalue", "=x"])("rejects %s", (bad) => {
    expect(() => parseTags(bad)).toThrow(/Invalid tag/);
  });
});

describe("armRequest", () => {
  it("adds api-version and JSON body", async () => {
    const f = mockFetch({ ok: true });
    await armRequest("PUT", "/subscriptions/x", { a: 1 }, f);
    const [url, init] = f.mock.calls[0];
    expect(String(url)).toBe("/dashboard/api/v1/azure/arm/subscriptions/x?api-version=2021-04-01");
    expect(init?.method).toBe("PUT");
    expect(init?.body).toBe('{"a":1}');
  });

  it("surfaces ARM error envelopes", async () => {
    const f = mockFetch(
      { error: { code: "ResourceGroupNotFound", message: "Resource group not found." } },
      404,
    );
    await expect(armRequest("GET", "/x", undefined, f)).rejects.toMatchObject({
      name: "ArmError",
      status: 404,
      code: "ResourceGroupNotFound",
      message: "Resource group not found.",
    });
  });

  it("falls back to the raw body for non-JSON errors", async () => {
    const f = mockFetch("boom", 502);
    const err = await armRequest("GET", "/x", undefined, f).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ArmError);
    expect((err as ArmError).message).toBe("boom");
  });

  it("wraps network failures", async () => {
    const f = vi.fn<typeof fetch>(() => Promise.reject(new Error("offline")));
    await expect(armRequest("GET", "/x", undefined, f)).rejects.toMatchObject({
      code: "NetworkError",
      message: "offline",
    });
  });

  it("returns undefined for empty bodies", async () => {
    expect(await armRequest("DELETE", "/x", undefined, mockFetch())).toBeUndefined();
  });
});

describe("operations", () => {
  it("getSubscriptionId picks the first subscription", async () => {
    expect(await getSubscriptionId(mockFetch({ value: [{ subscriptionId: SUB }] }))).toBe(SUB);
    await expect(getSubscriptionId(mockFetch({ value: [] }))).rejects.toMatchObject({
      status: 404,
    });
  });

  it("listResourceGroups sorts by name", async () => {
    const f = mockFetch({ value: [{ name: "b" }, { name: "a" }] });
    expect((await listResourceGroups(SUB, f)).map((g) => g.name)).toEqual(["a", "b"]);
  });

  it("putStorageAccount sends sku and kind", async () => {
    const f = mockFetch({ id: ACCT_ID, name: "acct1", type: "t" });
    await putStorageAccount(
      SUB,
      {
        name: "acct1",
        resourceGroup: "Demo",
        location: "eastus",
        sku: "Standard_GRS",
        kind: "StorageV2",
        tags: { a: "b" },
      },
      f,
    );
    const [url, init] = f.mock.calls[0];
    expect(String(url)).toContain(
      "/resourceGroups/Demo/providers/Microsoft.Storage/storageAccounts/acct1",
    );
    expect(JSON.parse(String(init?.body))).toEqual({
      location: "eastus",
      kind: "StorageV2",
      sku: { name: "Standard_GRS" },
      tags: { a: "b" },
    });
  });

  it("deleteResourceGroup issues DELETE", async () => {
    const f = mockFetch();
    await deleteResourceGroup(SUB, "my rg", f);
    expect(String(f.mock.calls[0][0])).toContain("/resourcegroups/my%20rg");
    expect(f.mock.calls[0][1]?.method).toBe("DELETE");
  });

  it("listStorageKeys POSTs listKeys", async () => {
    const f = mockFetch({ keys: [{ keyName: "key1", value: "v" }] });
    const keys = await listStorageKeys(SUB, { id: ACCT_ID, name: "acct1", type: "t" }, f);
    expect(keys).toEqual([{ keyName: "key1", value: "v" }]);
    expect(String(f.mock.calls[0][0])).toContain("/storageAccounts/acct1/listKeys?");
    expect(f.mock.calls[0][1]?.method).toBe("POST");
  });

  it("listNamespaceKeys targets the authorization rule", async () => {
    const id = `/subscriptions/${SUB}/resourceGroups/Demo/providers/Microsoft.ServiceBus/namespaces/bus1`;
    const f = mockFetch({ primaryKey: "k" });
    expect(
      (await listNamespaceKeys(SUB, { id, name: "bus1", type: "t" }, undefined, f)).primaryKey,
    ).toBe("k");
    expect(String(f.mock.calls[0][0])).toContain(
      "/namespaces/bus1/authorizationRules/RootManageSharedAccessKey/listKeys",
    );
  });
});
