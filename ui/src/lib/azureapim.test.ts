import { describe, expect, it, vi } from "vitest";

import {
  APIM_PROXY_BASE,
  getPolicy,
  isActiveSubscription,
  listApis,
  listNamedValues,
  listOperations,
  listProducts,
  listServices,
  listSubscriptions,
  operationPath,
  parseNamedValue,
  parseService,
  tryGateway,
} from "./azureapim";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const asFetch = (f: unknown) => f as typeof fetch;
const urlOf = (f: { mock: { calls: unknown[][] } }, i: number): string =>
  String((f.mock.calls[i] ?? [])[0]);
const initOf = (f: { mock: { calls: unknown[][] } }, i: number): RequestInit =>
  (f.mock.calls[i] ?? [])[1] as RequestInit;

const sub = "00000000-0000-0000-0000-000000000000";
const target = { resourceGroup: "demo", name: "gw1" };
const base = `${APIM_PROXY_BASE}/subscriptions/${sub}/resourceGroups/demo/providers/Microsoft.ApiManagement/service/gw1`;
const ver = "?api-version=2024-05-01";

describe("parsing", () => {
  it("parses a service", () => {
    expect(
      parseService({
        id: `/subscriptions/${sub}/resourceGroups/demo/providers/Microsoft.ApiManagement/service/gw1`,
        name: "gw1",
        location: "westus",
        sku: { name: "Developer", capacity: 1 },
        properties: { provisioningState: "Succeeded", gatewayUrl: "https://gw1.azure-api.net" },
      }),
    ).toMatchObject({
      name: "gw1",
      resourceGroup: "demo",
      sku: "Developer",
      capacity: 1,
      state: "Succeeded",
      gatewayUrl: "https://gw1.azure-api.net",
    });
  });

  it("never carries the value of a secret named value", () => {
    expect(
      parseNamedValue({
        name: "h",
        properties: { displayName: "h", isSecret: true, value: "leak" },
      }),
    ).toMatchObject({ secret: true, value: "" });
    expect(
      parseNamedValue({ name: "h", properties: { displayName: "h", secret: true, value: "leak" } })
        .value,
    ).toBe("");
    expect(
      parseNamedValue({ name: "s", properties: { displayName: "s", value: "vis" } }),
    ).toMatchObject({
      secret: false,
      value: "vis",
    });
    expect(
      parseNamedValue({ name: "k", properties: { keyVault: { secretIdentifier: "x" } } }).keyVault,
    ).toBe(true);
  });

  it("joins an API path and an operation template", () => {
    expect(operationPath({ path: "echo" }, { urlTemplate: "/items/{id}" })).toBe(
      "/echo/items/{id}",
    );
    expect(operationPath({ path: "/echo/" }, { urlTemplate: "items" })).toBe("/echo/items");
    expect(operationPath({ path: "" }, { urlTemplate: "/x" })).toBe("/x");
  });

  it("recognises active subscriptions case-insensitively", () => {
    expect(isActiveSubscription({ state: "active" })).toBe(true);
    expect(isActiveSubscription({ state: "Active" })).toBe(true);
    expect(isActiveSubscription({ state: "suspended" })).toBe(false);
  });
});

describe("requests", () => {
  it("lists services sorted", async () => {
    const f = vi.fn().mockResolvedValue(
      json({
        value: [
          {
            id: `/subscriptions/${sub}/resourceGroups/b/providers/Microsoft.ApiManagement/service/z`,
            name: "z",
          },
          {
            id: `/subscriptions/${sub}/resourceGroups/a/providers/Microsoft.ApiManagement/service/y`,
            name: "y",
          },
        ],
      }),
    );
    expect((await listServices(sub, asFetch(f))).map((s) => s.name)).toEqual(["y", "z"]);
    expect(urlOf(f, 0)).toBe(
      `${APIM_PROXY_BASE}/subscriptions/${sub}/providers/Microsoft.ApiManagement/service${ver}`,
    );
  });

  it("reads each child collection", async () => {
    const f = vi
      .fn()
      .mockImplementation(() =>
        Promise.resolve(
          json({ value: [{ name: "x", properties: { displayName: "X", method: "get" } }] }),
        ),
      );
    await listApis(sub, target, asFetch(f));
    await listOperations(sub, target, "echo", asFetch(f));
    await listProducts(sub, target, asFetch(f));
    await listSubscriptions(sub, target, asFetch(f));
    await listNamedValues(sub, target, asFetch(f));
    expect([0, 1, 2, 3, 4].map((i) => urlOf(f, i))).toEqual([
      `${base}/apis${ver}`,
      `${base}/apis/echo/operations${ver}`,
      `${base}/products${ver}`,
      `${base}/subscriptions${ver}`,
      `${base}/namedValues${ver}`,
    ]);
    expect((await listOperations(sub, target, "echo", asFetch(f)))[0].method).toBe("GET");
  });

  it("reads policies at each scope and treats 404 as empty", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(json({ properties: { value: "<policies/>" } }))
      .mockResolvedValueOnce(json({ properties: { value: "<a/>" } }))
      .mockResolvedValueOnce(json({ properties: { value: "<b/>" } }))
      .mockResolvedValueOnce(json({ error: { code: "NotFound", message: "x" } }, 404));
    expect(await getPolicy(sub, target, {}, asFetch(f))).toBe("<policies/>");
    expect(await getPolicy(sub, target, { api: "echo" }, asFetch(f))).toBe("<a/>");
    expect(await getPolicy(sub, target, { api: "echo", operation: "get" }, asFetch(f))).toBe(
      "<b/>",
    );
    expect(await getPolicy(sub, target, {}, asFetch(f))).toBe("");
    expect(urlOf(f, 0)).toBe(`${base}/policies/policy${ver}&format=rawxml`);
    expect(urlOf(f, 1)).toBe(`${base}/apis/echo/policies/policy${ver}&format=rawxml`);
    expect(urlOf(f, 2)).toBe(
      `${base}/apis/echo/operations/get/policies/policy${ver}&format=rawxml`,
    );
  });

  it("rethrows other policy errors", async () => {
    const f = vi.fn().mockResolvedValue(json({ error: { code: "Boom", message: "bad" } }, 500));
    await expect(getPolicy(sub, target, {}, asFetch(f))).rejects.toThrow();
  });

  it("sends try-it with only the subscription id, never a key", async () => {
    const f = vi.fn().mockResolvedValue(json({ status: 200, headers: {}, body: "ok" }));
    const res = await tryGateway(
      sub,
      target,
      {
        method: "GET",
        path: "/echo/x",
        query: "?q=1",
        body: "",
        headers: [],
        subscription: "k1",
        api: "echo",
      },
      asFetch(f),
    );
    expect(res.status).toBe(200);
    expect(urlOf(f, 0)).toBe(`${base}/invoke`);
    expect(initOf(f, 0).method).toBe("POST");
    const sent = JSON.parse(String(initOf(f, 0).body));
    expect(sent).toMatchObject({ subscription: "k1", api: "echo", query: "q=1" });
    expect(JSON.stringify(sent)).not.toMatch(/key/i);
  });
});
