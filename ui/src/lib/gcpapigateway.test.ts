import { describe, expect, it, vi } from "vitest";

import {
  GW_PROXY_BASE,
  createGateway,
  deleteGateway,
  diagnosticsFor,
  getConfig,
  isActive,
  listApis,
  listConfigs,
  listGateways,
  listOperations,
  listProjects,
  parseConfig,
  parseGateway,
  parseOperation,
  tryGateway,
  validGatewayId,
} from "./gcpapigateway";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const asFetch = (f: unknown) => f as typeof fetch;
const urlOf = (f: { mock: { calls: unknown[][] } }, i: number): string =>
  String((f.mock.calls[i] ?? [])[0]);
const initOf = (f: { mock: { calls: unknown[][] } }, i: number): RequestInit =>
  (f.mock.calls[i] ?? [])[1] as RequestInit;

const cfgName = "projects/p/locations/global/apis/orders/configs/good";
const gwName = "projects/p/locations/us-central1/gateways/edge";

describe("parsing", () => {
  it("decodes the OpenAPI documents of a config", () => {
    const cfg = parseConfig({
      name: cfgName,
      state: "ACTIVE",
      serviceConfigId: "svc-1",
      openapiDocuments: [
        { document: { path: "openapi.yaml", contents: btoa("swagger: '2.0'") } },
        { document: { path: "bin", contents: btoa("\u00FF\u00FE") } },
      ],
    });
    expect(cfg).toMatchObject({ id: "good", api: "projects/p/locations/global/apis/orders" });
    expect(cfg.documents[0]).toEqual({
      path: "openapi.yaml",
      contents: "swagger: '2.0'",
      decoded: true,
    });
    expect(cfg.documents[1].decoded).toBe(false);
  });

  it("parses a gateway and its region", () => {
    expect(
      parseGateway({ name: gwName, state: "ACTIVE", defaultHostname: "edge-x.uc.gateway.dev" }),
    ).toMatchObject({
      id: "edge",
      location: "us-central1",
      defaultHostname: "edge-x.uc.gateway.dev",
    });
    expect(isActive({ state: "ACTIVE" })).toBe(true);
    expect(isActive({ state: "CREATING" })).toBe(false);
  });

  it("derives operation status", () => {
    const base = {
      name: "projects/p/locations/global/operations/o1",
      metadata: { verb: "create" },
    };
    expect(parseOperation(base).status).toBe("RUNNING");
    expect(parseOperation({ ...base, done: true }).status).toBe("SUCCEEDED");
    const failed = parseOperation({
      ...base,
      done: true,
      error: { code: 3, message: "bad spec" },
      metadata: { verb: "create", target: cfgName, diagnostics: [{ location: "a", message: "m" }] },
    });
    expect(failed).toMatchObject({ status: "FAILED", statusMessage: "bad spec" });
    expect(failed.diagnostics).toEqual([{ location: "a", message: "m" }]);
  });

  it("finds the newest diagnostics for a target", () => {
    const op = (id: string, createTime: string, message: string) =>
      parseOperation({
        name: `projects/p/locations/global/operations/${id}`,
        metadata: { target: cfgName, createTime, diagnostics: [{ location: "l", message }] },
      });
    const ops = [op("a", "2026-01-01", "old"), op("b", "2026-02-01", "new")];
    expect(diagnosticsFor(cfgName, ops)[0].message).toBe("new");
    expect(diagnosticsFor("other", ops)).toEqual([]);
  });

  it("validates gateway ids", () => {
    expect(validGatewayId("edge-1")).toBe(true);
    expect(validGatewayId("Edge")).toBe(false);
    expect(validGatewayId("1edge")).toBe(false);
    expect(validGatewayId("")).toBe(false);
  });
});

describe("requests", () => {
  it("lists projects, APIs, configs and gateways", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(json({ projects: ["p"] }))
      .mockResolvedValueOnce(json({ apis: [{ name: "projects/p/locations/global/apis/orders" }] }))
      .mockResolvedValueOnce(json({ apiConfigs: [{ name: cfgName, state: "FAILED" }] }))
      .mockResolvedValueOnce(json({ gateways: [{ name: gwName }] }));
    expect(await listProjects(asFetch(f))).toEqual(["p"]);
    expect((await listApis("p", asFetch(f)))[0].id).toBe("orders");
    expect(
      (await listConfigs("projects/p/locations/global/apis/orders", asFetch(f)))[0].state,
    ).toBe("FAILED");
    expect((await listGateways("p", asFetch(f)))[0].id).toBe("edge");
    expect(urlOf(f, 0)).toBe(`${GW_PROXY_BASE}/projects`);
    expect(urlOf(f, 1)).toBe(`${GW_PROXY_BASE}/projects/p/locations/global/apis`);
    expect(urlOf(f, 2)).toBe(`${GW_PROXY_BASE}/projects/p/locations/global/apis/orders/configs`);
    expect(urlOf(f, 3)).toBe(`${GW_PROXY_BASE}/projects/p/locations/-/gateways`);
  });

  it("fetches a full config", async () => {
    const f = vi.fn().mockResolvedValue(json({ name: cfgName }));
    await getConfig(cfgName, asFetch(f));
    expect(urlOf(f, 0)).toBe(`${GW_PROXY_BASE}/${cfgName}`);
  });

  it("creates and deletes a gateway", async () => {
    const f = vi.fn().mockImplementation(() => Promise.resolve(json({ name: "operations/x" })));
    await createGateway("p", "us-central1", "edge", " Edge ", cfgName, asFetch(f));
    expect(urlOf(f, 0)).toBe(
      `${GW_PROXY_BASE}/projects/p/locations/us-central1/gateways?gatewayId=edge`,
    );
    expect(initOf(f, 0).method).toBe("POST");
    expect(JSON.parse(String(initOf(f, 0).body))).toEqual({
      displayName: "Edge",
      apiConfig: cfgName,
    });
    await deleteGateway(gwName, asFetch(f));
    expect(urlOf(f, 1)).toBe(`${GW_PROXY_BASE}/${gwName}`);
    expect(initOf(f, 1).method).toBe("DELETE");
  });

  it("lists operations of global and every region, newest first", async () => {
    const f = vi.fn().mockImplementation((u: string) =>
      Promise.resolve(
        json({
          operations: [
            {
              name: `${u.includes("global") ? "g" : "r"}`,
              metadata: { createTime: u.includes("global") ? "2026-01-01" : "2026-03-01" },
            },
          ],
        }),
      ),
    );
    const ops = await listOperations("p", ["us-central1", "us-central1"], asFetch(f));
    expect(f).toHaveBeenCalledTimes(2);
    expect(ops.map((o) => o.name)).toEqual(["r", "g"]);
  });

  it("sends try-it requests to the selected gateway with the key in the body only", async () => {
    const f = vi
      .fn()
      .mockResolvedValue(json({ status: 200, headers: {}, body: "ok", encoding: "utf8" }));
    const res = await tryGateway(
      gwName,
      {
        method: "GET",
        path: "/open",
        query: "?a=1",
        body: "",
        headers: [{ name: " ", value: "x" }],
        apiKey: "k",
      },
      asFetch(f),
    );
    expect(res.status).toBe(200);
    expect(urlOf(f, 0)).toBe(`${GW_PROXY_BASE}/${gwName}:invoke`);
    expect(urlOf(f, 0)).not.toContain("k=");
    expect(JSON.parse(String(initOf(f, 0).body))).toEqual({
      method: "GET",
      path: "/open",
      query: "a=1",
      body: "",
      headers: [],
      apiKey: "k",
    });
  });

  it("omits an empty key", async () => {
    const f = vi.fn().mockResolvedValue(json({ status: 200 }));
    await tryGateway(
      gwName,
      { method: "GET", path: "/", query: "", body: "", headers: [], apiKey: "" },
      asFetch(f),
    );
    expect(JSON.parse(String(initOf(f, 0).body))).not.toHaveProperty("apiKey");
  });

  it("surfaces proxy errors", async () => {
    const f = vi.fn().mockResolvedValue(
      json(
        {
          error: {
            code: 400,
            message: "Gateway is in state CREATING",
            status: "FAILED_PRECONDITION",
          },
        },
        400,
      ),
    );
    await expect(
      tryGateway(
        gwName,
        { method: "GET", path: "/", query: "", body: "", headers: [], apiKey: "" },
        asFetch(f),
      ),
    ).rejects.toThrow(/CREATING/);
  });
});
