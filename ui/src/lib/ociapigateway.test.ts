import { describe, expect, it, vi } from "vitest";

import {
  APIGW_PROXY_BASE,
  backendLabel,
  getGateway,
  isServing,
  joinPath,
  listCertificates,
  listCompartments,
  listDeployments,
  listGateways,
  parseCertificate,
  parseDeployment,
  parseGateway,
  parseRoute,
  prettyBody,
  summarizePolicies,
  tryIt,
} from "./ociapigateway";

const json = (body: unknown, status = 200, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
const asFetch = (f: unknown) => f as typeof fetch;
const comp = "ocid1.compartment.oc1..aaaa";
const q = `compartmentId=${encodeURIComponent(comp)}`;

const deployment = {
  id: "d1",
  displayName: "orders",
  lifecycleState: "ACTIVE",
  gatewayId: "g1",
  pathPrefix: "/v1",
  endpoint: "https://gw.example/v1",
  specification: {
    requestPolicies: { rateLimiting: { rateInRequestsPerSecond: 5, rateKey: "CLIENT_IP" } },
    routes: [
      {
        path: "/hello",
        methods: ["GET", "POST"],
        backend: { type: "STOCK_RESPONSE_BACKEND", status: 200, body: "hi" },
        requestPolicies: {
          cors: { allowedOrigins: ["*"] },
          headerTransformations: { setHeaders: { items: [{ name: "a" }, { name: "b" }] } },
        },
        responsePolicies: { headerTransformations: { filterHeaders: { items: [{ name: "x" }] } } },
      },
      {
        path: "/up",
        methods: ["ANY"],
        backend: { type: "HTTP_BACKEND", url: "https://up.example" },
      },
    ],
  },
};

describe("parsing", () => {
  it("parses a gateway", () => {
    expect(
      parseGateway({
        id: "g1",
        displayName: "edge",
        lifecycleState: "ACTIVE",
        hostname: "h.apigateway.us-ashburn-1.oci.customer-oci.com",
        endpointType: "PUBLIC",
      }),
    ).toMatchObject({
      name: "edge",
      state: "ACTIVE",
      endpointType: "PUBLIC",
      hostname: expect.any(String),
    });
    expect(isServing({ state: "ACTIVE" })).toBe(true);
    expect(isServing({ state: "CREATING" })).toBe(false);
  });

  it("parses routes with backends and policy summaries", () => {
    const d = parseDeployment(deployment);
    expect(d.policies).toEqual(["rate 5/s client_ip"]);
    expect(d.routes[0]).toEqual({
      path: "/hello",
      methods: ["GET", "POST"],
      backendType: "STOCK_RESPONSE_BACKEND",
      backend: "HTTP 200",
      policies: ["CORS", "2 request header rules", "1 response header rule"],
    });
    expect(d.routes[1]).toMatchObject({ backend: "https://up.example" });
    expect(
      parseRoute({ path: "/x", backend: { type: "ORACLE_FUNCTIONS_BACKEND", functionId: "fn" } })
        .backend,
    ).toBe("fn");
    expect(
      parseRoute({
        path: "/x",
        backend: { type: "DYNAMIC_ROUTING_BACKEND", routingBackends: [{}, {}] },
      }).backend,
    ).toBe("2 backends");
  });

  it("summarizes authentication and authorization", () => {
    expect(
      summarizePolicies({
        authentication: { type: "JWT_AUTHENTICATION" },
        authorization: { type: "ANY_OF" },
      }),
    ).toEqual(["JWT auth", "authz any_of"]);
    expect(summarizePolicies({})).toEqual([]);
  });

  it("labels backends", () => {
    expect(backendLabel("HTTP_BACKEND")).toBe("HTTP");
    expect(backendLabel("")).toBe("—");
  });

  it("parses certificates without key material", () => {
    const c = parseCertificate({
      id: "c1",
      displayName: "tls",
      lifecycleState: "ACTIVE",
      subjectNames: ["a.example", "b.example"],
      timeNotValidAfter: "2030-01-01T00:00:00Z",
      privateKey: "must-not-surface",
    });
    expect(c).toEqual({
      id: "c1",
      name: "tls",
      state: "ACTIVE",
      subjects: ["a.example", "b.example"],
      notAfter: "2030-01-01T00:00:00Z",
      created: "",
    });
    expect(JSON.stringify(c)).not.toContain("must-not-surface");
  });

  it("joins prefixes and route paths", () => {
    expect(joinPath("/v1", "/hello")).toBe("/v1/hello");
    expect(joinPath("/v1/", "hello")).toBe("/v1/hello");
    expect(joinPath("/", "/x")).toBe("/x");
    expect(joinPath("/", "/")).toBe("/");
  });

  it("pretty-prints JSON and flags binary bodies", () => {
    expect(prettyBody({ body: '{"a":1}', encoding: "utf8" })).toBe('{\n  "a": 1\n}');
    expect(prettyBody({ body: "plain", encoding: "utf8" })).toBe("plain");
    expect(prettyBody({ body: "AAE=", encoding: "base64" })).toContain("binary");
  });
});

describe("client", () => {
  it("lists compartments, gateways and certificates", async () => {
    const fetchFn = vi.fn((url: string) =>
      Promise.resolve(
        json(
          url.endsWith("/compartments")
            ? { compartments: [comp] }
            : { items: [{ id: "x", displayName: "n" }] },
        ),
      ),
    );
    expect(await listCompartments(asFetch(fetchFn))).toEqual([comp]);
    expect(await listGateways(comp, asFetch(fetchFn))).toHaveLength(1);
    expect(await listCertificates(comp, asFetch(fetchFn))).toHaveLength(1);
    expect(fetchFn.mock.calls[1]?.[0]).toBe(`${APIGW_PROXY_BASE}/api/gateways?${q}`);
  });

  it("reads one gateway", async () => {
    const fetchFn = vi.fn(() => Promise.resolve(json({ id: "g1", displayName: "edge" })));
    expect((await getGateway("g1", asFetch(fetchFn))).name).toBe("edge");
  });

  it("fetches each deployment of a gateway for its routes", async () => {
    const fetchFn = vi.fn((url: string) =>
      Promise.resolve(
        json(url.includes("/deployments/d1") ? deployment : { items: [{ id: "d1" }] }),
      ),
    );
    const out = await listDeployments(comp, "g1", asFetch(fetchFn));
    expect(out).toHaveLength(1);
    expect(out[0]?.routes).toHaveLength(2);
    expect(fetchFn.mock.calls[0]?.[0]).toBe(
      `${APIGW_PROXY_BASE}/api/deployments?${q}&gatewayId=g1`,
    );
  });

  it("sends a try-it request to the selected gateway only", async () => {
    const fetchFn = vi.fn((_url: string, _init?: RequestInit) =>
      Promise.resolve(
        json({
          status: 200,
          headers: { "Content-Type": "text/plain" },
          body: "hi",
          encoding: "utf8",
          truncated: false,
        }),
      ),
    );
    const res = await tryIt(
      "ocid1.apigateway.oc1..g",
      {
        method: "POST",
        path: "/v1/hello",
        query: "?a=1",
        body: "x",
        headers: [
          { name: "X-A", value: "1" },
          { name: " ", value: "skip" },
        ],
      },
      asFetch(fetchFn),
    );
    expect(res).toEqual({
      status: 200,
      headers: { "Content-Type": "text/plain" },
      body: "hi",
      encoding: "utf8",
      truncated: false,
    });
    expect(fetchFn.mock.calls[0]?.[0]).toBe(
      `${APIGW_PROXY_BASE}/api/gateways/ocid1.apigateway.oc1..g/invoke`,
    );
    expect(JSON.parse(String(fetchFn.mock.calls[0]?.[1]?.body))).toEqual({
      method: "POST",
      path: "/v1/hello",
      query: "a=1",
      body: "x",
      headers: [{ name: "X-A", value: "1" }],
    });
  });

  it("surfaces proxy errors from try-it", async () => {
    const fetchFn = vi.fn(() =>
      Promise.resolve(json({ code: "InvalidParameter", message: "bad" }, 400)),
    );
    await expect(
      tryIt("g", { method: "GET", path: "x", query: "", body: "", headers: [] }, asFetch(fetchFn)),
    ).rejects.toMatchObject({ httpStatus: 400, code: "InvalidParameter" });
  });
});
