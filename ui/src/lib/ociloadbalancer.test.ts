import { describe, expect, it, vi } from "vitest";

import {
  LB_PROXY_BASE,
  addBackend,
  backendKey,
  canTry,
  createLoadBalancer,
  deleteBackend,
  deleteLoadBalancer,
  getBackendHealth,
  getBackendSetHealth,
  getLoadBalancer,
  healthTone,
  isServing,
  listCompartments,
  listLoadBalancers,
  listWorkRequests,
  loadHealth,
  parseCertificate,
  parseLoadBalancer,
  prettyBody,
  tryIt,
  updateBackend,
  validBandwidth,
  validIp,
  validPort,
  validWeight,
} from "./ociloadbalancer";

const json = (body: unknown, status = 200, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
const asFetch = (f: unknown) => f as typeof fetch;
const comp = "ocid1.compartment.oc1..aaaa";

const lbRaw = {
  id: "lb1",
  displayName: "edge",
  lifecycleState: "ACTIVE",
  shapeName: "flexible",
  shapeDetails: { minimumBandwidthInMbps: 10, maximumBandwidthInMbps: 100 },
  isPrivate: false,
  ipAddresses: [{ ipAddress: "198.18.0.4", isPublic: true }],
  subnetIds: ["s1"],
  listeners: {
    http: {
      name: "http",
      protocol: "HTTP",
      port: 80,
      defaultBackendSetName: "web",
      hostnameNames: ["h1"],
      gopherstackAddress: "127.0.0.1:40001",
    },
    tls: {
      name: "tls",
      protocol: "HTTP",
      port: 443,
      defaultBackendSetName: "web",
      sslConfiguration: { certificateName: "c" },
      gopherstackAddress: "127.0.0.1:40002",
    },
  },
  backendSets: {
    web: {
      name: "web",
      policy: "ROUND_ROBIN",
      healthChecker: {
        protocol: "HTTP",
        port: 0,
        urlPath: "/",
        returnCode: 200,
        retries: 1,
        timeoutInMillis: 3000,
        intervalInMillis: 1000,
      },
      backends: [
        { name: "127.0.0.1:9001", ipAddress: "127.0.0.1", port: 9001, weight: 2, offline: true },
        {
          name: "127.0.0.1:9002",
          ipAddress: "127.0.0.1",
          port: 9002,
          weight: 1,
          maxConnections: 5,
        },
      ],
    },
  },
  pathRouteSets: {
    p: {
      name: "p",
      pathRoutes: [
        { path: "/a", pathMatchType: { matchType: "PREFIX_MATCH" }, backendSetName: "web" },
      ],
    },
  },
  routingPolicies: {
    r: {
      name: "r",
      rules: [
        {
          name: "r1",
          condition: "all(http.request.url.path eq '/x')",
          actions: [{ backendSetName: "web" }],
        },
      ],
    },
  },
  ruleSets: { rs: { name: "rs", items: [{ action: "ADD_HTTP_REQUEST_HEADER", header: "X-A" }] } },
  certificates: {
    c: {
      certificateName: "c",
      subjectNames: ["lb.example"],
      issuer: "ca",
      notAfter: "2030-01-01T00:00:00Z",
    },
  },
};

describe("parsing", () => {
  it("parses a load balancer with all sub-resources", () => {
    const lb = parseLoadBalancer(lbRaw);
    expect(lb).toMatchObject({
      id: "lb1",
      name: "edge",
      state: "ACTIVE",
      shape: "flexible 10-100 Mbps",
    });
    expect(lb.ips).toEqual([{ address: "198.18.0.4", isPublic: true }]);
    expect(lb.listeners.map((l) => [l.name, l.address, l.tls])).toEqual([
      ["http", "127.0.0.1:40001", false],
      ["tls", "127.0.0.1:40002", true],
    ]);
    expect(lb.listeners[0]?.hostnames).toEqual(["h1"]);
    expect(lb.backendSets[0]?.healthChecker).toMatchObject({
      protocol: "HTTP",
      intervalMillis: 1000,
    });
    expect(lb.backendSets[0]?.backends[0]).toMatchObject({
      offline: true,
      drain: false,
      maxConnections: null,
    });
    expect(lb.backendSets[0]?.backends[1]?.maxConnections).toBe(5);
    expect(lb.pathRouteSets[0]?.routes[0]).toEqual({
      path: "/a",
      matchType: "PREFIX_MATCH",
      backendSet: "web",
    });
    expect(lb.routingPolicies[0]?.rules[0]?.backendSets).toEqual(["web"]);
    expect(lb.ruleSets[0]?.items).toEqual(["ADD_HTTP_REQUEST_HEADER X-A"]);
    expect(lb.certificates[0]?.subjects).toEqual(["lb.example"]);
  });

  it("copes with an empty load balancer", () => {
    expect(parseLoadBalancer({ id: "x" })).toMatchObject({
      name: "x",
      listeners: [],
      backendSets: [],
      ips: [],
    });
  });

  it("parses certificates without key material", () => {
    const c = parseCertificate({
      certificateName: "c",
      subjectNames: ["a"],
      publicCertificate: "must-not-surface",
      privateKey: "must-not-surface",
    });
    expect(c.subjects).toEqual(["a"]);
    expect(JSON.stringify(c)).not.toContain("must-not-surface");
  });

  it("maps health statuses to tones", () => {
    expect(healthTone("OK")).toBe("ok");
    expect(healthTone("WARNING")).toBe("warn");
    expect(healthTone("CRITICAL")).toBe("bad");
    expect(healthTone("UNKNOWN")).toBe("unknown");
    expect(healthTone("")).toBe("unknown");
  });

  it("flags servable listeners", () => {
    expect(canTry({ address: "127.0.0.1:1", tls: false })).toBe(true);
    expect(canTry({ address: "127.0.0.1:1", tls: true })).toBe(false);
    expect(canTry({ address: "", tls: false })).toBe(false);
    expect(isServing({ state: "ACTIVE" })).toBe(true);
    expect(isServing({ state: "CREATING" })).toBe(false);
  });

  it("validates form input", () => {
    expect(validPort("80")).toBe(true);
    expect(validPort("0")).toBe(false);
    expect(validPort("70000")).toBe(false);
    expect(validPort("8x")).toBe(false);
    expect(validWeight("3")).toBe(true);
    expect(validWeight("0")).toBe(false);
    expect(validBandwidth("10")).toBe(true);
    expect(validBandwidth("9")).toBe(false);
    expect(validBandwidth("8001")).toBe(false);
    expect(validIp("127.0.0.1")).toBe(true);
    expect(validIp("999.0.0.1")).toBe(false);
    expect(validIp("::1")).toBe(true);
    expect(validIp("host")).toBe(false);
  });

  it("pretty-prints JSON and flags binary bodies", () => {
    expect(prettyBody({ body: '{"a":1}', encoding: "utf8" })).toBe('{\n  "a": 1\n}');
    expect(prettyBody({ body: "plain", encoding: "utf8" })).toBe("plain");
    expect(prettyBody({ body: "AAE=", encoding: "base64" })).toContain("binary");
  });
});

describe("client", () => {
  it("lists compartments and fetches each load balancer's detail", async () => {
    const fetchFn = vi.fn((url: string) =>
      Promise.resolve(
        json(
          url.endsWith("/compartments")
            ? { compartments: [comp] }
            : url.includes("/loadBalancers/lb1")
              ? lbRaw
              : [{ id: "lb1" }],
        ),
      ),
    );
    expect(await listCompartments(asFetch(fetchFn))).toEqual([comp]);
    const out = await listLoadBalancers(comp, asFetch(fetchFn));
    expect(out[0]?.listeners).toHaveLength(2);
    expect(fetchFn.mock.calls[1]?.[0]).toBe(
      `${LB_PROXY_BASE}/api/loadBalancers?compartmentId=${encodeURIComponent(comp)}`,
    );
  });

  it("reads one load balancer and its work requests", async () => {
    const fetchFn = vi.fn((url: string) =>
      Promise.resolve(
        json(
          url.endsWith("/workRequests")
            ? [{ id: "w", type: "CreateLoadBalancer", lifecycleState: "SUCCEEDED" }]
            : lbRaw,
        ),
      ),
    );
    expect((await getLoadBalancer("lb1", asFetch(fetchFn))).name).toBe("edge");
    const wr = await listWorkRequests("lb1", asFetch(fetchFn));
    expect(wr[0]).toMatchObject({ type: "CreateLoadBalancer", state: "SUCCEEDED" });
  });

  it("reads backend and backend set health, escaping the backend name", async () => {
    const fetchFn = vi.fn((url: string) =>
      Promise.resolve(
        json(
          url.endsWith("/health") && url.includes("/backends/")
            ? { status: "OK", healthCheckResults: [{ healthCheckStatus: "OK" }] }
            : {
                status: "WARNING",
                totalBackendCount: 2,
                warningStateBackendNames: ["a"],
                criticalStateBackendNames: [],
              },
        ),
      ),
    );
    expect(await getBackendHealth("lb1", "web", "127.0.0.1:9001", asFetch(fetchFn))).toEqual({
      status: "OK",
      detail: "OK",
    });
    expect(fetchFn.mock.calls[0]?.[0]).toBe(
      `${LB_PROXY_BASE}/api/loadBalancers/lb1/backendSets/web/backends/127.0.0.1%3A9001/health`,
    );
    expect((await getBackendSetHealth("lb1", "web", asFetch(fetchFn))).status).toBe("WARNING");
  });

  it("loads health for every set and backend", async () => {
    const fetchFn = vi.fn(() => Promise.resolve(json({ status: "OK" })));
    const lb = parseLoadBalancer(lbRaw);
    const h = await loadHealth(lb, asFetch(fetchFn));
    expect(Object.keys(h).toSorted()).toEqual(
      ["web", backendKey("web", "127.0.0.1:9001"), backendKey("web", "127.0.0.1:9002")].toSorted(),
    );
    expect(fetchFn).toHaveBeenCalledTimes(3);
  });

  it("adds, updates and removes backends", async () => {
    const fetchFn = vi.fn((_url: string, _init?: RequestInit) => Promise.resolve(json({})));
    await addBackend(
      "lb1",
      "web",
      { ipAddress: "127.0.0.1", port: 9003, weight: 1 },
      asFetch(fetchFn),
    );
    expect(fetchFn.mock.calls[0]?.[0]).toBe(
      `${LB_PROXY_BASE}/api/loadBalancers/lb1/backendSets/web/backends`,
    );
    expect(fetchFn.mock.calls[0]?.[1]?.method).toBe("POST");

    const be = parseLoadBalancer(lbRaw).backendSets[0]!.backends[1]!;
    await updateBackend("lb1", "web", be, { offline: true }, asFetch(fetchFn));
    expect(fetchFn.mock.calls[1]?.[1]?.method).toBe("PUT");
    expect(JSON.parse(String(fetchFn.mock.calls[1]?.[1]?.body))).toEqual({
      weight: 1,
      backup: false,
      drain: false,
      offline: true,
      maxConnections: 5,
    });

    await deleteBackend("lb1", "web", "127.0.0.1:9002", asFetch(fetchFn));
    expect(fetchFn.mock.calls[2]?.[0]).toContain("/backends/127.0.0.1%3A9002");
    expect(fetchFn.mock.calls[2]?.[1]?.method).toBe("DELETE");
  });

  it("creates and deletes load balancers", async () => {
    const fetchFn = vi.fn((_url: string, _init?: RequestInit) => Promise.resolve(json({})));
    const base = {
      compartmentId: comp,
      displayName: "n",
      subnetId: "s",
      isPrivate: false,
      minimumBandwidthInMbps: 10,
      maximumBandwidthInMbps: 10,
    };
    await createLoadBalancer({ ...base, listenerPort: null }, asFetch(fetchFn));
    expect(JSON.parse(String(fetchFn.mock.calls[0]?.[1]?.body))).not.toHaveProperty("listenerPort");
    await createLoadBalancer({ ...base, listenerPort: 80 }, asFetch(fetchFn));
    expect(JSON.parse(String(fetchFn.mock.calls[1]?.[1]?.body))).toMatchObject({
      listenerPort: 80,
    });
    await deleteLoadBalancer("lb1", asFetch(fetchFn));
    expect(fetchFn.mock.calls[2]?.[1]?.method).toBe("DELETE");
  });

  it("sends a try-it request to the selected listener only", async () => {
    const fetchFn = vi.fn((_url: string, _init?: RequestInit) =>
      Promise.resolve(
        json({
          status: 200,
          headers: { "X-Backend": "alpha" },
          body: "hi",
          encoding: "utf8",
          truncated: false,
        }),
      ),
    );
    const res = await tryIt(
      "lb1",
      "http",
      {
        method: "GET",
        path: "/x",
        query: "?a=1",
        body: "",
        virtualHost: " app.test ",
        headers: [
          { name: "X-A", value: "1" },
          { name: " ", value: "drop" },
        ],
      },
      asFetch(fetchFn),
    );
    expect(res).toMatchObject({ status: 200, body: "hi", headers: { "X-Backend": "alpha" } });
    expect(fetchFn.mock.calls[0]?.[0]).toBe(
      `${LB_PROXY_BASE}/api/loadBalancers/lb1/listeners/http/invoke`,
    );
    expect(JSON.parse(String(fetchFn.mock.calls[0]?.[1]?.body))).toEqual({
      method: "GET",
      path: "/x",
      query: "a=1",
      body: "",
      virtualHost: "app.test",
      headers: [{ name: "X-A", value: "1" }],
    });
  });

  it("surfaces proxy errors", async () => {
    const fetchFn = vi.fn(() =>
      Promise.resolve(
        json(
          { error: { status: "IncorrectState", message: "Load balancer is in state CREATING" } },
          409,
        ),
      ),
    );
    await expect(
      tryIt(
        "lb1",
        "http",
        { method: "GET", path: "/", query: "", body: "", virtualHost: "", headers: [] },
        asFetch(fetchFn),
      ),
    ).rejects.toThrow(/CREATING/);
  });
});
