import { describe, expect, it, vi } from "vitest";

import {
  WEB_BASE,
  codeLocation,
  defaultLogGroup,
  invokeEndpoint,
  listEndpoints,
  listFunctions,
  listRevisions,
  logGroupLink,
  parseEndpoint,
  parseFunction,
  parseHeaders,
  parseRevision,
  sparkline,
  stateTone,
  statusTone,
  validateInvoke,
  weightColor,
  weightSegments,
} from "./lambdaweb";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
const asFetch = (f: unknown) => f as typeof fetch;
const callUrl = (f: { mock: { calls: unknown[] } }, i: number): string =>
  (f.mock.calls[i] as [string])[0];

describe("parsing", () => {
  it("parses a function", () => {
    expect(
      parseFunction({ functionName: "api", functionArn: "arn", state: "Active", createdAt: "t" }),
    ).toMatchObject({ name: "api", arn: "arn", state: "Active", created: "t" });
  });

  it("parses a revision and hides environment values", () => {
    const r = parseRevision({
      revisionId: "abc",
      state: "Active",
      createdAt: "t",
      buildConfig: {
        codeConfig: { s3Object: { bucket: "b", key: "app.zip", versionId: "v1" } },
        runtimeConfig: { runtime: "nodejs24.x" },
      },
      serviceConfig: {
        timeoutSeconds: 30,
        maxConcurrencyPerEnvironment: 4,
        executionRoleArn: "arn:role",
        environmentVariables: { Z: "<hidden>", A: "<hidden>" },
        loggingConfig: { logGroup: "/aws/lambda/web/api" },
      },
      errors: [{ errorCode: "CodeUnavailable", errorMessage: "gone" }],
    });
    expect(r).toMatchObject({ id: "abc", runtime: "nodejs24.x", timeout: 30, concurrency: 4 });
    expect(r.envNames).toEqual(["A", "Z"]);
    expect(r.errors).toEqual(["CodeUnavailable: gone"]);
    expect(codeLocation(r)).toBe("s3://b/app.zip (version v1)");
    expect(codeLocation(parseRevision({}))).toBe("—");
  });

  it("parses an endpoint with weights, scaling and throttle", () => {
    const e = parseEndpoint({
      endpointName: "prod",
      endpointType: "PerRegion",
      authType: "ApplicationManaged",
      domainName: "x.y.lambda-web.us-east-1.on.aws",
      state: "Active",
      revisionWeights: [
        { revisionId: "a", weight: 90 },
        { revisionId: "b", weight: 10 },
      ],
      scalingConfig: { maxEnvironments: 5 },
      throttleConfig: { rateLimit: 100 },
    });
    expect(e).toMatchObject({
      name: "prod",
      auth: "ApplicationManaged",
      maxEnvironments: 5,
      rateLimit: 100,
    });
    expect(e.weights).toHaveLength(2);
    expect(parseEndpoint({}).maxEnvironments).toBeUndefined();
  });
});

describe("weights", () => {
  it("normalizes weights into shares", () => {
    const segs = weightSegments([
      { revisionId: "a", weight: 75 },
      { revisionId: "b", weight: 25 },
    ]);
    expect(segs.map((s) => s.percent)).toEqual([75, 25]);
  });

  it("gives empty shares for a zero total", () => {
    expect(weightSegments([{ revisionId: "a", weight: 0 }])[0]!.percent).toBe(0);
    expect(weightSegments([])).toEqual([]);
  });

  it("cycles the palette", () => {
    expect(weightColor(0)).toBe(weightColor(6));
  });
});

describe("sparkline", () => {
  it("scales points into the box", () => {
    expect(
      sparkline(
        [
          { time: 0, value: 0 },
          { time: 10, value: 10 },
        ],
        100,
        20,
      ),
    ).toBe("0.0,20.0 100.0,0.0");
  });

  it("draws one point as a flat line and none as nothing", () => {
    expect(sparkline([{ time: 5, value: 2 }], 100, 10)).toBe("0,0.0 100,0.0");
    expect(sparkline([], 100, 10)).toBe("");
  });

  it("sorts points by time", () => {
    const pts = sparkline(
      [
        { time: 10, value: 1 },
        { time: 0, value: 1 },
      ],
      10,
      10,
    );
    expect(pts.startsWith("0.0,")).toBe(true);
  });
});

describe("tones and links", () => {
  it("maps states and statuses", () => {
    expect(stateTone("Active")).toBe("good");
    expect(stateTone("Pending")).toBe("busy");
    expect(stateTone("Failed")).toBe("bad");
    expect(stateTone("")).toBe("muted");
    expect(statusTone(200)).toBe("good");
    expect(statusTone(404)).toBe("warn");
    expect(statusTone(503)).toBe("bad");
  });

  it("names the default log group", () => {
    expect(defaultLogGroup("api")).toBe("/aws/lambda/web/api");
  });

  it("links to the log group", () => {
    expect(logGroupLink("/aws/lambda/web/api")).toBe(
      "/dashboard/cloudwatchlogs?q=%2Faws%2Flambda%2Fweb%2Fapi",
    );
  });
});

describe("invoke validation", () => {
  it("parses header lines", () => {
    expect(parseHeaders("Accept: text/html\n\nX-Test:  1 ")).toEqual({
      headers: [
        { name: "Accept", value: "text/html" },
        { name: "X-Test", value: "1" },
      ],
      error: "",
    });
    expect(parseHeaders("no colon").error).toContain("no colon");
    expect(parseHeaders(": empty").error).not.toBe("");
  });

  it("validates path, headers and body", () => {
    expect(validateInvoke("/", "", "")).toBe("");
    expect(validateInvoke("x", "", "")).not.toBe("");
    expect(validateInvoke("//evil", "", "")).not.toBe("");
    expect(validateInvoke("/a b", "", "")).not.toBe("");
    expect(validateInvoke("/", "bad header", "")).not.toBe("");
    expect(validateInvoke("/", "", "x".repeat((1 << 17) + 1))).not.toBe("");
  });
});

describe("requests", () => {
  it("follows list pages and passes the region", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(json({ functions: [{ functionName: "b" }], nextToken: "t1" }))
      .mockResolvedValueOnce(json({ functions: [{ functionName: "a" }] }));
    const out = await listFunctions("eu-west-1", asFetch(f));
    expect(out.map((x) => x.name)).toEqual(["a", "b"]);
    expect(callUrl(f, 0)).toBe(`${WEB_BASE}/functions?region=eu-west-1`);
    expect(callUrl(f, 1)).toBe(`${WEB_BASE}/functions?region=eu-west-1&nextToken=t1`);
  });

  it("reads every revision in full, newest first", async () => {
    const f = vi.fn((url: string) => {
      if (url.endsWith("/revisions?region=us-east-1"))
        return Promise.resolve(json({ revisions: [{ revisionId: "r1" }, { revisionId: "r2" }] }));
      const id = url.split("/revisions/")[1]!.split("?")[0]!;
      return Promise.resolve(
        json({
          revisionId: id,
          createdAt: id === "r2" ? "2026-02-01T00:00:00Z" : "2026-01-01T00:00:00Z",
        }),
      );
    });
    const out = await listRevisions("api", "us-east-1", asFetch(f));
    expect(out.map((r) => r.id)).toEqual(["r2", "r1"]);
  });

  it("lists endpoints without a region", async () => {
    const f = vi.fn(() => Promise.resolve(json({ endpoints: [{ endpointName: "prod" }] })));
    expect((await listEndpoints("api", undefined, asFetch(f)))[0]!.name).toBe("prod");
    expect(callUrl(f, 0)).toBe(`${WEB_BASE}/functions/api/endpoints`);
  });

  it("sends an invoke and reads the reply", async () => {
    const f = vi.fn(() =>
      Promise.resolve(
        json({
          status: 200,
          domain: "d",
          durationMs: 12,
          headers: { "Content-Type": ["text/plain"] },
          body: "hi",
          encoding: "text",
          truncated: false,
        }),
      ),
    );
    const reply = await invokeEndpoint(
      {
        region: "us-east-1",
        function: "api",
        endpoint: "prod",
        method: "GET",
        path: "/",
        headers: [],
        body: "",
      },
      asFetch(f),
    );
    expect(reply).toMatchObject({ status: 200, body: "hi", durationMs: 12, encoding: "text" });
    expect(reply.headers["Content-Type"]).toEqual(["text/plain"]);
    const call = f.mock.calls[0] as unknown as [string, RequestInit];
    expect(call[0]).toBe(`${WEB_BASE}/invoke`);
    expect(call[1].method).toBe("POST");
  });
});
