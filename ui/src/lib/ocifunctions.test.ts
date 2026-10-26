import { describe, expect, it, vi } from "vitest";

import {
  FUNCTIONS_PROXY_BASE,
  getFunction,
  invokeFunction,
  listApplications,
  listFunctions,
  listWorkRequestLogs,
  listWorkRequests,
  parseApp,
  parseFunction,
  parseWorkRequest,
  shortOcid,
} from "./ocifunctions";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

function reply(f: (url: string, init?: RequestInit) => Response) {
  return vi.fn((...a: unknown[]) => Promise.resolve(f(...(a as [string, RequestInit?]))));
}

describe("parsing", () => {
  it("flags masked config keys and sorts the entries", () => {
    const a = parseApp({
      id: "ocid1.fnapp.oc1..x",
      displayName: "app",
      lifecycleState: "ACTIVE",
      config: { Z: "1", DB_PASSWORD: "••••••••", A: "2" },
      maskedConfigKeys: ["DB_PASSWORD"],
    });
    expect(a.config.map((c) => c.key)).toEqual(["A", "DB_PASSWORD", "Z"]);
    expect(a.config.find((c) => c.key === "DB_PASSWORD")?.masked).toBe(true);
    expect(a.config.find((c) => c.key === "A")?.masked).toBe(false);
  });

  it("reads image, limits and invoke endpoint of a function", () => {
    const f = parseFunction({
      id: "f1",
      displayName: "fn",
      sourceDetails: { image: "iad.ocir.io/ns/img:1", imageDigest: "sha256:ab" },
      memoryInMBs: 256,
      timeoutInSeconds: 30,
      invokeEndpoint: "http://localhost:8154",
    });
    expect(f).toMatchObject({ image: "iad.ocir.io/ns/img:1", memoryMb: 256, timeoutSec: 30 });
    expect(f.invokeEndpoint).toBe("http://localhost:8154");
  });

  it("parses work requests", () => {
    expect(
      parseWorkRequest({
        id: "w",
        operationType: "CREATE_FUNCTION",
        status: "SUCCEEDED",
        percentComplete: 100,
        resources: [{ entityType: "function", actionType: "CREATED", identifier: "f" }],
      }),
    ).toMatchObject({ operation: "CREATE_FUNCTION", percentComplete: 100 });
  });
});

describe("client", () => {
  it("lists applications and functions", async () => {
    const f = reply((url) =>
      url.endsWith("/functions")
        ? json({ functions: [{ displayName: "fn" }] })
        : json({ applications: [{ displayName: "app" }] }),
    );
    expect((await listApplications(f))[0]?.name).toBe("app");
    expect((await listFunctions("a/b", f))[0]?.name).toBe("fn");
    expect(f.mock.calls[1]?.[0]).toBe(`${FUNCTIONS_PROXY_BASE}/applications/a%2Fb/functions`);
  });

  it("fetches one function", async () => {
    const f = reply(() => json({ displayName: "fn" }));
    expect((await getFunction("id", f)).name).toBe("fn");
  });

  it("invokes with the body and content type and returns status, headers and text", async () => {
    const f = reply(() =>
      json({
        status: 200,
        callId: "C1",
        headers: { "Fn-Call-Id": "C1", "Content-Type": "text/plain" },
        isText: true,
        text: "hi",
        base64: "aGk=",
      }),
    );
    const out = await invokeFunction("fn1", { body: "{}", contentType: "application/json" }, f);
    expect(out).toMatchObject({ status: 200, callId: "C1", text: "hi" });
    expect(out.headers.map(([k]) => k)).toEqual(["Content-Type", "Fn-Call-Id"]);
    const init = f.mock.calls[0]?.[1] as RequestInit;
    expect(JSON.parse(init.body as string)).toEqual({
      body: "{}",
      contentType: "application/json",
    });
  });

  it("returns null text for a binary response", async () => {
    const f = reply(() => json({ status: 200, isText: false, base64: "/w==" }));
    expect((await invokeFunction("fn1", { body: "", contentType: "" }, f)).text).toBeNull();
  });

  it("rejects with the backend error", async () => {
    const f = reply(() =>
      json({ error: { code: 503, status: "ServiceUnavailable", message: "engine off" } }, 503),
    );
    await expect(invokeFunction("fn1", { body: "", contentType: "" }, f)).rejects.toMatchObject({
      httpStatus: 503,
    });
  });

  it("lists work requests newest first, optionally per resource", async () => {
    const f = reply(() =>
      json({
        workRequests: [
          { id: "old", timeAccepted: "2026-01-01T00:00:00Z" },
          { id: "new", timeAccepted: "2026-02-01T00:00:00Z" },
        ],
      }),
    );
    expect((await listWorkRequests("", f)).map((w) => w.id)).toEqual(["new", "old"]);
    await listWorkRequests("res 1", f);
    expect(f.mock.calls[1]?.[0]).toBe(`${FUNCTIONS_PROXY_BASE}/workrequests?resourceId=res%201`);
  });

  it("lists work request logs", async () => {
    const f = reply(() => json({ logs: [{ timestamp: "t", message: "accepted" }] }));
    expect(await listWorkRequestLogs("w", f)).toEqual([{ timestamp: "t", message: "accepted" }]);
  });
});

describe("shortOcid", () => {
  it("keeps the kind and the tail", () => {
    expect(shortOcid("ocid1.fnfunc.oc1.iad.abcdefghijklmnop")).toBe("fnfunc…ijklmnop");
    expect(shortOcid("plain")).toBe("plain");
  });
});
