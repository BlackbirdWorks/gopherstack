import { describe, expect, it, vi } from "vitest";

import {
  FN_PROXY_BASE,
  createBody,
  createFunction,
  inlineTemplate,
  invokeFunction,
  parseFunction,
  parseHeaders,
  runtimeUnavailable,
  sourceZip,
  updateSource,
  validateEntryPoint,
  validateFunctionId,
  validateSource,
  validateTopic,
} from "./gcpfunctions";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const scope = { project: "p", location: "l" };
const loc = `${FN_PROXY_BASE}/projects/p/locations/l`;
const slot = {
  uploadUrl: "http://localhost:8129/_gcf/upload/b/o.zip",
  storageSource: { bucket: "b", object: "o.zip" },
};

describe("validators", () => {
  it.each([
    ["", false],
    ["Hello", false],
    ["1fn", false],
    ["hello-1", true],
  ])("function id %j ok=%s", (id, ok) => {
    expect(validateFunctionId(id) === null).toBe(ok);
  });

  it("checks entry point, topic and source", () => {
    expect(validateEntryPoint(" ")).not.toBeNull();
    expect(validateTopic("pubsub", "")).not.toBeNull();
    expect(validateTopic("http", "")).toBeNull();
    expect(validateSource({ kind: "inline", filename: "a", code: " " })).not.toBeNull();
    expect(validateSource({ kind: "zip", data: new Uint8Array() })).not.toBeNull();
    expect(validateSource({ kind: "zip", data: new Uint8Array(2 << 20) })).not.toBeNull();
    expect(validateSource({ kind: "zip", data: new Uint8Array(4) })).toBeNull();
  });

  it("parses header lines", () => {
    expect(parseHeaders("A: b\n\nC:d: e").headers).toEqual({ A: "b", C: "d: e" });
    expect(parseHeaders("nocolon").error).toMatch(/bad header/);
  });
});

describe("parsing", () => {
  it("maps an http function", () => {
    const f = parseFunction({
      name: "projects/p/locations/l/functions/hello",
      state: "ACTIVE",
      buildConfig: { runtime: "nodejs22", entryPoint: "hello" },
      serviceConfig: { uri: "http://hello.localhost:8129" },
    });
    expect(f).toMatchObject({
      id: "hello",
      state: "active",
      trigger: "http",
      url: "http://hello.localhost:8129",
    });
  });

  it("maps a pubsub function and unknown state", () => {
    const f = parseFunction({ name: "x/y", eventTrigger: { pubsubTopic: "projects/p/topics/t" } });
    expect(f).toMatchObject({ trigger: "pubsub", topic: "projects/p/topics/t", state: "unknown" });
  });

  it("detects a missing runtime", () => {
    expect(runtimeUnavailable(["container runtime is unavailable: no docker"])).toBe(true);
    expect(runtimeUnavailable(["fine"])).toBe(false);
  });

  it("has inline templates for node and python only", () => {
    expect(inlineTemplate("nodejs22")?.filename).toBe("index.js");
    expect(inlineTemplate("python312")?.filename).toBe("main.py");
    expect(inlineTemplate("go125")).toBeNull();
  });
});

describe("requests", () => {
  const inline = { kind: "inline", filename: "index.js", code: "x" } as const;

  it("zips inline source and passes a zip through", () => {
    expect(sourceZip(inline)[0]).toBe(0x50);
    const data = new Uint8Array([1]);
    expect(sourceZip({ kind: "zip", data })).toBe(data);
  });

  it("builds http and pubsub bodies", () => {
    const n = {
      id: "a",
      runtime: "nodejs22",
      entryPoint: " hello ",
      trigger: "http",
      topic: "",
      source: inline,
    } as const;
    const b = createBody(n, { bucket: "b" }) as Record<string, Record<string, unknown>>;
    expect(b.buildConfig.entryPoint).toBe("hello");
    expect(b.eventTrigger).toBeUndefined();
    const e = createBody({ ...n, trigger: "pubsub", topic: "projects/p/topics/t" }, {}) as Record<
      string,
      Record<string, unknown>
    >;
    expect(e.eventTrigger.pubsubTopic).toBe("projects/p/topics/t");
  });

  it("uploads then creates", async () => {
    const f = vi.fn((url: string | URL | Request, init?: RequestInit) => {
      const u = String(url);
      if (u.endsWith(":generateUploadUrl")) return Promise.resolve(json(slot));
      if (u.includes(":upload")) {
        expect(init?.method).toBe("PUT");
        expect(u).toContain(encodeURIComponent(slot.uploadUrl));
        return Promise.resolve(json({}));
      }
      return Promise.resolve(json({ name: "operations/x" }));
    });
    await createFunction(
      scope,
      {
        id: "hello",
        runtime: "nodejs22",
        entryPoint: "hello",
        trigger: "http",
        topic: "",
        source: inline,
      },
      f as never,
    );
    expect(f.mock.calls.map((c) => String(c[0]))).toEqual([
      `${loc}/functions:generateUploadUrl`,
      `${loc}/functions:upload?uploadUrl=${encodeURIComponent(slot.uploadUrl)}`,
      `${loc}/functions?functionId=hello`,
    ]);
    const body = JSON.parse(String(f.mock.calls[2]![1]!.body));
    expect(body.buildConfig.source.storageSource).toEqual(slot.storageSource);
  });

  it("patches the source mask on update", async () => {
    const f = vi.fn((url: string | URL | Request) =>
      Promise.resolve(json(String(url).endsWith(":generateUploadUrl") ? slot : {})),
    );
    await updateSource(scope, "hello", inline, f as never);
    expect(String(f.mock.calls[2]![0])).toBe(
      `${loc}/functions/hello?updateMask=buildConfig.source`,
    );
  });

  it("surfaces upload failures", async () => {
    const f = vi.fn((url: string | URL | Request) =>
      Promise.resolve(
        String(url).endsWith(":generateUploadUrl")
          ? json(slot)
          : json({ error: { message: "nope" } }, 400),
      ),
    );
    await expect(updateSource(scope, "hello", inline, f as never)).rejects.toThrow("nope");
  });

  it("invokes and maps the response", async () => {
    const f = vi.fn(() =>
      Promise.resolve(json({ status: 200, headers: { "Content-Type": "text/plain" }, body: "hi" })),
    );
    const out = await invokeFunction(
      scope,
      "hello",
      { method: "GET", path: "", body: "", headers: "X: 1" },
      f as never,
    );
    expect(out).toEqual({ status: 200, headers: { "Content-Type": "text/plain" }, body: "hi" });
    const sent = JSON.parse(
      String(
        (f.mock.calls[0] as unknown[])[1]
          ? ((f.mock.calls[0] as unknown[])[1] as RequestInit).body
          : "",
      ),
    );
    expect(sent).toMatchObject({ path: "/", headers: { X: "1" } });
    await expect(
      invokeFunction(
        scope,
        "hello",
        { method: "GET", path: "/", body: "", headers: "bad" },
        f as never,
      ),
    ).rejects.toThrow(/bad header/);
  });
});
