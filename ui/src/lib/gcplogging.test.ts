import { describe, expect, it, vi } from "vitest";

import {
  LOGGING_PROXY_BASE,
  listEntries,
  listSinks,
  parseEntry,
  severityTone,
  validateLogId,
  writeEntry,
} from "./gcplogging";

const json = (body: unknown) =>
  new Response(JSON.stringify(body), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });

describe("parseEntry", () => {
  it.each([
    [{ textPayload: "hi" }, "hi"],
    [{ jsonPayload: { a: 1 } }, '{"a":1}'],
    [{ protoPayload: { b: 2 } }, '{"b":2}'],
    [{}, ""],
  ])("message of %j", (raw, want) => expect(parseEntry(raw).message).toBe(want));

  it("shortens the log name and defaults severity", () => {
    const e = parseEntry({ logName: "projects/p/logs/app", resource: { type: "global" } });
    expect(e).toMatchObject({ logName: "app", severity: "DEFAULT", resourceType: "global" });
  });
});

describe("client", () => {
  it("queries entries newest first within the project", async () => {
    const f = vi
      .fn()
      .mockImplementation(() => Promise.resolve(json({ entries: [{ textPayload: "x" }] })));
    const out = await listEntries("p", "severity>=ERROR", 50, f);
    expect(out).toHaveLength(1);
    expect(f.mock.calls[0][0]).toBe(`${LOGGING_PROXY_BASE}/v1/entries:list`);
    expect(JSON.parse(f.mock.calls[0][1].body as string)).toEqual({
      resourceNames: ["projects/p"],
      filter: "severity>=ERROR",
      orderBy: "timestamp desc",
      pageSize: 50,
    });
  });

  it("lists sinks and writes entries", async () => {
    const f = vi
      .fn()
      .mockImplementation(() =>
        Promise.resolve(json({ sinks: [{ name: "_Default", disabled: false }] })),
      );
    expect((await listSinks("p", f))[0].name).toBe("_Default");
    expect(f.mock.calls[0][0]).toBe(`${LOGGING_PROXY_BASE}/v1/projects/p/sinks`);
    await writeEntry("p", "app", "INFO", "hello", f);
    expect(JSON.parse(f.mock.calls[1][1].body as string).logName).toBe("projects/p/logs/app");
  });

  it.each([
    ["", "Log ID is required"],
    ["a b", "Letters, digits, . _ - (max 100)"],
    ["app.log", ""],
  ])("log id %j", (id, want) => expect(validateLogId(id)).toBe(want));

  it.each([
    ["ERROR", "red"],
    ["WARNING", "amber"],
    ["INFO", "blue"],
    ["DEBUG", "gray"],
  ])("tone %s", (s, color) => expect(severityTone(s)).toContain(color));
});
