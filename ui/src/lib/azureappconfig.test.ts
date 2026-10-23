import { describe, expect, it, vi } from "vitest";

import {
  AC_PROXY_BASE,
  NULL_LABEL,
  deleteKeyValue,
  listKeyValues,
  putKeyValue,
  validateKey,
  validateLabel,
} from "./azureappconfig";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

describe("validation", () => {
  it.each([
    ["", "Key is required"],
    [".", "Key cannot be . or .."],
    ["..", "Key cannot be . or .."],
    ["app/setting", ""],
    ["k".repeat(10241), "Key is too long"],
  ])("key %j", (k, want) => expect(validateKey(k)).toBe(want));

  it.each([
    ["", ""],
    ["prod", ""],
    ["a\0b", "Label cannot contain NUL"],
    ["l".repeat(257), "Label is too long"],
  ])("label %j", (l, want) => expect(validateLabel(l)).toBe(want));
});

describe("client", () => {
  it("builds the filter query", async () => {
    const f = vi
      .fn()
      .mockImplementation(() =>
        Promise.resolve(json({ items: [{ key: "a", label: "p", value: "1", locked: true }] })),
      );
    const out = await listKeyValues("st", "app*", "prod", f);
    expect(f.mock.calls[0][0]).toBe(`${AC_PROXY_BASE}/stores/st/kv?key=app*&label=prod`);
    expect(out[0]).toMatchObject({ key: "a", label: "p", locked: true });
  });

  it("omits empty filters and encodes the null label", async () => {
    const f = vi.fn().mockImplementation(() => Promise.resolve(json({ items: [] })));
    await listKeyValues("st", "", "", f);
    expect(f.mock.calls[0][0]).toBe(`${AC_PROXY_BASE}/stores/st/kv`);
    await listKeyValues("st", "", NULL_LABEL, f);
    expect(f.mock.calls[1][0]).toBe(`${AC_PROXY_BASE}/stores/st/kv?label=%00`);
  });

  it("puts and deletes", async () => {
    const f = vi.fn().mockImplementation(() => Promise.resolve(json({ key: "k", value: "v" })));
    const kv = await putKeyValue("st", { key: "k", label: "", value: "v", contentType: "" }, f);
    expect(kv.value).toBe("v");
    expect(f.mock.calls[0][1].method).toBe("PUT");
    await deleteKeyValue("st", "k", "l", f);
    expect(f.mock.calls[1][0]).toBe(`${AC_PROXY_BASE}/stores/st/kv?key=k&label=l`);
    expect(f.mock.calls[1][1].method).toBe("DELETE");
  });
});
