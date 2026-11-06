import { describe, expect, it, vi } from "vitest";

import {
  AC_PROXY_BASE,
  NULL_LABEL,
  deleteKeyValue,
  listAccessKeys,
  listFeatures,
  listKeyValues,
  listRevisions,
  listSnapshotItems,
  listSnapshots,
  putKeyValue,
  revealAccessKey,
  toggleFeature,
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

const ok = (body: unknown) => vi.fn().mockImplementation(() => Promise.resolve(json(body)));

describe("snapshots, features, revisions and keys", () => {
  it("lists snapshots", async () => {
    const f = ok({
      snapshots: [{ name: "s1", status: "ready", itemCount: 3, filters: ["key=* label="] }],
    });
    const out = await listSnapshots("st", f);
    expect(f.mock.calls[0][0]).toBe(`${AC_PROXY_BASE}/stores/st/snapshots`);
    expect(out[0]).toMatchObject({ name: "s1", status: "ready", itemCount: 3 });
  });

  it("lists snapshot items", async () => {
    const f = ok({ items: [{ key: "a", value: "1" }] });
    expect((await listSnapshotItems("st", "s 1", f))[0].key).toBe("a");
    expect(f.mock.calls[0][0]).toBe(`${AC_PROXY_BASE}/stores/st/snapshots/s%201/kv`);
  });

  it.each([
    ["", "", ""],
    ["app*", "", "?key=app*"],
    ["app*", "prod", "?key=app*&label=prod"],
  ])("revisions filter %j %j", async (key, label, qs) => {
    const f = ok({ items: [] });
    await listRevisions("st", key, label, f);
    expect(f.mock.calls[0][0]).toBe(`${AC_PROXY_BASE}/stores/st/revisions${qs}`);
  });

  it("lists and toggles feature flags", async () => {
    const f = ok({ features: [{ id: "beta", enabled: true, filterCount: 2 }] });
    expect((await listFeatures("st", f))[0]).toMatchObject({
      id: "beta",
      enabled: true,
      filterCount: 2,
    });
    const g = ok({});
    await toggleFeature("st", "beta", "prod", false, g);
    expect(g.mock.calls[0][0]).toBe(`${AC_PROXY_BASE}/stores/st/features/toggle`);
    expect(JSON.parse(g.mock.calls[0][1].body)).toEqual({
      id: "beta",
      label: "prod",
      enabled: false,
    });
  });

  it("never receives secrets when listing keys and reveals by POST", async () => {
    const f = ok({ keys: [{ id: "k1", name: "Primary", readOnly: false, masked: "••••" }] });
    const keys = await listAccessKeys("st", f);
    expect(keys[0]).toEqual({
      id: "k1",
      name: "Primary",
      readOnly: false,
      lastModified: "",
      masked: "••••",
    });
    expect(JSON.stringify(keys)).not.toContain("secret");
    const g = ok({ id: "k1", secret: "s3cr3t" });
    expect(await revealAccessKey("st", "k1", g)).toBe("s3cr3t");
    expect(g.mock.calls[0][1].method).toBe("POST");
    expect(JSON.parse(g.mock.calls[0][1].body)).toEqual({ id: "k1" });
  });
});
