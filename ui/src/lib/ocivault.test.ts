import { describe, expect, it, vi } from "vitest";

import {
  VAULT_PROXY_BASE,
  listKeys,
  listSecretVersions,
  listSecrets,
  listVaults,
  parseKey,
  parseSecret,
  parseSecretVersion,
  revealSecretVersion,
  shortOcid,
} from "./ocivault";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

function reply(f: (url: string) => Response) {
  return vi.fn((...a: unknown[]) => Promise.resolve(f(...(a as [string]))));
}

describe("parsing", () => {
  it("parses a secret without any content field", () => {
    const s = parseSecret({
      id: "ocid1.vaultsecret.oc1..x",
      secretName: "db",
      lifecycleState: "ACTIVE",
    });
    expect(s).toMatchObject({ name: "db", state: "ACTIVE" });
    expect(Object.keys(s)).not.toContain("content");
  });

  it("reads the algorithm from the summary or the shape", () => {
    expect(parseKey({ algorithm: "AES" }).algorithm).toBe("AES");
    expect(parseKey({ keyShape: { algorithm: "RSA", length: 256 } })).toMatchObject({
      algorithm: "RSA",
      length: 256,
    });
  });

  it("parses secret versions with stages", () => {
    expect(parseSecretVersion({ versionNumber: 2, stages: ["CURRENT", "LATEST"] })).toMatchObject({
      versionNumber: 2,
      stages: ["CURRENT", "LATEST"],
    });
  });

  it("shortens OCIDs", () => {
    expect(shortOcid("ocid1.vault.oc1.iad.abcdefghijklmnop")).toBe("vault…ijklmnop");
    expect(shortOcid("plain")).toBe("plain");
  });
});

describe("requests", () => {
  it("lists vaults, keys and secrets", async () => {
    const fetchFn = reply((url: string) => {
      if (url.endsWith("/vaults")) return json({ vaults: [{ id: "v1", displayName: "one" }] });
      if (url.includes("/keys")) return json({ keys: [{ id: "k1", displayName: "key" }] });
      return json({ secrets: [{ id: "s1", secretName: "db" }] });
    }) as unknown as typeof fetch;
    expect((await listVaults(fetchFn))[0]?.displayName).toBe("one");
    expect((await listKeys("v1", fetchFn))[0]?.id).toBe("k1");
    expect((await listSecrets("v1", fetchFn))[0]?.name).toBe("db");
  });

  it("never requests content when listing secret versions", async () => {
    const fetchFn = reply(() => json({ versions: [{ versionNumber: 1 }] }));
    await listSecretVersions("s1", fetchFn as unknown as typeof fetch);
    expect(String(fetchFn.mock.calls[0]?.[0])).toBe(`${VAULT_PROXY_BASE}/secrets/s1/versions`);
    expect(fetchFn.mock.calls[0]?.[1]).toEqual({ method: "GET" });
  });

  it("reveals through an explicit POST and decodes text", async () => {
    const fetchFn = reply(() =>
      json({ isText: true, text: "hunter2", base64: "aHVudGVyMg==", versionNumber: 3 }),
    );
    const got = await revealSecretVersion("s1", 3, fetchFn as unknown as typeof fetch);
    expect(fetchFn.mock.calls[0]?.[0]).toBe(`${VAULT_PROXY_BASE}/secrets/s1/versions/3/reveal`);
    expect(fetchFn.mock.calls[0]?.[1]).toMatchObject({ method: "POST" });
    expect(got).toEqual({ text: "hunter2", base64: "aHVudGVyMg==", versionNumber: 3 });
  });

  it("returns no text for binary content", async () => {
    const fetchFn = reply(() => json({ isText: false, base64: "/w==", versionNumber: 1 }));
    expect(
      (await revealSecretVersion("s1", 1, fetchFn as unknown as typeof fetch)).text,
    ).toBeNull();
  });
});
