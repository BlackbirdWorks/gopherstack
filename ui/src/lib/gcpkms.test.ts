import { describe, expect, it, vi } from "vitest";

import {
  KMS_PROXY_BASE,
  asymmetricDecryptText,
  decryptText,
  deleteKey,
  deleteVersion,
  encryptText,
  keyBody,
  listKeys,
  listRetired,
  macVerifyText,
  oaepHash,
  rsaEncryptText,
  updateKey,
  updateKeyPlan,
  parseKey,
  parseVersion,
  rotationFields,
  setPrimary,
  signText,
  validateKmsId,
  versionAction,
} from "./gcpkms";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });

const ref = { project: "p", location: "global", ring: "r", key: "k" };
const keyUrl = `${KMS_PROXY_BASE}/projects/p/locations/global/keyRings/r/cryptoKeys/k`;

describe("validation", () => {
  it.each([
    ["", "name is required"],
    ["bad name", "use letters, digits, - and _ (max 63)"],
    ["my-key_1", null],
  ])("id %s", (id, want) => {
    expect(validateKmsId(id)).toBe(want);
  });
});

describe("parsing", () => {
  it("reads a key with primary version and rotation", () => {
    const k = parseKey({
      name: "projects/p/locations/global/keyRings/r/cryptoKeys/k",
      purpose: "ENCRYPT_DECRYPT",
      primary: {
        name: "projects/p/locations/global/keyRings/r/cryptoKeys/k/cryptoKeyVersions/3",
        algorithm: "GOOGLE_SYMMETRIC_ENCRYPTION",
        protectionLevel: "SOFTWARE",
      },
      rotationPeriod: "172800s",
    });
    expect(k).toMatchObject({
      id: "k",
      primaryId: "3",
      algorithm: "GOOGLE_SYMMETRIC_ENCRYPTION",
      rotationPeriod: "2 days",
    });
  });

  it("falls back to the version template without a primary", () => {
    const k = parseKey({ name: "a/b/k", versionTemplate: { algorithm: "HMAC_SHA256" } });
    expect(k.algorithm).toBe("HMAC_SHA256");
    expect(k.primaryId).toBe("");
  });

  it("reads a version", () => {
    expect(parseVersion({ name: "x/cryptoKeyVersions/2", state: "ENABLED" })).toMatchObject({
      id: "2",
      state: "ENABLED",
    });
  });
});

describe("rotation", () => {
  const now = new Date("2026-01-01T00:00:00Z");
  it("is null when blank", () => expect(rotationFields(" ", now)).toBeNull());
  it("rejects non-numbers", () => expect(rotationFields("0", now)).toHaveProperty("error"));
  it("schedules from now", () => {
    expect(rotationFields("2", now)).toEqual({
      rotationPeriod: "172800s",
      nextRotationTime: "2026-01-03T00:00:00.000Z",
    });
  });
  it("builds key bodies", () => {
    const b = keyBody(
      {
        purpose: "MAC",
        algorithm: "HMAC_SHA256",
        protectionLevel: "SOFTWARE",
        rotationDays: "",
        labels: { a: "b" },
      },
      now,
    );
    expect(b).toEqual({
      purpose: "MAC",
      versionTemplate: { algorithm: "HMAC_SHA256", protectionLevel: "SOFTWARE" },
      labels: { a: "b" },
    });
  });
});

describe("calls", () => {
  it("enable patches state with a mask", async () => {
    const f = vi.fn().mockResolvedValue(json({ name: "v/1", state: "ENABLED" }));
    await versionAction("p", "global", "r", "k", "1", "enable", f);
    expect(f).toHaveBeenCalledWith(
      `${keyUrl}/cryptoKeyVersions/1?updateMask=state`,
      expect.objectContaining({ method: "PATCH", body: '{"state":"ENABLED"}' }),
    );
  });

  it("destroy posts the verb", async () => {
    const f = vi.fn().mockResolvedValue(json({ name: "v/1", state: "DESTROY_SCHEDULED" }));
    await versionAction("p", "global", "r", "k", "1", "destroy", f);
    expect(f.mock.calls[0][0]).toBe(`${keyUrl}/cryptoKeyVersions/1:destroy`);
  });

  it("sets the primary version", async () => {
    const f = vi.fn().mockResolvedValue(json({ name: "k" }));
    await setPrimary("p", "global", "r", "k", "2", f);
    expect(f.mock.calls[0][0]).toBe(`${keyUrl}:updatePrimaryVersion`);
    expect(f.mock.calls[0][1].body).toBe('{"cryptoKeyVersionId":"2"}');
  });

  it("encrypts utf-8 text and decrypts back", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(json({ ciphertext: "Q1Q=", name: "k/cryptoKeyVersions/1" }));
    const out = await encryptText(ref, "héllo", "ctx", f);
    expect(out).toEqual({ ciphertext: "Q1Q=", version: "1" });
    expect(JSON.parse(f.mock.calls[0][1].body)).toEqual({
      plaintext: "aMOpbGxv",
      additionalAuthenticatedData: "Y3R4",
    });
    const g = vi.fn().mockResolvedValue(json({ plaintext: "aMOpbGxv" }));
    expect((await decryptText(ref, " Q1Q= ", "", g)).text).toBe("héllo");
    expect(JSON.parse(g.mock.calls[0][1].body)).toEqual({ ciphertext: "Q1Q=" });
  });

  it("signs data", async () => {
    const f = vi.fn().mockResolvedValue(json({ signature: "c2ln" }));
    expect(await signText(ref, "1", "hi", f)).toBe("c2ln");
    expect(f.mock.calls[0][0]).toBe(`${keyUrl}/cryptoKeyVersions/1:asymmetricSign`);
  });
});

describe("mac verify, rsa decrypt, update, delete, retired", () => {
  it.each([
    [{ success: true }, true],
    [{}, false],
  ])("macVerifyText %j", async (resp, want) => {
    const fetchFn = vi.fn(() => Promise.resolve(json(resp)));
    expect(await macVerifyText(ref, "1", "hi", " bWFj ", fetchFn)).toBe(want);
    const [url, init] = fetchFn.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe(`${keyUrl}/cryptoKeyVersions/1:macVerify`);
    expect(JSON.parse(init.body as string)).toEqual({ data: "aGk=", mac: "bWFj" });
  });

  it.each([
    ["RSA_DECRYPT_OAEP_2048_SHA256", "SHA-256"],
    ["RSA_DECRYPT_OAEP_3072_SHA1", "SHA-1"],
    ["RSA_DECRYPT_OAEP_4096_SHA512", "SHA-512"],
  ])("oaepHash %s", (alg, want) => {
    expect(oaepHash(alg)).toBe(want);
  });

  it("oaepHash rejects non-OAEP names", () => {
    expect(() => oaepHash("EC_SIGN_P256")).toThrow();
  });

  it("rsa encrypt then calls asymmetricDecrypt", async () => {
    const pair = await crypto.subtle.generateKey(
      {
        name: "RSA-OAEP",
        modulusLength: 2048,
        publicExponent: new Uint8Array([1, 0, 1]),
        hash: "SHA-256",
      },
      true,
      ["encrypt", "decrypt"],
    );
    const spki = new Uint8Array(await crypto.subtle.exportKey("spki", pair.publicKey));
    const pem = `-----BEGIN PUBLIC KEY-----\n${btoa(String.fromCodePoint(...spki))}\n-----END PUBLIC KEY-----\n`;
    const ct = await rsaEncryptText(pem, "RSA_DECRYPT_OAEP_2048_SHA256", "secret");
    const plain = await crypto.subtle.decrypt(
      { name: "RSA-OAEP" },
      pair.privateKey,
      Uint8Array.from(atob(ct), (c) => c.codePointAt(0) ?? 0),
    );
    expect(new TextDecoder().decode(plain)).toBe("secret");

    const fetchFn = vi.fn(() => Promise.resolve(json({ plaintext: "c2VjcmV0" })));
    const res = await asymmetricDecryptText(ref, "1", ct, fetchFn);
    expect(res.text).toBe("secret");
    expect((fetchFn.mock.calls[0] as unknown as [string])[0]).toBe(
      `${keyUrl}/cryptoKeyVersions/1:asymmetricDecrypt`,
    );
  });

  it("updateKey patches labels and rotation with a mask", async () => {
    const fetchFn = vi.fn(() =>
      Promise.resolve(json({ name: "projects/p/locations/global/keyRings/r/cryptoKeys/k" })),
    );
    await updateKey(ref, { labels: { a: "b" }, rotationDays: "30" }, fetchFn);
    const [url, init] = fetchFn.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe(`${keyUrl}?updateMask=labels%2CrotationPeriod%2CnextRotationTime`);
    expect(init.method).toBe("PATCH");
    const body = JSON.parse(init.body as string);
    expect(body.labels).toEqual({ a: "b" });
    expect(body.rotationPeriod).toBe("2592000s");
  });

  it("updateKeyPlan clears rotation when blank and rejects bad days", () => {
    const clear = updateKeyPlan({ labels: {}, rotationDays: "" });
    expect("body" in clear && clear.body).toEqual({ labels: {} });
    expect(updateKeyPlan({ labels: {}, rotationDays: "x" })).toHaveProperty("error");
  });

  it("deletes versions and keys", async () => {
    const fetchFn = vi.fn(() => Promise.resolve(json({ done: true })));
    await deleteVersion(ref, "2", fetchFn);
    await deleteKey(ref, fetchFn);
    const calls = fetchFn.mock.calls as unknown as [string, RequestInit][];
    expect(calls.map((c) => [c[1].method, c[0]])).toEqual([
      ["DELETE", `${keyUrl}/cryptoKeyVersions/2`],
      ["DELETE", keyUrl],
    ]);
  });

  it("lists retired resources and passes key list filter and order", async () => {
    const fetchFn = vi.fn(() =>
      Promise.resolve(
        json({
          retiredResources: [
            {
              name: "projects/p/locations/global/retiredResources/abc",
              originalResource: `projects/p/locations/global/keyRings/r/cryptoKeys/k`,
              resourceType: "CRYPTO_KEY",
              deleteTime: "2026-01-01T00:00:00Z",
            },
          ],
        }),
      ),
    );
    const out = await listRetired("p", "global", fetchFn);
    expect(out[0]).toMatchObject({ id: "abc", originalId: "k", resourceType: "CRYPTO_KEY" });
    expect((fetchFn.mock.calls[0] as unknown as [string])[0]).toBe(
      `${KMS_PROXY_BASE}/projects/p/locations/global/retiredResources?pageSize=100`,
    );

    const f2 = vi.fn(() => Promise.resolve(json({})));
    await listKeys("p", "global", "r", f2, { filter: "labels.a=b", orderBy: "name desc" });
    expect((f2.mock.calls[0] as unknown as [string])[0]).toContain(
      "filter=labels.a%3Db&orderBy=name+desc",
    );
  });
});
