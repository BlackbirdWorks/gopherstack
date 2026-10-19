import { describe, expect, it, vi } from "vitest";

import {
  KMS_PROXY_BASE,
  decryptText,
  encryptText,
  keyBody,
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
