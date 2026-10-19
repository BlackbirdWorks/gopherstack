import { describe, expect, it } from "vitest";

import {
  KvError,
  algsFor,
  b64uDecode,
  b64uEncode,
  certBody,
  certPem,
  createKey,
  decryptText,
  digestFor,
  listItems,
  listVaults,
  objectUrl,
  parseId,
  revealSecret,
  thumbprintHex,
  validateMonths,
  validateObjectName,
  validateVaultName,
  vaultUrl,
} from "./azurekeyvault";

const recorder = (bodies: unknown[], status = 200) => {
  const calls: { url: string; init: RequestInit }[] = [];
  const fn = ((url: string, init: RequestInit) => {
    calls.push({ url, init: init ?? {} });
    const body = bodies[Math.min(calls.length - 1, bodies.length - 1)];
    return Promise.resolve(
      new Response(body === undefined ? "" : JSON.stringify(body), { status }),
    );
  }) as unknown as typeof fetch;
  return { calls, fn };
};

describe("validation", () => {
  it.each([
    ["", "required"],
    ["ab", "3-24"],
    ["1abc", "3-24"],
    ["good-vault", ""],
  ])("vault %j", (name, want) => {
    expect(validateVaultName(name)).toContain(want);
  });

  it.each([
    ["", false],
    ["a b", false],
    ["ok-name1", true],
  ])("object %j", (name, ok) => {
    expect(validateObjectName(name) === "").toBe(ok);
  });

  it.each([
    ["12", true],
    ["0", false],
    ["1.5", false],
    ["1201", false],
    ["", false],
  ])("months %j", (raw, ok) => {
    expect(validateMonths(raw) === "").toBe(ok);
  });
});

describe("encoding", () => {
  it("round-trips base64url", () => {
    const bytes = new Uint8Array([251, 255, 0, 1, 2]);
    const enc = b64uEncode(bytes);
    expect(enc).toBe("-_8AAQI");
    expect(Array.from(b64uDecode(enc))).toEqual(Array.from(bytes));
  });

  it("hashes with the algorithm's digest size", async () => {
    expect(b64uDecode(await digestFor("RS256", "x")).length).toBe(32);
    expect(b64uDecode(await digestFor("ES384", "x")).length).toBe(48);
    expect(b64uDecode(await digestFor("ES512", "x")).length).toBe(64);
  });

  it("formats PEM and thumbprint", () => {
    expect(certPem("AAAA")).toBe("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n");
    expect(thumbprintHex("-_8")).toBe("FB:FF");
    expect(thumbprintHex("")).toBe("");
  });

  it.each([
    ["https://h/secrets/db-pass/abc123", "db-pass", "abc123"],
    ["https://h/keys/k1", "k1", ""],
    ["https://h/deletedkeys/k2", "k2", ""],
  ])("parses id %s", (id, name, version) => {
    expect(parseId(id)).toEqual({ name, version });
  });
});

describe("urls", () => {
  it("builds vault and object urls", () => {
    expect(vaultUrl("v 1", "secrets", "a=b")).toBe(
      "/dashboard/api/v1/azure/keyvault/vaults/v%201/secrets?a=b",
    );
    expect(objectUrl("v", "keys", "k", "create")).toBe(
      "/dashboard/api/v1/azure/keyvault/vaults/v/keys/k/create",
    );
  });
});

describe("algorithms", () => {
  it("selects by key type", () => {
    expect(algsFor("RSA", "").encrypt).toContain("RSA-OAEP");
    expect(algsFor("EC", "P-384")).toEqual({ encrypt: [], sign: ["ES384"] });
    expect(algsFor("oct", "")).toEqual({ encrypt: [], sign: [] });
  });
});

describe("client", () => {
  it("lists vaults", async () => {
    const r = recorder([{ vaults: ["a", "b"] }]);
    expect(await listVaults(r.fn)).toEqual(["a", "b"]);
  });

  it("follows nextLink pages and sorts", async () => {
    const r = recorder([
      {
        value: [{ id: "https://h/secrets/b", attributes: { enabled: false } }],
        nextLink: "https://h/x?$skiptoken=T1",
      },
      { value: [{ id: "https://h/secrets/a" }] },
    ]);
    const out = await listItems("v", "secrets", false, r.fn);
    expect(out.map((i) => i.name)).toEqual(["a", "b"]);
    expect(out[1].enabled).toBe(false);
    expect(r.calls[1].url).toContain("$skiptoken=T1");
  });

  it("hides managed secrets backing certificates", async () => {
    const r = recorder([
      {
        value: [{ id: "https://h/secrets/cert", managed: true }, { id: "https://h/secrets/real" }],
      },
    ]);
    expect((await listItems("v", "secrets", false, r.fn)).map((i) => i.name)).toEqual(["real"]);
  });

  it("lists deleted keys from the deleted area", async () => {
    const r = recorder([{ value: [{ kid: "https://h/keys/k", deletedDate: 5 }] }]);
    const out = await listItems("v", "keys", true, r.fn);
    expect(r.calls[0].url).toContain("/deletedkeys?");
    expect(out[0].deletedDate).toBe(5);
  });

  it("reveals a secret value", async () => {
    const r = recorder([{ value: "hush" }]);
    expect(await revealSecret("v", "s", "ver", r.fn)).toBe("hush");
    expect(r.calls[0].url).toBe("/dashboard/api/v1/azure/keyvault/vaults/v/secrets/s/ver");
  });

  it("maps Key Vault errors", async () => {
    const r = recorder([{ error: { code: "SecretNotFound", message: "nope" } }], 404);
    await expect(revealSecret("v", "s", "", r.fn)).rejects.toMatchObject({
      code: "SecretNotFound",
      status: 404,
    });
    await expect(revealSecret("v", "s", "", r.fn)).rejects.toBeInstanceOf(KvError);
  });

  it("creates keys with the right parameters", async () => {
    const r = recorder([{ key: { kid: "https://h/keys/k/v1", kty: "EC", crv: "P-384" } }]);
    const k = await createKey("v", { name: "k", kty: "EC", crv: "P-384" }, r.fn);
    expect(JSON.parse(String(r.calls[0].init.body))).toEqual({ kty: "EC", crv: "P-384" });
    expect(k.crv).toBe("P-384");
  });

  it("decrypts to text", async () => {
    const r = recorder([{ value: b64uEncode(new TextEncoder().encode("plain")) }]);
    expect(await decryptText("v", "k", "", "RSA-OAEP", "abc", r.fn)).toBe("plain");
    expect(r.calls[0].url).toContain("/keys/k/decrypt");
  });

  it("builds a self-signed certificate policy", () => {
    const body = certBody({ name: "c", subject: "CN=x", validityMonths: 6 }) as {
      policy: { x509_props: unknown; issuer: unknown };
    };
    expect(body.policy.x509_props).toEqual({ subject: "CN=x", validity_months: 6 });
    expect(body.policy.issuer).toEqual({ name: "Self" });
  });
});
