import { describe, expect, it, vi } from "vitest";

import {
  ACR_PROXY_BASE,
  deleteTag,
  formatSize,
  listManifests,
  listRegistries,
  listRepositories,
  listTags,
  platform,
  revealCredentials,
  shortDigest,
} from "./azureacr";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const ok = (body: unknown) => vi.fn().mockImplementation(() => Promise.resolve(json(body)));

describe("helpers", () => {
  it.each([
    [0, "-"],
    [512, "512 B"],
    [2048, "2.0 KiB"],
    [5 * 1024 * 1024, "5.0 MiB"],
  ])("size %j", (n, want) => expect(formatSize(n)).toBe(want));

  it.each([
    ["sha256:" + "a".repeat(64), "sha256:" + "a".repeat(12)],
    ["short", "short"],
  ])("digest %j", (d, want) => expect(shortDigest(d)).toBe(want));

  it.each([
    [{ os: "linux", architecture: "amd64", index: false }, "linux/amd64"],
    [{ os: "", architecture: "", index: false }, "-"],
    [{ os: "", architecture: "", index: true }, "multi-platform"],
  ])("platform %j", (m, want) => expect(platform(m)).toBe(want));
});

describe("client", () => {
  it("lists registries and repositories", async () => {
    const f = ok({ registries: [{ name: "reg", adminEnabled: true, repositoryCount: 2 }] });
    expect((await listRegistries(f))[0]).toMatchObject({
      name: "reg",
      adminEnabled: true,
      repositoryCount: 2,
    });
    const g = ok({ repositories: [{ name: "team/app", tagCount: 3 }] });
    expect((await listRepositories("reg", g))[0]).toMatchObject({ name: "team/app", tagCount: 3 });
    expect(g.mock.calls[0][0]).toBe(`${ACR_PROXY_BASE}/registries/reg/repositories`);
  });

  it("encodes the repository in tag and manifest listings", async () => {
    const f = ok({
      tags: [{ name: "v1", digest: "sha256:x", os: "linux", architecture: "arm64", sizeBytes: 10 }],
    });
    const tags = await listTags("reg", "team/app", f);
    expect(f.mock.calls[0][0]).toBe(`${ACR_PROXY_BASE}/registries/reg/tags?repo=team%2Fapp`);
    expect(tags[0]).toMatchObject({ name: "v1", digest: "sha256:x", architecture: "arm64" });
    const g = ok({ manifests: [{ digest: "sha256:x", tags: ["v1"] }] });
    expect((await listManifests("reg", "team/app", g))[0].tags).toEqual(["v1"]);
  });

  it("deletes a tag by DELETE with repo and tag", async () => {
    const f = vi
      .fn()
      .mockImplementation(() => Promise.resolve(new Response(null, { status: 204 })));
    await deleteTag("reg", "team/app", "v1", f);
    expect(f.mock.calls[0][1].method).toBe("DELETE");
    expect(f.mock.calls[0][0]).toBe(`${ACR_PROXY_BASE}/registries/reg/tags?repo=team%2Fapp&tag=v1`);
  });

  it("reveals credentials only by POST", async () => {
    const f = ok({ username: "reg", password: "p1", password2: "p2" });
    expect(await revealCredentials("reg", f)).toEqual({
      username: "reg",
      password: "p1",
      password2: "p2",
    });
    expect(f.mock.calls[0][1].method).toBe("POST");
    expect(f.mock.calls[0][0]).toBe(`${ACR_PROXY_BASE}/registries/reg/credentials/reveal`);
  });
});
