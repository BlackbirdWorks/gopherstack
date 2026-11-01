import { describe, expect, it, vi } from "vitest";

import {
  ART_PROXY_BASE,
  artifactDownloadUrl,
  createGenericRepo,
  createRepo,
  deleteArtifact,
  deleteGenericRepo,
  deleteImage,
  deleteRepo,
  formatBytes,
  getConfig,
  imageTags,
  listArtifacts,
  listCompartments,
  listGenericRepos,
  listImages,
  listRepos,
  listSignatures,
  parseArtifact,
  parseImage,
  parseRepo,
  parseSignature,
  registryHints,
  restoreImage,
  setCreatedOnFirstPush,
  shortDigest,
  updateRepo,
  validName,
} from "./ociartifacts";

const json = (body: unknown, status = 200, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
const asFetch = (f: unknown) => f as typeof fetch;
const comp = "ocid1.compartment.oc1..aaaa";

const repoRaw = {
  id: "repo1",
  displayName: "team/web",
  lifecycleState: "AVAILABLE",
  isPublic: true,
  isImmutable: false,
  imageCount: 2,
  layerCount: 5,
  layersSizeInBytes: 2048,
  timeCreated: "2026-01-01T00:00:00Z",
  readme: { content: "# hi", format: "TEXT_MARKDOWN" },
};

const imageRaw = {
  id: "img1",
  digest: "sha256:0123456789abcdef0123456789abcdef",
  lifecycleState: "AVAILABLE",
  repositoryId: "repo1",
  repositoryName: "team/web",
  versions: [{ version: "v1", timeCreated: "2026-01-02T00:00:00Z" }, { version: "latest" }],
  layers: [{ digest: "sha256:aa", sizeInBytes: 10 }],
  layersSizeInBytes: 10,
  pullCount: 3,
  timeLastPulled: "2026-01-03T00:00:00Z",
};

describe("parsers", () => {
  it("parses a repository", () => {
    expect(parseRepo(repoRaw)).toMatchObject({
      name: "team/web",
      isPublic: true,
      isImmutable: false,
      imageCount: 2,
      layerCount: 5,
      sizeBytes: 2048,
      readme: "# hi",
    });
    expect(parseRepo({ id: "x" }).name).toBe("x");
  });

  it("parses an image with versions and layers", () => {
    const img = parseImage(imageRaw);
    expect(imageTags(img)).toEqual(["v1", "latest"]);
    expect(img.layers).toEqual([{ digest: "sha256:aa", sizeBytes: 10 }]);
    expect(img.pullCount).toBe(3);
    expect(img.lastPulled).toBe("2026-01-03T00:00:00Z");
    expect(parseImage({}).versions).toEqual([]);
  });

  it("parses a signature without its message or signature blobs", () => {
    const sig = parseSignature({
      id: "s1",
      kmsKeyId: "key",
      signingAlgorithm: "SHA_256_RSA_PKCS_PSS",
      displayName: "desc",
      message: "SECRET",
      signature: "SECRET",
    });
    expect(sig).toMatchObject({
      keyId: "key",
      algorithm: "SHA_256_RSA_PKCS_PSS",
      description: "desc",
    });
    expect(JSON.stringify(sig)).not.toContain("SECRET");
  });

  it("parses an artifact", () => {
    expect(
      parseArtifact({
        id: "a",
        artifactPath: "d/f.bin",
        version: "1",
        sizeInBytes: 4,
        sha256: "AB",
      }),
    ).toMatchObject({ path: "d/f.bin", version: "1", sizeBytes: 4, sha256: "AB" });
  });
});

describe("reads", () => {
  it("lists compartments", async () => {
    const f = vi.fn().mockResolvedValue(json({ compartments: ["c1", "c2"] }));
    expect(await listCompartments(asFetch(f))).toEqual(["c1", "c2"]);
    expect(f.mock.calls[0][0]).toBe(`${ART_PROXY_BASE}/compartments`);
  });

  it("lists repositories across pages with full records", async () => {
    const f = vi.fn().mockImplementation((url: string) => {
      if (url.includes("/containerRepositories/")) {
        return Promise.resolve(json({ ...repoRaw, id: url.split("/").pop(), isImmutable: true }));
      }
      return Promise.resolve(
        url.includes("page=p2")
          ? json({ items: [{ id: "repo2" }] })
          : json({ items: [{ id: "repo1" }] }, 200, { "opc-next-page": "p2" }),
      );
    });
    const repos = await listRepos(comp, asFetch(f));
    expect(repos.map((r) => [r.id, r.isImmutable])).toEqual([
      ["repo1", true],
      ["repo2", true],
    ]);
    expect(f.mock.calls[0][0]).toBe(
      `${ART_PROXY_BASE}/api/containerRepositories?compartmentId=${encodeURIComponent(comp)}`,
    );
  });

  it("lists images with details and can ask for deleted ones", async () => {
    const f = vi
      .fn()
      .mockImplementation((url: string) =>
        Promise.resolve(
          url.includes("/containerImages/") ? json(imageRaw) : json({ items: [{ id: "img1" }] }),
        ),
      );
    const imgs = await listImages(comp, "repo1", true, asFetch(f));
    expect(imgs).toHaveLength(1);
    expect(imgs[0].layers).toHaveLength(1);
    expect(f.mock.calls[0][0]).toContain("repositoryId=repo1");
    expect(f.mock.calls[0][0]).toContain("lifecycleState=DELETED");
  });

  it("lists signatures of an image", async () => {
    const f = vi.fn().mockResolvedValue(json({ items: [{ id: "s", kmsKeyId: "k" }] }));
    expect((await listSignatures(comp, "img1", asFetch(f)))[0].keyId).toBe("k");
    expect(f.mock.calls[0][0]).toContain("imageId=img1");
  });

  it("reads the container configuration", async () => {
    const f = vi
      .fn()
      .mockResolvedValue(json({ namespace: "ns", isRepositoryCreatedOnFirstPush: true }));
    expect(await getConfig(comp, asFetch(f))).toEqual({
      namespace: "ns",
      createdOnFirstPush: true,
    });
  });

  it("lists generic repositories and artifacts", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(
        json({ items: [{ id: "g", displayName: "files", isImmutable: true }] }),
      )
      .mockResolvedValueOnce(json({ items: [{ id: "a", artifactPath: "p", version: "1" }] }));
    expect((await listGenericRepos(comp, asFetch(f)))[0]).toMatchObject({
      name: "files",
      isImmutable: true,
    });
    expect((await listArtifacts(comp, "g", asFetch(f)))[0].path).toBe("p");
    expect(f.mock.calls[1][0]).toContain("repositoryId=g");
  });

  it("surfaces proxy errors", async () => {
    const f = vi
      .fn()
      .mockResolvedValue(json({ code: "NotAuthorizedOrNotFound", message: "gone" }, 404));
    await expect(listRepos(comp, asFetch(f))).rejects.toMatchObject({
      httpStatus: 404,
      code: "NotAuthorizedOrNotFound",
    });
  });
});

const ok = () => vi.fn().mockImplementation(() => Promise.resolve(json({})));

describe("writes", () => {
  it("creates a repository with an optional readme", async () => {
    const f = ok();
    await createRepo(
      { compartmentId: comp, displayName: "a/b", isPublic: true, isImmutable: false, readme: "" },
      asFetch(f),
    );
    expect(JSON.parse(f.mock.calls[0][1].body)).toEqual({
      compartmentId: comp,
      displayName: "a/b",
      isPublic: true,
      isImmutable: false,
    });
    await createRepo(
      { compartmentId: comp, displayName: "a/b", isPublic: false, isImmutable: true, readme: "x" },
      asFetch(f),
    );
    expect(JSON.parse(f.mock.calls[1][1].body).readme).toEqual({
      content: "x",
      format: "TEXT_MARKDOWN",
    });
  });

  it("updates only the given repository fields", async () => {
    const f = ok();
    await updateRepo("repo1", { isPublic: false }, asFetch(f));
    expect(f.mock.calls[0][1].method).toBe("PUT");
    expect(JSON.parse(f.mock.calls[0][1].body)).toEqual({ isPublic: false });
    await updateRepo("repo1", { readme: "r" }, asFetch(f));
    expect(JSON.parse(f.mock.calls[1][1].body)).toEqual({
      readme: { content: "r", format: "TEXT_MARKDOWN" },
    });
  });

  it("deletes and restores", async () => {
    const f = ok();
    await deleteRepo("r 1", asFetch(f));
    await deleteImage("i1", asFetch(f));
    await restoreImage("i1", asFetch(f));
    await deleteGenericRepo("g1", asFetch(f));
    await deleteArtifact("a1", asFetch(f));
    expect(f.mock.calls.map((c) => [c[1].method, c[0].replace(ART_PROXY_BASE, "")])).toEqual([
      ["DELETE", "/api/containerRepositories/r%201"],
      ["DELETE", "/api/containerImages/i1"],
      ["POST", "/api/containerImages/i1/restore"],
      ["DELETE", "/api/genericRepositories/g1"],
      ["DELETE", "/api/genericArtifacts/a1"],
    ]);
  });

  it("toggles repository creation on first push", async () => {
    const f = ok();
    await setCreatedOnFirstPush(comp, false, asFetch(f));
    expect(f.mock.calls[0][1].method).toBe("PUT");
    expect(JSON.parse(f.mock.calls[0][1].body)).toEqual({ isRepositoryCreatedOnFirstPush: false });
  });

  it("creates a generic repository", async () => {
    const f = ok();
    await createGenericRepo(
      { compartmentId: comp, displayName: "files", description: "d", isImmutable: false },
      asFetch(f),
    );
    expect(f.mock.calls[0][0]).toBe(`${ART_PROXY_BASE}/api/genericRepositories`);
    expect(JSON.parse(f.mock.calls[0][1].body).displayName).toBe("files");
  });
});

describe("helpers", () => {
  it("builds the download url through the proxy", () => {
    expect(artifactDownloadUrl("a/b")).toBe(`${ART_PROXY_BASE}/api/genericArtifacts/a%2Fb/content`);
  });

  it("formats sizes and digests", () => {
    expect(formatBytes(12)).toBe("12 B");
    expect(formatBytes(1536)).toBe("1.5 KiB");
    expect(formatBytes(5 * 1024 * 1024)).toBe("5.0 MiB");
    expect(shortDigest("sha256:0123456789abcdef0123")).toBe("sha256:0123456789ab");
    expect(shortDigest("plain")).toBe("plain");
  });

  it("builds docker commands without any credential value", () => {
    const h = registryHints("localhost", 8166, "ns", "team/web", "v1");
    expect(h.repo).toBe("localhost:8166/ns/team/web");
    expect(h.push).toContain("docker push localhost:8166/ns/team/web:v1");
    expect(h.pull).toBe("docker pull localhost:8166/ns/team/web:v1");
    expect(h.login).toContain("docker login localhost:8166 -u ns/<username>");
    expect(h.login).not.toMatch(/(-p|--password)\s/);
    expect(registryHints("h", undefined, "ns", "r", "").pull).toBe("docker pull h/ns/r:latest");
  });

  it("validates repository names", () => {
    expect(validName("team/web-1.x")).toBe(true);
    expect(validName("Upper")).toBe(false);
    expect(validName("")).toBe(false);
    expect(validName("a//b")).toBe(false);
  });
});
