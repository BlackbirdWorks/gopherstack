import { describe, expect, it, vi } from "vitest";

import {
  AR_PROXY_BASE,
  deleteResource,
  fileDownloadUrl,
  formatBytes,
  listDockerImages,
  modeLabel,
  parseCleanupPolicies,
  parseDockerImage,
  parseUpstream,
  previewCleanup,
  summarizeCleanup,
  listPackages,
  listRepositories,
  listVersions,
  parseFile,
  parsePackage,
  parseRepo,
  parseTag,
  parseVersion,
  unescapeName,
} from "./gcpartifactregistry";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

describe("formatBytes", () => {
  it.each([
    [0, "0 B"],
    [512, "512 B"],
    [2048, "2.0 KB"],
    [5 * 1024 * 1024, "5.0 MB"],
    [Number.NaN, "0 B"],
  ])("%d", (n, want) => expect(formatBytes(n)).toBe(want));
});

describe("names", () => {
  it("unescapes slashes and survives malformed input", () => {
    expect(unescapeName("team%2Fapp")).toBe("team/app");
    expect(unescapeName("100%")).toBe("100%");
  });
});

describe("parsers", () => {
  it("parses a repository", () => {
    const r = parseRepo({
      name: "projects/p/locations/l/repositories/r1",
      format: "DOCKER",
      sizeBytes: "1024",
      labels: { a: "b" },
    });
    expect(r).toMatchObject({
      id: "r1",
      format: "DOCKER",
      mode: "STANDARD_REPOSITORY",
      sizeBytes: 1024,
      labels: { a: "b" },
    });
  });

  it("parses packages with escaped slashes", () => {
    expect(
      parsePackage({ name: "projects/p/locations/l/repositories/r/packages/team%2Fapp" }).id,
    ).toBe("team/app");
  });

  it("parses versions with their tags", () => {
    const v = parseVersion({
      name: "projects/p/locations/l/repositories/r/packages/a/versions/sha256:abc",
      relatedTags: [{ name: "projects/p/locations/l/repositories/r/packages/a/tags/latest" }],
    });
    expect(v.id).toBe("sha256:abc");
    expect(v.relatedTags).toEqual(["latest"]);
  });

  it("parses tags and files", () => {
    expect(parseTag({ name: "x/tags/latest", version: "x/versions/1.0" })).toMatchObject({
      id: "latest",
      version: "1.0",
    });
    const f = parseFile({
      name: "projects/p/locations/l/repositories/r/files/a%2Fb.txt",
      sizeBytes: "10",
      hashes: [{ type: "SHA256", value: "ff" }],
      owner: "x/packages/a",
    });
    expect(f).toMatchObject({ id: "a/b.txt", sizeBytes: 10, hashes: ["SHA256:ff"], owner: "a" });
  });
});

describe("requests", () => {
  const scope = { project: "p", location: "l" };

  it("lists repositories for a scope", async () => {
    const f = vi.fn((..._args: unknown[]) =>
      Promise.resolve(json({ repositories: [{ name: "projects/p/locations/l/repositories/r1" }] })),
    );
    expect((await listRepositories(scope, f)).map((r) => r.id)).toEqual(["r1"]);
    expect(f.mock.calls[0][0]).toBe(`${AR_PROXY_BASE}/projects/p/locations/l/repositories`);
  });

  it("keeps escaped package names in version paths", async () => {
    const f = vi.fn((..._args: unknown[]) => Promise.resolve(json({})));
    await listVersions("projects/p/locations/l/repositories/r/packages/team%2Fapp", f);
    expect(f.mock.calls[0][0]).toBe(
      `${AR_PROXY_BASE}/projects/p/locations/l/repositories/r/packages/team%2Fapp/versions?view=BASIC`,
    );
  });

  it("lists packages and deletes by name", async () => {
    const f = vi.fn((..._args: unknown[]) =>
      Promise.resolve(json({ packages: [{ name: "x/packages/a" }] })),
    );
    expect(
      (await listPackages("projects/p/locations/l/repositories/r", f)).map((p) => p.id),
    ).toEqual(["a"]);
    await deleteResource("projects/p/locations/l/repositories/r", f);
    expect(f.mock.calls[1][1]).toEqual({ method: "DELETE" });
  });
});

describe("modes and docker images", () => {
  it.each([
    ["STANDARD_REPOSITORY", "STANDARD"],
    ["REMOTE_REPOSITORY", "REMOTE"],
    ["VIRTUAL_REPOSITORY", "VIRTUAL"],
    ["", "STANDARD"],
  ])("%s", (mode, want) => expect(modeLabel(mode)).toBe(want));

  it("parses a docker image into digest, tags and size", () => {
    const i = parseDockerImage({
      name: "projects/p/locations/l/repositories/r/dockerImages/team%2Fapp@sha256:abc",
      tags: ["v1", "latest"],
      imageSizeBytes: "2048",
      uploadTime: "2026-01-01T00:00:00Z",
    });
    expect(i).toMatchObject({ image: "team/app", digest: "sha256:abc", sizeBytes: 2048 });
    expect(i.tags).toEqual(["v1", "latest"]);
  });

  it("lists docker images and builds download links", async () => {
    const f = vi.fn((..._a: unknown[]) =>
      Promise.resolve(json({ dockerImages: [{ name: "x/y@sha256:1" }] })),
    );
    expect((await listDockerImages("projects/p/locations/l/repositories/r", f)).length).toBe(1);
    expect(f.mock.calls[0][0]).toBe(
      `${AR_PROXY_BASE}/projects/p/locations/l/repositories/r/dockerImages`,
    );
    expect(fileDownloadUrl("projects/p/locations/l/repositories/r/files/a%2Fb")).toBe(
      "/dashboard/api/v1/gcp/artifactregistry/download?name=projects%2Fp%2Flocations%2Fl%2Frepositories%2Fr%2Ffiles%2Fa%252Fb",
    );
  });
});

describe("cleanup and upstream", () => {
  const raw = {
    cleanupPolicyDryRun: true,
    cleanupPolicies: {
      keep2: { id: "keep2", action: "KEEP", mostRecentVersions: { keepCount: 2 } },
      old: {
        id: "old",
        action: "DELETE",
        condition: { tagState: "UNTAGGED", olderThan: "86400s" },
      },
    },
  };

  it("summarizes policies", () => {
    expect(summarizeCleanup(raw)).toBe("2 policies (1 delete, 1 keep), dry run");
    expect(summarizeCleanup({})).toBe("");
    expect(parseCleanupPolicies(raw).map((p) => p.summary)).toEqual([
      "keep the 2 most recent versions",
      "untagged, older than 86400s",
    ]);
  });

  it("previews deletions", async () => {
    const f = vi.fn((..._a: unknown[]) =>
      Promise.resolve(json({ dryRun: true, policies: 1, versions: ["v/1"] })),
    );
    expect(await previewCleanup("projects/p/locations/l/repositories/r", f)).toEqual({
      dryRun: true,
      policies: 1,
      versions: ["v/1"],
    });
    expect(f.mock.calls[0][0]).toContain("repositories/r:cleanupPreview");
  });

  it("shows the secret version name for remote credentials and virtual priorities", () => {
    const remote = parseUpstream({
      remoteRepositoryConfig: {
        description: "hub",
        dockerRepository: { publicRepository: "DOCKER_HUB" },
        upstreamCredentials: {
          usernamePasswordCredentials: {
            username: "bot",
            passwordSecretVersion: "projects/p/secrets/s/versions/1",
          },
        },
      },
    });
    expect(remote.kind).toBe("remote");
    expect(remote.lines).toContain("Upstream (docker): DOCKER_HUB");
    expect(remote.lines.join("\n")).toContain("password secret projects/p/secrets/s/versions/1");
    const virtual = parseUpstream({
      virtualRepositoryConfig: {
        upstreamPolicies: [
          { id: "a", repository: "projects/p/locations/l/repositories/x", priority: 10 },
        ],
      },
    });
    expect(virtual).toEqual({
      kind: "virtual",
      lines: ["a: projects/p/locations/l/repositories/x (priority 10)"],
    });
    expect(parseUpstream({}).kind).toBe("none");
  });
});

describe("tag deletion", () => {
  it("deletes by resource name", async () => {
    const f = vi.fn((..._a: unknown[]) => Promise.resolve(json({})));
    await deleteResource("projects/p/locations/l/repositories/r/packages/img/tags/v1", f);
    expect((f.mock.calls[0][1] as RequestInit).method).toBe("DELETE");
    expect(f.mock.calls[0][0]).toContain("/packages/img/tags/v1");
  });
});
