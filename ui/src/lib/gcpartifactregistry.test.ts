import { describe, expect, it, vi } from "vitest";

import {
  AR_PROXY_BASE,
  deleteResource,
  formatBytes,
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
