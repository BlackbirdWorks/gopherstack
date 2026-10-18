import { describe, expect, it, vi } from "vitest";

import {
  GcsError,
  base64ToHex,
  breadcrumbs,
  createBucket,
  deleteObject,
  diffMap,
  downloadUrl,
  formatBytes,
  gsUri,
  jsonUrl,
  listBuckets,
  listObjects,
  listVersions,
  metadataToRows,
  patchObject,
  readStoredProject,
  uploadObject,
  uploadUrl,
  validateBucketName,
  validateLabelKey,
  validateObjectName,
  validateRows,
  validateMetadataKey,
} from "./gcpstorage";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });

describe("urls", () => {
  it.each([
    [undefined, undefined, "", "/dashboard/api/v1/gcp/storage/storage/v1/b"],
    ["bkt", undefined, "", "/dashboard/api/v1/gcp/storage/storage/v1/b/bkt"],
    [
      "bkt",
      "a b/c#d.txt",
      "",
      "/dashboard/api/v1/gcp/storage/storage/v1/b/bkt/o/a%20b%2Fc%23d.txt",
    ],
    ["bkt", "x", "generation=1", "/dashboard/api/v1/gcp/storage/storage/v1/b/bkt/o/x?generation=1"],
  ])("jsonUrl %s %s", (bucket, object, query, want) => {
    expect(jsonUrl(bucket, object, query)).toBe(want);
  });

  it("builds download and upload urls", () => {
    expect(downloadUrl("bkt", "d/a.txt", "7")).toBe(
      "/dashboard/api/v1/gcp/storage/download/storage/v1/b/bkt/o/d%2Fa.txt?alt=media&generation=7",
    );
    expect(uploadUrl("bkt", "d/a.txt")).toBe(
      "/dashboard/api/v1/gcp/storage/upload/storage/v1/b/bkt/o?uploadType=media&name=d%2Fa.txt",
    );
    expect(gsUri("bkt", "d/a.txt")).toBe("gs://bkt/d/a.txt");
    expect(gsUri("bkt")).toBe("gs://bkt");
  });
});

describe("validation", () => {
  it.each([
    ["", false],
    ["ab", false],
    ["Bucket", false],
    ["-abc", false],
    ["abc-", false],
    ["googbucket", false],
    ["my-bucket.v1_x", true],
    ["abc", true],
  ])("bucket name %j", (name, ok) => {
    expect(validateBucketName(name) === null).toBe(ok);
  });

  it.each([
    ["", false],
    ["/a", false],
    ["a/", false],
    ["a/../b", false],
    ["a/b.txt", true],
  ])("object name %j", (name, ok) => {
    expect(validateObjectName(name) === null).toBe(ok);
  });

  it.each([
    ["env", true],
    ["Env", false],
    ["1env", false],
    ["", false],
  ])("label key %j", (key, ok) => {
    expect(validateLabelKey(key) === null).toBe(ok);
  });

  it("validates rows for duplicates", () => {
    expect(
      validateRows(
        [
          { key: "a", value: "" },
          { key: "a", value: "" },
        ],
        validateMetadataKey,
      ),
    ).toMatch(/duplicate/);
    expect(validateRows([{ key: "", value: "" }], validateMetadataKey)).toMatch(/required/);
    expect(validateRows([{ key: "a", value: "" }], validateMetadataKey)).toBeNull();
  });
});

describe("helpers", () => {
  it("diffMap nulls removed keys", () => {
    expect(diffMap({ a: "1", b: "2" }, { b: "3", c: "4" })).toEqual({
      a: null,
      b: "3",
      c: "4",
    });
    expect(diffMap({ a: "1" }, { a: "1" })).toEqual({});
  });

  it("metadataToRows sorts", () => {
    expect(metadataToRows({ b: "2", a: "1" }).map((r) => r.key)).toEqual(["a", "b"]);
  });

  it("breadcrumbs and bytes", () => {
    expect(breadcrumbs("a/b/")).toEqual([
      { label: "a", prefix: "a/" },
      { label: "b", prefix: "a/b/" },
    ]);
    expect(formatBytes(512)).toBe("512 B");
    expect(formatBytes(2048)).toBe("2.00 KiB");
  });

  it("base64ToHex", () => {
    expect(base64ToHex("XUFAKrxLKna5cZ2REBfFkg==")).toBe("5d41402abc4b2a76b9719d911017c592");
    expect(base64ToHex("")).toBe("");
    expect(base64ToHex("!!")).toBe("");
  });

  it("readStoredProject falls back", () => {
    expect(readStoredProject({ getItem: () => " p1 " })).toBe("p1");
    expect(
      readStoredProject({
        getItem: () => {
          throw new Error("blocked");
        },
      }),
    ).toBe("gopherstack-local");
  });
});

describe("client", () => {
  it("listBuckets pages and parses", async () => {
    const fetchFn = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        json({
          items: [
            {
              name: "a",
              location: "US",
              versioning: { enabled: true },
              labels: { k: "v" },
            },
          ],
          nextPageToken: "t",
        }),
      )
      .mockResolvedValueOnce(json({ items: [{ name: "b" }] }));
    const got = await listBuckets("p", fetchFn);
    expect(got.map((b) => b.name)).toEqual(["a", "b"]);
    expect(got[0]).toMatchObject({ versioning: true, labels: { k: "v" } });
    expect(String(fetchFn.mock.calls[1]?.[0])).toContain("pageToken=t");
  });

  it("createBucket sends the body", async () => {
    const fetchFn = vi.fn<typeof fetch>().mockResolvedValue(json({ name: "bkt" }));
    await createBucket(
      "p",
      "bkt",
      {
        location: "EU",
        storageClass: "NEARLINE",
        versioning: true,
        labels: { a: "b" },
      },
      fetchFn,
    );
    const init = fetchFn.mock.calls[0]?.[1];
    expect(JSON.parse(String(init?.body))).toEqual({
      name: "bkt",
      location: "EU",
      storageClass: "NEARLINE",
      versioning: { enabled: true },
      labels: { a: "b" },
    });
    expect(String(fetchFn.mock.calls[0]?.[0])).toContain("project=p");
  });

  it("listObjects maps prefixes and items relative to the prefix", async () => {
    const fetchFn = vi.fn<typeof fetch>().mockResolvedValue(
      json({
        prefixes: ["d/sub/"],
        items: [{ name: "d/a.txt", size: "5" }],
        nextPageToken: "n",
      }),
    );
    const got = await listObjects("bkt", { prefix: "d/", delimiter: "/" }, fetchFn);
    expect(got.entries.map((e) => e.name)).toEqual(["sub", "a.txt"]);
    expect(got.nextToken).toBe("n");
    expect(got.entries[1]).toMatchObject({
      kind: "object",
      object: { size: 5 },
    });
  });

  it("listVersions keeps exact name matches, newest first", async () => {
    const fetchFn = vi.fn<typeof fetch>().mockResolvedValue(
      json({
        items: [
          { name: "a", generation: "10" },
          { name: "a", generation: "30" },
          { name: "ab", generation: "99" },
        ],
      }),
    );
    expect((await listVersions("bkt", "a", fetchFn)).map((o) => o.generation)).toEqual([
      "30",
      "10",
    ]);
  });

  it("surfaces GCS errors", async () => {
    const fetchFn = vi
      .fn<typeof fetch>()
      .mockResolvedValue(
        json({ error: { message: "nope", errors: [{ reason: "notFound" }] } }, 404),
      );
    const err = await deleteObject("bkt", "x", "", fetchFn).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(GcsError);
    expect(err).toMatchObject({
      status: 404,
      reason: "notFound",
      message: "nope",
    });
  });

  it("wraps network failures", async () => {
    const fetchFn = vi.fn<typeof fetch>().mockRejectedValue(new Error("offline"));
    await expect(deleteObject("bkt", "x", "", fetchFn)).rejects.toMatchObject({
      status: 0,
    });
  });

  it("patchObject sends merge-patch and upload posts the file", async () => {
    const fetchFn = vi
      .fn<typeof fetch>()
      .mockImplementation(() => Promise.resolve(json({ name: "x" })));
    await patchObject("bkt", "x", { metadata: { a: null } }, fetchFn);
    expect(fetchFn.mock.calls[0]?.[1]?.method).toBe("PATCH");
    await uploadObject("bkt", "x", new Blob(["hi"]), "", fetchFn);
    const init = fetchFn.mock.calls[1]?.[1];
    expect(init?.method).toBe("POST");
    expect(init?.headers).toEqual({
      "Content-Type": "application/octet-stream",
    });
  });
});
