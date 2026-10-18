import { describe, expect, it } from "vitest";

import {
  BlobError,
  blobUrl,
  breadcrumbs,
  browse,
  createContainer,
  formatBytes,
  getBlobProperties,
  listBlobs,
  listContainers,
  parseBlobList,
  uploadBlob,
  validateBlobName,
  validateContainerName,
  type BlobItem,
} from "./azureblob";

const blob = (name: string, size = 1): BlobItem => ({
  name,
  lastModified: "",
  etag: "",
  contentType: "text/plain",
  contentLength: size,
});

const xml = (body: string, status = 200) => new Response(body, { status });

describe("blobUrl", () => {
  it.each([
    ["acct", undefined, undefined, "", "/dashboard/api/v1/azure/blob/acct"],
    [
      "acct",
      "c1",
      undefined,
      "restype=container",
      "/dashboard/api/v1/azure/blob/acct/c1?restype=container",
    ],
    ["acct", "c1", "a b/c#d.txt", "", "/dashboard/api/v1/azure/blob/acct/c1/a%20b/c%23d.txt"],
  ])("%s %s %s", (account, container, name, query, want) => {
    expect(blobUrl(account, container, name, query)).toBe(want);
  });
});

describe("browse", () => {
  const blobs = [
    blob("a.txt"),
    blob("dir/b.txt"),
    blob("dir/sub/c.txt"),
    blob("dir/sub/d.txt"),
    blob("z/"),
  ];

  it.each([
    ["", ["dir/", "z/", "a.txt"]],
    ["dir/", ["sub/", "b.txt"]],
    ["dir/sub/", ["c.txt", "d.txt"]],
    ["none/", []],
  ])("prefix %j", (prefix, want) => {
    const got = browse(blobs, prefix).map((e) => (e.kind === "folder" ? `${e.name}/` : e.name));
    expect(got).toEqual(want);
  });

  it("builds breadcrumbs", () => {
    expect(breadcrumbs("a/b/")).toEqual([
      { label: "a", prefix: "a/" },
      { label: "b", prefix: "a/b/" },
    ]);
    expect(breadcrumbs("")).toEqual([]);
  });
});

describe("validation", () => {
  it.each([
    ["", "Name is required"],
    ["ab", "Must be 3-63 characters"],
    ["Abc", "Use lowercase letters, numbers and hyphens only"],
    ["-abc", "Must start with a letter or number and not end with a hyphen"],
    ["a--b", "Consecutive hyphens are not allowed"],
    ["good-name", null],
  ])("container %j", (name, want) => {
    expect(validateContainerName(name)).toBe(want);
  });

  it.each([
    ["", true],
    ["/a", true],
    ["a/../b", true],
    ["a/b.txt", false],
  ])("blob %j invalid=%s", (name, invalid) => {
    expect(validateBlobName(name) !== null).toBe(invalid);
  });

  it.each([
    [10, "10 B"],
    [2048, "2.00 KiB"],
    [5 * 1024 * 1024, "5.00 MiB"],
  ])("formatBytes %d", (n, want) => {
    expect(formatBytes(n)).toBe(want);
  });
});

describe("requests", () => {
  it("parses container and blob lists", async () => {
    const containers = await listContainers("a", () =>
      Promise.resolve(
        xml(
          "<EnumerationResults><Containers><Container><Name>c1</Name><Properties><Last-Modified>x</Last-Modified><Etag>e</Etag></Properties></Container></Containers></EnumerationResults>",
        ),
      ),
    );
    expect(containers).toEqual([{ name: "c1", lastModified: "x", etag: "e" }]);

    const blobs = parseBlobList(
      "<EnumerationResults><Blobs><Blob><Name>a/b</Name><Properties><Last-Modified>t</Last-Modified><Etag>e</Etag><Content-Type>text/plain</Content-Type><Content-Length>5</Content-Length></Properties></Blob></Blobs></EnumerationResults>",
    );
    expect(blobs[0]).toMatchObject({ name: "a/b", contentLength: 5, contentType: "text/plain" });
  });

  it("maps Azure XML errors", async () => {
    const err = await createContainer("a", "c", () =>
      Promise.resolve(
        xml("<Error><Code>ContainerAlreadyExists</Code><Message>exists</Message></Error>", 409),
      ),
    ).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(BlobError);
    expect(err).toMatchObject({ status: 409, code: "ContainerAlreadyExists", message: "exists" });
  });

  it("maps network failures", async () => {
    const err = await listBlobs("a", "c", () => Promise.reject(new Error("boom"))).catch(
      (e: unknown) => e,
    );
    expect(err).toMatchObject({ status: 0, code: "NetworkError" });
  });

  it("uploads with blob type header and content type", async () => {
    let init: RequestInit | undefined;
    await uploadBlob("a", "c", "x/y.txt", new Blob(["hi"]), "text/plain", (_u, i) => {
      init = i;
      return Promise.resolve(new Response(null, { status: 201 }));
    });
    expect(init?.method).toBe("PUT");
    expect(init?.headers).toMatchObject({
      "x-ms-blob-type": "BlockBlob",
      "Content-Type": "text/plain",
    });
  });

  it("reads properties from HEAD headers", async () => {
    const props = await getBlobProperties("a", "c", "x", () =>
      Promise.resolve(
        new Response(null, {
          status: 200,
          headers: {
            ETag: '"1"',
            "Last-Modified": "t",
            "Content-Type": "text/plain",
            "x-ms-blob-type": "BlockBlob",
          },
        }),
      ),
    );
    expect(props).toMatchObject({ etag: '"1"', blobType: "BlockBlob", contentType: "text/plain" });
  });
});
