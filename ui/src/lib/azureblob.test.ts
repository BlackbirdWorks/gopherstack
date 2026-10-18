import { describe, expect, it } from "vitest";

import {
  BlobError,
  blobUrl,
  breadcrumbs,
  createContainer,
  formatBytes,
  aclBody,
  createSnapshot,
  deleteBlob,
  getBlobProperties,
  getContainerAcl,
  getContainerProperties,
  listBlobs,
  listContainers,
  listSnapshots,
  metadataToRows,
  rowsToMetadata,
  validateRows,
  parseBlobListing,
  setBlobMetadata,
  setBlobProperties,
  setBlobTier,
  setContainerAcl,
  setContainerMetadata,
  uploadBlob,
  validateBlobName,
  validateContainerName,
  validateMetadataKey,
} from "./azureblob";

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

describe("metadata rows", () => {
  it("round-trips sorted rows", () => {
    const rows = metadataToRows({ b: "2", a: "1" });
    expect(rows).toEqual([
      { key: "a", value: "1" },
      { key: "b", value: "2" },
    ]);
    expect(rowsToMetadata([{ key: "Foo", value: "x" }])).toEqual({ foo: "x" });
  });

  it.each([
    [[{ key: "a", value: "" }], null],
    [[{ key: "", value: "" }], "(empty): Key is required"],
    [
      [{ key: "a-b", value: "" }],
      "a-b: Use letters, digits and underscores; must not start with a digit",
    ],
    [
      [
        { key: "a", value: "" },
        { key: "A", value: "" },
      ],
      "A: duplicate key",
    ],
  ])("validates %j", (rows, want) => {
    expect(validateRows(rows)).toBe(want);
  });
});

describe("breadcrumbs", () => {
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
    ["", false],
    ["ok_key1", true],
    ["1bad", false],
    ["has-dash", false],
  ])("metadata key %j valid=%s", (key, valid) => {
    expect(validateMetadataKey(key) === null).toBe(valid);
  });

  it.each([
    [10, "10 B"],
    [2048, "2.00 KiB"],
    [5 * 1024 * 1024, "5.00 MiB"],
  ])("formatBytes %d", (n, want) => {
    expect(formatBytes(n)).toBe(want);
  });
});

type Call = { url: string; init?: RequestInit };

function recorder(res: () => Response = () => new Response(null, { status: 200 })) {
  const calls: Call[] = [];
  const fetchFn = (url: RequestInfo | URL, init?: RequestInit) => {
    calls.push({ url: String(url), init });
    return Promise.resolve(res());
  };
  return { calls, fetchFn };
}

describe("requests", () => {
  it("parses container list with access level and metadata", async () => {
    const containers = await listContainers("a", () =>
      Promise.resolve(
        xml(
          "<EnumerationResults><Containers><Container><Name>c1</Name><Properties><Last-Modified>x</Last-Modified><Etag>e</Etag><PublicAccess>blob</PublicAccess></Properties><Metadata><owner>me</owner></Metadata></Container></Containers></EnumerationResults>",
        ),
      ),
    );
    expect(containers).toEqual([
      { name: "c1", lastModified: "x", etag: "e", publicAccess: "blob", metadata: { owner: "me" } },
    ]);
  });

  it("parses blob listings into folders, blobs and a marker", () => {
    const listing = parseBlobListing(
      "<EnumerationResults><Blobs><BlobPrefix><Name>dir/sub/</Name></BlobPrefix><Blob><Name>dir/b</Name><Snapshot>s1</Snapshot><Properties><Last-Modified>t</Last-Modified><Etag>e</Etag><Content-Type>text/plain</Content-Type><AccessTier>Cool</AccessTier><Content-Length>5</Content-Length></Properties><Metadata><k>v</k></Metadata></Blob></Blobs><NextMarker>m1</NextMarker></EnumerationResults>",
      "dir/",
    );
    expect(listing.nextMarker).toBe("m1");
    expect(listing.entries[0]).toEqual({ kind: "folder", name: "sub", prefix: "dir/sub/" });
    expect(listing.entries[1]).toMatchObject({
      kind: "blob",
      name: "b",
      blob: {
        name: "dir/b",
        snapshot: "s1",
        accessTier: "Cool",
        contentLength: 5,
        metadata: { k: "v" },
      },
    });
  });

  it("sends prefix, delimiter, marker and paging to List Blobs", async () => {
    const { calls, fetchFn } = recorder(() =>
      xml("<EnumerationResults><Blobs></Blobs><NextMarker></NextMarker></EnumerationResults>"),
    );
    await listBlobs("a", "c", { prefix: "d/", marker: "m", maxResults: 50 }, fetchFn);
    const q = new URL(calls[0].url, "http://x").searchParams;
    expect(q.get("prefix")).toBe("d/");
    expect(q.get("delimiter")).toBe("/");
    expect(q.get("marker")).toBe("m");
    expect(q.get("maxresults")).toBe("50");
    expect(q.get("comp")).toBe("list");
  });

  it("lists only the snapshots of the exact blob", async () => {
    const body =
      "<EnumerationResults><Blobs>" +
      "<Blob><Name>x</Name><Snapshot>s1</Snapshot><Properties></Properties></Blob>" +
      "<Blob><Name>x</Name><Properties></Properties></Blob>" +
      "<Blob><Name>x2</Name><Snapshot>s9</Snapshot><Properties></Properties></Blob>" +
      "</Blobs><NextMarker></NextMarker></EnumerationResults>";
    const snaps = await listSnapshots("a", "c", "x", () => Promise.resolve(xml(body)));
    expect(snaps.map((s) => s.snapshot)).toEqual(["s1"]);
  });

  it("maps Azure XML errors", async () => {
    const err = await createContainer("a", "c", {}, () =>
      Promise.resolve(
        xml("<Error><Code>ContainerAlreadyExists</Code><Message>exists</Message></Error>", 409),
      ),
    ).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(BlobError);
    expect(err).toMatchObject({ status: 409, code: "ContainerAlreadyExists", message: "exists" });
  });

  it("maps network failures", async () => {
    const err = await listBlobs("a", "c", {}, () => Promise.reject(new Error("boom"))).catch(
      (e: unknown) => e,
    );
    expect(err).toMatchObject({ status: 0, code: "NetworkError" });
  });

  it("creates a container with access level and metadata headers", async () => {
    const { calls, fetchFn } = recorder();
    await createContainer("a", "c", { publicAccess: "container", metadata: { k: "v" } }, fetchFn);
    expect(calls[0].init?.headers).toEqual({
      "x-ms-meta-k": "v",
      "x-ms-blob-public-access": "container",
    });
  });

  it("uploads with blob type header and content type", async () => {
    const { calls, fetchFn } = recorder(() => new Response(null, { status: 201 }));
    await uploadBlob("a", "c", "x/y.txt", new Blob(["hi"]), "text/plain", fetchFn);
    expect(calls[0].init?.method).toBe("PUT");
    expect(calls[0].init?.headers).toMatchObject({
      "x-ms-blob-type": "BlockBlob",
      "Content-Type": "text/plain",
    });
  });

  it("reads properties, metadata and tier from HEAD headers", async () => {
    const props = await getBlobProperties("a", "c", "x", "", () =>
      Promise.resolve(
        new Response(null, {
          status: 200,
          headers: {
            ETag: '"1"',
            "Last-Modified": "t",
            "Content-Type": "text/plain",
            "Cache-Control": "no-cache",
            "x-ms-blob-type": "BlockBlob",
            "x-ms-access-tier": "Hot",
            "x-ms-access-tier-inferred": "true",
            "x-ms-meta-owner": "me",
          },
        }),
      ),
    );
    expect(props).toMatchObject({
      etag: '"1"',
      blobType: "BlockBlob",
      contentType: "text/plain",
      cacheControl: "no-cache",
      accessTier: "Hot",
      accessTierInferred: true,
      metadata: { owner: "me" },
    });
  });

  it("sends metadata, properties, tier and snapshot requests", async () => {
    const { calls, fetchFn } = recorder(
      () => new Response(null, { status: 201, headers: { "x-ms-snapshot": "snap1" } }),
    );
    await setBlobMetadata("a", "c", "x", { k: "v" }, fetchFn);
    await setBlobProperties(
      "a",
      "c",
      "x",
      {
        contentType: "text/csv",
        contentEncoding: "",
        contentLanguage: "en",
        contentDisposition: "",
        cacheControl: "",
      },
      fetchFn,
    );
    await setBlobTier("a", "c", "x", "Cool", fetchFn);
    expect(await createSnapshot("a", "c", "x", fetchFn)).toBe("snap1");
    await deleteBlob("a", "c", "x", { snapshot: "s 1" }, fetchFn);
    await deleteBlob("a", "c", "x", { deleteSnapshots: "include" }, fetchFn);
    await setContainerMetadata("a", "c", { z: "1" }, fetchFn);

    expect(calls.map((c) => c.url.split("?")[1])).toEqual([
      "comp=metadata",
      "comp=properties",
      "comp=tier",
      "comp=snapshot",
      "snapshot=s%201",
      undefined,
      "restype=container&comp=metadata",
    ]);
    expect(calls[0].init?.headers).toEqual({ "x-ms-meta-k": "v" });
    expect(calls[1].init?.headers).toEqual({
      "x-ms-blob-content-type": "text/csv",
      "x-ms-blob-content-language": "en",
    });
    expect(calls[2].init?.headers).toEqual({ "x-ms-access-tier": "Cool" });
    expect(calls[5].init?.headers).toEqual({ "x-ms-delete-snapshots": "include" });
  });

  it("reads container properties and ACL, and writes the ACL body", async () => {
    const props = await getContainerProperties("a", "c", () =>
      Promise.resolve(
        new Response(null, {
          status: 200,
          headers: { ETag: '"c"', "x-ms-blob-public-access": "blob", "x-ms-meta-team": "x" },
        }),
      ),
    );
    expect(props).toMatchObject({ publicAccess: "blob", metadata: { team: "x" } });

    const acl = await getContainerAcl("a", "c", () =>
      Promise.resolve(
        new Response(
          "<SignedIdentifiers><SignedIdentifier><Id>p1</Id><AccessPolicy><Permission>r</Permission></AccessPolicy></SignedIdentifier></SignedIdentifiers>",
          { status: 200, headers: { "x-ms-blob-public-access": "container" } },
        ),
      ),
    );
    expect(acl.publicAccess).toBe("container");
    expect(acl.identifiers).toEqual([{ id: "p1", start: "", expiry: "", permission: "r" }]);

    const { calls, fetchFn } = recorder();
    await setContainerAcl("a", "c", "blob", acl.identifiers, fetchFn);
    expect(calls[0].init?.headers).toMatchObject({ "x-ms-blob-public-access": "blob" });
    expect(calls[0].init?.body).toBe(aclBody(acl.identifiers));
    expect(aclBody([{ id: "a<b", start: "", expiry: "", permission: "" }])).toContain(
      "<Id>a&lt;b</Id>",
    );
  });
});
