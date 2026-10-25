import { describe, expect, it, vi } from "vitest";

import {
  FILE_PROXY_BASE,
  FileError,
  deleteFile,
  fileDownloadUrl,
  filePath,
  listAccounts,
  listDirectory,
  listShares,
  parseDirectory,
  parseShares,
} from "./azurefile";

const sharesXml = `<?xml version="1.0"?><EnumerationResults><Shares>
<Share><Name>share1</Name><Properties><Quota>5</Quota><AccessTier>TransactionOptimized</AccessTier><Last-Modified>x</Last-Modified><Etag>"1"</Etag></Properties></Share>
<Share><Name>share1</Name><Snapshot>2026-01-01T00:00:00.0000000Z</Snapshot><Properties><Quota>5</Quota></Properties></Share>
</Shares><NextMarker/></EnumerationResults>`;

const dirXml = `<?xml version="1.0"?><EnumerationResults><Entries>
<Directory><Name>docs</Name></Directory>
<File><Name>a.txt</Name><Properties><Content-Length>5</Content-Length></Properties></File>
</Entries><NextMarker>m</NextMarker></EnumerationResults>`;

const xml = (body: string, status = 200) =>
  new Response(body, { status, headers: { "Content-Type": "application/xml" } });

function reply(f: (url: string) => Response) {
  return vi.fn((...a: unknown[]) => Promise.resolve(f(...(a as [string]))));
}

describe("parsing", () => {
  it("parses shares and their snapshots", () => {
    const shares = parseShares(sharesXml);
    expect(shares).toHaveLength(2);
    expect(shares[0]).toMatchObject({
      name: "share1",
      quotaGb: 5,
      accessTier: "TransactionOptimized",
      snapshot: "",
    });
    expect(shares[1]?.snapshot).toBe("2026-01-01T00:00:00.0000000Z");
  });

  it("parses directories and files", () => {
    const got = parseDirectory(dirXml);
    expect(got.entries).toEqual([
      { kind: "directory", name: "docs" },
      { kind: "file", name: "a.txt", size: 5 },
    ]);
    expect(got.nextMarker).toBe("m");
  });

  it("rejects unreadable XML", () => {
    expect(() => parseShares("<broken")).toThrow(FileError);
  });
});

describe("urls", () => {
  it("encodes each path segment but keeps the separators", () => {
    expect(filePath("acct", "share", "my dir/a b.txt")).toBe(
      `${FILE_PROXY_BASE}/acct/share/my%20dir/a%20b.txt`,
    );
    expect(fileDownloadUrl("acct", "share", "a.txt", "snap")).toBe(
      `${FILE_PROXY_BASE}/acct/share/a.txt?sharesnapshot=snap`,
    );
  });
});

describe("requests", () => {
  it("lists accounts, shares and directories", async () => {
    const fetchFn = reply((url: string) => {
      if (url.includes("fileaccounts"))
        return new Response(JSON.stringify({ accounts: ["devstoreaccount1"] }));
      if (url.includes("comp=list&include=snapshots")) return xml(sharesXml);
      return xml(dirXml);
    }) as unknown as typeof fetch;
    expect(await listAccounts(fetchFn)).toEqual(["devstoreaccount1"]);
    expect(await listShares("acct", true, fetchFn)).toHaveLength(2);
    const dir = await listDirectory("acct", "share", "docs", { snapshot: "s" }, fetchFn);
    expect(dir.entries).toHaveLength(2);
    expect(String((fetchFn as unknown as ReturnType<typeof vi.fn>).mock.calls[2]?.[0])).toContain(
      "sharesnapshot=s",
    );
  });

  it("maps XML errors to FileError", async () => {
    const fetchFn = reply(() =>
      xml("<Error><Code>ShareNotFound</Code><Message>nope</Message></Error>", 404),
    );
    await expect(
      deleteFile("a", "s", "f", fetchFn as unknown as typeof fetch),
    ).rejects.toMatchObject({
      code: "ShareNotFound",
      httpStatus: 404,
    });
  });

  it("reports an unavailable service", async () => {
    const fetchFn = reply(
      () =>
        new Response(JSON.stringify({ error: { message: "Azure Files is not enabled" } }), {
          status: 503,
        }),
    );
    await expect(listAccounts(fetchFn as unknown as typeof fetch)).rejects.toMatchObject({
      httpStatus: 503,
    });
  });
});
