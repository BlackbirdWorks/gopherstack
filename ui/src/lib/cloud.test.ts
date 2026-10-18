import { describe, expect, it } from "vitest";

import {
  AZURE_SERVICES,
  CLOUD_STORAGE_KEY,
  availableAzureServices,
  availableGcpServices,
  categoriesForCloud,
  parseCloud,
  readStoredCloud,
  resolveCloud,
  withCloud,
  writeStoredCloud,
} from "./cloud";
import { sidebarCategories } from "./nav";

describe("parseCloud", () => {
  it.each([
    ["aws", "aws"],
    ["Azure", "azure"],
    [" GCP ", "gcp"],
    ["oracle", null],
    ["", null],
    [null, null],
    [undefined, null],
  ])("parses %j", (input, want) => {
    expect(parseCloud(input)).toBe(want);
  });
});

describe("resolveCloud", () => {
  it.each([
    [null, null, "aws"],
    ["azure", null, "azure"],
    [null, "gcp", "gcp"],
    ["azure", "gcp", "azure"],
    ["bogus", "gcp", "gcp"],
    ["bogus", "nope", "aws"],
  ])("url=%j stored=%j", (url, stored, want) => {
    expect(resolveCloud(url, stored)).toBe(want);
  });
});

describe("storage", () => {
  it("round-trips through a storage object", () => {
    const store = new Map<string, string>();
    const storage = {
      getItem: (k: string) => store.get(k) ?? null,
      setItem: (k: string, v: string) => void store.set(k, v),
    };
    writeStoredCloud("azure", storage);
    expect(store.get(CLOUD_STORAGE_KEY)).toBe("azure");
    expect(readStoredCloud(storage)).toBe("azure");
  });

  it("swallows storage errors", () => {
    const throwing = {
      getItem: () => {
        throw new Error("blocked");
      },
      setItem: () => {
        throw new Error("blocked");
      },
    };
    expect(readStoredCloud(throwing)).toBeNull();
    expect(() => writeStoredCloud("gcp", throwing)).not.toThrow();
  });
});

describe("availableAzureServices", () => {
  it("lists only services the backend reports, case-insensitively", () => {
    const got = availableAzureServices(["S3", "azureblob", "CosmosDB", "AzureQueue"]);
    expect(got.map((s) => s.id)).toEqual(["blob", "queue", "cosmosdb"]);
  });

  it("is empty when no Azure service is served", () => {
    expect(availableAzureServices(["S3", "DynamoDB"])).toEqual([]);
  });

  it("has unique ids and ports", () => {
    expect(new Set(AZURE_SERVICES.map((s) => s.id)).size).toBe(AZURE_SERVICES.length);
    expect(new Set(AZURE_SERVICES.map((s) => s.defaultPort)).size).toBe(AZURE_SERVICES.length);
  });
});

describe("categoriesForCloud", () => {
  const azure = availableAzureServices(["AzureBlob", "AzureTable"]);

  it("passes AWS categories through unchanged", () => {
    expect(categoriesForCloud("aws", sidebarCategories, azure)).toBe(sidebarCategories);
  });

  it("builds an Azure category linking to per-service routes", () => {
    const cats = categoriesForCloud("azure", sidebarCategories, azure);
    expect(cats).toHaveLength(1);
    expect(cats[0]?.routes.map((r) => r.href)).toEqual([
      "/dashboard/azure/blob",
      "/dashboard/azure/table",
    ]);
  });

  it("is empty for Azure with nothing served and for GCP", () => {
    expect(categoriesForCloud("azure", sidebarCategories, [])).toEqual([]);
    expect(categoriesForCloud("gcp", sidebarCategories, azure)).toEqual([]);
  });

  it("lists served GCP services under the GCP cloud", () => {
    const cats = categoriesForCloud(
      "gcp",
      sidebarCategories,
      azure,
      availableGcpServices(["GCPStorage"]),
    );
    expect(cats[0]?.routes.map((r) => r.href)).toEqual(["/dashboard/gcp/storage"]);
    expect(availableGcpServices(["S3"])).toEqual([]);
  });
});

describe("withCloud", () => {
  it("sets the param, preserving path and other params", () => {
    const url = new URL("http://x/dashboard/s3?tab=a#h");
    expect(withCloud(url, "azure")).toBe("/dashboard/s3?tab=a&cloud=azure#h");
  });

  it("removes the param for the default cloud", () => {
    const url = new URL("http://x/dashboard?cloud=gcp");
    expect(withCloud(url, "aws")).toBe("/dashboard");
  });
});
