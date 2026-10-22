import { beforeEach, describe, expect, it } from "vitest";

import { CLOUD_STORAGE_KEY } from "./cloud";
import {
  backendInfo,
  currentCloud,
  loadBackendInfo,
  servedAzureServices,
  setCloud,
  syncCloudFromUrl,
} from "./cloud.svelte";

function jsonResponse(body: unknown, ok = true): Response {
  return { ok, json: () => Promise.resolve(body) } as Response;
}

describe("cloud store", () => {
  beforeEach(() => {
    window.localStorage.clear();
    syncCloudFromUrl(null);
  });

  it("defaults to aws", () => {
    expect(currentCloud()).toBe("aws");
  });

  it("setCloud persists to localStorage", () => {
    setCloud("azure");
    expect(currentCloud()).toBe("azure");
    expect(window.localStorage.getItem(CLOUD_STORAGE_KEY)).toBe("azure");
  });

  it("URL param wins over storage and is persisted for param-less in-app links", () => {
    setCloud("gcp");
    syncCloudFromUrl("azure");
    expect(currentCloud()).toBe("azure");
    expect(window.localStorage.getItem(CLOUD_STORAGE_KEY)).toBe("azure");
    syncCloudFromUrl(null);
    expect(currentCloud()).toBe("azure");
  });

  it("ignores an invalid URL param", () => {
    setCloud("gcp");
    syncCloudFromUrl("nope");
    expect(currentCloud()).toBe("gcp");
    expect(window.localStorage.getItem(CLOUD_STORAGE_KEY)).toBe("gcp");
  });
});

describe("loadBackendInfo", () => {
  it("derives served Azure services and ports from the backend", async () => {
    const fetchFn = ((url: string) =>
      Promise.resolve(
        url.includes("health")
          ? jsonResponse({ services: ["S3", "AzureBlob", "CosmosDB"] })
          : jsonResponse({
              allocated: [{ label: "azureblob", port: 10000 }],
            }),
      )) as unknown as typeof fetch;
    await loadBackendInfo(fetchFn);
    expect(servedAzureServices().map((s) => s.id)).toEqual(["blob", "cosmosdb"]);
    expect(backendInfo().ports).toEqual({ azureblob: 10000 });
    expect(backendInfo().loaded).toBe(true);
  });

  it.each([
    [
      [
        { label: "gcpsql", port: 8130, fixed: true },
        { label: "gcpsql", port: 40009, fixed: false },
      ],
    ],
    [
      [
        { label: "gcpsql", port: 40009, fixed: false },
        { label: "gcpsql", port: 8130, fixed: true },
      ],
    ],
  ])("keeps the fixed port over dynamic leases of the same label", async (allocated) => {
    const fetchFn = ((url: string) =>
      Promise.resolve(
        url.includes("health")
          ? jsonResponse({ services: ["GCPSQL"] })
          : jsonResponse({ allocated }),
      )) as unknown as typeof fetch;
    await loadBackendInfo(fetchFn);
    expect(backendInfo().ports).toEqual({ gcpsql: 8130 });
  });

  it("reports unreachable backend as null services", async () => {
    const fetchFn = (() => Promise.reject(new Error("down"))) as unknown as typeof fetch;
    await loadBackendInfo(fetchFn);
    expect(backendInfo().services).toBeNull();
    expect(servedAzureServices()).toEqual([]);
  });
});
