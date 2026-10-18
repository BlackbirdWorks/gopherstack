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

  it("URL param wins over storage and does not overwrite it", () => {
    setCloud("gcp");
    syncCloudFromUrl("azure");
    expect(currentCloud()).toBe("azure");
    expect(window.localStorage.getItem(CLOUD_STORAGE_KEY)).toBe("gcp");
    syncCloudFromUrl(null);
    expect(currentCloud()).toBe("gcp");
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

  it("reports unreachable backend as null services", async () => {
    const fetchFn = (() => Promise.reject(new Error("down"))) as unknown as typeof fetch;
    await loadBackendInfo(fetchFn);
    expect(backendInfo().services).toBeNull();
    expect(servedAzureServices()).toEqual([]);
  });
});
