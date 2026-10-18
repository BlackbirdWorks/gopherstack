import {
  type AzureService,
  type Cloud,
  availableAzureServices,
  readStoredCloud,
  resolveCloud,
  writeStoredCloud,
} from "./cloud";

let cloud = $state<Cloud>(resolveCloud(null, readStoredCloud()));

export function currentCloud(): Cloud {
  return cloud;
}

/** Adopts the URL's ?cloud= (falling back to storage) without writing storage. */
export function syncCloudFromUrl(urlValue: string | null): void {
  cloud = resolveCloud(urlValue, readStoredCloud());
}

export function setCloud(next: Cloud): void {
  cloud = next;
  writeStoredCloud(next);
}

export type BackendInfo = {
  /** Names reported by /_gopherstack/health; null until loaded or if unreachable. */
  services: string[] | null;
  /** Port reservations by label from /dashboard/api/system/state. */
  ports: Record<string, number>;
  loaded: boolean;
};

let backend = $state<BackendInfo>({ services: null, ports: {}, loaded: false });

export function backendInfo(): BackendInfo {
  return backend;
}

/** Served Azure services; empty until the backend answers. */
export function servedAzureServices(): AzureService[] {
  return availableAzureServices(backend.services ?? []);
}

export async function loadBackendInfo(fetchFn: typeof fetch = fetch): Promise<void> {
  let services: string[] | null = null;
  const ports: Record<string, number> = {};
  try {
    const res = await fetchFn("/_gopherstack/health");
    if (res.ok) {
      const body = (await res.json()) as { services?: unknown };
      if (Array.isArray(body.services)) {
        services = body.services.filter((s): s is string => typeof s === "string");
      }
    }
  } catch {
    services = null;
  }
  try {
    const res = await fetchFn("/dashboard/api/system/state");
    if (res.ok) {
      const body = (await res.json()) as { allocated?: { label?: string; port?: number }[] };
      for (const a of body.allocated ?? []) {
        if (typeof a.label === "string" && typeof a.port === "number") ports[a.label] = a.port;
      }
    }
  } catch {
    // ports stay unknown; the UI shows only what the backend reported
  }
  backend = { services, ports, loaded: true };
}
