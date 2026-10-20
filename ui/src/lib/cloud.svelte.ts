import {
  type AzureService,
  type Cloud,
  type GcpService,
  availableAzureServices,
  availableGcpServices,
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
  /** Services disabled via --services / --disable-services. */
  disabled: string[];
  /** Port reservations by label from /dashboard/api/system/state. */
  ports: Record<string, number>;
  loaded: boolean;
};

let backend = $state<BackendInfo>({ services: null, disabled: [], ports: {}, loaded: false });

export function backendInfo(): BackendInfo {
  return backend;
}

/** Served Azure services; empty until the backend answers. */
export function servedAzureServices(): AzureService[] {
  return availableAzureServices(backend.services ?? []);
}

/** Served GCP services; empty until the backend answers. */
export function servedGcpServices(): GcpService[] {
  return availableGcpServices(backend.services ?? []);
}

export async function loadBackendInfo(fetchFn: typeof fetch = fetch): Promise<void> {
  let services: string[] | null = null;
  let disabled: string[] = [];
  const ports: Record<string, number> = {};
  try {
    const res = await fetchFn("/_gopherstack/health");
    if (res.ok) {
      const body = (await res.json()) as { services?: unknown; disabled?: unknown };
      if (Array.isArray(body.disabled)) {
        disabled = body.disabled.filter((s): s is string => typeof s === "string");
      }
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
      const body = (await res.json()) as {
        allocated?: { label?: string; port?: number }[];
      };
      for (const a of body.allocated ?? []) {
        if (typeof a.label === "string" && typeof a.port === "number") ports[a.label] = a.port;
      }
    }
  } catch {
    // ports stay unknown; the UI shows only what the backend reported
  }
  backend = { services, disabled, ports, loaded: true };
}

const routeKey = (s: string) => s.toLowerCase().replace(/[-_. ]/g, "");

/** True when the backend reports this dashboard route's service as disabled. */
export function isRouteDisabled(routeId: string): boolean {
  const id = routeKey(routeId);
  return backend.disabled.some((name) => routeKey(name) === id);
}
