import type { ClusterEndpoint } from "@aws-sdk/client-route53-recovery-control-config";
import type { Route53RecoveryClusterClient } from "@aws-sdk/client-route53-recovery-cluster";
import { getRoute53RecoveryClusterClient } from "#lib/aws-client.ts";

export function describeError(e: unknown): string {
  if (e && typeof e === "object") {
    const rec = e as { name?: unknown; message?: unknown; $metadata?: { httpStatusCode?: number } };
    const name = rec.name ? String(rec.name) : "Error";
    const message = rec.message ? String(rec.message) : String(e);
    const status = rec.$metadata?.httpStatusCode;
    return status ? `${name} (HTTP ${status}): ${message}` : `${name}: ${message}`;
  }
  return String(e);
}

export function isApiError(e: unknown): boolean {
  const status = (e as { $metadata?: { httpStatusCode?: number } } | null)?.$metadata
    ?.httpStatusCode;
  return typeof status === "number" && status > 0;
}

export function endpointPath(endpoint: string | undefined): string {
  if (!endpoint) {
    return "";
  }
  try {
    return new URL(endpoint).pathname.replace(/\/+$/, "");
  } catch {
    return endpoint.replace(/\/+$/, "");
  }
}

// Tries the preferred region's endpoint first, then the rest, moving on only when the
// request never reached the service; an API-level answer (e.g. ConflictException) is final.
export async function withClusterEndpoints<T>(
  endpoints: ClusterEndpoint[],
  preferredRegion: string,
  call: (client: Route53RecoveryClusterClient) => Promise<T>,
): Promise<T> {
  const usable = endpoints.filter((e) => e.Endpoint && e.Region);
  if (usable.length === 0) {
    throw new Error("Cluster has no endpoints");
  }
  const ordered = [
    ...usable.filter((e) => e.Region === preferredRegion),
    ...usable.filter((e) => e.Region !== preferredRegion),
  ];
  let last: unknown;
  for (const ep of ordered) {
    try {
      return await call(
        getRoute53RecoveryClusterClient(endpointPath(ep.Endpoint), ep.Region as string),
      );
    } catch (e) {
      if (isApiError(e)) {
        throw e;
      }
      last = e;
    }
  }
  throw last;
}

export function statusBadgeClass(status: string | undefined): string {
  switch (status) {
    case "DEPLOYED":
    case "READY":
    case "ACTIVE":
    case "ENABLED":
    case "On":
      return "bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400";
    case "PENDING":
    case "PENDING_DELETION":
    case "NOT_READY":
    case "SKIPPED":
    case "UNKNOWN":
      return "bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400";
    case "FAILED":
    case "CANCELED":
      return "bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400";
    default:
      return "bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400";
  }
}
