// Shared helpers used across every Network Manager tab panel.
//
// services/networkmanager's wire convention uses PascalCase field names
// throughout (Tag.Key/Tag.Value, GlobalNetwork.GlobalNetworkId, ...) -- this
// is a real, confirmed difference from mgn/directconnect's camelCase client
// shapes, not a typo: NetworkManager's SDK types genuinely use PascalCase
// (see @aws-sdk/client-networkmanager/dist-types/models/models_0.d.ts).
//
// Every op's exception shape reduces to the same wire fields the AWS SDK
// exposes client-side -- `name`, `message`, `$metadata.httpStatusCode` -- so
// one generic renderer (matching every other restored page in this
// campaign) already surfaces exactly what this service returns.
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

// services/networkmanager/tagging.go's resourceKindFromARN dispatches purely
// off the resource-kind segment right after the ARN's last ":", and
// lastARNSegment (used by the backend's Get/reverse lookups) always takes
// the FINAL "/"-separated segment as the resource's own ID -- so a
// placeholder account ID and partition are safe here exactly as every other
// restored service's own taggableArn documents.
export const PLACEHOLDER_ACCOUNT_ID = "000000000000";

// All 9 taggable NetworkManager resource kinds (services/networkmanager/
// tagging.go's tagResolvers, PARITY.md's ARN table). Every kind is a GLOBAL
// ARN -- no region segment at all (double colon after the partition) --
// confirmed directly from AWS's own IAM Service Authorization Reference page
// for this service. Four kinds (site/device/link/connection) nest under
// their owning GlobalNetworkId in the resource path; the backend's
// lastARNSegment only ever reads the final segment, so this nesting is
// purely cosmetic/documentation-accurate, not load-bearing for lookups.
export type NetworkManagerTaggableKind =
  | "global-network"
  | "site"
  | "device"
  | "link"
  | "connection"
  | "connect-peer"
  | "core-network"
  | "peering"
  | "attachment";

const GLOBAL_NETWORK_SCOPED_KINDS: ReadonlySet<NetworkManagerTaggableKind> = new Set([
  "site",
  "device",
  "link",
  "connection",
]);

export function taggableArn(
  kind: NetworkManagerTaggableKind,
  id: string,
  globalNetworkId?: string,
): string {
  const resourcePath =
    GLOBAL_NETWORK_SCOPED_KINDS.has(kind) && globalNetworkId
      ? `${kind}/${globalNetworkId}/${id}`
      : `${kind}/${id}`;
  return `arn:aws:networkmanager::${PLACEHOLDER_ACCOUNT_ID}:${resourcePath}`;
}
