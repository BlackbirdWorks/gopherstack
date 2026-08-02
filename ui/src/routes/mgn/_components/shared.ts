// Shared helpers used across every MGN tab panel.
//
// MGN has two structurally distinct error generations (services/mgn/PARITY.md):
// the 69 "legacy" ops (source servers, jobs, applications, waves, templates,
// connectors, vCenter clients, export/import, actions, service init) draw
// from {AccessDenied, Conflict, ResourceNotFound, ServiceQuotaExceeded,
// UninitializedAccount, Validation} and NEVER InternalServer/Throttling; the
// tagging trio plus all 25 /network-migration/ ops draw from {AccessDenied,
// Conflict, InternalServer (tagging only), ResourceNotFound,
// ServiceQuotaExceeded, Throttling, Validation} and NEVER
// UninitializedAccount. Both generations still reduce to the same wire shape
// the AWS SDK exposes client-side -- a `name` (the exception class),
// `message`, and `$metadata.httpStatusCode` -- so one generic renderer
// (matching directconnect/resiliencehub's own `describeError`) already
// surfaces exactly what each generation returns; there is no richer
// per-op error detail this UI needs to special-case.
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

export function rethrowDescribed(e: unknown): never {
  throw new Error(describeError(e));
}

// services/mgn/store.go's ARN builders key every one of the 12 taggable
// resource kinds off a "arn:...:<kind>/<id>" suffix, matched by
// resourceIDFromARN via strings.LastIndex on the "<kind>/" marker -- the
// backend never validates the partition/account segments, only that the
// marker substring is present (same lenient match directconnect's own
// taggableArn relies on). A fixed placeholder account ID is therefore safe
// here, exactly as directconnect/+page.svelte documents.
export const PLACEHOLDER_ACCOUNT_ID = "000000000000";

// The 12 taggable resource kinds this service's tagging trio (TagResource/
// UntagResource/ListTagsForResource) can address, per services/mgn/tagging.go's
// resolveTaggableLocked/resolveTaggableGroup2Locked/resolveTaggableGroup3Locked.
export type MgnTaggableKind =
  | "source-server"
  | "job"
  | "application"
  | "wave"
  | "connector"
  | "vcenter-client"
  | "launch-configuration-template"
  | "replication-configuration-template"
  | "export-task"
  | "import-task"
  | "network-migration-definition"
  | "network-migration-execution";

export function taggableArn(kind: MgnTaggableKind, id: string, region: string): string {
  return `arn:aws:mgn:${region}:${PLACEHOLDER_ACCOUNT_ID}:${kind}/${id}`;
}
