// Shared helpers used across every Lightsail tab panel.
//
// Lightsail's exception shapes (services/lightsail/errors.go) all reduce to
// the same wire fields the AWS SDK exposes client-side -- `name`, `message`,
// `$metadata.httpStatusCode` -- so one generic renderer (matching every
// other restored page in this campaign) already surfaces exactly what this
// service returns.
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

// Lightsail's TagResource/UntagResource are name-first, not ARN-first
// (services/lightsail/tagging_vpc_misc.go's resolveTaggableLocked resolves
// purely off `resourceName` -- ResourceArn is accepted but never consulted).
// This is the reverse of every other service in this codebase, and it means
// panels never need to build an ARN client-side at all: just pass the
// resource's own name/id straight through.
export type LightsailTag = { key?: string; value?: string };

export function tagsToRecord(tags: LightsailTag[] | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  for (const t of tags ?? []) {
    if (t.key) out[t.key] = t.value ?? "";
  }
  return out;
}

// Four of Lightsail's 20 ResourceType kinds carry no `tags` field on the
// wire at all (services/lightsail/tagging_vpc_misc.go's
// tagsNotSupportedKinds): StaticIp, PeeredVpc, ExportSnapshotRecord,
// CloudFormationStackRecord. TagResource against one of these resolves the
// resource by name but honestly refuses -- so these panels never render tag
// UI in the first place, rather than offering a control that always errors.
export const TAGS_NOT_SUPPORTED = new Set([
  "StaticIp",
  "PeeredVpc",
  "ExportSnapshotRecord",
  "CloudFormationStackRecord",
]);

// Formats a Lightsail Operation's status for an inline badge -- see
// OperationsPanel and every detail modal's "Recent operations" section.
// OperationStatus has exactly 5 values (services/lightsail/consts.go):
// NotStarted, Started, Failed, Completed, Succeeded.
export function operationStatusClass(status: string | undefined): string {
  if (status === "Succeeded" || status === "Completed") {
    return "bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400";
  }
  if (status === "Failed") {
    return "bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400";
  }
  return "bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400";
}
