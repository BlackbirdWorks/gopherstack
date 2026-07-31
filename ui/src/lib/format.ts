// Shared formatting helpers for the AWS resource dashboards.
//
// `formatDate` follows the dominant convention found across the existing
// per-page `formatDate` implementations (redshift, glue, autoscaling, emr,
// cloudformation, …): `new Date(d).toLocaleString()`. Unlike those copies —
// which mostly fall back to the string `'-'` — nullish input here returns an
// em dash ('—') per the shared-component contract.

const EM_DASH = "—";

export function formatDate(d: string | Date | undefined | null): string {
  if (d === undefined || d === null || d === "") {
    return EM_DASH;
  }

  const date = d instanceof Date ? d : new Date(d);
  if (Number.isNaN(date.getTime())) {
    return EM_DASH;
  }

  return date.toLocaleString();
}

const BYTE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"];

export function formatBytes(bytes: number | undefined | null): string {
  if (bytes === undefined || bytes === null || Number.isNaN(bytes)) {
    return EM_DASH;
  }

  if (bytes < 1024) {
    return `${bytes} B`;
  }

  let value = bytes;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < BYTE_UNITS.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }

  return `${value.toFixed(1)} ${BYTE_UNITS[unitIndex]}`;
}

export function formatNumber(n: number | undefined | null): string {
  if (n === undefined || n === null || Number.isNaN(n)) {
    return EM_DASH;
  }

  return n.toLocaleString();
}
