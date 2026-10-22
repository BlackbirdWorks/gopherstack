import { restCall, str, type FetchFn, type Json } from "./gcprest";

export const LOGGING_PROXY_BASE = "/dashboard/api/v1/gcp/logging";
export const LOGGING_PROJECT_KEY = "gopherstack_logging_project";
export const MAX_PAGE_SIZE = 200;

export type LogEntry = {
  insertId: string;
  timestamp: string;
  severity: string;
  logName: string;
  resourceType: string;
  message: string;
};

export type LogSink = {
  name: string;
  destination: string;
  filter: string;
  disabled: boolean;
  writerIdentity: string;
};

export const SEVERITIES = [
  "DEFAULT",
  "DEBUG",
  "INFO",
  "NOTICE",
  "WARNING",
  "ERROR",
  "CRITICAL",
  "ALERT",
  "EMERGENCY",
] as const;

export function entryMessage(raw: Json): string {
  if (typeof raw.textPayload === "string") return raw.textPayload;
  if (raw.jsonPayload) return JSON.stringify(raw.jsonPayload);
  if (raw.protoPayload) return JSON.stringify(raw.protoPayload);
  return "";
}

export function parseEntry(raw: Json): LogEntry {
  const resource = (raw.resource ?? {}) as Json;
  return {
    insertId: str(raw.insertId),
    timestamp: str(raw.timestamp),
    severity: str(raw.severity) || "DEFAULT",
    logName: str(raw.logName).split("/logs/").pop() ?? "",
    resourceType: str(resource.type),
    message: entryMessage(raw),
  };
}

export function parseSink(raw: Json): LogSink {
  return {
    name: str(raw.name),
    destination: str(raw.destination),
    filter: str(raw.filter),
    disabled: raw.disabled === true,
    writerIdentity: str(raw.writerIdentity),
  };
}

export async function listEntries(
  project: string,
  filter: string,
  pageSize = 100,
  fetchFn?: FetchFn,
): Promise<LogEntry[]> {
  const body = await restCall(
    LOGGING_PROXY_BASE,
    "POST",
    "/v1/entries:list",
    {
      resourceNames: [`projects/${project}`],
      filter,
      orderBy: "timestamp desc",
      pageSize: Math.min(pageSize, MAX_PAGE_SIZE),
    },
    fetchFn,
  );
  return Array.isArray(body.entries) ? (body.entries as Json[]).map((x) => parseEntry(x)) : [];
}

export async function listSinks(project: string, fetchFn?: FetchFn): Promise<LogSink[]> {
  const body = await restCall(
    LOGGING_PROXY_BASE,
    "GET",
    `/v1/projects/${encodeURIComponent(project)}/sinks`,
    undefined,
    fetchFn,
  );
  return Array.isArray(body.sinks) ? (body.sinks as Json[]).map((x) => parseSink(x)) : [];
}

export async function writeEntry(
  project: string,
  logId: string,
  severity: string,
  text: string,
  fetchFn?: FetchFn,
): Promise<void> {
  await restCall(
    LOGGING_PROXY_BASE,
    "POST",
    "/v1/entries:write",
    {
      logName: `projects/${project}/logs/${logId}`,
      resource: { type: "global" },
      entries: [{ severity, textPayload: text }],
    },
    fetchFn,
  );
}

export function validateLogId(id: string): string {
  if (!id) return "Log ID is required";
  return /^[A-Za-z0-9._-]{1,100}$/.test(id) ? "" : "Letters, digits, . _ - (max 100)";
}

/** Tailwind classes for a severity badge. */
export function severityTone(severity: string): string {
  if (["ERROR", "CRITICAL", "ALERT", "EMERGENCY"].includes(severity)) {
    return "bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300";
  }
  if (severity === "WARNING")
    return "bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300";
  if (severity === "INFO" || severity === "NOTICE") {
    return "bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300";
  }
  return "bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-300";
}
