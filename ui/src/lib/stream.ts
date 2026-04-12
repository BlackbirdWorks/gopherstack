export function parseSSEData(raw: string): unknown | null {
  const trimmed = raw.trim();
  if (trimmed === "") {
    return null;
  }

  try {
    return JSON.parse(trimmed);
  } catch {
    return null;
  }
}

export function toSummaryMetricCount(snapshot: unknown): number {
  if (!snapshot || typeof snapshot !== "object") {
    return 0;
  }

  const record = snapshot as Record<string, unknown>;
  return Object.keys(record).length;
}

export type ConsoleEvent = {
  method: string;
  path: string;
  statusCode: number;
  service: string;
  timestamp: string;
};

export function toConsoleEvent(payload: unknown): ConsoleEvent | null {
  if (!payload || typeof payload !== "object") {
    return null;
  }

  const record = payload as Record<string, unknown>;
  const method = typeof record.Method === "string" ? record.Method : "";
  const path = typeof record.Path === "string" ? record.Path : "";
  const service = typeof record.Service === "string" ? record.Service : "";
  const timestamp = typeof record.Timestamp === "string" ? record.Timestamp : "";
  const statusCode = typeof record.StatusCode === "number" ? record.StatusCode : 0;

  if (method === "" || path === "") {
    return null;
  }

  return { method, path, statusCode, service, timestamp };
}
