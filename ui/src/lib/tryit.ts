/** Shared helpers for the API gateway "try it" panels (request headers in, proxy response out). */

import { str, type Json } from "./gcprest.ts";
import type { TryResponse } from "./ociapigateway.ts";

export type HeaderPair = { name: string; value: string };

/** Parses "Name: value" lines; blank lines are skipped and a line without a colon becomes an empty value. */
export function parseHeaderLines(text: string): HeaderPair[] {
  return text
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean)
    .map((l) => {
      const i = l.indexOf(":");
      return i < 0
        ? { name: l, value: "" }
        : { name: l.slice(0, i).trim(), value: l.slice(i + 1).trim() };
    });
}

/** Normalises the proxy's invoke answer. */
export function readTryResponse(b: Json): TryResponse {
  const headers: Record<string, string> = {};
  const raw = b.headers && typeof b.headers === "object" ? (b.headers as Json) : {};
  for (const [k, v] of Object.entries(raw)) headers[k] = String(v);
  return {
    status: typeof b.status === "number" ? b.status : Number(b.status) || 0,
    headers,
    body: str(b.body),
    encoding: str(b.encoding) || "utf8",
    truncated: b.truncated === true,
  };
}

/** Request path/query as the proxy expects them: no leading "?" on the query. */
export const cleanQuery = (q: string): string => q.trim().replace(/^\?/, "");
