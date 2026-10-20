/** Client for the Azure Queue emulator via the dashboard's in-process proxy. */

export const QUEUE_PROXY_BASE = "/dashboard/api/v1/azure/queue";
export const DEFAULT_ACCOUNT = "devstoreaccount1";
export const MAX_MESSAGES = 32;
export const MAX_VISIBILITY_SECONDS = 7 * 24 * 60 * 60;
export const DEFAULT_RECEIVE_VISIBILITY = 30;

export type QueueInfo = { name: string };

export type QueueMessage = {
  id: string;
  insertionTime: string;
  expirationTime: string;
  popReceipt: string;
  timeNextVisible: string;
  dequeueCount: number | null;
  text: string;
};

export type SendOptions = {
  /** Seconds the message stays invisible after being enqueued. */
  visibilityTimeout?: number;
  /** Seconds to live; -1 means never expire. */
  ttl?: number;
};

export class QueueError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = "QueueError";
    this.status = status;
    this.code = code;
  }
}

type FetchFn = typeof fetch;

export function queueUrl(account: string, queue?: string, messageId?: string, query = ""): string {
  let url = `${QUEUE_PROXY_BASE}/${encodeURIComponent(account)}`;
  if (queue) {
    url += `/${encodeURIComponent(queue)}`;
    if (messageId !== undefined) {
      url += "/messages";
      if (messageId) url += `/${encodeURIComponent(messageId)}`;
    }
  }
  return query ? `${url}?${query}` : url;
}

function text(parent: Element, tag: string): string {
  return parent.querySelector(tag)?.textContent ?? "";
}

function parseXml(body: string): Document {
  const doc = new DOMParser().parseFromString(body, "application/xml");
  if (doc.querySelector("parsererror")) {
    throw new QueueError(0, "InvalidResponse", "Queue service returned malformed XML");
  }
  return doc;
}

export function parseQueueList(body: string): QueueInfo[] {
  const doc = parseXml(body);
  return Array.from(doc.querySelectorAll("Queue")).map((q) => ({
    name: text(q, "Name"),
  }));
}

export function parseMessages(body: string): QueueMessage[] {
  const doc = parseXml(body);
  return Array.from(doc.querySelectorAll("QueueMessage")).map((m) => {
    const count = text(m, "DequeueCount");
    return {
      id: text(m, "MessageId"),
      insertionTime: text(m, "InsertionTime"),
      expirationTime: text(m, "ExpirationTime"),
      popReceipt: text(m, "PopReceipt"),
      timeNextVisible: text(m, "TimeNextVisible"),
      dequeueCount: count === "" ? null : Number(count),
      text: text(m, "MessageText"),
    };
  });
}

async function toQueueError(res: Response): Promise<QueueError> {
  const body = await res.text().catch(() => "");
  if (body.trim().startsWith("<")) {
    try {
      const doc = parseXml(body);
      const code = doc.querySelector("Code")?.textContent;
      const message = doc.querySelector("Message")?.textContent;
      if (code || message) {
        return new QueueError(
          res.status,
          code ?? `HTTP${res.status}`,
          message ?? code ?? `HTTP ${res.status}`,
        );
      }
    } catch {
      // fall through to the raw body
    }
  }
  return new QueueError(res.status, `HTTP${res.status}`, body.trim() || `HTTP ${res.status}`);
}

async function send(url: string, init: RequestInit, fetchFn: FetchFn): Promise<Response> {
  let res: Response;
  try {
    res = await fetchFn(url, init);
  } catch (e) {
    throw new QueueError(0, "NetworkError", e instanceof Error ? e.message : String(e));
  }
  if (!res.ok) throw await toQueueError(res);
  return res;
}

export async function listQueues(account: string, fetchFn: FetchFn = fetch): Promise<QueueInfo[]> {
  const res = await send(
    queueUrl(account, undefined, undefined, "comp=list"),
    { method: "GET" },
    fetchFn,
  );
  return parseQueueList(await res.text());
}

/** Resolves true when the queue was created, false when it already existed. */
export async function createQueue(
  account: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<boolean> {
  const res = await send(queueUrl(account, name), { method: "PUT" }, fetchFn);
  return res.status === 201;
}

export async function deleteQueue(
  account: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await send(queueUrl(account, name), { method: "DELETE" }, fetchFn);
}

export function escapeXml(s: string): string {
  return s
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
}

export function sendQuery(opts: SendOptions): string {
  const params = new URLSearchParams();
  if (opts.visibilityTimeout !== undefined) {
    params.set("visibilitytimeout", String(opts.visibilityTimeout));
  }
  if (opts.ttl !== undefined) params.set("messagettl", String(opts.ttl));
  return params.toString();
}

export async function putMessage(
  account: string,
  queue: string,
  body: string,
  opts: SendOptions = {},
  fetchFn: FetchFn = fetch,
): Promise<QueueMessage> {
  const res = await send(
    queueUrl(account, queue, "", sendQuery(opts)),
    {
      method: "POST",
      headers: { "Content-Type": "application/xml" },
      body: `<QueueMessage><MessageText>${escapeXml(body)}</MessageText></QueueMessage>`,
    },
    fetchFn,
  );
  return parseMessages(await res.text())[0];
}

export async function peekMessages(
  account: string,
  queue: string,
  count = MAX_MESSAGES,
  fetchFn: FetchFn = fetch,
): Promise<QueueMessage[]> {
  const res = await send(
    queueUrl(account, queue, "", `peekonly=true&numofmessages=${count}`),
    { method: "GET" },
    fetchFn,
  );
  return parseMessages(await res.text());
}

export async function receiveMessages(
  account: string,
  queue: string,
  count = 1,
  visibilityTimeout = DEFAULT_RECEIVE_VISIBILITY,
  fetchFn: FetchFn = fetch,
): Promise<QueueMessage[]> {
  const res = await send(
    queueUrl(account, queue, "", `numofmessages=${count}&visibilitytimeout=${visibilityTimeout}`),
    { method: "GET" },
    fetchFn,
  );
  return parseMessages(await res.text());
}

export async function deleteMessage(
  account: string,
  queue: string,
  id: string,
  popReceipt: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await send(
    queueUrl(account, queue, id, `popreceipt=${encodeURIComponent(popReceipt)}`),
    { method: "DELETE" },
    fetchFn,
  );
}

export async function clearMessages(
  account: string,
  queue: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await send(queueUrl(account, queue, ""), { method: "DELETE" }, fetchFn);
}

export type QueueProperties = { approximateMessages: number; metadata: Record<string, string> };

export type UpdatedMessage = { popReceipt: string; timeNextVisible: string };

export function parseQueueProperties(headers: Headers): QueueProperties {
  const metadata: Record<string, string> = {};
  headers.forEach((value, key) => {
    if (key.toLowerCase().startsWith("x-ms-meta-"))
      metadata[key.slice("x-ms-meta-".length)] = value;
  });
  return {
    approximateMessages: Number(headers.get("x-ms-approximate-messages-count") ?? "0"),
    metadata,
  };
}

export async function getQueueProperties(
  account: string,
  queue: string,
  fetchFn: FetchFn = fetch,
): Promise<QueueProperties> {
  const res = await send(
    queueUrl(account, queue, undefined, "comp=metadata"),
    { method: "GET" },
    fetchFn,
  );
  return parseQueueProperties(res.headers);
}

/** Update Message: sets a new visibility timeout, optionally replacing the text; rotates the pop receipt. */
export async function updateMessage(
  account: string,
  queue: string,
  id: string,
  popReceipt: string,
  visibilityTimeout: number,
  newText?: string,
  fetchFn: FetchFn = fetch,
): Promise<UpdatedMessage> {
  const query = `popreceipt=${encodeURIComponent(popReceipt)}&visibilitytimeout=${visibilityTimeout}`;
  const init: RequestInit = { method: "PUT" };
  if (newText !== undefined) {
    init.headers = { "Content-Type": "application/xml" };
    init.body = `<QueueMessage><MessageText>${escapeXml(newText)}</MessageText></QueueMessage>`;
  }
  const res = await send(queueUrl(account, queue, id, query), init, fetchFn);
  return {
    popReceipt: res.headers.get("x-ms-popreceipt") ?? "",
    timeNextVisible: res.headers.get("x-ms-time-next-visible") ?? "",
  };
}

export function validateQueueName(name: string): string | null {
  if (!name) return "Name is required";
  if (name.length < 3 || name.length > 63) return "Must be 3-63 characters";
  if (!/^[a-z0-9-]+$/.test(name)) return "Use lowercase letters, numbers and hyphens only";
  if (!/^[a-z0-9]/.test(name) || name.endsWith("-"))
    return "Must start and end with a letter or number";
  if (name.includes("--")) return "Consecutive hyphens are not allowed";
  return null;
}

/** Validates a whole-number seconds field; returns null when valid. */
export function validateSeconds(
  raw: string,
  min: number,
  max: number,
  allowNeverExpire = false,
): string | null {
  const v = raw.trim();
  if (v === "") return null;
  if (!/^-?\d+$/.test(v)) return "Must be a whole number of seconds";
  const n = Number(v);
  if (allowNeverExpire && n === -1) return null;
  if (n < min || n > max) return `Must be between ${min} and ${max}`;
  return null;
}

export function toBase64(s: string): string {
  const bytes = new TextEncoder().encode(s);
  let bin = "";
  for (const b of bytes) bin += String.fromCodePoint(b);
  return btoa(bin);
}

/** Decodes standard base64 as UTF-8, or null when the text is not base64 text. */
export function fromBase64(s: string): string | null {
  const t = s.trim();
  if (t === "" || t.length % 4 !== 0 || !/^[A-Za-z0-9+/]+={0,2}$/.test(t)) return null;
  try {
    const bin = atob(t);
    const bytes = Uint8Array.from(bin, (c) => c.codePointAt(0) ?? 0);
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    return null;
  }
}
