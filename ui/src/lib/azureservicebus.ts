/** Client for the Azure Service Bus emulator via the dashboard's in-process proxy. */

export const BUS_PROXY_BASE = "/dashboard/api/v1/azure/servicebus";
export const MAX_LOCK_SECONDS = 5 * 60;
export const MAX_RECEIVE_BATCH = 10;
export const MAX_TTL_SECONDS = 14 * 24 * 60 * 60;

export type QueueInfo = {
  name: string;
  lockSeconds: number | null;
  ttlSeconds: number | null;
  maxDeliveryCount: number | null;
};
export type TopicInfo = { name: string; ttlSeconds: number | null };
export type SubscriptionInfo = {
  name: string;
  lockSeconds: number | null;
  maxDeliveryCount: number | null;
};

/** A message-bearing entity: a queue or a topic subscription, or either one's dead-letter queue. */
export type Target = { entity: string; subscription?: string; deadLetter?: boolean };

export type EntityConfig = {
  lockSeconds?: number;
  maxDeliveryCount?: number;
  ttlSeconds?: number;
};

export type SendOptions = {
  contentType?: string;
  messageId?: string;
  label?: string;
  correlationId?: string;
  replyTo?: string;
  sessionId?: string;
  ttlSeconds?: number;
};

export type ReceivedMessage = {
  messageId: string;
  lockToken: string;
  lockedUntil: string;
  enqueuedTime: string;
  sequenceNumber: number | null;
  deliveryCount: number | null;
  label: string;
  correlationId: string;
  replyTo: string;
  sessionId: string;
  contentType: string;
  body: string;
};

export class BusError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = "BusError";
    this.status = status;
    this.code = code;
  }
}

type FetchFn = typeof fetch;

const seg = (s: string) => encodeURIComponent(s);

export function entityUrl(entity: string, subscription?: string): string {
  let url = `${BUS_PROXY_BASE}/${seg(entity)}`;
  if (subscription) url += `/subscriptions/${seg(subscription)}`;
  return url;
}

export function messagesUrl(target: Target, tail = "", query = ""): string {
  let url = entityUrl(target.entity, target.subscription);
  if (target.deadLetter) url += "/$DeadLetterQueue";
  url += "/messages";
  if (tail) url += `/${tail}`;
  return query ? `${url}?${query}` : url;
}

function parseXml(body: string): Document {
  const doc = new DOMParser().parseFromString(body, "application/xml");
  if (doc.querySelector("parsererror")) {
    throw new BusError(0, "InvalidResponse", "Service Bus returned malformed XML");
  }
  return doc;
}

/** Parses an ISO 8601 duration (PnDTnHnMnS) into seconds, or null when absent or invalid. */
export function parseDuration(iso: string): number | null {
  const m = /^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$/.exec(iso.trim());
  if (!m || iso.trim() === "P" || iso.trim().endsWith("T")) return null;
  const [, d, h, min, s] = m;
  return Number(d ?? 0) * 86400 + Number(h ?? 0) * 3600 + Number(min ?? 0) * 60 + Number(s ?? 0);
}

export function formatDuration(seconds: number | null): string {
  if (seconds === null) return "—";
  if (seconds === 0) return "0s";
  const parts: string[] = [];
  let rest = Math.round(seconds);
  for (const [unit, size] of [
    ["d", 86400],
    ["h", 3600],
    ["m", 60],
    ["s", 1],
  ] as const) {
    const n = Math.floor(rest / size);
    if (n > 0) parts.push(`${n}${unit}`);
    rest -= n * size;
  }
  return parts.join(" ");
}

export function toIsoDuration(seconds: number): string {
  return `PT${Math.round(seconds)}S`;
}

function field(entry: Element, tag: string): string | null {
  const el = entry.querySelector(tag);
  return el?.textContent ?? null;
}

function numberField(entry: Element, tag: string): number | null {
  const v = field(entry, tag);
  return v === null || v.trim() === "" ? null : Number(v);
}

function durationField(entry: Element, tag: string): number | null {
  const v = field(entry, tag);
  return v === null ? null : parseDuration(v);
}

function entries(body: string): Element[] {
  const doc = parseXml(body);
  return Array.from(doc.querySelectorAll("entry"));
}

function titleOf(entry: Element): string {
  return Array.from(entry.children).find((e) => e.localName === "title")?.textContent ?? "";
}

export function parseQueues(body: string): QueueInfo[] {
  return entries(body).map((e) => ({
    name: titleOf(e),
    lockSeconds: durationField(e, "LockDuration"),
    ttlSeconds: durationField(e, "DefaultMessageTimeToLive"),
    maxDeliveryCount: numberField(e, "MaxDeliveryCount"),
  }));
}

export function parseTopics(body: string): TopicInfo[] {
  return entries(body).map((e) => ({
    name: titleOf(e),
    ttlSeconds: durationField(e, "DefaultMessageTimeToLive"),
  }));
}

export function parseSubscriptions(body: string): SubscriptionInfo[] {
  return entries(body).map((e) => ({
    name: titleOf(e),
    lockSeconds: durationField(e, "LockDuration"),
    maxDeliveryCount: numberField(e, "MaxDeliveryCount"),
  }));
}

async function toBusError(res: Response): Promise<BusError> {
  const body = (await res.text().catch(() => "")).trim();
  try {
    const json = JSON.parse(body) as { Code?: string; Message?: string };
    if (json.Code || json.Message) {
      return new BusError(
        res.status,
        json.Code ?? `HTTP${res.status}`,
        json.Message ?? json.Code ?? `HTTP ${res.status}`,
      );
    }
  } catch {
    // fall through to the raw body
  }
  return new BusError(res.status, `HTTP${res.status}`, body || `HTTP ${res.status}`);
}

async function send(url: string, init: RequestInit, fetchFn: FetchFn): Promise<Response> {
  let res: Response;
  try {
    res = await fetchFn(url, init);
  } catch (e) {
    throw new BusError(0, "NetworkError", e instanceof Error ? e.message : String(e));
  }
  if (!res.ok) throw await toBusError(res);
  return res;
}

export async function listQueues(fetchFn: FetchFn = fetch): Promise<QueueInfo[]> {
  const res = await send(`${BUS_PROXY_BASE}/$Resources/Queues`, { method: "GET" }, fetchFn);
  return parseQueues(await res.text());
}

export async function listTopics(fetchFn: FetchFn = fetch): Promise<TopicInfo[]> {
  const res = await send(`${BUS_PROXY_BASE}/$Resources/Topics`, { method: "GET" }, fetchFn);
  return parseTopics(await res.text());
}

export async function listSubscriptions(
  topic: string,
  fetchFn: FetchFn = fetch,
): Promise<SubscriptionInfo[]> {
  const res = await send(`${entityUrl(topic)}/subscriptions`, { method: "GET" }, fetchFn);
  return parseSubscriptions(await res.text());
}

type Kind = "Queue" | "Topic" | "Subscription";

export function descriptionBody(kind: Kind, cfg: EntityConfig): string {
  const parts: string[] = [];
  if (cfg.lockSeconds !== undefined && kind !== "Topic") {
    parts.push(`<LockDuration>${toIsoDuration(cfg.lockSeconds)}</LockDuration>`);
  }
  if (cfg.ttlSeconds !== undefined && kind !== "Subscription") {
    parts.push(
      `<DefaultMessageTimeToLive>${toIsoDuration(cfg.ttlSeconds)}</DefaultMessageTimeToLive>`,
    );
  }
  if (cfg.maxDeliveryCount !== undefined && kind !== "Topic") {
    parts.push(`<MaxDeliveryCount>${cfg.maxDeliveryCount}</MaxDeliveryCount>`);
  }
  return (
    `<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml">` +
    `<${kind}Description xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect">` +
    `${parts.join("")}</${kind}Description></content></entry>`
  );
}

async function createEntity(
  url: string,
  kind: Kind,
  cfg: EntityConfig,
  fetchFn: FetchFn,
): Promise<boolean> {
  const res = await send(
    url,
    {
      method: "PUT",
      headers: { "Content-Type": "application/atom+xml" },
      body: descriptionBody(kind, cfg),
    },
    fetchFn,
  );
  return res.status === 201;
}

/** Resolves true when created, false when the entity already existed. */
export function createQueue(
  name: string,
  cfg: EntityConfig = {},
  fetchFn: FetchFn = fetch,
): Promise<boolean> {
  return createEntity(entityUrl(name), "Queue", cfg, fetchFn);
}

export function createTopic(
  name: string,
  cfg: EntityConfig = {},
  fetchFn: FetchFn = fetch,
): Promise<boolean> {
  return createEntity(entityUrl(name), "Topic", cfg, fetchFn);
}

export function createSubscription(
  topic: string,
  name: string,
  cfg: EntityConfig = {},
  fetchFn: FetchFn = fetch,
): Promise<boolean> {
  return createEntity(entityUrl(topic, name), "Subscription", cfg, fetchFn);
}

/** Deletes a queue or topic. */
export async function deleteEntity(name: string, fetchFn: FetchFn = fetch): Promise<void> {
  await send(entityUrl(name), { method: "DELETE" }, fetchFn);
}

export async function deleteSubscription(
  topic: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await send(entityUrl(topic, name), { method: "DELETE" }, fetchFn);
}

export function brokerProperties(opts: SendOptions): Record<string, string | number> {
  const bp: Record<string, string | number> = {};
  if (opts.messageId) bp.MessageId = opts.messageId;
  if (opts.label) bp.Label = opts.label;
  if (opts.correlationId) bp.CorrelationId = opts.correlationId;
  if (opts.replyTo) bp.ReplyTo = opts.replyTo;
  if (opts.sessionId) bp.SessionId = opts.sessionId;
  if (opts.ttlSeconds !== undefined) bp.TimeToLive = opts.ttlSeconds;
  return bp;
}

/** Sends to a queue or a topic (topics fan out to every subscription). */
export async function sendMessage(
  entity: string,
  body: string,
  opts: SendOptions = {},
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const headers: Record<string, string> = {
    "Content-Type": opts.contentType || "text/plain",
  };
  const bp = brokerProperties(opts);
  if (Object.keys(bp).length > 0) headers["BrokerProperties"] = JSON.stringify(bp);
  await send(messagesUrl({ entity }), { method: "POST", headers, body }, fetchFn);
}

type Broker = {
  MessageId?: string;
  LockToken?: string;
  LockedUntilUtc?: string;
  EnqueuedTimeUtc?: string;
  SequenceNumber?: number;
  DeliveryCount?: number;
  Label?: string;
  CorrelationId?: string;
  ReplyTo?: string;
  SessionId?: string;
};

export function parseReceived(headers: Headers, body: string): ReceivedMessage {
  let bp: Broker = {};
  try {
    bp = JSON.parse(headers.get("BrokerProperties") ?? "{}") as Broker;
  } catch {
    bp = {};
  }
  return {
    messageId: bp.MessageId ?? "",
    lockToken: bp.LockToken ?? "",
    lockedUntil: bp.LockedUntilUtc ?? "",
    enqueuedTime: bp.EnqueuedTimeUtc ?? "",
    sequenceNumber: bp.SequenceNumber ?? null,
    deliveryCount: bp.DeliveryCount ?? null,
    label: bp.Label ?? "",
    correlationId: bp.CorrelationId ?? "",
    replyTo: bp.ReplyTo ?? "",
    sessionId: bp.SessionId ?? "",
    contentType: headers.get("Content-Type") ?? "",
    body,
  };
}

/** Peek-locks the oldest available message; resolves null when none is available. */
export async function receiveMessage(
  target: Target,
  fetchFn: FetchFn = fetch,
): Promise<ReceivedMessage | null> {
  const res = await send(messagesUrl(target, "head"), { method: "POST" }, fetchFn);
  if (res.status === 204) return null;
  return parseReceived(res.headers, await res.text());
}

/** Peek-locks up to count messages, stopping early once the entity is drained. */
export async function receiveMessages(
  target: Target,
  count: number,
  fetchFn: FetchFn = fetch,
): Promise<ReceivedMessage[]> {
  const out: ReceivedMessage[] = [];
  for (let i = 0; i < count; i++) {
    const m = await receiveMessage(target, fetchFn);
    if (!m) break;
    out.push(m);
  }
  return out;
}

function lockTail(m: ReceivedMessage): string {
  return `${seg(m.messageId)}/${seg(m.lockToken)}`;
}

export async function completeMessage(
  target: Target,
  m: ReceivedMessage,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await send(messagesUrl(target, lockTail(m)), { method: "DELETE" }, fetchFn);
}

export async function abandonMessage(
  target: Target,
  m: ReceivedMessage,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await send(messagesUrl(target, lockTail(m)), { method: "PUT" }, fetchFn);
}

/** Receive-and-delete: peek-lock then complete in one step. */
export async function receiveAndDelete(
  target: Target,
  fetchFn: FetchFn = fetch,
): Promise<ReceivedMessage | null> {
  const m = await receiveMessage(target, fetchFn);
  if (m) await completeMessage(target, m, fetchFn);
  return m;
}

/** Re-sends a dead-lettered queue message to its queue, then removes it from the DLQ. */
export async function resubmit(
  queue: string,
  m: ReceivedMessage,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await sendMessage(
    queue,
    m.body,
    {
      contentType: m.contentType,
      label: m.label,
      correlationId: m.correlationId,
      replyTo: m.replyTo,
      sessionId: m.sessionId,
    },
    fetchFn,
  );
  await completeMessage({ entity: queue, deadLetter: true }, m, fetchFn);
}

export function validateEntityName(name: string): string | null {
  if (!name) return "Name is required";
  if (name.length > 260) return "Must be at most 260 characters";
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(name)) {
    return "Use letters, numbers, '.', '_' and '-', starting with a letter or number";
  }
  return null;
}

/** Validates an optional whole-number field; returns null when blank or valid. */
export function validateWhole(raw: string, min: number, max: number): string | null {
  const v = raw.trim();
  if (v === "") return null;
  if (!/^\d+$/.test(v)) return "Must be a whole number";
  const n = Number(v);
  if (n < min || n > max) return `Must be between ${min} and ${max}`;
  return null;
}

/** Validates a message id: it becomes a URL path segment on complete/abandon. */
export function validateMessageId(id: string): string | null {
  if (id === "") return null;
  if (/[/\\?#%]/.test(id)) return "Must not contain / \\ ? # or %";
  return null;
}
