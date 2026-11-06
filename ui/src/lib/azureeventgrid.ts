import { restCall, str, type FetchFn, type Json } from "./gcprest";

export const EG_PROXY_BASE = "/dashboard/api/v1/azure/eventgrid";
export const SCHEMA_CLOUDEVENT = "CloudEventSchemaV1_0";
export const MAX_DATA_BYTES = 64 << 10;
export const MAX_SUBJECT_LEN = 256;
export const MAX_TYPE_LEN = 128;

export type EgSubscription = {
  name: string;
  endpointType: string;
  state: string;
  deliverySchema: string;
  subjectBeginsWith: string;
  subjectEndsWith: string;
  includedEventTypes: string[];
  advancedFilterCount: number;
  maxDeliveryAttempts: number;
  eventTimeToLiveMinutes: number;
  deadLetterType: string;
  deadLetterContainer: string;
  queueLockSeconds: number;
  queueMaxDelivery: number;
  queueEventTimeToLive: string;
};
export type EgTopic = {
  name: string;
  inputSchema: string;
  subscriptions: EgSubscription[];
};
export type EgChild = { name: string; subscriptions: EgSubscription[] };
export type EgDomain = {
  name: string;
  inputSchema: string;
  autoCreateTopics: boolean;
  autoDeleteTopics: boolean;
  subscriptions: EgSubscription[];
  topics: EgChild[];
};
export type EgNamespace = { name: string; topics: EgChild[] };
export type EgScope = { id: string; subscriptions: EgSubscription[] };
export type EgOverview = {
  topics: EgTopic[];
  domains: EgDomain[];
  namespaces: EgNamespace[];
  scopes: EgScope[];
};
export type EgKeys = { key1: string; key2: string };
export type EgKind = "topics" | "domains" | "namespaces";
export type TestEvent = {
  subject: string;
  eventType: string;
  data: string;
  domainTopic: string;
};
export type PublishTarget =
  | { kind: "topic"; topic: string }
  | { kind: "domain"; domain: string }
  | { kind: "namespace"; namespace: string; topic: string };

const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const num = (v: unknown) => (typeof v === "number" ? v : 0);
const strs = (v: unknown) =>
  Array.isArray(v) ? v.filter((x): x is string => typeof x === "string") : [];
const seg = (s: string) => encodeURIComponent(s);

export function parseSubscription(raw: Json): EgSubscription {
  return {
    name: str(raw.name),
    endpointType: str(raw.endpointType),
    state: str(raw.state),
    deliverySchema: str(raw.deliverySchema),
    subjectBeginsWith: str(raw.subjectBeginsWith),
    subjectEndsWith: str(raw.subjectEndsWith),
    includedEventTypes: strs(raw.includedEventTypes),
    advancedFilterCount: num(raw.advancedFilterCount),
    maxDeliveryAttempts: num(raw.maxDeliveryAttempts),
    eventTimeToLiveMinutes: num(raw.eventTimeToLiveMinutes),
    deadLetterType: str(raw.deadLetterType),
    deadLetterContainer: str(raw.deadLetterContainer),
    queueLockSeconds: num(raw.queueLockSeconds),
    queueMaxDelivery: num(raw.queueMaxDelivery),
    queueEventTimeToLive: str(raw.queueEventTimeToLive),
  };
}

const subs = (v: unknown) => list(v).map((x) => parseSubscription(x));
const child = (c: Json): EgChild => ({
  name: str(c.name),
  subscriptions: subs(c.subscriptions),
});

export async function listTopics(fetchFn?: FetchFn): Promise<EgTopic[]> {
  const body = await restCall(EG_PROXY_BASE, "GET", "/topics", undefined, fetchFn);
  return list(body.topics).map((t) => ({
    name: str(t.name),
    inputSchema: str(t.inputSchema),
    subscriptions: subs(t.subscriptions),
  }));
}

export async function getOverview(fetchFn?: FetchFn): Promise<EgOverview> {
  const body = await restCall(EG_PROXY_BASE, "GET", "/overview", undefined, fetchFn);
  return {
    topics: list(body.topics).map((t) => ({
      name: str(t.name),
      inputSchema: str(t.inputSchema),
      subscriptions: subs(t.subscriptions),
    })),
    domains: list(body.domains).map((d) => ({
      name: str(d.name),
      inputSchema: str(d.inputSchema),
      autoCreateTopics: d.autoCreateTopics === true,
      autoDeleteTopics: d.autoDeleteTopics === true,
      subscriptions: subs(d.subscriptions),
      topics: list(d.topics).map((c) => child(c)),
    })),
    namespaces: list(body.namespaces).map((n) => ({
      name: str(n.name),
      topics: list(n.topics).map((c) => child(c)),
    })),
    scopes: list(body.scopes).map((s) => ({
      id: str(s.id),
      subscriptions: subs(s.subscriptions),
    })),
  };
}

/** Plain-language retry/dead-letter summary of a subscription. */
export function deliverySummary(s: EgSubscription): string {
  if (s.queueMaxDelivery) {
    return `lock ${s.queueLockSeconds}s, max ${s.queueMaxDelivery} deliveries, ttl ${s.queueEventTimeToLive || "-"}`;
  }
  const retry = `${s.maxDeliveryAttempts} attempts, ttl ${s.eventTimeToLiveMinutes} min`;
  return s.deadLetterType
    ? `${retry}, dead-letter ${s.deadLetterContainer || s.deadLetterType}`
    : `${retry}, no dead-letter`;
}

/** Plain-language filter summary of a subscription. */
export function filterSummary(s: EgSubscription): string {
  const parts: string[] = [];
  if (s.includedEventTypes.length > 0) parts.push(`types: ${s.includedEventTypes.join(", ")}`);
  if (s.subjectBeginsWith) parts.push(`subject starts ${s.subjectBeginsWith}`);
  if (s.subjectEndsWith) parts.push(`subject ends ${s.subjectEndsWith}`);
  if (s.advancedFilterCount) parts.push(`${s.advancedFilterCount} advanced`);
  return parts.join("; ") || "none";
}

/** Returns an error message, or "" when the test event is valid. */
export function validateTestEvent(e: TestEvent, needsDomainTopic: boolean): string {
  if (!e.eventType.trim()) return "Event type is required";
  if (e.eventType.length > MAX_TYPE_LEN) return `Event type is at most ${MAX_TYPE_LEN} characters`;
  if (e.subject.length > MAX_SUBJECT_LEN) return `Subject is at most ${MAX_SUBJECT_LEN} characters`;
  if (new TextEncoder().encode(e.data).length > MAX_DATA_BYTES) return "Data exceeds 64 KiB";
  if (needsDomainTopic && !e.domainTopic.trim()) return "Domain topic is required";
  return "";
}

function publishPath(t: PublishTarget): string {
  switch (t.kind) {
    case "topic":
      return `/topics/${seg(t.topic)}/events`;
    case "domain":
      return `/domains/${seg(t.domain)}/events`;
    default:
      return `/namespaces/${seg(t.namespace)}/topics/${seg(t.topic)}/events`;
  }
}

/** Publishes one test event; the server rebuilds it in the resource's input schema. */
export async function publishTestEvent(
  target: PublishTarget,
  event: TestEvent,
  fetchFn?: FetchFn,
): Promise<number> {
  const payload: Json = {
    subject: event.subject,
    eventType: event.eventType,
    data: event.data,
  };
  if (target.kind === "domain") payload.domainTopic = event.domainTopic;
  const res = await restCall<{ published?: number }>(
    EG_PROXY_BASE,
    "POST",
    publishPath(target),
    payload,
    fetchFn,
  );
  return num(res.published);
}

/** Fetches the key pair. Only call this from an explicit reveal action. */
export async function revealKeys(kind: EgKind, name: string, fetchFn?: FetchFn): Promise<EgKeys> {
  const body = await restCall(
    EG_PROXY_BASE,
    "POST",
    `/${kind}/${seg(name)}/keys/reveal`,
    undefined,
    fetchFn,
  );
  return { key1: str(body.key1), key2: str(body.key2) };
}
