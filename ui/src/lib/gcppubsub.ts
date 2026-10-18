/** Client for the Pub/Sub emulator via the dashboard's in-process REST proxy. */

import {
  base64ToUtf8,
  restCall,
  shortName,
  str,
  strMap,
  utf8ToBase64,
  type FetchFn,
  type Json,
} from "./gcprest.ts";

export const PUBSUB_PROXY_BASE = "/dashboard/api/v1/gcp/pubsub/v1";
export const PUBSUB_PROJECT_KEY = "gopherstack_pubsub_project";
export const DEFAULT_PUBSUB_PROJECT = "gopherstack-local";
const PAGE = 100;

export type Labels = Record<string, string>;

export type DeadLetterPolicy = { deadLetterTopic: string; maxDeliveryAttempts: number };

export type TopicInfo = {
  name: string;
  id: string;
  labels: Labels;
  retention: string;
};

export type SubscriptionInfo = {
  name: string;
  id: string;
  topic: string;
  topicId: string;
  ackDeadlineSeconds: number;
  filter: string;
  enableMessageOrdering: boolean;
  pushEndpoint: string;
  deadLetter: DeadLetterPolicy | null;
  retainAckedMessages: boolean;
  retention: string;
  labels: Labels;
  state: string;
};

export type SnapshotInfo = {
  name: string;
  id: string;
  topic: string;
  expireTime: string;
};

export type PulledMessage = {
  ackId: string;
  messageId: string;
  data: string;
  base64: string;
  attributes: Labels;
  orderingKey: string;
  publishTime: string;
  deliveryAttempt: number;
};

const projectPath = (project: string) => `/projects/${encodeURIComponent(project)}`;
const topicPath = (project: string, id: string) =>
  `${projectPath(project)}/topics/${encodeURIComponent(id)}`;
const subPath = (project: string, id: string) =>
  `${projectPath(project)}/subscriptions/${encodeURIComponent(id)}`;
const snapPath = (project: string, id: string) =>
  `${projectPath(project)}/snapshots/${encodeURIComponent(id)}`;

export const topicName = (project: string, id: string) => `projects/${project}/topics/${id}`;
export const subscriptionName = (project: string, id: string) =>
  `projects/${project}/subscriptions/${id}`;
export const snapshotName = (project: string, id: string) => `projects/${project}/snapshots/${id}`;

/** Pub/Sub resource ids: 3-255 chars, start with a letter, not "goog" prefixed. */
export function validateResourceId(id: string): string | null {
  if (!id) return "name is required";
  if (id.length < 3 || id.length > 255) return "must be 3 to 255 characters";
  if (!/^[A-Za-z]/.test(id)) return "must start with a letter";
  if (!/^[A-Za-z0-9\-_.~+%]+$/.test(id)) return "may contain letters, digits and -_.~+%";
  if (id.toLowerCase().startsWith("goog")) return 'must not start with "goog"';
  return null;
}

export function parseTopic(raw: Json): TopicInfo {
  const name = str(raw.name);
  return {
    name,
    id: shortName(name),
    labels: strMap(raw.labels),
    retention: str(raw.messageRetentionDuration),
  };
}

export function parseSubscription(raw: Json): SubscriptionInfo {
  const push = raw.pushConfig as { pushEndpoint?: string } | undefined;
  const dl = raw.deadLetterPolicy as
    | { deadLetterTopic?: string; maxDeliveryAttempts?: number }
    | undefined;
  const topic = str(raw.topic);
  return {
    name: str(raw.name),
    id: shortName(str(raw.name)),
    topic,
    topicId: shortName(topic),
    ackDeadlineSeconds: Number(raw.ackDeadlineSeconds ?? 0) || 0,
    filter: str(raw.filter),
    enableMessageOrdering: raw.enableMessageOrdering === true,
    pushEndpoint: str(push?.pushEndpoint),
    deadLetter: dl?.deadLetterTopic
      ? { deadLetterTopic: dl.deadLetterTopic, maxDeliveryAttempts: dl.maxDeliveryAttempts ?? 0 }
      : null,
    retainAckedMessages: raw.retainAckedMessages === true,
    retention: str(raw.messageRetentionDuration),
    labels: strMap(raw.labels),
    state: str(raw.state),
  };
}

export function parseSnapshot(raw: Json): SnapshotInfo {
  const name = str(raw.name);
  return { name, id: shortName(name), topic: str(raw.topic), expireTime: str(raw.expireTime) };
}

export function parsePulled(raw: Json): PulledMessage {
  const msg = (raw.message ?? {}) as Json;
  const b64 = str(msg.data);
  return {
    ackId: str(raw.ackId),
    messageId: str(msg.messageId),
    data: base64ToUtf8(b64) ?? "",
    base64: b64,
    attributes: strMap(msg.attributes),
    orderingKey: str(msg.orderingKey),
    publishTime: str(msg.publishTime),
    deliveryAttempt: Number(raw.deliveryAttempt ?? 0) || 0,
  };
}

async function listAll<T>(
  base: string,
  key: string,
  parse: (raw: Json) => T,
  fetchFn: FetchFn,
): Promise<T[]> {
  const out: T[] = [];
  let token = "";
  do {
    const sep = base.includes("?") ? "&" : "?";
    const url = `${base}${sep}pageSize=${PAGE}${token ? `&pageToken=${encodeURIComponent(token)}` : ""}`;
    const body = await restCall<Json>(PUBSUB_PROXY_BASE, "GET", url, undefined, fetchFn);
    out.push(...((body[key] as Json[] | undefined) ?? []).map((r) => parse(r)));
    token = str(body.nextPageToken);
  } while (token);
  return out;
}

export const listTopics = (project: string, fetchFn: FetchFn = fetch) =>
  listAll(`${projectPath(project)}/topics`, "topics", parseTopic, fetchFn);

export async function createTopic(
  project: string,
  id: string,
  opts: { labels: Labels; retentionSeconds: string },
  fetchFn: FetchFn = fetch,
): Promise<TopicInfo> {
  const body: Json = {};
  if (Object.keys(opts.labels).length > 0) body.labels = opts.labels;
  if (opts.retentionSeconds.trim())
    body.messageRetentionDuration = `${opts.retentionSeconds.trim()}s`;
  return parseTopic(
    await restCall(PUBSUB_PROXY_BASE, "PUT", topicPath(project, id), body, fetchFn),
  );
}

export async function updateTopic(
  project: string,
  id: string,
  topic: Json,
  mask: string,
  fetchFn: FetchFn = fetch,
): Promise<TopicInfo> {
  return parseTopic(
    await restCall(
      PUBSUB_PROXY_BASE,
      "PATCH",
      topicPath(project, id),
      { topic, updateMask: mask },
      fetchFn,
    ),
  );
}

export async function deleteTopic(project: string, id: string, fetchFn: FetchFn = fetch) {
  await restCall(PUBSUB_PROXY_BASE, "DELETE", topicPath(project, id), undefined, fetchFn);
}

export function listTopicSubscriptions(
  project: string,
  id: string,
  fetchFn: FetchFn = fetch,
): Promise<string[]> {
  return listAll(`${topicPath(project, id)}/subscriptions`, "subscriptions", String, fetchFn);
}

export type PublishInput = {
  data: string;
  attributes: Labels;
  orderingKey: string;
};

export async function publish(
  project: string,
  id: string,
  msg: PublishInput,
  fetchFn: FetchFn = fetch,
): Promise<string[]> {
  const m: Json = {};
  if (msg.data) m.data = utf8ToBase64(msg.data);
  if (Object.keys(msg.attributes).length > 0) m.attributes = msg.attributes;
  if (msg.orderingKey) m.orderingKey = msg.orderingKey;
  const res = await restCall<{ messageIds?: string[] }>(
    PUBSUB_PROXY_BASE,
    "POST",
    `${topicPath(project, id)}:publish`,
    { messages: [m] },
    fetchFn,
  );
  return res.messageIds ?? [];
}

export const listSubscriptions = (project: string, fetchFn: FetchFn = fetch) =>
  listAll(`${projectPath(project)}/subscriptions`, "subscriptions", parseSubscription, fetchFn);

export type CreateSubscriptionInput = {
  topic: string;
  ackDeadlineSeconds: number;
  filter: string;
  enableMessageOrdering: boolean;
  pushEndpoint: string;
  deadLetterTopic: string;
  maxDeliveryAttempts: number;
  labels: Labels;
};

export function subscriptionBody(i: CreateSubscriptionInput): Json {
  const body: Json = { topic: i.topic, ackDeadlineSeconds: i.ackDeadlineSeconds };
  if (i.filter) body.filter = i.filter;
  if (i.enableMessageOrdering) body.enableMessageOrdering = true;
  if (i.pushEndpoint) body.pushConfig = { pushEndpoint: i.pushEndpoint };
  if (i.deadLetterTopic) {
    body.deadLetterPolicy = {
      deadLetterTopic: i.deadLetterTopic,
      maxDeliveryAttempts: i.maxDeliveryAttempts,
    };
  }
  if (Object.keys(i.labels).length > 0) body.labels = i.labels;
  return body;
}

export async function createSubscription(
  project: string,
  id: string,
  input: CreateSubscriptionInput,
  fetchFn: FetchFn = fetch,
): Promise<SubscriptionInfo> {
  return parseSubscription(
    await restCall(
      PUBSUB_PROXY_BASE,
      "PUT",
      subPath(project, id),
      subscriptionBody(input),
      fetchFn,
    ),
  );
}

export async function deleteSubscription(project: string, id: string, fetchFn: FetchFn = fetch) {
  await restCall(PUBSUB_PROXY_BASE, "DELETE", subPath(project, id), undefined, fetchFn);
}

export async function pull(
  project: string,
  id: string,
  maxMessages: number,
  fetchFn: FetchFn = fetch,
): Promise<PulledMessage[]> {
  const res = await restCall<{ receivedMessages?: Json[] }>(
    PUBSUB_PROXY_BASE,
    "POST",
    `${subPath(project, id)}:pull`,
    { maxMessages },
    fetchFn,
  );
  return (res.receivedMessages ?? []).map((m) => parsePulled(m));
}

export async function acknowledge(
  project: string,
  id: string,
  ackIds: string[],
  fetchFn: FetchFn = fetch,
) {
  await restCall(
    PUBSUB_PROXY_BASE,
    "POST",
    `${subPath(project, id)}:acknowledge`,
    { ackIds },
    fetchFn,
  );
}

export async function seekToSnapshot(
  project: string,
  id: string,
  snapshot: string,
  fetchFn: FetchFn = fetch,
) {
  await restCall(PUBSUB_PROXY_BASE, "POST", `${subPath(project, id)}:seek`, { snapshot }, fetchFn);
}

export async function seekToTime(
  project: string,
  id: string,
  isoTime: string,
  fetchFn: FetchFn = fetch,
) {
  await restCall(
    PUBSUB_PROXY_BASE,
    "POST",
    `${subPath(project, id)}:seek`,
    { time: isoTime },
    fetchFn,
  );
}

export const listSnapshots = (project: string, fetchFn: FetchFn = fetch) =>
  listAll(`${projectPath(project)}/snapshots`, "snapshots", parseSnapshot, fetchFn);

export async function createSnapshot(
  project: string,
  id: string,
  subscription: string,
  fetchFn: FetchFn = fetch,
): Promise<SnapshotInfo> {
  return parseSnapshot(
    await restCall(PUBSUB_PROXY_BASE, "PUT", snapPath(project, id), { subscription }, fetchFn),
  );
}

export async function deleteSnapshot(project: string, id: string, fetchFn: FetchFn = fetch) {
  await restCall(PUBSUB_PROXY_BASE, "DELETE", snapPath(project, id), undefined, fetchFn);
}

export function readStoredPubSubProject(storage?: Pick<Storage, "getItem">): string {
  try {
    const s = storage ?? (typeof window === "undefined" ? undefined : window.localStorage);
    return s?.getItem(PUBSUB_PROJECT_KEY)?.trim() || DEFAULT_PUBSUB_PROJECT;
  } catch {
    return DEFAULT_PUBSUB_PROJECT;
  }
}

export function writeStoredPubSubProject(
  project: string,
  storage?: Pick<Storage, "setItem">,
): void {
  try {
    const s = storage ?? (typeof window === "undefined" ? undefined : window.localStorage);
    s?.setItem(PUBSUB_PROJECT_KEY, project);
  } catch {
    // storage unavailable: the project applies for this session only
  }
}
