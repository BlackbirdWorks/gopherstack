/** Client for the Oracle Cloud Notifications emulator (topics, subscriptions, recorded deliveries) via the dashboard's in-process proxy. */

import {
  ociCompartments,
  ociGetOne,
  ociListPages,
  readStoredValue,
  writeStoredValue,
} from "./ocirest.ts";
import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const NOTIF_PROXY_BASE = "/dashboard/api/v1/oci/notifications";
export const NOTIF_COMPARTMENT_KEY = "gopherstack_ocinotifications_compartment";

const enc = encodeURIComponent;
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);

export type Topic = {
  id: string;
  name: string;
  description: string;
  state: string;
  created: string;
  endpoint: string;
};

export type Subscription = {
  id: string;
  topicId: string;
  protocol: string;
  endpoint: string;
  state: string;
  created: string;
};

export type Delivery = {
  id: string;
  kind: string;
  status: string;
  topicId: string;
  topicName: string;
  subscriptionId: string;
  protocol: string;
  endpoint: string;
  title: string;
  body: string;
  attempts: number;
  published: string;
  lastError: string;
  confirmationUrl: string;
  unsubscribeUrl: string;
};

export function parseTopic(raw: Json): Topic {
  return {
    id: str(raw.topicId),
    name: str(raw.name),
    description: str(raw.description),
    state: str(raw.lifecycleState),
    created: str(raw.timeCreated),
    endpoint: str(raw.apiEndpoint),
  };
}

export function parseSubscription(raw: Json): Subscription {
  const created = num(raw.createdTime);
  return {
    id: str(raw.id),
    topicId: str(raw.topicId),
    protocol: str(raw.protocol),
    endpoint: str(raw.endpoint),
    state: str(raw.lifecycleState),
    created: created ? new Date(created).toISOString() : "",
  };
}

export function parseDelivery(raw: Json): Delivery {
  return {
    id: str(raw.id),
    kind: str(raw.kind),
    status: str(raw.status),
    topicId: str(raw.topicId),
    topicName: str(raw.topicName),
    subscriptionId: str(raw.subscriptionId),
    protocol: str(raw.protocol),
    endpoint: str(raw.endpoint),
    title: str(raw.title),
    body: str(raw.body),
    attempts: num(raw.attempts),
    published: str(raw.published),
    lastError: str(raw.lastError),
    confirmationUrl: str(raw.confirmationUrl),
    unsubscribeUrl: str(raw.unsubscribeUrl),
  };
}

/** Only http(s) links from deliveries are rendered as anchors. */
export const isHttpUrl = (u: string): boolean => /^https?:\/\//i.test(u);

/** The query (token and protocol) of a delivery's confirmation link, without the leading "?". */
export function confirmationQuery(url: string): string {
  try {
    return new URL(url).search.replace(/^\?/, "");
  } catch {
    return "";
  }
}

export const listCompartments = (fetchFn: FetchFn = fetch) =>
  ociCompartments(NOTIF_PROXY_BASE, fetchFn);

export async function listTopics(compartment: string, fetchFn: FetchFn = fetch): Promise<Topic[]> {
  const items = await ociListPages(
    NOTIF_PROXY_BASE,
    "/api/topics",
    `compartmentId=${enc(compartment)}`,
    "",
    fetchFn,
  );
  return items.map((r) => parseTopic(r));
}

export async function listSubscriptions(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<Subscription[]> {
  const items = await ociListPages(
    NOTIF_PROXY_BASE,
    "/api/subscriptions",
    `compartmentId=${enc(compartment)}`,
    "",
    fetchFn,
  );
  return items.map((r) => parseSubscription(r));
}

export async function listDeliveries(fetchFn: FetchFn = fetch): Promise<Delivery[]> {
  const body = await restCall<unknown>(
    NOTIF_PROXY_BASE,
    "GET",
    "/api/deliveries",
    undefined,
    fetchFn,
  );
  return (Array.isArray(body) ? (body as Json[]) : []).map((r) => parseDelivery(r)).toReversed();
}

/** Confirms a subscription with the query of its confirmation link. */
export async function confirmSubscription(
  subscriptionId: string,
  query: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await ociGetOne(
    NOTIF_PROXY_BASE,
    `/api/subscriptions/${enc(subscriptionId)}/confirmation?${query}`,
    fetchFn,
  );
}

export async function resendConfirmation(
  subscriptionId: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    NOTIF_PROXY_BASE,
    "POST",
    `/api/subscriptions/${enc(subscriptionId)}/resendConfirmation`,
    undefined,
    fetchFn,
  );
}

export const readStoredNotifCompartment = () => readStoredValue(NOTIF_COMPARTMENT_KEY);
export const writeStoredNotifCompartment = (id: string) =>
  writeStoredValue(NOTIF_COMPARTMENT_KEY, id);
