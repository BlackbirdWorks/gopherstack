import { restCall, str, type FetchFn, type Json } from "./gcprest";

export const EG_PROXY_BASE = "/dashboard/api/v1/azure/eventgrid";
export const SCHEMA_CLOUDEVENT = "CloudEventSchemaV1_0";

export type EgSubscription = {
  name: string;
  endpointType: string;
  state: string;
  deliverySchema: string;
};
export type EgTopic = { name: string; inputSchema: string; subscriptions: EgSubscription[] };

const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);

export function parseSubscription(raw: Json): EgSubscription {
  return {
    name: str(raw.name),
    endpointType: str(raw.endpointType),
    state: str(raw.state),
    deliverySchema: str(raw.deliverySchema),
  };
}

export async function listTopics(fetchFn?: FetchFn): Promise<EgTopic[]> {
  const body = await restCall(EG_PROXY_BASE, "GET", "/topics", undefined, fetchFn);
  return list(body.topics).map((t) => ({
    name: str(t.name),
    inputSchema: str(t.inputSchema),
    subscriptions: list(t.subscriptions).map((x) => parseSubscription(x)),
  }));
}

/** A sample event valid for the topic's input schema. */
export function sampleEvent(schema: string, now = new Date()): Json {
  const id = crypto.randomUUID();
  if (schema === SCHEMA_CLOUDEVENT) {
    return {
      specversion: "1.0",
      id,
      source: "/gopherstack/dashboard",
      type: "Gopherstack.Test",
      time: now.toISOString(),
      data: { message: "hello from the dashboard" },
    };
  }
  return {
    id,
    subject: "/gopherstack/dashboard/test",
    eventType: "Gopherstack.Test",
    eventTime: now.toISOString(),
    dataVersion: "1.0",
    data: { message: "hello from the dashboard" },
  };
}

/** Returns an error message, or "" when the text is a JSON event or array of events. */
export function validateEventJson(text: string): string {
  if (!text.trim()) return "Event JSON is required";
  try {
    const v: unknown = JSON.parse(text);
    const events = Array.isArray(v) ? v : [v];
    if (events.length === 0) return "Provide at least one event";
    if (events.some((e) => typeof e !== "object" || e === null || Array.isArray(e))) {
      return "Each event must be a JSON object";
    }
    return "";
  } catch {
    return "Not valid JSON";
  }
}

/** Resolves to the number of events accepted. */
export async function publishEvents(
  topic: string,
  text: string,
  fetchFn?: FetchFn,
): Promise<number> {
  const body = JSON.parse(text) as unknown;
  const res = await restCall<{ published?: number }>(
    EG_PROXY_BASE,
    "POST",
    `/topics/${encodeURIComponent(topic)}/events`,
    body,
    fetchFn,
  );
  return typeof res.published === "number" ? res.published : 0;
}
