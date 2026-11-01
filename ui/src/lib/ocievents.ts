/** Client for the Oracle Cloud Events emulator: rules, their actions and the delivery log via the dashboard's in-process proxy. */

import {
  ociCompartments,
  ociGetOne,
  ociListPages,
  readStoredValue,
  writeStoredValue,
} from "./ocirest.ts";
import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";
import { listApplications, listFunctions } from "./ocifunctions.ts";
import { listTopics } from "./ocinotifications.ts";
import { listStreams } from "./ocistreaming.ts";

export const EVENTS_PROXY_BASE = "/dashboard/api/v1/oci/events";
export const EVENTS_COMPARTMENT_KEY = "gopherstack_ocievents_compartment";
export const DELIVERY_REFRESH_MS = 3000;

const enc = encodeURIComponent;
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);

export type ActionType = "ONS" | "OSS" | "FAAS";

export const ACTION_TYPES: { id: ActionType; label: string }[] = [
  { id: "ONS", label: "Notifications topic" },
  { id: "OSS", label: "Streaming stream" },
  { id: "FAAS", label: "Functions function" },
];

export type EventsAction = {
  id: string;
  type: string;
  state: string;
  targetId: string;
  enabled: boolean;
  description: string;
};

export type EventsRule = {
  id: string;
  name: string;
  state: string;
  enabled: boolean;
  description: string;
  condition: string;
  actions: EventsAction[];
  created: string;
};

export type EventsDelivery = {
  time: string;
  eventId: string;
  eventType: string;
  ruleId: string;
  actionId: string;
  actionType: string;
  targetId: string;
  status: string;
  error: string;
};

export type ActionInput = {
  type: ActionType;
  targetId: string;
  enabled: boolean;
  description?: string;
};

export type RuleInput = {
  name: string;
  description: string;
  condition: string;
  enabled: boolean;
  actions: ActionInput[];
};

export type TargetOption = { id: string; label: string };

export function parseAction(raw: Json): EventsAction {
  return {
    id: str(raw.id),
    type: str(raw.actionType),
    state: str(raw.lifecycleState),
    targetId: str(raw.topicId) || str(raw.streamId) || str(raw.functionId),
    enabled: raw.isEnabled === true,
    description: str(raw.description),
  };
}

export function parseRule(raw: Json): EventsRule {
  return {
    id: str(raw.id),
    name: str(raw.displayName) || str(raw.id),
    state: str(raw.lifecycleState),
    enabled: raw.isEnabled === true,
    description: str(raw.description),
    condition: str(raw.condition),
    actions: list(obj(raw.actions).actions).map((a) => parseAction(a)),
    created: str(raw.timeCreated),
  };
}

export function parseDelivery(raw: Json): EventsDelivery {
  return {
    time: str(raw.time),
    eventId: str(raw.eventId),
    eventType: str(raw.eventType),
    ruleId: str(raw.ruleId),
    actionId: str(raw.actionId),
    actionType: str(raw.actionType),
    targetId: str(raw.targetId),
    status: str(raw.status),
    error: str(raw.error),
  };
}

/** Pretty-prints a rule condition; text that is not JSON is returned unchanged. */
export function prettyCondition(condition: string): string {
  try {
    return JSON.stringify(JSON.parse(condition), null, 2);
  } catch {
    return condition;
  }
}

function leaves(prefix: string, v: unknown, out: string[]): void {
  if (Array.isArray(v)) {
    out.push(`${prefix}=${v.map(String).join("|")}`);
  } else if (v && typeof v === "object") {
    for (const [k, child] of Object.entries(v as Json)) {
      leaves(prefix ? `${prefix}.${k}` : k, child, out);
    }
  } else {
    out.push(`${prefix}=${String(v)}`);
  }
}

/** One-line summary of a condition: its leaf filters as key=value. */
export function summarizeCondition(condition: string, max = 90): string {
  let parsed: unknown;
  try {
    parsed = JSON.parse(condition);
  } catch {
    return condition.length > max ? `${condition.slice(0, max - 1)}…` : condition;
  }
  const out: string[] = [];
  leaves("", parsed, out);
  const text = out.length > 0 ? out.join(", ") : "matches every event";
  return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}

function checkNode(path: string, v: unknown): string {
  if (Array.isArray(v)) {
    if (v.length === 0) return `${path}: an array must list at least one value`;
    if (v.some((e) => e === null || typeof e === "object")) {
      return `${path}: array elements must be strings, numbers or booleans`;
    }
    return "";
  }
  if (v === null) return `${path}: null is not a valid filter value`;
  if (typeof v === "object") {
    for (const [k, child] of Object.entries(v as Json)) {
      if (k.includes("*")) {
        return `${path}: wildcards are allowed in values only, not in the field name "${k}"`;
      }
      const err = checkNode(`${path}.${k}`, child);
      if (err) return err;
    }
  }
  return "";
}

/** Client-side check of a condition (JSON object, scalar or non-empty scalar-array leaves); "" when valid. */
export function validateCondition(text: string): string {
  if (!text.trim()) return "condition is required";
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (e) {
    return `condition is not valid JSON: ${e instanceof Error ? e.message : String(e)}`;
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return "condition must be a JSON object";
  }
  return checkNode("condition", parsed);
}

export const actionTypeLabel = (type: string): string =>
  ACTION_TYPES.find((t) => t.id === type)?.label ?? (type || "—");

export const listCompartments = (fetchFn: FetchFn = fetch) =>
  ociCompartments(EVENTS_PROXY_BASE, fetchFn);

/** Rules of a compartment with their actions (the list answer omits actions, so each rule is fetched). */
export async function listRules(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<EventsRule[]> {
  const items = await ociListPages(
    EVENTS_PROXY_BASE,
    "/api/rules",
    `compartmentId=${enc(compartment)}`,
    "",
    fetchFn,
  );
  return Promise.all(
    items.map(async (r) =>
      parseRule(await ociGetOne(EVENTS_PROXY_BASE, `/api/rules/${enc(str(r.id))}`, fetchFn)),
    ),
  );
}

export async function getRule(id: string, fetchFn: FetchFn = fetch): Promise<EventsRule> {
  return parseRule(await ociGetOne(EVENTS_PROXY_BASE, `/api/rules/${enc(id)}`, fetchFn));
}

function actionBody(a: ActionInput): Json {
  const body: Json = { actionType: a.type, isEnabled: a.enabled };
  const target = a.targetId.trim();
  if (a.type === "ONS") body.topicId = target;
  else if (a.type === "OSS") body.streamId = target;
  else body.functionId = target;
  if (a.description?.trim()) body.description = a.description.trim();
  return body;
}

export async function createRule(
  compartment: string,
  input: RuleInput,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    EVENTS_PROXY_BASE,
    "POST",
    "/api/rules",
    {
      compartmentId: compartment,
      displayName: input.name.trim(),
      description: input.description.trim(),
      condition: input.condition,
      isEnabled: input.enabled,
      actions: { actions: input.actions.map(actionBody) },
    },
    fetchFn,
  );
}

export async function setRuleEnabled(
  id: string,
  enabled: boolean,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    EVENTS_PROXY_BASE,
    "PUT",
    `/api/rules/${enc(id)}`,
    { isEnabled: enabled },
    fetchFn,
  );
}

export async function deleteRule(id: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(EVENTS_PROXY_BASE, "DELETE", `/api/rules/${enc(id)}`, undefined, fetchFn);
}

/** The delivery log, newest first. */
export async function listDeliveries(fetchFn: FetchFn = fetch): Promise<EventsDelivery[]> {
  const items = await ociListPages(EVENTS_PROXY_BASE, "/deliveries", "", "", fetchFn);
  return items.map((d) => parseDelivery(d)).toReversed();
}

const settled = async (load: () => Promise<TargetOption[]>): Promise<TargetOption[]> => {
  try {
    return await load();
  } catch {
    return [];
  }
};

/** ACTIVE topics of a compartment (the backend rejects other targets); empty when the Notifications proxy is unavailable. */
export const listTopicOptions = (compartment: string, fetchFn: FetchFn = fetch) =>
  settled(async () =>
    (await listTopics(compartment, fetchFn))
      .filter((t) => t.state === "ACTIVE")
      .map((t) => ({ id: t.id, label: t.name || t.id })),
  );

/** ACTIVE streams of the Streaming service; empty when its proxy is unavailable. */
export const listStreamOptions = (fetchFn: FetchFn = fetch) =>
  settled(async () =>
    (await listStreams("", fetchFn))
      .filter((s) => s.state === "ACTIVE")
      .map((s) => ({ id: s.id, label: s.name || s.id })),
  );

/** ACTIVE functions across every application; empty when the Functions proxy is unavailable. */
export const listFunctionOptions = (fetchFn: FetchFn = fetch) =>
  settled(async () => {
    const apps = await listApplications(fetchFn);
    const groups = await Promise.all(apps.map((a) => listFunctions(a.id, fetchFn)));
    return groups
      .flat()
      .filter((f) => f.state === "ACTIVE")
      .map((f) => ({ id: f.id, label: f.name || f.id }));
  });

export const readStoredEventsCompartment = () => readStoredValue(EVENTS_COMPARTMENT_KEY);
export const writeStoredEventsCompartment = (id: string) =>
  writeStoredValue(EVENTS_COMPARTMENT_KEY, id);
