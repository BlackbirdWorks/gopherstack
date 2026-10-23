// Data logic for the IAM Policy Stream page (/_gopherstack/iam/policy-stream API).

export type PolicyRef = {
  type: string;
  arn?: string;
  name?: string;
  owner?: string;
};

export type BoundaryRef = {
  arn?: string;
  sid?: string;
  statementIndex: number;
  matched: boolean;
};

export type StreamEvent = {
  seq: number;
  timestamp: string;
  requestId?: string;
  service: string;
  operation: string;
  action: string;
  resource: string;
  principal: string;
  decision: "allow" | "deny";
  denyType?: "explicit" | "implicit";
  reason?: string;
  matchedSid?: string;
  policy?: PolicyRef;
  boundary?: BoundaryRef;
  conditionKeys?: string[];
  enforced: boolean;
};

export type DecisionKind = "allow" | "explicit-deny" | "implicit-deny";

export type StreamFilters = {
  principal: string;
  action: string;
  decision: "" | DecisionKind;
};

export const emptyFilters: StreamFilters = { principal: "", action: "", decision: "" };

export function decisionKind(e: StreamEvent): DecisionKind {
  if (e.decision === "allow") return "allow";
  return e.denyType === "explicit" ? "explicit-deny" : "implicit-deny";
}

export function decisionLabel(kind: DecisionKind): string {
  switch (kind) {
    case "allow":
      return "Allow";
    case "explicit-deny":
      return "Explicit deny";
    default:
      return "Implicit deny";
  }
}

function contains(haystack: string, needle: string): boolean {
  return haystack.toLowerCase().includes(needle.trim().toLowerCase());
}

export function matchesFilters(e: StreamEvent, f: StreamFilters): boolean {
  if (f.decision && decisionKind(e) !== f.decision) return false;
  if (f.principal.trim() && !contains(e.principal, f.principal)) return false;
  if (f.action.trim() && !contains(e.action, f.action)) return false;
  return true;
}

// Merge incoming events into existing, newest first, de-duplicated by seq.
export function mergeEvents(
  existing: StreamEvent[],
  incoming: StreamEvent[],
  cap: number,
): StreamEvent[] {
  if (incoming.length === 0) return existing;
  const seen = new Set(existing.map((e) => e.seq));
  const fresh = incoming.filter((e) => !seen.has(e.seq));
  if (fresh.length === 0) return existing;
  return [...fresh, ...existing].toSorted((a, b) => b.seq - a.seq).slice(0, cap);
}

export function parseEvent(data: string): StreamEvent | null {
  try {
    const v = JSON.parse(data) as StreamEvent;
    return typeof v.seq === "number" && typeof v.action === "string" ? v : null;
  } catch {
    return null;
  }
}

export function describePolicy(ref?: PolicyRef): string {
  if (!ref) return "";
  switch (ref.type) {
    case "managed":
      return ref.arn ?? "managed policy";
    case "inline":
      return `inline ${ref.name ?? ""}${ref.owner ? ` (${ref.owner})` : ""}`.trim();
    case "resource":
      return `resource policy${ref.owner ? ` (${ref.owner})` : ""}`;
    case "permission-boundary":
      return ref.arn || "permissions boundary";
    default:
      return "identity policy";
  }
}

export function describeBoundary(b?: BoundaryRef): string {
  if (!b) return "";
  const arn = b.arn || "permissions boundary";
  if (!b.matched) return `${arn}: no matching statement`;
  return b.sid ? `${arn}: Sid ${b.sid}` : `${arn}: statement #${b.statementIndex} (no Sid)`;
}

export type Suggestion = {
  policy: {
    Version: string;
    Statement: { Effect: string; Action: string[]; Resource: string[] }[];
  };
  blocked: string[];
  consumed: number;
};

const FIXABLE_REASONS = new Set(["implicit-deny", "unresolved-principal"]);

function isFixable(e: StreamEvent): boolean {
  return e.decision === "deny" && FIXABLE_REASONS.has(e.reason ?? "");
}

function blockedNote(e: StreamEvent): string {
  const by = e.boundary?.arn || e.policy?.arn || e.reason || "explicit deny";
  return `${e.action} on ${e.resource || "*"} (blocked by ${by})`;
}

// Least-privilege Allow policy from denied events. Denies an identity Allow
// cannot fix (explicit deny, permissions boundary) are listed in `blocked`.
export function suggestPolicy(events: StreamEvent[]): Suggestion {
  const byResource = new Map<string, Set<string>>();
  const blocked = new Set<string>();
  let consumed = 0;

  for (const e of events) {
    if (e.decision !== "deny") continue;
    if (!isFixable(e)) {
      blocked.add(blockedNote(e));
      continue;
    }
    const res = e.resource || "*";
    const acts = byResource.get(res) ?? new Set<string>();
    acts.add(e.action);
    byResource.set(res, acts);
    consumed++;
  }

  const Statement = [...byResource.keys()].toSorted().map((res) => ({
    Effect: "Allow",
    Action: [...(byResource.get(res) ?? [])].toSorted(),
    Resource: [res],
  }));

  return {
    policy: { Version: "2012-10-17", Statement },
    blocked: [...blocked].toSorted(),
    consumed,
  };
}

export function suggestionJSON(s: Suggestion): string {
  return JSON.stringify(s.policy, null, 2);
}
