import { describe, expect, it } from "vitest";
import {
  decisionKind,
  describeBoundary,
  describePolicy,
  emptyFilters,
  matchesFilters,
  mergeEvents,
  parseEvent,
  suggestPolicy,
  suggestionJSON,
  type StreamEvent,
} from "./iamstream";

function ev(over: Partial<StreamEvent>): StreamEvent {
  return {
    seq: 1,
    timestamp: "2026-10-19T00:00:00Z",
    service: "s3",
    operation: "GetObject",
    action: "s3:GetObject",
    resource: "arn:aws:s3:::b/k",
    principal: "arn:aws:iam::000000000000:user/alice",
    decision: "deny",
    denyType: "implicit",
    reason: "implicit-deny",
    enforced: true,
    ...over,
  };
}

describe("decisionKind", () => {
  it.each([
    ["allow", { decision: "allow", denyType: undefined }, "allow"],
    ["explicit", { denyType: "explicit" }, "explicit-deny"],
    ["implicit", { denyType: "implicit" }, "implicit-deny"],
    ["missing denyType", { denyType: undefined }, "implicit-deny"],
  ] as const)("%s", (_n, over, want) => {
    expect(decisionKind(ev(over as Partial<StreamEvent>))).toBe(want);
  });
});

describe("matchesFilters", () => {
  it.each([
    ["empty", {}, true],
    ["principal hit", { principal: "ALICE" }, true],
    ["principal miss", { principal: "bob" }, false],
    ["action hit", { action: "getobject" }, true],
    ["action miss", { action: "sqs:" }, false],
    ["decision hit", { decision: "implicit-deny" }, true],
    ["decision miss", { decision: "allow" }, false],
  ] as const)("%s", (_n, f, want) => {
    expect(matchesFilters(ev({}), { ...emptyFilters, ...f })).toBe(want);
  });
});

describe("mergeEvents", () => {
  it("dedupes by seq, sorts newest first and caps", () => {
    const merged = mergeEvents(
      [ev({ seq: 2 }), ev({ seq: 1 })],
      [ev({ seq: 2 }), ev({ seq: 3 })],
      2,
    );
    expect(merged.map((e) => e.seq)).toEqual([3, 2]);
  });

  it("returns existing when nothing is new", () => {
    const existing = [ev({ seq: 1 })];
    expect(mergeEvents(existing, [ev({ seq: 1 })], 10)).toBe(existing);
  });
});

describe("parseEvent", () => {
  it.each([
    ["valid", JSON.stringify(ev({})), true],
    ["bad json", "{", false],
    ["wrong shape", '{"x":1}', false],
  ])("%s", (_n, data, ok) => {
    expect(parseEvent(data) !== null).toBe(ok);
  });
});

describe("describe helpers", () => {
  it.each([
    [undefined, ""],
    [{ type: "managed", arn: "arn:aws:iam::1:policy/p" }, "arn:aws:iam::1:policy/p"],
    [{ type: "inline", name: "n", owner: "user/alice" }, "inline n (user/alice)"],
    [{ type: "resource", owner: "arn:aws:s3:::b" }, "resource policy (arn:aws:s3:::b)"],
    [{ type: "permission-boundary", arn: "arn:b" }, "arn:b"],
    [{ type: "identity" }, "identity policy"],
  ])("policy %#", (ref, want) => {
    expect(describePolicy(ref)).toBe(want);
  });

  it.each([
    [undefined, ""],
    [{ arn: "arn:b", statementIndex: -1, matched: false }, "arn:b: no matching statement"],
    [{ arn: "arn:b", sid: "S", statementIndex: 0, matched: true }, "arn:b: Sid S"],
    [{ arn: "arn:b", statementIndex: 2, matched: true }, "arn:b: statement #2 (no Sid)"],
  ])("boundary %#", (b, want) => {
    expect(describeBoundary(b)).toBe(want);
  });
});

describe("suggestPolicy", () => {
  it("groups fixable denies by resource and lists blocked ones", () => {
    const s = suggestPolicy([
      ev({ seq: 1, action: "s3:PutObject" }),
      ev({ seq: 2, action: "s3:GetObject" }),
      ev({ seq: 3, action: "s3:GetObject" }),
      ev({ seq: 4, action: "sqs:SendMessage", resource: "" }),
      ev({ seq: 5, action: "s3:DeleteObject", denyType: "explicit", reason: "identity-policy" }),
      ev({
        seq: 6,
        action: "s3:ListBucket",
        reason: "permission-boundary",
        boundary: { arn: "arn:b", statementIndex: -1, matched: false },
      }),
      ev({ seq: 7, decision: "allow", denyType: undefined }),
    ]);

    expect(s.consumed).toBe(4);
    expect(s.policy.Statement).toEqual([
      { Effect: "Allow", Action: ["sqs:SendMessage"], Resource: ["*"] },
      { Effect: "Allow", Action: ["s3:GetObject", "s3:PutObject"], Resource: ["arn:aws:s3:::b/k"] },
    ]);
    expect(s.blocked).toEqual([
      "s3:DeleteObject on arn:aws:s3:::b/k (blocked by identity-policy)",
      "s3:ListBucket on arn:aws:s3:::b/k (blocked by arn:b)",
    ]);
    expect(JSON.parse(suggestionJSON(s)).Version).toBe("2012-10-17");
  });
});
