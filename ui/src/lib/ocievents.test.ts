import { describe, expect, it, vi } from "vitest";

import {
  EVENTS_PROXY_BASE,
  actionTypeLabel,
  createRule,
  deleteRule,
  getRule,
  listCompartments,
  listDeliveries,
  listFunctionOptions,
  listRules,
  listStreamOptions,
  listTopicOptions,
  parseAction,
  parseDelivery,
  parseRule,
  prettyCondition,
  setRuleEnabled,
  summarizeCondition,
  validateCondition,
} from "./ocievents";

const json = (body: unknown, status = 200, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
const asFetch = (f: unknown) => f as typeof fetch;
const comp = "ocid1.compartment.oc1..aaaa";
const cond = '{"eventType":["com.oraclecloud.objectstorage.createobject"]}';

const rule = {
  id: "r1",
  displayName: "uploads",
  lifecycleState: "ACTIVE",
  isEnabled: true,
  description: "d",
  condition: cond,
  timeCreated: "2026-01-01T00:00:00Z",
  actions: {
    actions: [
      { id: "a1", actionType: "ONS", topicId: "t1", isEnabled: true, lifecycleState: "ACTIVE" },
      { id: "a2", actionType: "OSS", streamId: "s1", isEnabled: false },
      { id: "a3", actionType: "FAAS", functionId: "f1", isEnabled: true },
    ],
  },
};

describe("parsing", () => {
  it("reads a rule with its actions", () => {
    const r = parseRule(rule);
    expect(r).toMatchObject({ id: "r1", name: "uploads", state: "ACTIVE", enabled: true });
    expect(r.actions.map((a) => a.targetId)).toEqual(["t1", "s1", "f1"]);
    expect(parseAction(rule.actions.actions[1]!).enabled).toBe(false);
  });

  it("tolerates missing fields", () => {
    expect(parseRule({ id: "x" })).toMatchObject({ name: "x", actions: [], enabled: false });
    expect(parseDelivery({}).status).toBe("");
  });

  it("reads a delivery", () => {
    expect(
      parseDelivery({ time: "t", eventId: "e", ruleId: "r", status: "FAILED", error: "boom" }),
    ).toMatchObject({ eventId: "e", ruleId: "r", status: "FAILED", error: "boom" });
  });
});

describe("conditions", () => {
  it("pretty-prints JSON and leaves other text", () => {
    expect(prettyCondition('{"a":1}')).toBe('{\n  "a": 1\n}');
    expect(prettyCondition("nope")).toBe("nope");
  });

  it("summarizes leaves", () => {
    expect(summarizeCondition(cond)).toBe("eventType=com.oraclecloud.objectstorage.createobject");
    expect(summarizeCondition('{"a":{"b":"x"},"c":["1","2"]}')).toBe("a.b=x, c=1|2");
    expect(summarizeCondition("{}")).toBe("matches every event");
    expect(summarizeCondition("x".repeat(200), 10)).toHaveLength(10);
    expect(summarizeCondition(`{"k":"${"v".repeat(200)}"}`, 12)).toHaveLength(12);
  });

  it.each([
    ["", "condition is required"],
    ["{", "not valid JSON"],
    ["[]", "must be a JSON object"],
    ["3", "must be a JSON object"],
    ['{"a*":"x"}', "wildcards are allowed in values only"],
    ['{"a":[]}', "at least one value"],
    ['{"a":[{"b":1}]}', "array elements"],
    ['{"a":null}', "null is not a valid"],
    ['{"a":{"b":null}}', "condition.a.b"],
  ])("rejects %j", (text, msg) => {
    expect(validateCondition(text)).toContain(msg);
  });

  it.each(["{}", cond, '{"data":{"resourceName":"a*"}}', '{"n":1,"b":true}'])("accepts %j", (t) => {
    expect(validateCondition(t)).toBe("");
  });

  it("labels action types", () => {
    expect(actionTypeLabel("OSS")).toBe("Streaming stream");
    expect(actionTypeLabel("")).toBe("—");
  });
});

describe("client", () => {
  it("lists compartments", async () => {
    const f = vi.fn().mockResolvedValue(json({ compartments: [comp] }));
    expect(await listCompartments(asFetch(f))).toEqual([comp]);
    expect(f.mock.calls[0]?.[0]).toBe(`${EVENTS_PROXY_BASE}/compartments`);
  });

  it("lists rules then fetches each for its actions", async () => {
    const f = vi.fn((url: string) =>
      Promise.resolve(url.includes("/api/rules/r1") ? json(rule) : json([{ id: "r1" }], 200)),
    );
    const rules = await listRules(comp, asFetch(f));
    expect(rules[0]?.actions).toHaveLength(3);
    expect(f.mock.calls[0]?.[0]).toBe(
      `${EVENTS_PROXY_BASE}/api/rules?compartmentId=${encodeURIComponent(comp)}`,
    );
  });

  it("follows opc-next-page", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(json([{ id: "r1" }], 200, { "opc-next-page": "p2" }))
      .mockResolvedValueOnce(json([{ id: "r2" }]))
      .mockImplementation(() => Promise.resolve(json(rule)));
    expect(await listRules(comp, asFetch(f))).toHaveLength(2);
    expect(String(f.mock.calls[1]?.[0])).toContain("page=p2");
  });

  it("gets a rule", async () => {
    const f = vi.fn().mockResolvedValue(json(rule));
    expect((await getRule("r/1", asFetch(f))).name).toBe("uploads");
    expect(f.mock.calls[0]?.[0]).toBe(`${EVENTS_PROXY_BASE}/api/rules/r%2F1`);
  });

  it("creates a rule with a fixed body per action type", async () => {
    const f = vi.fn().mockResolvedValue(json(rule));
    await createRule(
      comp,
      {
        name: " n ",
        description: " d ",
        condition: cond,
        enabled: true,
        actions: [
          { type: "ONS", targetId: " t1 ", enabled: true, description: " x " },
          { type: "OSS", targetId: "s1", enabled: false },
          { type: "FAAS", targetId: "f1", enabled: true },
        ],
      },
      asFetch(f),
    );
    const init = f.mock.calls[0]?.[1] as RequestInit;
    expect(init.method).toBe("POST");
    expect(JSON.parse(String(init.body))).toEqual({
      compartmentId: comp,
      displayName: "n",
      description: "d",
      condition: cond,
      isEnabled: true,
      actions: {
        actions: [
          { actionType: "ONS", isEnabled: true, topicId: "t1", description: "x" },
          { actionType: "OSS", isEnabled: false, streamId: "s1" },
          { actionType: "FAAS", isEnabled: true, functionId: "f1" },
        ],
      },
    });
  });

  it("surfaces backend validation errors", async () => {
    const f = vi
      .fn()
      .mockResolvedValue(json({ code: "InvalidParameter", message: "condition bad" }, 400));
    await expect(
      createRule(
        comp,
        { name: "n", description: "", condition: "{", enabled: true, actions: [] },
        asFetch(f),
      ),
    ).rejects.toThrow("condition bad");
  });

  it("toggles and deletes", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(json(rule))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    await setRuleEnabled("r1", false, asFetch(f));
    const put = f.mock.calls[0]?.[1] as RequestInit;
    expect(put.method).toBe("PUT");
    expect(JSON.parse(String(put.body))).toEqual({ isEnabled: false });
    await deleteRule("r1", asFetch(f));
    const del = f.mock.calls[1]?.[1] as RequestInit;
    expect(del.method).toBe("DELETE");
  });

  it("lists deliveries newest first", async () => {
    const f = vi.fn().mockResolvedValue(json([{ eventId: "old" }, { eventId: "new" }]));
    expect((await listDeliveries(asFetch(f))).map((d) => d.eventId)).toEqual(["new", "old"]);
    expect(f.mock.calls[0]?.[0]).toBe(`${EVENTS_PROXY_BASE}/deliveries`);
  });
});

const reply = (url: string) => {
  if (url.includes("/notifications/"))
    return json([
      { topicId: "t1", name: "alerts", lifecycleState: "ACTIVE" },
      { topicId: "t2", name: "pending", lifecycleState: "CREATING" },
    ]);
  if (url.includes("/streaming/"))
    return json({
      streams: [
        { id: "s1", name: "feed", lifecycleState: "ACTIVE" },
        { id: "s2", name: "busy", lifecycleState: "CREATING" },
      ],
    });
  if (url.endsWith("/applications")) return json({ applications: [{ id: "app1" }] });
  return json({
    functions: [
      { id: "f1", displayName: "fn", lifecycleState: "ACTIVE" },
      { id: "f2", displayName: "gone", lifecycleState: "DELETED" },
    ],
  });
};

describe("target options", () => {
  it("lists topics, streams and functions", async () => {
    const f = vi.fn((url: string) => Promise.resolve(reply(url)));
    expect(await listTopicOptions(comp, asFetch(f))).toEqual([{ id: "t1", label: "alerts" }]);
    expect(await listStreamOptions(asFetch(f))).toEqual([{ id: "s1", label: "feed" }]);
    expect(await listFunctionOptions(asFetch(f))).toEqual([{ id: "f1", label: "fn" }]);
  });

  it("is empty when a proxy is unavailable", async () => {
    const f = vi.fn().mockResolvedValue(json({ error: { message: "down" } }, 503));
    expect(await listTopicOptions(comp, asFetch(f))).toEqual([]);
    expect(await listStreamOptions(asFetch(f))).toEqual([]);
    expect(await listFunctionOptions(asFetch(f))).toEqual([]);
  });
});
