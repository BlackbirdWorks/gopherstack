import { describe, expect, it, vi } from "vitest";

import {
  EG_PROXY_BASE,
  SCHEMA_CLOUDEVENT,
  listTopics,
  publishEvents,
  sampleEvent,
  validateEventJson,
} from "./azureeventgrid";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

describe("validateEventJson", () => {
  it.each([
    ["", "Event JSON is required"],
    ["{", "Not valid JSON"],
    ["[]", "Provide at least one event"],
    ["[1]", "Each event must be a JSON object"],
    ['"x"', "Each event must be a JSON object"],
    ["{}", ""],
    ['[{"a":1},{"b":2}]', ""],
  ])("%j", (text, want) => expect(validateEventJson(text)).toBe(want));
});

describe("sampleEvent", () => {
  it.each([
    ["EventGridSchema", ["id", "subject", "eventType", "eventTime", "dataVersion"]],
    [SCHEMA_CLOUDEVENT, ["specversion", "id", "source", "type"]],
  ])("%s has its required fields", (schema, fields) => {
    const ev = sampleEvent(schema);
    for (const f of fields) expect(ev[f]).toBeTruthy();
  });
});

describe("client", () => {
  it("lists topics with subscriptions", async () => {
    const f = vi.fn().mockResolvedValue(
      json({
        topics: [
          {
            name: "t1",
            inputSchema: "EventGridSchema",
            subscriptions: [{ name: "s1", endpointType: "WebHook", state: "Succeeded" }],
          },
        ],
      }),
    );
    const out = await listTopics(f);
    expect(out[0].subscriptions[0]).toMatchObject({ name: "s1", endpointType: "WebHook" });
  });

  it("publishes the parsed JSON and reports the count", async () => {
    const f = vi.fn().mockImplementation(() => Promise.resolve(json({ published: 2 })));
    expect(await publishEvents("t 1", '[{"a":1},{"a":2}]', f)).toBe(2);
    expect(f.mock.calls[0][0]).toBe(`${EG_PROXY_BASE}/topics/t%201/events`);
    expect(JSON.parse(f.mock.calls[0][1].body as string)).toEqual([{ a: 1 }, { a: 2 }]);
  });
});
