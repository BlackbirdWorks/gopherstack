import { describe, expect, it, vi } from "vitest";

import {
  EG_PROXY_BASE,
  deliverySummary,
  filterSummary,
  getOverview,
  listTopics,
  parseSubscription,
  publishTestEvent,
  revealKeys,
  validateTestEvent,
} from "./azureeventgrid";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });

const ev = { subject: "/s", eventType: "T", data: "{}", domainTopic: "" };

describe("validateTestEvent", () => {
  it.each([
    [{ ...ev, eventType: "" }, false, "Event type is required"],
    [{ ...ev, eventType: "t".repeat(129) }, false, "Event type is at most 128 characters"],
    [{ ...ev, subject: "s".repeat(257) }, false, "Subject is at most 256 characters"],
    [{ ...ev, data: "d".repeat((64 << 10) + 1) }, false, "Data exceeds 64 KiB"],
    [ev, true, "Domain topic is required"],
    [{ ...ev, domainTopic: "orders" }, true, ""],
    [ev, false, ""],
  ])("%j", (e, needs, want) => expect(validateTestEvent(e, needs)).toBe(want));
});

describe("summaries", () => {
  const base = parseSubscription({ name: "s", endpointType: "WebHook" });

  it("describes filters", () => {
    expect(filterSummary(base)).toBe("none");
    expect(
      filterSummary({
        ...base,
        includedEventTypes: ["A", "B"],
        subjectBeginsWith: "/x",
        advancedFilterCount: 2,
      }),
    ).toBe("types: A, B; subject starts /x; 2 advanced");
  });

  it("describes retry and dead-letter", () => {
    const s = { ...base, maxDeliveryAttempts: 5, eventTimeToLiveMinutes: 60 };
    expect(deliverySummary(s)).toBe("5 attempts, ttl 60 min, no dead-letter");
    expect(
      deliverySummary({
        ...s,
        deadLetterType: "StorageBlob",
        deadLetterContainer: "dead",
      }),
    ).toBe("5 attempts, ttl 60 min, dead-letter dead");
    expect(
      deliverySummary({
        ...base,
        queueMaxDelivery: 10,
        queueLockSeconds: 60,
        queueEventTimeToLive: "P7D",
      }),
    ).toBe("lock 60s, max 10 deliveries, ttl P7D");
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
    expect(out[0].subscriptions[0]).toMatchObject({
      name: "s1",
      endpointType: "WebHook",
    });
  });

  it("parses the overview of every resource kind", async () => {
    const f = vi.fn().mockResolvedValue(
      json({
        topics: [{ name: "t1", inputSchema: "EventGridSchema", subscriptions: [] }],
        domains: [
          {
            name: "d1",
            inputSchema: "EventGridSchema",
            autoCreateTopics: true,
            subscriptions: [],
            topics: [
              {
                name: "orders",
                subscriptions: [{ name: "s", endpointType: "WebHook" }],
              },
            ],
          },
        ],
        namespaces: [{ name: "n1", topics: [{ name: "nt", subscriptions: [] }] }],
        scopes: [{ id: "/subscriptions/x", subscriptions: [] }],
      }),
    );
    const o = await getOverview(f);
    expect(f.mock.calls[0][0]).toBe(`${EG_PROXY_BASE}/overview`);
    expect(o.domains[0]).toMatchObject({ name: "d1", autoCreateTopics: true });
    expect(o.domains[0].topics[0].subscriptions[0].name).toBe("s");
    expect(o.namespaces[0].topics[0].name).toBe("nt");
    expect(o.scopes[0].id).toBe("/subscriptions/x");
  });

  it.each([
    [{ kind: "topic", topic: "t 1" }, "/topics/t%201/events", false],
    [{ kind: "domain", domain: "d1" }, "/domains/d1/events", true],
    [{ kind: "namespace", namespace: "n1", topic: "nt" }, "/namespaces/n1/topics/nt/events", false],
  ] as const)("publishes %j", async (target, path, hasTopic) => {
    const f = vi.fn().mockImplementation(() => Promise.resolve(json({ published: 1 })));
    expect(await publishTestEvent(target, { ...ev, domainTopic: "orders" }, f)).toBe(1);
    expect(f.mock.calls[0][0]).toBe(`${EG_PROXY_BASE}${path}`);
    const body = JSON.parse(f.mock.calls[0][1].body as string) as Record<string, unknown>;
    expect(body).toEqual({
      subject: "/s",
      eventType: "T",
      data: "{}",
      ...(hasTopic ? { domainTopic: "orders" } : {}),
    });
  });

  it("reveals keys with a bodyless POST", async () => {
    const f = vi.fn().mockResolvedValue(json({ key1: "a", key2: "b" }));
    expect(await revealKeys("domains", "d 1", f)).toEqual({
      key1: "a",
      key2: "b",
    });
    expect(f.mock.calls[0][0]).toBe(`${EG_PROXY_BASE}/domains/d%201/keys/reveal`);
    expect(f.mock.calls[0][1].method).toBe("POST");
    expect(f.mock.calls[0][1].body).toBeUndefined();
  });
});
