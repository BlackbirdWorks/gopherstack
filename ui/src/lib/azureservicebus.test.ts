import { describe, expect, it } from "vitest";

import {
  BusError,
  abandonMessage,
  brokerProperties,
  completeMessage,
  createQueue,
  createSubscription,
  deleteEntity,
  descriptionBody,
  entityUrl,
  formatDuration,
  listQueues,
  listSubscriptions,
  listTopics,
  messagesUrl,
  parseDuration,
  parseReceived,
  receiveAndDelete,
  receiveMessage,
  receiveMessages,
  resubmit,
  sendMessage,
  validateEntityName,
  validateMessageId,
  validateWhole,
  type ReceivedMessage,
} from "./azureservicebus";

const recorder = (res: (n: number) => Response) => {
  const calls: { url: string; init: RequestInit }[] = [];
  const fn = ((url: string, init: RequestInit) => {
    calls.push({ url, init: init ?? {} });
    return Promise.resolve(res(calls.length));
  }) as unknown as typeof fetch;
  return { calls, fn };
};

const feed = (entries: string) =>
  `<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom"><title>t</title>${entries}</feed>`;

const locked = (id = "m1", token = "tok") =>
  new Response("hello", {
    status: 201,
    headers: {
      "Content-Type": "text/plain",
      BrokerProperties: JSON.stringify({ MessageId: id, LockToken: token, DeliveryCount: 1 }),
    },
  });

const msg: ReceivedMessage = parseReceived(locked().headers, "hello");

describe("urls", () => {
  it.each([
    [{ entity: "q" }, "", "", "/dashboard/api/v1/azure/servicebus/q/messages"],
    [{ entity: "q" }, "head", "", "/dashboard/api/v1/azure/servicebus/q/messages/head"],
    [
      { entity: "t", subscription: "s", deadLetter: true },
      "head",
      "",
      "/dashboard/api/v1/azure/servicebus/t/subscriptions/s/$DeadLetterQueue/messages/head",
    ],
    [{ entity: "a b" }, "", "x=1", "/dashboard/api/v1/azure/servicebus/a%20b/messages?x=1"],
  ])("messagesUrl %j", (target, tail, query, want) => {
    expect(messagesUrl(target, tail, query)).toBe(want);
  });

  it("entityUrl", () => {
    expect(entityUrl("t", "s")).toBe("/dashboard/api/v1/azure/servicebus/t/subscriptions/s");
  });
});

describe("durations", () => {
  it.each([
    ["PT30S", 30],
    ["PT1M", 60],
    ["P14D", 1209600],
    ["P1DT2H3M4S", 93784],
    ["", null],
    ["P", null],
    ["PT", null],
    ["nope", null],
  ])("parseDuration %s", (iso, want) => {
    expect(parseDuration(iso)).toBe(want);
  });

  it.each([
    [null, "—"],
    [0, "0s"],
    [90, "1m 30s"],
    [1209600, "14d"],
  ])("formatDuration %s", (s, want) => {
    expect(formatDuration(s)).toBe(want);
  });
});

describe("listing", () => {
  it("parses queues, topics and subscriptions", async () => {
    const q = recorder(
      () =>
        new Response(
          feed(
            `<entry><title>q1</title><content><QueueDescription xmlns="x"><LockDuration>PT1M</LockDuration><DefaultMessageTimeToLive>P14D</DefaultMessageTimeToLive><MaxDeliveryCount>10</MaxDeliveryCount></QueueDescription></content></entry>`,
          ),
        ),
    );
    expect(await listQueues(q.fn)).toEqual([
      { name: "q1", lockSeconds: 60, ttlSeconds: 1209600, maxDeliveryCount: 10 },
    ]);
    expect(q.calls[0].url).toBe("/dashboard/api/v1/azure/servicebus/$Resources/Queues");

    const t = recorder(
      () =>
        new Response(
          feed(
            `<entry><title>t1</title><content><TopicDescription xmlns="x"><DefaultMessageTimeToLive>PT5S</DefaultMessageTimeToLive></TopicDescription></content></entry>`,
          ),
        ),
    );
    expect(await listTopics(t.fn)).toEqual([{ name: "t1", ttlSeconds: 5 }]);

    const s = recorder(
      () =>
        new Response(
          feed(
            `<entry><title>s1</title><content><SubscriptionDescription xmlns="x"><LockDuration>PT30S</LockDuration><MaxDeliveryCount>3</MaxDeliveryCount></SubscriptionDescription></content></entry>`,
          ),
        ),
    );
    expect(await listSubscriptions("t1", s.fn)).toEqual([
      { name: "s1", lockSeconds: 30, maxDeliveryCount: 3 },
    ]);
    expect(s.calls[0].url).toBe("/dashboard/api/v1/azure/servicebus/t1/subscriptions");
  });

  it("rejects malformed xml", async () => {
    await expect(listQueues(recorder(() => new Response("<a")).fn)).rejects.toBeInstanceOf(
      BusError,
    );
  });
});

describe("create/delete", () => {
  it("builds the description body per kind", () => {
    const cfg = { lockSeconds: 30, maxDeliveryCount: 4, ttlSeconds: 60 };
    expect(descriptionBody("Queue", cfg)).toContain("<LockDuration>PT30S</LockDuration>");
    expect(descriptionBody("Queue", cfg)).toContain("<MaxDeliveryCount>4</MaxDeliveryCount>");
    expect(descriptionBody("Topic", cfg)).not.toContain("LockDuration");
    expect(descriptionBody("Topic", cfg)).toContain("TopicDescription");
    expect(descriptionBody("Subscription", cfg)).not.toContain("DefaultMessageTimeToLive");
  });

  it("creates and reports existence", async () => {
    const r = recorder((n) => new Response("", { status: n === 1 ? 201 : 200 }));
    expect(await createQueue("q", {}, r.fn)).toBe(true);
    expect(await createQueue("q", {}, r.fn)).toBe(false);
    expect(r.calls[0].init.method).toBe("PUT");
  });

  it("creates a subscription and deletes an entity", async () => {
    const r = recorder(() => new Response("", { status: 201 }));
    await createSubscription("t", "s", {}, r.fn);
    await deleteEntity("t", r.fn);
    expect(r.calls[0].url).toBe("/dashboard/api/v1/azure/servicebus/t/subscriptions/s");
    expect(r.calls[1].init.method).toBe("DELETE");
  });

  it("surfaces JSON errors", async () => {
    const r = recorder(
      () => new Response(JSON.stringify({ Code: "NotFound", Message: "gone" }), { status: 404 }),
    );
    await expect(deleteEntity("x", r.fn)).rejects.toMatchObject({
      status: 404,
      code: "NotFound",
      message: "gone",
    });
  });

  it("wraps network failures", async () => {
    const fn = (() => Promise.reject(new Error("down"))) as unknown as typeof fetch;
    await expect(deleteEntity("x", fn)).rejects.toMatchObject({ code: "NetworkError" });
  });
});

describe("send", () => {
  it("omits BrokerProperties when empty", async () => {
    const r = recorder(() => new Response("", { status: 201 }));
    await sendMessage("q", "hi", {}, r.fn);
    expect(r.calls[0].init.headers).toEqual({ "Content-Type": "text/plain" });
    expect(r.calls[0].init.body).toBe("hi");
  });

  it("encodes options", async () => {
    const r = recorder(() => new Response("", { status: 201 }));
    await sendMessage(
      "q",
      "{}",
      { contentType: "application/json", messageId: "a", sessionId: "s", ttlSeconds: 30 },
      r.fn,
    );
    const h = r.calls[0].init.headers as Record<string, string>;
    expect(h["Content-Type"]).toBe("application/json");
    expect(JSON.parse(h["BrokerProperties"])).toEqual({
      MessageId: "a",
      SessionId: "s",
      TimeToLive: 30,
    });
  });

  it("brokerProperties skips blanks", () => {
    expect(brokerProperties({ label: "", correlationId: "c" })).toEqual({ CorrelationId: "c" });
  });
});

describe("receive", () => {
  it("returns null on 204", async () => {
    const r = recorder(() => new Response(null, { status: 204 }));
    expect(await receiveMessage({ entity: "q" }, r.fn)).toBeNull();
  });

  it("parses a locked message", async () => {
    const r = recorder(() => locked());
    const m = await receiveMessage({ entity: "q" }, r.fn);
    expect(m).toMatchObject({ messageId: "m1", lockToken: "tok", deliveryCount: 1, body: "hello" });
    expect(r.calls[0].init.method).toBe("POST");
  });

  it("tolerates a missing or broken header", () => {
    expect(parseReceived(new Headers({ BrokerProperties: "{" }), "b").messageId).toBe("");
    expect(parseReceived(new Headers(), "b").sequenceNumber).toBeNull();
  });

  it("stops once drained", async () => {
    const r = recorder((n) => (n <= 2 ? locked(`m${n}`) : new Response(null, { status: 204 })));
    const got = await receiveMessages({ entity: "q" }, 5, r.fn);
    expect(got.map((m) => m.messageId)).toEqual(["m1", "m2"]);
    expect(r.calls).toHaveLength(3);
  });

  it("completes and abandons by lock token", async () => {
    const r = recorder(() => new Response("", { status: 200 }));
    await completeMessage({ entity: "q" }, msg, r.fn);
    await abandonMessage({ entity: "t", subscription: "s" }, msg, r.fn);
    expect(r.calls[0]).toMatchObject({
      url: "/dashboard/api/v1/azure/servicebus/q/messages/m1/tok",
      init: { method: "DELETE" },
    });
    expect(r.calls[1].url).toBe(
      "/dashboard/api/v1/azure/servicebus/t/subscriptions/s/messages/m1/tok",
    );
    expect(r.calls[1].init.method).toBe("PUT");
  });

  it("receive-and-delete completes what it received", async () => {
    const r = recorder((n) => (n === 1 ? locked() : new Response("", { status: 200 })));
    const m = await receiveAndDelete({ entity: "q" }, r.fn);
    expect(m?.messageId).toBe("m1");
    expect(r.calls[1].init.method).toBe("DELETE");
  });

  it("receive-and-delete on empty does not complete", async () => {
    const r = recorder(() => new Response(null, { status: 204 }));
    expect(await receiveAndDelete({ entity: "q" }, r.fn)).toBeNull();
    expect(r.calls).toHaveLength(1);
  });

  it("resubmits to the queue then completes from the dead-letter queue", async () => {
    const r = recorder(() => new Response("", { status: 201 }));
    await resubmit("q", msg, r.fn);
    expect(r.calls[0].url).toBe("/dashboard/api/v1/azure/servicebus/q/messages");
    expect(r.calls[1].url).toBe(
      "/dashboard/api/v1/azure/servicebus/q/$DeadLetterQueue/messages/m1/tok",
    );
    expect(r.calls[1].init.method).toBe("DELETE");
  });
});

describe("validation", () => {
  it.each([
    ["", "Name is required"],
    ["-a", "Use letters, numbers, '.', '_' and '-', starting with a letter or number"],
    ["a/b", "Use letters, numbers, '.', '_' and '-', starting with a letter or number"],
    ["My.Queue_1", null],
  ])("validateEntityName %s", (n, want) => {
    expect(validateEntityName(n)).toBe(want);
  });

  it.each([
    ["", null],
    ["5", null],
    ["x", "Must be a whole number"],
    ["0", "Must be between 1 and 10"],
    ["11", "Must be between 1 and 10"],
  ])("validateWhole %s", (v, want) => {
    expect(validateWhole(v, 1, 10)).toBe(want);
  });

  it.each([
    ["", null],
    ["abc", null],
    ["a/b", "Must not contain / \\ ? # or %"],
  ])("validateMessageId %s", (v, want) => {
    expect(validateMessageId(v)).toBe(want);
  });

  it("entity url encodes", () => {
    expect(entityUrl("a b")).toContain("a%20b");
  });
});
