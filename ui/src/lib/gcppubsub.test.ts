import { describe, expect, it, vi } from "vitest";

import { GcpRestError, base64ToUtf8, durationToSeconds, isBase64, utf8ToBase64 } from "./gcprest";
import {
  PUBSUB_PROXY_BASE,
  createTopic,
  listTopics,
  parsePulled,
  parseSubscription,
  publish,
  pull,
  subscriptionBody,
  validateResourceId,
} from "./gcppubsub";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });

describe("validateResourceId", () => {
  it.each([
    ["", "name is required"],
    ["ab", "must be 3 to 255 characters"],
    ["1abc", "must start with a letter"],
    ["goog-x", 'must not start with "goog"'],
    ["bad name", "may contain letters, digits and -_.~+%"],
    ["good-name", null],
  ])("%s", (id, want) => {
    expect(validateResourceId(id)).toBe(want);
  });
});

describe("encoding", () => {
  it("round-trips utf8", () => {
    expect(base64ToUtf8(utf8ToBase64("héllo"))).toBe("héllo");
  });
  it("rejects non-utf8", () => {
    expect(base64ToUtf8("/w==")).toBeNull();
  });
  it.each([
    ["aGk=", true],
    ["a", false],
    ["***", false],
  ])("isBase64 %s", (s, want) => {
    expect(isBase64(s)).toBe(want);
  });
  it("strips the duration suffix", () => {
    expect(durationToSeconds("600s")).toBe("600");
  });
});

describe("requests", () => {
  it("builds subscription body with dead letter and push", () => {
    const body = subscriptionBody({
      topic: "projects/p/topics/t",
      ackDeadlineSeconds: 20,
      filter: "",
      enableMessageOrdering: true,
      pushEndpoint: "http://x/h",
      deadLetterTopic: "projects/p/topics/dl",
      maxDeliveryAttempts: 5,
      labels: {},
    });
    expect(body).toEqual({
      topic: "projects/p/topics/t",
      ackDeadlineSeconds: 20,
      enableMessageOrdering: true,
      pushConfig: { pushEndpoint: "http://x/h" },
      deadLetterPolicy: { deadLetterTopic: "projects/p/topics/dl", maxDeliveryAttempts: 5 },
    });
  });

  it("creates a topic with PUT and a retention duration", async () => {
    const f = vi.fn().mockResolvedValue(json({ name: "projects/p/topics/t1" }));
    const t = await createTopic("p", "t1", { labels: { a: "b" }, retentionSeconds: "600" }, f);
    expect(t.id).toBe("t1");
    expect(f).toHaveBeenCalledWith(
      `${PUBSUB_PROXY_BASE}/projects/p/topics/t1`,
      expect.objectContaining({
        method: "PUT",
        body: JSON.stringify({ labels: { a: "b" }, messageRetentionDuration: "600s" }),
      }),
    );
  });

  it("publishes base64 data and returns ids", async () => {
    const f = vi.fn().mockResolvedValue(json({ messageIds: ["7"] }));
    const ids = await publish("p", "t1", { data: "hi", attributes: {}, orderingKey: "k" }, f);
    expect(ids).toEqual(["7"]);
    const init = f.mock.calls[0]?.[1] as RequestInit;
    expect(JSON.parse(String(init.body))).toEqual({
      messages: [{ data: "aGk=", orderingKey: "k" }],
    });
  });

  it("pulls and decodes", async () => {
    const f = vi
      .fn()
      .mockResolvedValue(
        json({ receivedMessages: [{ ackId: "a1", message: { data: "aGk=", messageId: "9" } }] }),
      );
    const msgs = await pull("p", "s1", 5, f);
    expect(msgs[0]).toMatchObject({ ackId: "a1", data: "hi", messageId: "9" });
  });

  it("pages topic lists", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(
        json({ topics: [{ name: "projects/p/topics/a" }], nextPageToken: "n" }),
      )
      .mockResolvedValueOnce(json({ topics: [{ name: "projects/p/topics/b" }] }));
    const ts = await listTopics("p", f);
    expect(ts.map((t) => t.id)).toEqual(["a", "b"]);
  });

  it("surfaces REST errors", async () => {
    const f = vi
      .fn()
      .mockResolvedValue(json({ error: { message: "nope", status: "NOT_FOUND" } }, 404));
    await expect(listTopics("p", f)).rejects.toMatchObject({
      name: "GcpRestError",
      httpStatus: 404,
      code: "NOT_FOUND",
    });
    expect(new GcpRestError(0, "", "x").message).toBe("x");
  });
});

describe("parsers", () => {
  it("parses a subscription", () => {
    const s = parseSubscription({
      name: "projects/p/subscriptions/s",
      topic: "projects/p/topics/t",
      ackDeadlineSeconds: 30,
      deadLetterPolicy: { deadLetterTopic: "projects/p/topics/dl", maxDeliveryAttempts: 6 },
    });
    expect(s.id).toBe("s");
    expect(s.topicId).toBe("t");
    expect(s.deadLetter).toEqual({
      deadLetterTopic: "projects/p/topics/dl",
      maxDeliveryAttempts: 6,
    });
  });
  it("keeps base64 for non-utf8 pulled data", () => {
    expect(parsePulled({ ackId: "a", message: { data: "/w==" } }).data).toBe("");
  });
});
