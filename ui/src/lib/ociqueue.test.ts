import { describe, expect, it, vi } from "vitest";

import {
  QUEUE_PROXY_BASE,
  formatBytes,
  formatSeconds,
  getQueue,
  getStats,
  listCompartments,
  listQueues,
  parsePeeked,
  parseQueue,
  parseStats,
  peekMessages,
  purgeQueue,
} from "./ociqueue";

const json = (body: unknown, status = 200, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
const asFetch = (f: unknown) => f as typeof fetch;

const comp = "ocid1.compartment.oc1..aaaa";
const queue = (id: string, name: string) => ({
  id,
  displayName: name,
  lifecycleState: "ACTIVE",
  messagesEndpoint: "http://localhost:8156",
  retentionInSeconds: 86400,
  visibilityInSeconds: 30,
  timeoutInSeconds: 30,
  deadLetterQueueDeliveryCount: 5,
});

describe("parsing", () => {
  it("parses a queue", () => {
    expect(parseQueue(queue("q1", "jobs"))).toMatchObject({
      id: "q1",
      name: "jobs",
      state: "ACTIVE",
      retention: 86400,
      dlqDeliveryCount: 5,
    });
  });

  it("parses stats for the queue and its dead letter queue", () => {
    expect(
      parseStats({
        queue: { visibleMessages: 3, inFlightMessages: 1, sizeInBytes: 40 },
        dlq: { visibleMessages: 2 },
      }),
    ).toEqual({
      queue: { visible: 3, inFlight: 1, sizeBytes: 40 },
      dlq: { visible: 2, inFlight: 0, sizeBytes: 0 },
    });
  });

  it("parses a peeked message without a receipt", () => {
    const m = parsePeeked({
      id: 7,
      content: "hello",
      deliveryCount: 2,
      inFlight: true,
      channelId: "c",
    });
    expect(m).toMatchObject({
      id: 7,
      content: "hello",
      deliveryCount: 2,
      inFlight: true,
      channel: "c",
    });
    expect(m).not.toHaveProperty("receipt");
  });

  it.each([
    [10, "10 B"],
    [2048, "2.0 KB"],
    [3 * 1024 * 1024, "3.0 MB"],
  ])("formats %d bytes", (n, want) => {
    expect(formatBytes(n)).toBe(want);
  });

  it.each([
    [30, "30s"],
    [90, "1m 30s"],
    [3600, "1h"],
    [86400, "1d"],
  ])("formats %d seconds", (n, want) => {
    expect(formatSeconds(n)).toBe(want);
  });
});

describe("client", () => {
  it("lists compartments", async () => {
    const fetchFn = vi.fn(() => Promise.resolve(json({ compartments: [comp] })));
    expect(await listCompartments(asFetch(fetchFn))).toEqual([comp]);
  });

  it("follows opc-next-page when listing queues", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce(json({ items: [queue("q1", "a")] }, 200, { "opc-next-page": "p2" }))
      .mockResolvedValueOnce(json({ items: [queue("q2", "b")] }));
    const out = await listQueues(comp, asFetch(fetchFn));
    expect(out.map((q) => q.id)).toEqual(["q1", "q2"]);
    expect(fetchFn.mock.calls.map((c) => c[0])).toEqual([
      `${QUEUE_PROXY_BASE}/api/queues?compartmentId=${encodeURIComponent(comp)}`,
      `${QUEUE_PROXY_BASE}/api/queues?compartmentId=${encodeURIComponent(comp)}&page=p2`,
    ]);
  });

  it("reads a queue, its stats and a peek of the dead letter queue", async () => {
    const fetchFn = vi.fn((url: string) =>
      Promise.resolve(
        json(
          url.endsWith("/stats")
            ? { queue: { visibleMessages: 1 }, dlq: {} }
            : url.includes("/peek")
              ? { messages: [{ id: 1, content: "x" }] }
              : queue("q1", "jobs"),
        ),
      ),
    );
    expect((await getQueue("q1", asFetch(fetchFn))).name).toBe("jobs");
    expect((await getStats("q1", asFetch(fetchFn))).queue.visible).toBe(1);
    expect(await peekMessages("q1", true, 5, asFetch(fetchFn))).toHaveLength(1);
    expect(fetchFn.mock.calls[2]?.[0]).toBe(
      `${QUEUE_PROXY_BASE}/api/queues/q1/peek?limit=5&dlq=true`,
    );
  });

  it("posts a purge with its type", async () => {
    const fetchFn = vi.fn((_url: string, _init?: RequestInit) =>
      Promise.resolve(new Response(null, { status: 202 })),
    );
    await purgeQueue("q1", "BOTH", asFetch(fetchFn));
    const [url, init] = fetchFn.mock.calls[0] ?? [];
    expect(url).toBe(`${QUEUE_PROXY_BASE}/api/queues/q1/actions/purge`);
    expect(JSON.parse(String((init as RequestInit).body))).toEqual({ purgeType: "BOTH" });
  });

  it("surfaces a service error", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValue(json({ code: "NotFound", message: "no such queue" }, 404));
    await expect(getQueue("x", asFetch(fetchFn))).rejects.toMatchObject({
      httpStatus: 404,
      code: "NotFound",
    });
  });
});
