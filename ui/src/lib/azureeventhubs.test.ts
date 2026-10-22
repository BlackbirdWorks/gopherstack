import { describe, expect, it, vi } from "vitest";

import {
  EH_PROXY_BASE,
  listNamespaces,
  parseEvent,
  peekEvents,
  sendEvent,
  validateSend,
} from "./azureeventhubs";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

describe("validateSend", () => {
  it.each([
    [{ body: "", partitionKey: "", partition: "" }, "Event body is required"],
    [{ body: "x", partitionKey: "", partition: "" }, ""],
    [{ body: "x", partitionKey: "k", partition: "" }, ""],
    [{ body: "x", partitionKey: "", partition: "9" }, "Partition must be 0-3"],
    [{ body: "x", partitionKey: "", partition: "-1" }, "Partition must be 0-3"],
    [{ body: "x", partitionKey: "", partition: "2" }, ""],
    [
      { body: "x", partitionKey: "k", partition: "2" },
      "Choose either a partition or a partition key, not both",
    ],
    [
      { body: "x", partitionKey: "k".repeat(129), partition: "" },
      "Partition key is at most 128 characters",
    ],
  ])("%j", (input, want) => expect(validateSend(input, 4)).toBe(want));
});

describe("client", () => {
  it("parses namespaces and hubs", async () => {
    const f = vi.fn().mockResolvedValue(
      json({
        namespaces: [
          {
            name: "ns1",
            hubs: [
              {
                name: "h",
                partitionIds: ["0", "1"],
                consumerGroups: ["$Default"],
                retentionHours: 24,
              },
            ],
          },
        ],
      }),
    );
    const out = await listNamespaces(f);
    expect(f).toHaveBeenCalledWith(`${EH_PROXY_BASE}/namespaces`, { method: "GET" });
    expect(out[0].hubs[0]).toEqual({
      name: "h",
      partitionIds: ["0", "1"],
      consumerGroups: ["$Default"],
      retentionHours: 24,
    });
  });

  it("peeks with an escaped path and limit", async () => {
    const f = vi
      .fn()
      .mockImplementation(() => Promise.resolve(json({ events: [{ sequence: 3, body: "hi" }] })));
    const out = await peekEvents("n s", "h", "1", 5, f);
    expect(f.mock.calls[0][0]).toBe(
      `${EH_PROXY_BASE}/namespaces/n%20s/hubs/h/partitions/1/events?limit=5`,
    );
    expect(out[0].body).toBe("hi");
  });

  it("sends the partition only when chosen", async () => {
    const f = vi.fn().mockImplementation(() => Promise.resolve(json({ partition: 2 })));
    expect(await sendEvent("n", "h", { body: "b", partitionKey: "", partition: "2" }, f)).toBe(2);
    expect(JSON.parse(f.mock.calls[0][1].body as string)).toEqual({
      body: "b",
      partitionKey: "",
      partition: 2,
    });
    await sendEvent("n", "h", { body: "b", partitionKey: "k", partition: "" }, f);
    expect(JSON.parse(f.mock.calls[1][1].body as string)).toEqual({ body: "b", partitionKey: "k" });
  });

  it("surfaces proxy errors", async () => {
    const f = vi
      .fn()
      .mockResolvedValue(
        json({ error: { message: "event hub not found", status: "NOT_FOUND" } }, 404),
      );
    await expect(peekEvents("n", "h", "0", 1, f)).rejects.toThrow("event hub not found");
  });

  it("parseEvent tolerates missing fields", () => {
    expect(parseEvent({}).body).toBe("");
  });
});
