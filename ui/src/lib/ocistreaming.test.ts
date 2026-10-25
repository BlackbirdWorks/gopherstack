import { describe, expect, it, vi } from "vitest";

import {
  STREAM_PROXY_BASE,
  listPartitions,
  listPools,
  listStreams,
  parseMessage,
  parseStream,
  readMessages,
  showBytes,
} from "./ocistreaming";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

function reply(f: (url: string) => Response) {
  return vi.fn((...a: unknown[]) => Promise.resolve(f(...(a as [string]))));
}

describe("parsing", () => {
  it("parses a stream", () => {
    expect(
      parseStream({ id: "s", name: "events", partitions: 2, streamPoolId: "p" }),
    ).toMatchObject({
      name: "events",
      partitions: 2,
      poolId: "p",
    });
  });

  it("shows text values and base64-prefixes binary ones", () => {
    const text = parseMessage({
      offset: 4,
      key: { isText: true, text: "k" },
      value: { isText: true, text: "v" },
    });
    expect(showBytes(text.value)).toBe("v");
    const bin = parseMessage({ value: { isText: false, base64: "/w==" } });
    expect(showBytes(bin.value)).toBe("base64:/w==");
    expect(showBytes(bin.key)).toBe("");
  });
});

describe("requests", () => {
  it("lists pools, streams and partitions", async () => {
    const fetchFn = reply((url: string) => {
      if (url.endsWith("/pools")) return json({ pools: [{ id: "p", name: "pool" }] });
      if (url.endsWith("/partitions"))
        return json({ partitions: [{ partition: "0", base: 2, end: 9 }] });
      return json({ streams: [{ id: "s", name: "events" }] });
    }) as unknown as typeof fetch;
    expect((await listPools(fetchFn))[0]?.name).toBe("pool");
    expect((await listStreams("p", fetchFn))[0]?.name).toBe("events");
    expect(await listPartitions("s", fetchFn)).toEqual([{ partition: "0", base: 2, end: 9 }]);
  });

  it("reads the newest messages by default and an explicit offset on request", async () => {
    const fetchFn = reply(() =>
      json({
        messages: [{ offset: 7, value: { isText: true, text: "x" } }],
        base: 0,
        end: 8,
        start: 7,
      }),
    );
    const got = await readMessages(
      "s1",
      { partition: "0", limit: 5 },
      fetchFn as unknown as typeof fetch,
    );
    expect(fetchFn.mock.calls[0]?.[0]).toBe(
      `${STREAM_PROXY_BASE}/streams/s1/messages?partition=0&limit=5`,
    );
    expect(got).toMatchObject({ base: 0, end: 8, start: 7 });
    await readMessages("s1", { partition: "1", offset: 3 }, fetchFn as unknown as typeof fetch);
    expect(String(fetchFn.mock.calls[1]?.[0])).toContain("offset=3");
  });

  it("surfaces proxy errors", async () => {
    const fetchFn = reply(() =>
      json({ error: { code: 400, status: "INVALID_ARGUMENT", message: "bad" } }, 400),
    );
    await expect(
      readMessages("s", { partition: "9" }, fetchFn as unknown as typeof fetch),
    ).rejects.toMatchObject({ code: "INVALID_ARGUMENT" });
  });
});
