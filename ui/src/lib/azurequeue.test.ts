import { describe, expect, it } from "vitest";

import {
  QueueError,
  clearMessages,
  createQueue,
  deleteMessage,
  escapeXml,
  fromBase64,
  listQueues,
  parseMessages,
  peekMessages,
  putMessage,
  queueUrl,
  receiveMessages,
  sendQuery,
  toBase64,
  validateQueueName,
  validateSeconds,
} from "./azurequeue";

const xml = (body: string, status = 200) => new Response(body, { status });

const recorder = (res: () => Response) => {
  const calls: { url: string; init: RequestInit }[] = [];
  const fn = ((url: string, init: RequestInit) => {
    calls.push({ url, init });
    return Promise.resolve(res());
  }) as unknown as typeof fetch;
  return { calls, fn };
};

describe("queueUrl", () => {
  it.each([
    ["acct", undefined, undefined, "", "/dashboard/api/v1/azure/queue/acct"],
    ["acct", "q1", undefined, "", "/dashboard/api/v1/azure/queue/acct/q1"],
    [
      "acct",
      "q1",
      "",
      "peekonly=true",
      "/dashboard/api/v1/azure/queue/acct/q1/messages?peekonly=true",
    ],
    ["acct", "q1", "id/1", "", "/dashboard/api/v1/azure/queue/acct/q1/messages/id%2F1"],
  ])("%s %s %s", (account, queue, id, query, want) => {
    expect(queueUrl(account, queue, id, query)).toBe(want);
  });
});

describe("parsers", () => {
  it("parses queue lists", async () => {
    const r = recorder(() =>
      xml(
        "<EnumerationResults><Queues><Queue><Name>a</Name></Queue><Queue><Name>b</Name></Queue></Queues></EnumerationResults>",
      ),
    );
    expect(await listQueues("acct", r.fn)).toEqual([{ name: "a" }, { name: "b" }]);
    expect(r.calls[0].url).toContain("comp=list");
  });

  it("parses messages with and without dequeue count", () => {
    const msgs = parseMessages(
      `<QueueMessagesList>
        <QueueMessage><MessageId>1</MessageId><InsertionTime>i</InsertionTime><ExpirationTime>e</ExpirationTime><PopReceipt>p</PopReceipt><TimeNextVisible>t</TimeNextVisible><DequeueCount>3</DequeueCount><MessageText>hi</MessageText></QueueMessage>
        <QueueMessage><MessageId>2</MessageId><InsertionTime>i</InsertionTime><ExpirationTime>e</ExpirationTime></QueueMessage>
      </QueueMessagesList>`,
    );
    expect(msgs[0]).toMatchObject({
      id: "1",
      popReceipt: "p",
      dequeueCount: 3,
      text: "hi",
    });
    expect(msgs[1]).toMatchObject({ id: "2", dequeueCount: null, text: "" });
  });

  it("rejects malformed xml", () => {
    expect(() => parseMessages("<a>")).toThrow(QueueError);
  });
});

describe("operations", () => {
  it("createQueue reports created vs existing", async () => {
    expect(
      await createQueue("a", "q", recorder(() => new Response(null, { status: 201 })).fn),
    ).toBe(true);
    expect(
      await createQueue("a", "q", recorder(() => new Response(null, { status: 204 })).fn),
    ).toBe(false);
  });

  it("putMessage escapes the body and sends options", async () => {
    const r = recorder(() =>
      xml(
        "<QueueMessagesList><QueueMessage><MessageId>m</MessageId></QueueMessage></QueueMessagesList>",
        201,
      ),
    );
    const m = await putMessage("a", "q", "<x & y>", { visibilityTimeout: 5, ttl: -1 }, r.fn);
    expect(m.id).toBe("m");
    expect(r.calls[0].url).toBe(
      "/dashboard/api/v1/azure/queue/a/q/messages?visibilitytimeout=5&messagettl=-1",
    );
    expect(r.calls[0].init.body).toBe(
      "<QueueMessage><MessageText>&lt;x &amp; y&gt;</MessageText></QueueMessage>",
    );
  });

  it("peek, receive, delete and clear build the right requests", async () => {
    const r = recorder(() => xml("<QueueMessagesList/>"));
    await peekMessages("a", "q", 5, r.fn);
    await receiveMessages("a", "q", 2, 60, r.fn);
    expect(r.calls[0].url).toContain("peekonly=true&numofmessages=5");
    expect(r.calls[1].url).toContain("numofmessages=2&visibilitytimeout=60");

    const d = recorder(() => new Response(null, { status: 204 }));
    await deleteMessage("a", "q", "m1", "a/b=", d.fn);
    await clearMessages("a", "q", d.fn);
    expect(d.calls[0].url).toBe(
      "/dashboard/api/v1/azure/queue/a/q/messages/m1?popreceipt=a%2Fb%3D",
    );
    expect(d.calls[0].init.method).toBe("DELETE");
    expect(d.calls[1].url).toBe("/dashboard/api/v1/azure/queue/a/q/messages");
  });

  it("surfaces service errors and network failures", async () => {
    const err = recorder(() =>
      xml(
        "<Error><Code>QueueNotFound</Code><Message>The specified queue does not exist.</Message></Error>",
        404,
      ),
    );
    await expect(peekMessages("a", "q", 1, err.fn)).rejects.toMatchObject({
      status: 404,
      code: "QueueNotFound",
    });
    const net = (() => Promise.reject(new Error("boom"))) as unknown as typeof fetch;
    await expect(listQueues("a", net)).rejects.toMatchObject({
      code: "NetworkError",
    });
  });
});

describe("helpers", () => {
  it.each([
    ["", "Name is required"],
    ["ab", "Must be 3-63 characters"],
    ["Abc", "Use lowercase letters, numbers and hyphens only"],
    ["-abc", "Must start and end with a letter or number"],
    ["abc-", "Must start and end with a letter or number"],
    ["a--bc", "Consecutive hyphens are not allowed"],
    ["my-queue1", null],
  ])("validateQueueName(%j)", (name, want) => {
    expect(validateQueueName(name)).toBe(want);
  });

  it.each([
    ["", 0, 10, false, null],
    ["5", 0, 10, false, null],
    ["11", 0, 10, false, "Must be between 0 and 10"],
    ["1.5", 0, 10, false, "Must be a whole number of seconds"],
    ["-1", 1, 10, true, null],
    ["-1", 1, 10, false, "Must be between 1 and 10"],
  ])("validateSeconds(%j)", (raw, min, max, never, want) => {
    expect(validateSeconds(raw, min, max, never)).toBe(want);
  });

  it("sendQuery omits unset options", () => {
    expect(sendQuery({})).toBe("");
    expect(sendQuery({ ttl: 60 })).toBe("messagettl=60");
  });

  it("escapeXml escapes markup characters", () => {
    expect(escapeXml(`<a href="x">'&'</a>`)).toBe(
      "&lt;a href=&quot;x&quot;&gt;&apos;&amp;&apos;&lt;/a&gt;",
    );
  });

  it("round-trips base64 and rejects non-base64", () => {
    expect(fromBase64(toBase64("héllo"))).toBe("héllo");
    expect(fromBase64("hello world")).toBeNull();
    expect(fromBase64("")).toBeNull();
  });
});
