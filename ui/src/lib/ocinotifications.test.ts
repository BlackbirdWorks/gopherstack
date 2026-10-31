import { describe, expect, it, vi } from "vitest";

import {
  NOTIF_PROXY_BASE,
  confirmSubscription,
  confirmationQuery,
  isHttpUrl,
  listDeliveries,
  listSubscriptions,
  listTopics,
  parseDelivery,
  parseSubscription,
  parseTopic,
  resendConfirmation,
} from "./ocinotifications";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const asFetch = (f: unknown) => f as typeof fetch;
const comp = "ocid1.compartment.oc1..aaaa";
const cq = `compartmentId=${encodeURIComponent(comp)}`;

describe("parsing", () => {
  it("parses a topic", () => {
    expect(
      parseTopic({ topicId: "t1", name: "alerts", lifecycleState: "ACTIVE", description: "d" }),
    ).toMatchObject({ id: "t1", name: "alerts", state: "ACTIVE", description: "d" });
  });

  it("parses a subscription with its epoch-millisecond creation time", () => {
    expect(
      parseSubscription({
        id: "s1",
        topicId: "t1",
        protocol: "EMAIL",
        endpoint: "a@example.com",
        lifecycleState: "PENDING",
        createdTime: 1_700_000_000_000,
      }),
    ).toMatchObject({
      protocol: "EMAIL",
      endpoint: "a@example.com",
      state: "PENDING",
      created: "2023-11-14T22:13:20.000Z",
    });
  });

  it("parses a delivery", () => {
    expect(
      parseDelivery({
        id: "d1",
        kind: "CONFIRMATION",
        status: "RECORDED",
        attempts: 1,
        topicName: "alerts",
      }),
    ).toMatchObject({ id: "d1", kind: "CONFIRMATION", status: "RECORDED", attempts: 1 });
  });

  it.each([
    ["http://localhost:8157/x", true],
    ["https://example.com", true],
    ["javascript:alert(1)", false],
    ["", false],
  ])("isHttpUrl %j", (u, want) => {
    expect(isHttpUrl(u)).toBe(want);
  });

  it("extracts the confirmation query", () => {
    expect(
      confirmationQuery("http://h/20181201/subscriptions/s1/confirmation?token=abc&protocol=EMAIL"),
    ).toBe("token=abc&protocol=EMAIL");
    expect(confirmationQuery("not a url")).toBe("");
  });
});

describe("client", () => {
  it("lists topics and subscriptions of a compartment", async () => {
    const fetchFn = vi.fn((url: string) =>
      Promise.resolve(
        json(
          url.includes("/topics") ? [{ topicId: "t1", name: "a" }] : [{ id: "s1", topicId: "t1" }],
        ),
      ),
    );
    expect((await listTopics(comp, asFetch(fetchFn))).map((t) => t.id)).toEqual(["t1"]);
    expect((await listSubscriptions(comp, asFetch(fetchFn))).map((s) => s.id)).toEqual(["s1"]);
    expect(fetchFn.mock.calls.map((c) => c[0])).toEqual([
      `${NOTIF_PROXY_BASE}/api/topics?${cq}`,
      `${NOTIF_PROXY_BASE}/api/subscriptions?${cq}`,
    ]);
  });

  it("lists deliveries newest first", async () => {
    const fetchFn = vi.fn(() => Promise.resolve(json([{ id: "d1" }, { id: "d2" }])));
    expect((await listDeliveries(asFetch(fetchFn))).map((d) => d.id)).toEqual(["d2", "d1"]);
  });

  it("confirms with the link's query and resends a confirmation", async () => {
    const fetchFn = vi.fn((_url: string, _init?: RequestInit) => Promise.resolve(json({})));
    await confirmSubscription("s1", "token=abc&protocol=EMAIL", asFetch(fetchFn));
    await resendConfirmation("s1", asFetch(fetchFn));
    expect(
      fetchFn.mock.calls.map((c) => [c[0], (c[1] as RequestInit | undefined)?.method]),
    ).toEqual([
      [`${NOTIF_PROXY_BASE}/api/subscriptions/s1/confirmation?token=abc&protocol=EMAIL`, undefined],
      [`${NOTIF_PROXY_BASE}/api/subscriptions/s1/resendConfirmation`, "POST"],
    ]);
  });

  it("surfaces a service error", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValue(json({ code: "NotAuthorizedOrNotFound", message: "bad token" }, 404));
    await expect(confirmSubscription("s1", "token=x", asFetch(fetchFn))).rejects.toMatchObject({
      httpStatus: 404,
      message: "bad token",
    });
  });
});
