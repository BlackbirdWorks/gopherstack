import { describe, expect, it, vi } from "vitest";

import {
  EVENTARC_PROXY_BASE,
  deleteTrigger,
  describeDestination,
  listTriggers,
  parseChannel,
  parseProvider,
  parseTrigger,
} from "./gcpeventarc";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

describe("destinations", () => {
  it.each([
    [{ cloudRun: { service: "svc", path: "/in" } }, "Cloud Run", "svc /in"],
    [
      { cloudFunction: "projects/p/locations/l/functions/f" },
      "Cloud Function",
      "projects/p/locations/l/functions/f",
    ],
    [{ workflow: "wf" }, "Workflow", "wf"],
    [{ httpEndpoint: { uri: "http://x" } }, "HTTP", "http://x"],
    [{ gke: { service: "s" } }, "GKE", "s"],
    [undefined, "none", ""],
  ])("%j", (d, kind, target) => expect(describeDestination(d)).toEqual({ kind, target }));
});

describe("parsers", () => {
  it("parses a trigger", () => {
    const t = parseTrigger({
      name: "projects/p/locations/l/triggers/t1",
      eventFilters: [
        { attribute: "type", value: "x" },
        { attribute: "bucket", value: "b*", operator: "match-path-pattern" },
      ],
      destination: { cloudRun: { service: "svc" } },
      transport: { pubsub: { topic: "projects/p/topics/tt" } },
      serviceAccount: "sa@p.iam.gserviceaccount.com",
    });
    expect(t.id).toBe("t1");
    expect(t.eventFilters.map((f) => f.operator)).toEqual(["=", "match-path-pattern"]);
    expect(t.destinationKind).toBe("Cloud Run");
    expect(t.transportTopic).toBe("projects/p/topics/tt");
  });

  it("parses channels and providers", () => {
    expect(parseChannel({ name: "projects/p/locations/l/channels/c", state: "ACTIVE" }).state).toBe(
      "ACTIVE",
    );
    expect(parseChannel({ name: "x/c" }).state).toBe("STATE_UNSPECIFIED");
    const p = parseProvider({
      name: "projects/p/locations/l/providers/pubsub",
      eventTypes: [{ type: "a" }, { type: "b" }, {}],
    });
    expect(p.eventTypes).toEqual(["a", "b"]);
  });
});

describe("requests", () => {
  it("lists triggers for a scope", async () => {
    const f = vi.fn((..._args: unknown[]) =>
      Promise.resolve(json({ triggers: [{ name: "projects/p/locations/l/triggers/a" }] })),
    );
    expect((await listTriggers({ project: "p", location: "l" }, f)).map((t) => t.id)).toEqual([
      "a",
    ]);
    expect(f.mock.calls[0][0]).toBe(`${EVENTARC_PROXY_BASE}/projects/p/locations/l/triggers`);
  });

  it("deletes a trigger by name", async () => {
    const f = vi.fn((..._args: unknown[]) => Promise.resolve(json({ done: true })));
    await deleteTrigger("projects/p/locations/l/triggers/a", f);
    expect(f.mock.calls[0]).toEqual([
      `${EVENTARC_PROXY_BASE}/projects/p/locations/l/triggers/a`,
      { method: "DELETE" },
    ]);
  });

  it("surfaces proxy errors", async () => {
    const f = vi.fn((..._args: unknown[]) =>
      Promise.resolve(json({ error: { message: "nope", status: "NOT_FOUND" } }, 404)),
    );
    await expect(deleteTrigger("x/y", f)).rejects.toThrow("nope");
  });
});
