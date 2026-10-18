import { describe, it, expect, beforeEach, vi } from "vitest";
import { render, screen } from "@testing-library/svelte";
import AppTracePage from "./+page.svelte";

const list = {
  items: [
    {
      id: "r1",
      traceId: "r1",
      kind: "request",
      time: "2026-10-18T00:00:00Z",
      service: "sns",
      operation: "Publish",
      status: 200,
      latencyMs: 2.5,
      requestBytes: 10,
      responseBytes: 20,
      seq: 2,
    },
    {
      id: "r1.1",
      traceId: "r1",
      parentId: "r1",
      kind: "internal",
      time: "2026-10-18T00:00:01Z",
      service: "sqs",
      operation: "SendMessage",
      status: 500,
      errorCode: "DeliveryFailed",
      latencyMs: 0.5,
      requestBytes: 0,
      responseBytes: 0,
      seq: 3,
    },
  ],
  total: 2,
  capacity: 2000,
  recorded: 2,
};

describe("App Inspector page", () => {
  beforeEach(() => {
    vi.stubGlobal(
      "fetch",
      vi.fn((url: string) => {
        const body = url.includes("/config") ? { enabled: true, detail: false } : list;
        return Promise.resolve({ ok: true, json: () => Promise.resolve(body) });
      }),
    );
  });

  it("renders the title and filter controls", () => {
    render(AppTracePage);
    expect(screen.getByText("App Inspector")).toBeInTheDocument();
    expect(screen.getByLabelText("Status")).toBeInTheDocument();
  });

  it("lists recorded spans with error codes", async () => {
    render(AppTracePage);
    expect(await screen.findByText("Publish")).toBeInTheDocument();
    expect(screen.getByText("DeliveryFailed")).toBeInTheDocument();
  });
});
