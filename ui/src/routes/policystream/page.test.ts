import { describe, it, expect, beforeEach, vi } from "vitest";
import { render, screen, fireEvent } from "@testing-library/svelte";
import IamStreamPage from "./+page.svelte";

class FakeEventSource {
  static last: FakeEventSource | null = null;
  onopen: (() => void) | null = null;
  onmessage: ((m: { data: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  closed = false;
  constructor(public url: string) {
    FakeEventSource.last = this;
  }
  close() {
    this.closed = true;
  }
}

const base = {
  timestamp: "2026-10-19T00:00:00Z",
  service: "s3",
  operation: "GetObject",
  resource: "arn:aws:s3:::b/k",
  principal: "arn:aws:iam::000000000000:user/alice",
  enforced: true,
};

function push(over: Record<string, unknown>) {
  FakeEventSource.last?.onmessage?.({ data: JSON.stringify({ ...base, ...over }) });
}

describe("IAM Policy Stream page", () => {
  beforeEach(() => {
    FakeEventSource.last = null;
    vi.stubGlobal("EventSource", FakeEventSource);
  });

  it("connects to the follow stream", () => {
    render(IamStreamPage);
    expect(screen.getByText("IAM Policy Stream")).toBeInTheDocument();
    expect(FakeEventSource.last?.url).toContain("follow=1");
  });

  it("renders streamed decisions with boundary attribution", async () => {
    render(IamStreamPage);
    push({
      seq: 1,
      action: "s3:GetObject",
      decision: "deny",
      denyType: "implicit",
      reason: "permission-boundary",
      policy: { type: "permission-boundary", arn: "arn:aws:iam::0:policy/bound" },
      boundary: { arn: "arn:aws:iam::0:policy/bound", statementIndex: -1, matched: false },
    });
    expect(await screen.findByText("Implicit deny")).toBeInTheDocument();
    expect(
      screen.getByText("arn:aws:iam::0:policy/bound: no matching statement"),
    ).toBeInTheDocument();
  });

  it("suggests a policy from denied rows", async () => {
    render(IamStreamPage);
    push({
      seq: 1,
      action: "s3:GetObject",
      decision: "deny",
      denyType: "implicit",
      reason: "implicit-deny",
    });
    await screen.findByText("Implicit deny");
    await fireEvent.click(screen.getByRole("button", { name: /Suggest policy/ }));
    expect(await screen.findByText(/"s3:GetObject"/)).toBeInTheDocument();
  });

  it("pause closes the stream", async () => {
    render(IamStreamPage);
    const src = FakeEventSource.last;
    await fireEvent.click(screen.getByRole("button", { name: "Pause" }));
    expect(src?.closed).toBe(true);
  });
});
