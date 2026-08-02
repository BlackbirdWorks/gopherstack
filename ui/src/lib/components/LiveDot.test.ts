import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/svelte";
import LiveDot from "./LiveDot.svelte";

const streamConsole = vi.fn();

vi.mock("$lib/api/connect-client", () => ({
  dashboardClient: {
    streamConsole: (...args: unknown[]) => streamConsole(...args),
  },
}));

describe("LiveDot", () => {
  beforeEach(() => {
    streamConsole.mockReset();
  });

  it("renders grey and never opens a connection when no service is given", () => {
    render(LiveDot, { props: {} });

    const dot = screen.getByTestId("live-dot");
    expect(dot.dataset.state).toBe("grey");
    expect(dot.title).toBe("Live updates not implemented for this service");
    expect(streamConsole).not.toHaveBeenCalled();
  });

  it("renders grey for a service with no wired-up live support", () => {
    render(LiveDot, { props: { service: "route53" } });

    const dot = screen.getByTestId("live-dot");
    expect(dot.dataset.state).toBe("grey");
    expect(streamConsole).not.toHaveBeenCalled();
  });

  it("renders green once the console stream connects and yields for a supported service", async () => {
    streamConsole.mockImplementation(async function* () {
      yield { request: { method: "GET", path: "/", status: 200 } };
      // Stay open, like the real server stream does.
      await new Promise(() => {});
    });

    render(LiveDot, { props: { service: "s3" } });

    await waitFor(() => {
      expect(screen.getByTestId("live-dot").dataset.state).toBe("green");
    });
    expect(streamConsole).toHaveBeenCalledTimes(1);
  });

  it("renders red when the stream call rejects for a supported service", async () => {
    streamConsole.mockImplementation(async function* () {
      throw new Error("stream unavailable");
      // eslint-disable-next-line no-unreachable
      yield;
    });

    render(LiveDot, { props: { service: "dynamodb" } });

    await waitFor(() => {
      expect(screen.getByTestId("live-dot").dataset.state).toBe("red");
    });
  });

  it("does not report an error for an intentional abort (unmount)", async () => {
    let rejectSignal: (() => void) | null = null;
    streamConsole.mockImplementation(async function* (
      _req: unknown,
      opts: { signal?: AbortSignal },
    ) {
      await new Promise<void>((_resolve, reject) => {
        opts.signal?.addEventListener("abort", () => {
          const err = new Error("aborted");
          err.name = "AbortError";
          reject(err);
        });
        rejectSignal = () => {};
      });
      // eslint-disable-next-line no-unreachable
      yield;
    });

    const { unmount } = render(LiveDot, { props: { service: "ec2" } });
    await waitFor(() => expect(streamConsole).toHaveBeenCalledTimes(1));

    unmount();
    expect(rejectSignal).not.toBeNull();
    // No assertion possible on a torn-down component's DOM; this test's
    // purpose is only to confirm unmounting doesn't throw an unhandled
    // rejection (the AbortError branch is swallowed, not surfaced as red).
  });
});
