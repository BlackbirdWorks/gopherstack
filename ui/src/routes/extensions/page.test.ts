import { describe, it, expect, beforeEach, vi } from "vitest";
import { render, screen, waitFor, fireEvent } from "@testing-library/svelte";
import ExtensionsPage from "./+page.svelte";

const listBody = {
  extensions: [
    {
      name: "hello",
      type: "process",
      status: "ready",
      restarts: 2,
      endpoint: "http://127.0.0.1:9000",
      registration: {
        name: "hello",
        version: "1.0.0",
        routes: { path_prefixes: ["/hello"] },
        hooks: { on_startup: "/hooks/startup" },
      },
    },
  ],
};

function jsonResponse(body: unknown) {
  return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve(body) });
}

describe("Extensions page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.stubGlobal(
      "fetch",
      vi.fn((url: string, init?: RequestInit) => {
        if (url.endsWith("/output")) return jsonResponse({ name: "hello", lines: ["stdout: up"] });
        if (init?.method === "POST") return jsonResponse({ name: "hello", status: "restarting" });
        return jsonResponse(listBody);
      }),
    );
  });

  it("lists extensions with status, routes and hooks", async () => {
    render(ExtensionsPage);
    expect(await screen.findByText("hello")).toBeInTheDocument();
    expect(screen.getByTestId("status-hello")).toHaveTextContent("ready");
    expect(screen.getByText("path /hello")).toBeInTheDocument();
    expect(screen.getByText("on_startup")).toBeInTheDocument();
    expect(screen.getByTestId("restarts-hello")).toHaveTextContent("2");
  });

  it("posts a restart", async () => {
    render(ExtensionsPage);
    await screen.findByText("hello");
    await fireEvent.click(screen.getByText("Restart"));
    await waitFor(() =>
      expect(fetch).toHaveBeenCalledWith("/_gopherstack/extensions/hello/restart", {
        method: "POST",
      }),
    );
  });

  it("shows captured output", async () => {
    render(ExtensionsPage);
    await screen.findByText("hello");
    await fireEvent.click(screen.getByText("Output"));
    expect(await screen.findByText("stdout: up")).toBeInTheDocument();
  });
});
