import { describe, expect, it, vi } from "vitest";

import { saveSettings, toSettingsForm } from "./settings";

describe("settings api", () => {
  it("serializes update payload", () => {
    const body = toSettingsForm({
      accountID: "000000000000",
      region: "us-east-1",
      latencyMs: 25,
      enforceIAM: true,
    }).toString();

    expect(body).toContain("accountID=000000000000");
    expect(body).toContain("region=us-east-1");
    expect(body).toContain("latencyMs=25");
    expect(body).toContain("enforceIAM=true");
  });

  it("returns message for successful save", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ message: "ok" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    await expect(
      saveSettings(fetcher, {
        accountID: "111111111111",
        region: "us-west-2",
        latencyMs: 10,
        enforceIAM: false,
      }),
    ).resolves.toBe("ok");
  });

  it("returns default message when success payload has no message", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({}), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    await expect(
      saveSettings(fetcher, {
        accountID: "111111111111",
        region: "us-west-2",
        latencyMs: 10,
        enforceIAM: false,
      }),
    ).resolves.toBe("Settings updated");
  });

  it("throws response message on failures", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ message: "bad" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      }),
    );

    await expect(
      saveSettings(fetcher, {
        accountID: "111111111111",
        region: "us-west-2",
        latencyMs: 10,
        enforceIAM: false,
      }),
    ).rejects.toThrow("bad");
  });

  it("throws default error on failure without message", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({}), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      }),
    );

    await expect(
      saveSettings(fetcher, {
        accountID: "111111111111",
        region: "us-west-2",
        latencyMs: 10,
        enforceIAM: false,
      }),
    ).rejects.toThrow("settings update failed");
  });
});
