import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import ConfigPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getConfigClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

describe("AWS Config Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend
      .mockResolvedValueOnce({ ConfigRules: [] })
      .mockResolvedValueOnce({ ComplianceByConfigRules: [] });
    render(ConfigPage);
    expect(screen.getByText("AWS Config")).toBeInTheDocument();
  });

  it("shows tabs", () => {
    mockSend
      .mockResolvedValueOnce({ ConfigRules: [] })
      .mockResolvedValueOnce({ ComplianceByConfigRules: [] });
    render(ConfigPage);
    expect(screen.getByText("Config Rules")).toBeInTheDocument();
    expect(screen.getByText("Configuration Recorder")).toBeInTheDocument();
  });

  it("displays config rules", async () => {
    mockSend
      .mockResolvedValueOnce({
        ConfigRules: [
          {
            ConfigRuleName: "restricted-ssh",
            Source: { Owner: "AWS", SourceIdentifier: "RESTRICTED_INCOMING_SSH" },
          },
        ],
      })
      .mockResolvedValueOnce({
        ComplianceByConfigRules: [
          { ConfigRuleName: "restricted-ssh", Compliance: { ComplianceType: "COMPLIANT" } },
        ],
      });

    render(ConfigPage);

    await waitFor(() => {
      expect(screen.getByText("restricted-ssh")).toBeInTheDocument();
    });
  });

  it("shows add rule modal", async () => {
    mockSend
      .mockResolvedValueOnce({ ConfigRules: [] })
      .mockResolvedValueOnce({ ComplianceByConfigRules: [] });
    render(ConfigPage);
    await fireEvent.click(screen.getByText("Add Rule"));
    expect(screen.getByText("Add Config Rule")).toBeInTheDocument();
  });

  it("switches to recorder tab", async () => {
    mockSend
      .mockResolvedValueOnce({ ConfigRules: [] })
      .mockResolvedValueOnce({ ComplianceByConfigRules: [] })
      .mockResolvedValueOnce({ ConfigurationRecorders: [] })
      .mockResolvedValueOnce({ ConfigurationRecordersStatus: [] });
    render(ConfigPage);
    await fireEvent.click(screen.getByText("Configuration Recorder"));
    await waitFor(() => {
      expect(screen.getByText("No configuration recorders found")).toBeInTheDocument();
    });
  });
});
