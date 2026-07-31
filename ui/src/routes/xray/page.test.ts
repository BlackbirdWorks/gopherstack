import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import XRayPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getXRayClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: vi.fn(() => Promise.resolve(true)),
}));

describe("X-Ray Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ TraceSummaries: [] });
    render(XRayPage);
    expect(screen.getByText("AWS X-Ray")).toBeInTheDocument();
  });

  it("shows subtitle", () => {
    mockSend.mockResolvedValueOnce({ TraceSummaries: [] });
    render(XRayPage);
    expect(screen.getByText("Distributed tracing for application analysis")).toBeInTheDocument();
  });

  it("shows stat cards always", () => {
    mockSend.mockResolvedValueOnce({ TraceSummaries: [] });
    render(XRayPage);
    expect(screen.getByText("Total Traces")).toBeInTheDocument();
    expect(screen.getByText("Faults")).toBeInTheDocument();
    expect(screen.getByText("Errors")).toBeInTheDocument();
    expect(screen.getByText("Avg Duration")).toBeInTheDocument();
  });

  it("shows all six tabs", () => {
    mockSend.mockResolvedValueOnce({ TraceSummaries: [] });
    render(XRayPage);
    expect(screen.getByText("Traces")).toBeInTheDocument();
    expect(screen.getByText("Service Graph")).toBeInTheDocument();
    expect(screen.getByText("Groups")).toBeInTheDocument();
    expect(screen.getByText("Sampling Rules")).toBeInTheDocument();
    expect(screen.getByText("Resource Policies")).toBeInTheDocument();
    expect(screen.getByText("Encryption Config")).toBeInTheDocument();
  });

  it("displays traces", async () => {
    mockSend.mockResolvedValueOnce({
      TraceSummaries: [
        {
          Id: "1-5f0b2ea2-abc123",
          Duration: 0.342,
          HasFault: false,
          HasError: false,
          HasThrottle: false,
          Http: { HttpStatus: 200, HttpURL: "/api/users" },
        },
      ],
    });

    render(XRayPage);

    await waitFor(() => {
      expect(screen.getByText("1-5f0b2ea2-abc123")).toBeInTheDocument();
    });
  });

  it("paginates traces with Load More using NextToken", async () => {
    mockSend
      .mockResolvedValueOnce({
        TraceSummaries: [{ Id: "trace-1", Duration: 0.1 }],
        NextToken: "page-2",
      })
      .mockResolvedValueOnce({
        TraceSummaries: [{ Id: "trace-2", Duration: 0.2 }],
      });

    render(XRayPage);

    await waitFor(() => {
      expect(screen.getByText("trace-1")).toBeInTheDocument();
    });

    await fireEvent.click(screen.getByText("Load More"));

    await waitFor(() => {
      expect(screen.getByText("trace-2")).toBeInTheDocument();
    });

    expect(mockSend).toHaveBeenCalledTimes(2);
    const secondCallInput = mockSend.mock.calls[1][0].input;
    expect(secondCallInput.NextToken).toBe("page-2");
  });

  it("switches to groups tab and lists groups", async () => {
    mockSend.mockResolvedValueOnce({ TraceSummaries: [] }).mockResolvedValueOnce({
      Groups: [
        { GroupName: "my-group", GroupARN: "arn:aws:xray:us-east-1:123:group/my-group/abc" },
      ],
    });

    render(XRayPage);
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(1));

    await fireEvent.click(screen.getByText("Groups"));

    await waitFor(() => {
      expect(screen.getByText("my-group")).toBeInTheDocument();
    });
  });

  it("creates a group", async () => {
    mockSend
      .mockResolvedValueOnce({ TraceSummaries: [] })
      .mockResolvedValueOnce({ Groups: [] })
      .mockResolvedValueOnce({ Group: { GroupName: "new-group" } })
      .mockResolvedValueOnce({ Groups: [{ GroupName: "new-group" }] });

    render(XRayPage);
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(1));
    await fireEvent.click(screen.getByText("Groups"));
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(2));

    await fireEvent.click(screen.getByText("Create group"));
    const nameInput = screen.getByLabelText("Group Name");
    await fireEvent.input(nameInput, { target: { value: "new-group" } });
    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledTimes(4);
    });
    const createInput = mockSend.mock.calls[2][0].input;
    expect(createInput.GroupName).toBe("new-group");
  });

  it("deletes a group after confirmation", async () => {
    mockSend
      .mockResolvedValueOnce({ TraceSummaries: [] })
      .mockResolvedValueOnce({ Groups: [{ GroupName: "doomed-group" }] })
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({ Groups: [] });

    render(XRayPage);
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(1));
    await fireEvent.click(screen.getByText("Groups"));
    await waitFor(() => screen.getByText("doomed-group"));

    await fireEvent.click(screen.getByLabelText("Delete group doomed-group"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledTimes(4);
    });
    const deleteInput = mockSend.mock.calls[2][0].input;
    expect(deleteInput.GroupName).toBe("doomed-group");
  });

  it("switches to sampling rules tab and lists rules, guarding the Default rule from delete", async () => {
    mockSend.mockResolvedValueOnce({ TraceSummaries: [] }).mockResolvedValueOnce({
      SamplingRuleRecords: [
        {
          SamplingRule: { RuleName: "Default", Priority: 10000, FixedRate: 0.05, ReservoirSize: 1 },
          CreatedAt: new Date(),
          ModifiedAt: new Date(),
        },
      ],
    });

    render(XRayPage);
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(1));
    await fireEvent.click(screen.getByText("Sampling Rules"));

    await waitFor(() => {
      expect(screen.getByText("Default")).toBeInTheDocument();
    });
    expect(screen.queryByLabelText("Delete sampling rule Default")).not.toBeInTheDocument();
  });

  it("creates a sampling rule", async () => {
    mockSend
      .mockResolvedValueOnce({ TraceSummaries: [] })
      .mockResolvedValueOnce({ SamplingRuleRecords: [] })
      .mockResolvedValueOnce({ SamplingRuleRecord: { SamplingRule: { RuleName: "my-rule" } } })
      .mockResolvedValueOnce({ SamplingRuleRecords: [] });

    render(XRayPage);
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(1));
    await fireEvent.click(screen.getByText("Sampling Rules"));
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(2));

    await fireEvent.click(screen.getByText("Create rule"));
    await fireEvent.input(screen.getByLabelText("Rule Name"), { target: { value: "my-rule" } });
    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledTimes(4);
    });
    const createInput = mockSend.mock.calls[2][0].input;
    expect(createInput.SamplingRule.RuleName).toBe("my-rule");
  });

  it("switches to resource policies tab and lists policies", async () => {
    mockSend.mockResolvedValueOnce({ TraceSummaries: [] }).mockResolvedValueOnce({
      ResourcePolicies: [{ PolicyName: "my-policy", PolicyDocument: "{}", PolicyRevisionId: "1" }],
    });

    render(XRayPage);
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(1));
    await fireEvent.click(screen.getByText("Resource Policies"));

    await waitFor(() => {
      expect(screen.getByText("my-policy")).toBeInTheDocument();
    });
  });

  it("shows the encryption config panel and submits an update", async () => {
    mockSend
      .mockResolvedValueOnce({ TraceSummaries: [] })
      .mockResolvedValueOnce({ EncryptionConfig: { Type: "NONE", Status: "ACTIVE" } })
      .mockResolvedValueOnce({ EncryptionConfig: { Type: "NONE", Status: "ACTIVE" } });

    render(XRayPage);
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(1));
    await fireEvent.click(screen.getByText("Encryption Config"));

    await waitFor(() => {
      expect(screen.getByText("Update encryption configuration")).toBeInTheDocument();
    });

    await fireEvent.click(screen.getByText("Update encryption configuration"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledTimes(3);
    });
    const putInput = mockSend.mock.calls[2][0].input;
    expect(putInput.Type).toBe("NONE");
  });

  it("shows an inline error banner with the AWS error name on a failed tab load", async () => {
    mockSend.mockResolvedValueOnce({ TraceSummaries: [] }).mockRejectedValueOnce(
      Object.assign(new Error("group not found"), {
        name: "InvalidRequestException",
        $metadata: { httpStatusCode: 400 },
      }),
    );

    render(XRayPage);
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(1));
    await fireEvent.click(screen.getByText("Groups"));

    await waitFor(() => {
      expect(screen.getByText(/InvalidRequestException/)).toBeInTheDocument();
    });
  });
});
