import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import DetectivePage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getDetectiveClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleGraph = {
  Arn: "arn:aws:detective:us-east-1:123456789012:graph:abc123",
  CreatedTime: new Date("2024-01-01T00:00:00Z"),
};

describe("Detective Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ GraphList: [] });
    render(DetectivePage);
    expect(screen.getByText("Amazon Detective")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ GraphList: [] });
    render(DetectivePage);
    expect(screen.getByText("Behavior Graphs")).toBeInTheDocument();
    expect(screen.getByText("Members")).toBeInTheDocument();
    expect(screen.getByText("Invitations")).toBeInTheDocument();
    expect(screen.getByText("Investigations")).toBeInTheDocument();
  });

  it("shows empty state when no behavior graphs", async () => {
    mockSend.mockResolvedValueOnce({ GraphList: [] });
    render(DetectivePage);
    await waitFor(() => {
      expect(screen.getByText("No behavior graphs found")).toBeInTheDocument();
    });
  });

  it("lists behavior graphs", async () => {
    mockSend.mockResolvedValueOnce({ GraphList: [exampleGraph] });
    render(DetectivePage);
    await waitFor(() => {
      expect(screen.getByText(exampleGraph.Arn)).toBeInTheDocument();
    });
  });

  it("creates a behavior graph via the modal", async () => {
    mockSend.mockResolvedValueOnce({ GraphList: [] });
    render(DetectivePage);
    await waitFor(() => screen.getByText("No behavior graphs found"));

    await fireEvent.click(screen.getByText("Create graph"));
    expect(screen.getByText("Create Behavior Graph")).toBeInTheDocument();

    mockSend.mockResolvedValueOnce({ GraphArn: exampleGraph.Arn });
    mockSend.mockResolvedValueOnce({ GraphList: [exampleGraph] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText(exampleGraph.Arn)).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledTimes(3);
  });

  it("deletes a behavior graph after confirming", async () => {
    mockSend.mockResolvedValueOnce({ GraphList: [exampleGraph] });
    render(DetectivePage);
    await waitFor(() => screen.getByText(exampleGraph.Arn));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ GraphList: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No behavior graphs found")).toBeInTheDocument();
    });
  });

  it("does not delete a graph when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ GraphList: [exampleGraph] });
    render(DetectivePage);
    await waitFor(() => screen.getByText(exampleGraph.Arn));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial ListGraphs call — no DeleteGraph, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByText(exampleGraph.Arn)).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Graph not found."), {
      name: "ResourceNotFoundException",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(DetectivePage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ResourceNotFoundException (HTTP 404): Graph not found."),
      ).toBeInTheDocument();
    });
  });

  it("opens a graph's detail view", async () => {
    mockSend.mockResolvedValueOnce({ GraphList: [exampleGraph] });
    render(DetectivePage);
    await waitFor(() => screen.getByText(exampleGraph.Arn));

    await fireEvent.click(screen.getByTitle("View"));

    expect(screen.getAllByText(exampleGraph.Arn).length).toBeGreaterThan(1);
  });

  it("switches to the members tab and loads members for the selected graph", async () => {
    mockSend.mockResolvedValueOnce({ GraphList: [exampleGraph] });
    render(DetectivePage);
    await waitFor(() => screen.getByText(exampleGraph.Arn));

    mockSend.mockResolvedValueOnce({
      MemberDetails: [
        {
          AccountId: "222233334444",
          EmailAddress: "member@example.com",
          Status: "ENABLED",
        },
      ],
    });
    await fireEvent.click(screen.getByText("Members"));

    await waitFor(() => {
      expect(screen.getByText("222233334444")).toBeInTheDocument();
    });
  });
});
