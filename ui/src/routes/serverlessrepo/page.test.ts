import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import ServerlessRepoPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getServerlessRepoClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
  },
}));

vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: vi.fn().mockResolvedValue(true),
}));

describe("Serverless Application Repository Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title and Publish App button", () => {
    mockSend.mockResolvedValueOnce({ Applications: [] });

    render(ServerlessRepoPage);

    expect(screen.getByText("Serverless Application Repository")).toBeInTheDocument();
    expect(screen.getByText("Publish App")).toBeInTheDocument();
  });

  it("displays loaded applications", async () => {
    mockSend.mockResolvedValueOnce({
      Applications: [
        { ApplicationId: "arn:1", Name: "my-api-app", Author: "MyOrg", Description: "A REST API" },
        {
          ApplicationId: "arn:2",
          Name: "data-processor",
          Author: "AnotherOrg",
          Description: "Batch processor",
        },
      ],
    });

    render(ServerlessRepoPage);

    await waitFor(
      () => {
        expect(screen.getByText("my-api-app")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("data-processor")).toBeInTheDocument();
  });

  it("filters applications via search input", async () => {
    mockSend.mockResolvedValueOnce({
      Applications: [
        { ApplicationId: "arn:1", Name: "my-api-app", Author: "OrgA", Description: "API" },
        { ApplicationId: "arn:2", Name: "data-processor", Author: "OrgB", Description: "Data" },
        { ApplicationId: "arn:3", Name: "auth-service", Author: "OrgC", Description: "Auth" },
      ],
    });

    render(ServerlessRepoPage);

    await waitFor(
      () => {
        expect(screen.getByText("my-api-app")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    const searchInput = screen.getByPlaceholderText("Search...");
    await fireEvent.input(searchInput, { target: { value: "data" } });

    await waitFor(() => {
      expect(screen.queryByText("my-api-app")).not.toBeInTheDocument();
    });
    expect(screen.getByText("data-processor")).toBeInTheDocument();
    expect(screen.queryByText("auth-service")).not.toBeInTheDocument();
  });

  it("shows empty state when no applications exist", async () => {
    mockSend.mockResolvedValueOnce({ Applications: [] });

    render(ServerlessRepoPage);

    await waitFor(
      () => {
        expect(screen.getByText(/No applications found/)).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("not authorized"));

    render(ServerlessRepoPage);

    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });

  it("opens create application modal on Publish App click", async () => {
    mockSend.mockResolvedValueOnce({ Applications: [] });

    render(ServerlessRepoPage);

    const publishBtn = screen.getByText("Publish App");
    await fireEvent.click(publishBtn);

    await waitFor(() => {
      expect(screen.getByText("Publish New Application")).toBeInTheDocument();
    });
  });

  it("shows application details panel when app selected", async () => {
    mockSend
      .mockResolvedValueOnce({
        Applications: [
          { ApplicationId: "arn:1", Name: "my-api-app", Author: "MyOrg", Description: "A REST API" },
        ],
      })
      // loadVersions
      .mockResolvedValueOnce({ Versions: [] });

    render(ServerlessRepoPage);

    await waitFor(() => expect(screen.getByText("my-api-app")).toBeInTheDocument(), { timeout: 3000 });

    const appRow = screen.getAllByText("my-api-app")[0];
    await fireEvent.click(appRow);

    await waitFor(() => {
      expect(screen.getByText("Versions")).toBeInTheDocument();
      expect(screen.getByText("Policy")).toBeInTheDocument();
    });
  });
});
