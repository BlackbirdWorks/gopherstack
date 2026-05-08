import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import SSMPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getSSMClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("SSM Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ Parameters: [] });
    render(SSMPage);
    expect(screen.getByText("AWS Systems Manager")).toBeInTheDocument();
  });

  it("shows Create Parameter button", () => {
    mockSend.mockResolvedValue({ Parameters: [] });
    render(SSMPage);
    expect(screen.getByText("Create Parameter")).toBeInTheDocument();
  });

  it("shows empty state when no parameters", async () => {
    mockSend.mockResolvedValue({ Parameters: [] });
    render(SSMPage);
    await waitFor(
      () => {
        expect(screen.getByText("No parameters found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded parameters", async () => {
    mockSend.mockResolvedValueOnce({
      Parameters: [
        {
          Name: "/myapp/db/password",
          Type: "SecureString",
          LastModifiedDate: new Date(),
          Version: 3,
        },
        { Name: "/myapp/config/region", Type: "String", LastModifiedDate: new Date(), Version: 1 },
      ],
    });
    render(SSMPage);
    await waitFor(
      () => {
        expect(screen.getByText("/myapp/db/password")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("/myapp/config/region")).toBeInTheDocument();
  });

  it("opens create modal", async () => {
    mockSend.mockResolvedValue({ Parameters: [] });
    render(SSMPage);
    await fireEvent.click(screen.getByText("Create Parameter"));
    await waitFor(() => {
      expect(screen.getByText("Create Parameter", { selector: "h2" })).toBeInTheDocument();
    });
    expect(screen.getByPlaceholderText("e.g. /myapp/database/password")).toBeInTheDocument();
  });

  it("closes modal on cancel", async () => {
    mockSend.mockResolvedValue({ Parameters: [] });
    render(SSMPage);
    await fireEvent.click(screen.getByText("Create Parameter"));
    await waitFor(() => {
      expect(screen.getByPlaceholderText("e.g. /myapp/database/password")).toBeInTheDocument();
    });
    await fireEvent.click(screen.getByText("Cancel"));
    await waitFor(() => {
      expect(
        screen.queryByPlaceholderText("e.g. /myapp/database/password"),
      ).not.toBeInTheDocument();
    });
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("access denied"));
    render(SSMPage);
    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });
});
