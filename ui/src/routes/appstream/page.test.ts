import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import AppStreamPage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getAppStreamClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleStack = {
  Name: "my-stack",
  Description: "Test stack",
  Arn: "arn:aws:appstream:us-east-1:123:stack/my-stack",
};

const exampleFleet = {
  Name: "my-fleet",
  Arn: "arn:aws:appstream:us-east-1:123:fleet/my-fleet",
  InstanceType: "stream.standard.medium",
  State: "STOPPED",
  ImageName: "my-image",
};

describe("AppStream Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ Stacks: [] });
    render(AppStreamPage);
    expect(screen.getByText("Amazon AppStream 2.0")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No stacks found"));
  });

  it("shows empty state", async () => {
    mockSend.mockResolvedValueOnce({ Stacks: [] });
    render(AppStreamPage);
    await waitFor(() => {
      expect(screen.getByText("No stacks found")).toBeInTheDocument();
    });
  });

  it("lists loaded stacks", async () => {
    mockSend.mockResolvedValueOnce({ Stacks: [exampleStack] });
    render(AppStreamPage);
    await waitFor(() => {
      expect(screen.getByText("my-stack")).toBeInTheDocument();
    });
    expect(screen.getByText("Test stack")).toBeInTheDocument();
  });

  it("shows Fleets and Images tabs", async () => {
    mockSend.mockResolvedValueOnce({ Stacks: [] });
    render(AppStreamPage);
    await waitFor(() => screen.getByText("No stacks found"));
    expect(screen.getByRole("tab", { name: "Fleets" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Images" })).toBeInTheDocument();
  });

  it("creates a stack via the modal", async () => {
    mockSend.mockResolvedValueOnce({ Stacks: [] });
    render(AppStreamPage);
    await waitFor(() => screen.getByText("No stacks found"));

    await fireEvent.click(screen.getByText("Create stack"));
    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "my-stack" },
    });

    mockSend.mockResolvedValueOnce({ Stack: exampleStack });
    mockSend.mockResolvedValueOnce({ Stacks: [exampleStack] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("my-stack")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { Name: "my-stack", Description: undefined } }),
    );
  });

  it("deletes a stack after confirming", async () => {
    mockSend.mockResolvedValueOnce({ Stacks: [exampleStack] });
    render(AppStreamPage);
    await waitFor(() => screen.getByText("my-stack"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Stacks: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No stacks found")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(expect.objectContaining({ input: { Name: "my-stack" } }));
  });

  it("does not delete a stack when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ Stacks: [exampleStack] });
    render(AppStreamPage);
    await waitFor(() => screen.getByText("my-stack"));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByText("my-stack")).toBeInTheDocument();
  });

  it("starts a stopped fleet", async () => {
    mockSend.mockResolvedValueOnce({ Stacks: [] });
    render(AppStreamPage);
    await waitFor(() => screen.getByText("No stacks found"));

    mockSend.mockResolvedValueOnce({ Fleets: [exampleFleet] });
    await fireEvent.click(screen.getByRole("tab", { name: "Fleets" }));
    await waitFor(() => screen.getByText("my-fleet"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Fleets: [{ ...exampleFleet, State: "STARTING" }] });

    await fireEvent.click(screen.getByTitle("Start"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({ input: { Name: "my-fleet" } }),
      );
    });
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Access denied."), {
      name: "AccessDeniedException",
      $metadata: { httpStatusCode: 403 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(AppStreamPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("AccessDeniedException (HTTP 403): Access denied."),
      ).toBeInTheDocument();
    });
  });
});
