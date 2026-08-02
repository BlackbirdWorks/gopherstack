import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import OutpostsPage from "./+page.svelte";

// Every Modal in this page (there are a dozen+) stays mounted in the DOM
// once rendered -- closed <dialog> elements just lose the `open` attribute
// rather than unmounting -- so getByLabelText/getByRole across the whole
// document is ambiguous once more than one modal shares a label (e.g.
// "Name") or a footer button (e.g. "Create"). Scope to the dialog that is
// actually open, same pattern as accessanalyzer/page.test.ts and
// dlm/page.test.ts.
function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getOutpostsClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleOutpost = {
  OutpostId: "op-abc12345",
  OwnerId: "000000000000",
  OutpostArn: "arn:aws:outposts:us-east-1:000000000000:outpost/op-abc12345",
  SiteId: "os-site1",
  Name: "example",
  Description: "an example outpost",
  LifeCycleStatus: "ACTIVE",
  AvailabilityZone: "us-east-1a",
};

describe("Outposts Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ Outposts: [] });
    render(OutpostsPage);
    expect(screen.getByText("AWS Outposts")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ Outposts: [] });
    render(OutpostsPage);
    expect(screen.getByText("Outposts")).toBeInTheDocument();
    expect(screen.getByText("Sites")).toBeInTheDocument();
    expect(screen.getByText("Orders")).toBeInTheDocument();
    expect(screen.getByText("Quotes")).toBeInTheDocument();
    expect(screen.getByText("Capacity Tasks")).toBeInTheDocument();
    expect(screen.getByText("Assets")).toBeInTheDocument();
    expect(screen.getByText("Catalog")).toBeInTheDocument();
    expect(screen.getByText("Orderable Instance Types")).toBeInTheDocument();
  });

  it("shows empty state when no Outposts", async () => {
    mockSend.mockResolvedValueOnce({ Outposts: [] });
    render(OutpostsPage);
    await waitFor(() => {
      expect(screen.getByText("No Outposts found")).toBeInTheDocument();
    });
  });

  it("lists Outposts", async () => {
    mockSend.mockResolvedValueOnce({ Outposts: [exampleOutpost] });
    render(OutpostsPage);
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "example" })).toBeInTheDocument();
    });
  });

  it("creates an Outpost via the modal", async () => {
    mockSend.mockResolvedValueOnce({ Outposts: [] });
    render(OutpostsPage);
    await waitFor(() => screen.getByText("No Outposts found"));

    // The header action button and the modal's <h2> title are both
    // literally "Create Outpost" (unlike e.g. "Create workspace" / "Create
    // Workspace" elsewhere in this campaign) -- and Modal.svelte's <dialog>
    // stays mounted (just without the `open` attribute) even before it's
    // ever opened, so getByText("Create Outpost") is ambiguous from the very
    // first render. Scope the click to the button role to disambiguate.
    await fireEvent.click(screen.getByRole("button", { name: "Create Outpost" }));
    expect(screen.getAllByText("Create Outpost").length).toBe(2);

    const dialog = openDialog();
    await fireEvent.input(within(dialog).getByLabelText("Name"), {
      target: { value: "example" },
    });
    await fireEvent.input(within(dialog).getByLabelText("Site ID or ARN"), {
      target: { value: "os-site1" },
    });

    mockSend.mockResolvedValueOnce({ Outpost: exampleOutpost });
    mockSend.mockResolvedValueOnce({ Outposts: [exampleOutpost] });

    await fireEvent.click(within(dialog).getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "example" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledTimes(3);
  });

  it("deletes an Outpost after confirming", async () => {
    mockSend.mockResolvedValueOnce({ Outposts: [exampleOutpost] });
    render(OutpostsPage);
    await waitFor(() => screen.getByRole("cell", { name: "example" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Outposts: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No Outposts found")).toBeInTheDocument();
    });
  });

  it("does not delete an Outpost when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ Outposts: [exampleOutpost] });
    render(OutpostsPage);
    await waitFor(() => screen.getByRole("cell", { name: "example" }));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial ListOutposts call -- no DeleteOutpost, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("cell", { name: "example" })).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Outpost not found."), {
      name: "NotFoundException",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(OutpostsPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("NotFoundException (HTTP 404): Outpost not found."),
      ).toBeInTheDocument();
    });
  });

  it("opens an Outpost's detail view", async () => {
    mockSend.mockResolvedValueOnce({ Outposts: [exampleOutpost] });
    render(OutpostsPage);
    await waitFor(() => screen.getByRole("cell", { name: "example" }));

    mockSend.mockResolvedValueOnce({ Outpost: exampleOutpost });
    mockSend.mockResolvedValueOnce({ Tags: {} });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getAllByText(exampleOutpost.OutpostArn).length).toBeGreaterThan(0);
    });
  });

  it("switches to the sites tab and loads sites", async () => {
    mockSend.mockResolvedValueOnce({ Outposts: [exampleOutpost] });
    render(OutpostsPage);
    await waitFor(() => screen.getByRole("cell", { name: "example" }));

    mockSend.mockResolvedValueOnce({
      Sites: [{ SiteId: "os-site1", Name: "my-site" }],
    });
    await fireEvent.click(screen.getByText("Sites"));

    await waitFor(() => {
      expect(screen.getByText("my-site")).toBeInTheDocument();
    });
  });
});
