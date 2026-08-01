import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import OutpostsPage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
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
  OutpostId: "op-0000000000000001",
  Name: "my-outpost",
  OutpostArn: "arn:aws:outposts:us-east-1:123456789012:outpost/op-0000000000000001",
  SiteId: "os-0000000000000001",
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

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ Outposts: [] });
    render(OutpostsPage);
    expect(screen.getByText("AWS Outposts")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No Outposts found"));
  });

  it("shows empty state when no outposts", async () => {
    mockSend.mockResolvedValueOnce({ Outposts: [] });
    render(OutpostsPage);
    await waitFor(() => {
      expect(screen.getByText("No Outposts found")).toBeInTheDocument();
    });
  });

  it("lists outposts and renders the status badge snippet", async () => {
    mockSend.mockResolvedValueOnce({ Outposts: [exampleOutpost] });
    render(OutpostsPage);
    await waitFor(() => {
      expect(screen.getByText(exampleOutpost.Name)).toBeInTheDocument();
    });
    expect(screen.getByText("ACTIVE")).toBeInTheDocument();
  });

  it("creates an Outpost via the modal with exact command input", async () => {
    mockSend.mockResolvedValueOnce({ Outposts: [] });
    render(OutpostsPage);
    await waitFor(() => screen.getByText("No Outposts found"));

    await fireEvent.click(screen.getByRole("button", { name: "Create Outpost" }));
    expect(screen.getAllByText("Create Outpost").length).toBeGreaterThan(0);

    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "my-outpost" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Site ID"), {
      target: { value: "os-0000000000000001" },
    });

    mockSend.mockResolvedValueOnce({ Outpost: exampleOutpost });
    mockSend.mockResolvedValueOnce({ Outposts: [exampleOutpost] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText(exampleOutpost.Name)).toBeInTheDocument();
    });

    const createCall = mockSend.mock.calls[1][0];
    expect(createCall.constructor.name).toBe("CreateOutpostCommand");
    expect(createCall.input).toEqual({
      Name: "my-outpost",
      Description: undefined,
      SiteId: "os-0000000000000001",
      AvailabilityZone: undefined,
      SupportedHardwareType: undefined,
    });
  });

  it("deletes an Outpost after confirming", async () => {
    mockSend.mockResolvedValueOnce({ Outposts: [exampleOutpost] });
    render(OutpostsPage);
    await waitFor(() => screen.getByText(exampleOutpost.Name));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Outposts: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No Outposts found")).toBeInTheDocument();
    });

    const deleteCall = mockSend.mock.calls[1][0];
    expect(deleteCall.constructor.name).toBe("DeleteOutpostCommand");
    expect(deleteCall.input).toEqual({ OutpostId: exampleOutpost.OutpostId });
  });

  it("does not delete an Outpost when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ Outposts: [exampleOutpost] });
    render(OutpostsPage);
    await waitFor(() => screen.getByText(exampleOutpost.Name));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(1);
    // exampleOutpost.Name also appears as an <option> in the (closed) create
    // order modal's Outpost picker, so assert at least one match remains.
    expect(screen.getAllByText(exampleOutpost.Name).length).toBeGreaterThan(0);
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Not found."), {
      name: "NotFoundException",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(OutpostsPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(screen.getByText("NotFoundException (HTTP 404): Not found.")).toBeInTheDocument();
    });
  });

  it("opens an Outpost's detail view", async () => {
    mockSend.mockResolvedValueOnce({ Outposts: [exampleOutpost] });
    render(OutpostsPage);
    await waitFor(() => screen.getByText(exampleOutpost.Name));

    mockSend.mockResolvedValueOnce({ Outpost: exampleOutpost });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getByText(exampleOutpost.OutpostArn)).toBeInTheDocument();
    });

    const getCall = mockSend.mock.calls[1][0];
    expect(getCall.constructor.name).toBe("GetOutpostCommand");
    expect(getCall.input).toEqual({ OutpostId: exampleOutpost.OutpostId });
  });

  it("switches to the Sites tab and lists sites", async () => {
    mockSend.mockResolvedValueOnce({ Outposts: [] });
    render(OutpostsPage);
    await waitFor(() => screen.getByText("No Outposts found"));

    mockSend.mockResolvedValueOnce({
      Sites: [{ SiteId: "os-1", Name: "hq-site", OperatingAddressCity: "Seattle" }],
    });
    await fireEvent.click(screen.getByRole("tab", { name: "Sites" }));

    await waitFor(() => {
      expect(screen.getByText("hq-site")).toBeInTheDocument();
    });
    expect(screen.getByText("Seattle")).toBeInTheDocument();

    const listCall = mockSend.mock.calls[1][0];
    expect(listCall.constructor.name).toBe("ListSitesCommand");
  });

  it("switches to the Orders tab, lists orders, and cancels one (not delete)", async () => {
    mockSend.mockResolvedValueOnce({ Outposts: [] });
    render(OutpostsPage);
    await waitFor(() => screen.getByText("No Outposts found"));

    mockSend.mockResolvedValueOnce({
      Orders: [{ OrderId: "order-1", OutpostId: "op-1", Status: "RECEIVED", OrderType: "OUTPOST" }],
    });
    await fireEvent.click(screen.getByRole("tab", { name: "Orders" }));

    await waitFor(() => {
      expect(screen.getByText("order-1")).toBeInTheDocument();
    });
    expect(screen.getByText("RECEIVED")).toBeInTheDocument();
    // Orders have no delete -- only cancel -- so the destructive control must
    // be labeled "Cancel order", never "Delete".
    expect(screen.queryByTitle("Delete")).not.toBeInTheDocument();

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      Orders: [
        { OrderId: "order-1", OutpostId: "op-1", Status: "CANCELLED", OrderType: "OUTPOST" },
      ],
    });

    await fireEvent.click(screen.getByTitle("Cancel order"));
    expect(confirmDestructive).toHaveBeenCalled();

    await waitFor(() => {
      expect(screen.getByText("CANCELLED")).toBeInTheDocument();
    });

    const cancelCall = mockSend.mock.calls[2][0];
    expect(cancelCall.constructor.name).toBe("CancelOrderCommand");
    expect(cancelCall.input).toEqual({ OrderId: "order-1" });
  });
});
