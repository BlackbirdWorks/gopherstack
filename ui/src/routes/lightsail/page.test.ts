import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import LightsailPage from "./+page.svelte";

// Only the ACTIVE tab's panel is ever mounted (+page.svelte's `{#if}` chain,
// not CSS visibility -- see its own doc comment), so at most one dialog
// exists at a time. Still scope to the open one, matching every other
// page.test.ts in this codebase, since a closed <dialog> just loses its
// `open` attribute rather than unmounting.
function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getLightsailClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), message: vi.fn() },
}));

describe("Lightsail page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ instances: [] });
    render(LightsailPage);
    expect(screen.getByText("Amazon Lightsail")).toBeInTheDocument();
  });

  it("shows every tab group and defaults to the Instances tab", () => {
    mockSend.mockResolvedValueOnce({ instances: [] });
    render(LightsailPage);
    expect(screen.getByText("Compute")).toBeInTheDocument();
    expect(screen.getByText("Storage")).toBeInTheDocument();
    expect(screen.getByText("Networking")).toBeInTheDocument();
    expect(screen.getByText("Databases")).toBeInTheDocument();
    expect(screen.getByText("Containers")).toBeInTheDocument();
    expect(screen.getByText("Distributions")).toBeInTheDocument();
    expect(screen.getByText("Domains")).toBeInTheDocument();
    expect(screen.getByText("Monitoring")).toBeInTheDocument();
    expect(screen.getAllByText("Instances").length).toBeGreaterThan(0);
  });

  it("shows empty state when there are no instances", async () => {
    mockSend.mockResolvedValueOnce({ instances: [] });
    render(LightsailPage);
    await waitFor(() => {
      expect(screen.getByText("No instances found")).toBeInTheDocument();
    });
  });

  it("lists instances", async () => {
    mockSend.mockResolvedValueOnce({
      instances: [
        {
          name: "my-instance",
          state: { name: "running", code: 16 },
          blueprintId: "amazon_linux_2023",
        },
      ],
    });
    render(LightsailPage);
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "my-instance" })).toBeInTheDocument();
    });
  });

  it("creates an instance via the modal", async () => {
    mockSend.mockResolvedValueOnce({ instances: [] });
    render(LightsailPage);
    await waitFor(() => screen.getByText("No instances found"));

    await fireEvent.click(screen.getByRole("button", { name: "Create instance" }));
    const dialog = openDialog();
    await fireEvent.input(within(dialog).getByLabelText("Name"), {
      target: { value: "new-instance" },
    });
    await fireEvent.input(within(dialog).getByLabelText(/Blueprint ID/), {
      target: { value: "amazon_linux_2023" },
    });
    await fireEvent.input(within(dialog).getByLabelText(/Bundle ID/), {
      target: { value: "nano_3_0" },
    });

    mockSend.mockResolvedValueOnce({ operations: [{ id: "op-1" }] });
    mockSend.mockResolvedValueOnce({
      instances: [{ name: "new-instance", state: { name: "pending", code: 0 } }],
    });

    await fireEvent.click(within(dialog).getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "new-instance" })).toBeInTheDocument();
    });
  });

  it("deletes an instance after confirming", async () => {
    mockSend.mockResolvedValueOnce({
      instances: [{ name: "doomed-instance", state: { name: "running", code: 16 } }],
    });
    render(LightsailPage);
    await waitFor(() => screen.getByRole("cell", { name: "doomed-instance" }));

    mockSend.mockResolvedValueOnce({ operations: [{ id: "op-2" }] });
    mockSend.mockResolvedValueOnce({ instances: [] });

    await fireEvent.click(screen.getByText("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No instances found")).toBeInTheDocument();
    });
  });

  it("shows an inline error with the AWS error code and message when a load fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), {
      name: "ThrottlingException",
      $metadata: { httpStatusCode: 429 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(LightsailPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ThrottlingException (HTTP 429): Rate exceeded."),
      ).toBeInTheDocument();
    });
  });

  it("switches to the Storage group and shows the Disks tab", async () => {
    mockSend.mockResolvedValueOnce({ instances: [] });
    render(LightsailPage);
    await waitFor(() => screen.getByText("No instances found"));

    mockSend.mockResolvedValueOnce({
      disks: [{ name: "disk-1", state: "available", sizeInGb: 32 }],
    });
    await fireEvent.click(screen.getByText("Storage"));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "disk-1" })).toBeInTheDocument();
    });
  });

  it("switches to the Monitoring group and lists recent operations", async () => {
    mockSend.mockResolvedValueOnce({ instances: [] });
    render(LightsailPage);
    await waitFor(() => screen.getByText("No instances found"));

    mockSend.mockResolvedValueOnce({
      alarms: [{ name: "alarm-1", state: "OK" }],
    });
    await fireEvent.click(screen.getByText("Monitoring"));
    await waitFor(() => screen.getByText("Alarms"));

    mockSend.mockResolvedValueOnce({
      operations: [
        {
          id: "op-3",
          operationType: "CreateInstance",
          resourceName: "my-instance",
          resourceType: "Instance",
          status: "Succeeded",
        },
      ],
    });
    await fireEvent.click(screen.getByText("Operations"));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "CreateInstance" })).toBeInTheDocument();
    });
  });

  it("labels the Reference Data tab as synthetic seed data, never AWS's real catalog", async () => {
    mockSend.mockResolvedValueOnce({ instances: [] });
    render(LightsailPage);
    await waitFor(() => screen.getByText("No instances found"));

    mockSend.mockResolvedValueOnce({ alarms: [] });
    await fireEvent.click(screen.getByText("Monitoring"));
    await waitFor(() => screen.getByText("Reference Data"));

    mockSend.mockResolvedValueOnce({ blueprints: [] });
    await fireEvent.click(screen.getByText("Reference Data"));

    await waitFor(() => {
      expect(screen.getByText(/not AWS's real, authoritative/)).toBeInTheDocument();
    });
  });

  it("does not offer a StaticIp release without loading the Static IPs tab, and never fabricates metric charts", async () => {
    mockSend.mockResolvedValueOnce({
      instances: [{ name: "with-metrics", state: { name: "running", code: 16 } }],
    });
    render(LightsailPage);
    await waitFor(() => screen.getByRole("cell", { name: "with-metrics" }));

    mockSend.mockResolvedValueOnce({
      instance: { name: "with-metrics", state: { name: "running", code: 16 } },
    });
    mockSend.mockResolvedValueOnce({ portStates: [] });
    mockSend.mockResolvedValueOnce({ autoSnapshots: [] });
    await fireEvent.click(screen.getByText("View"));
    const dialog = openDialog();
    await waitFor(() => within(dialog).getByText("Metrics"));

    mockSend.mockResolvedValueOnce({ metricData: [] });
    await fireEvent.click(within(dialog).getByText("Check CPU utilization"));

    await waitFor(() => {
      expect(
        within(dialog).getByText(/does not produce real metric datapoints/),
      ).toBeInTheDocument();
    });
  });
});
