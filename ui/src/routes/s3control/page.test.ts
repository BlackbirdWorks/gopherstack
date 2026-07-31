import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import S3ControlPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getS3ControlClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

// The page resolves its AccountId from GET /dashboard/api/system/settings
// before making any S3 Control calls (see ensureAccountId in +page.svelte).
// Mock fetch so that resolves immediately and predictably in every test.
const fetchMock = vi.fn().mockResolvedValue({
  ok: true,
  json: () => Promise.resolve({ accountID: "123456789012" }),
});
vi.stubGlobal("fetch", fetchMock);

const exampleAccessPoint = {
  Name: "example-ap",
  Bucket: "example-bucket",
  NetworkOrigin: "Internet",
  AccessPointArn: "arn:aws:s3:us-east-1:123456789012:accesspoint/example-ap",
  Alias: "example-ap-abc123.s3-accesspoint",
};

const exampleStorageLensConfig = {
  Id: "example-config",
  StorageLensArn: "arn:aws:s3:us-east-1:123456789012:storage-lens/example-config",
  HomeRegion: "us-east-1",
  IsEnabled: true,
};

describe("S3 Control Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
    fetchMock.mockReset();
    fetchMock.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ accountID: "123456789012" }),
    });
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ AccessPointList: [] });
    render(S3ControlPage);
    expect(screen.getByText("Amazon S3 Control")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ AccessPointList: [] });
    render(S3ControlPage);
    expect(screen.getByText("Access Points")).toBeInTheDocument();
    expect(screen.getByText("Multi-Region APs")).toBeInTheDocument();
    expect(screen.getByText("Storage Lens")).toBeInTheDocument();
    expect(screen.getByText("Storage Lens Groups")).toBeInTheDocument();
    expect(screen.getByText("Access Grants Locations")).toBeInTheDocument();
    expect(screen.getByText("Outposts Buckets")).toBeInTheDocument();
  });

  it("shows empty state when no access points", async () => {
    mockSend.mockResolvedValueOnce({ AccessPointList: [] });
    render(S3ControlPage);
    await waitFor(() => {
      expect(screen.getByText("No access points found")).toBeInTheDocument();
    });
  });

  it("lists access points", async () => {
    mockSend.mockResolvedValueOnce({ AccessPointList: [exampleAccessPoint] });
    render(S3ControlPage);
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "example-ap" })).toBeInTheDocument();
    });
  });

  it("creates an access point via the modal", async () => {
    mockSend.mockResolvedValueOnce({ AccessPointList: [] });
    render(S3ControlPage);
    await waitFor(() => screen.getByText("No access points found"));

    await fireEvent.click(screen.getByText("Create access point"));
    expect(screen.getByText("Create Access Point")).toBeInTheDocument();

    await fireEvent.input(screen.getByLabelText("Access point name"), {
      target: { value: "example-ap" },
    });
    await fireEvent.input(screen.getByLabelText("Bucket"), {
      target: { value: "example-bucket" },
    });

    mockSend.mockResolvedValueOnce({
      AccessPointArn: exampleAccessPoint.AccessPointArn,
      Alias: exampleAccessPoint.Alias,
    });
    mockSend.mockResolvedValueOnce({ AccessPointList: [exampleAccessPoint] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "example-ap" })).toBeInTheDocument();
    });
    // ListAccessPoints (initial) + CreateAccessPoint + ListAccessPoints (refresh).
    expect(mockSend).toHaveBeenCalledTimes(3);
  });

  it("deletes an access point after confirming", async () => {
    mockSend.mockResolvedValueOnce({ AccessPointList: [exampleAccessPoint] });
    render(S3ControlPage);
    await waitFor(() => screen.getByRole("cell", { name: "example-ap" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ AccessPointList: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No access points found")).toBeInTheDocument();
    });
  });

  it("does not delete an access point when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ AccessPointList: [exampleAccessPoint] });
    render(S3ControlPage);
    await waitFor(() => screen.getByRole("cell", { name: "example-ap" }));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial ListAccessPoints call -- no DeleteAccessPoint, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("cell", { name: "example-ap" })).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Access point not found."), {
      name: "NoSuchAccessPoint",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(S3ControlPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("NoSuchAccessPoint (HTTP 404): Access point not found."),
      ).toBeInTheDocument();
    });
  });

  it("opens an access point's detail view", async () => {
    mockSend.mockResolvedValueOnce({ AccessPointList: [exampleAccessPoint] });
    render(S3ControlPage);
    await waitFor(() => screen.getByRole("cell", { name: "example-ap" }));

    mockSend.mockResolvedValueOnce({ ...exampleAccessPoint });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getAllByText(exampleAccessPoint.AccessPointArn).length).toBeGreaterThan(0);
    });
  });

  it("switches to the Storage Lens tab and loads configurations", async () => {
    mockSend.mockResolvedValueOnce({ AccessPointList: [exampleAccessPoint] });
    render(S3ControlPage);
    await waitFor(() => screen.getByRole("cell", { name: "example-ap" }));

    mockSend.mockResolvedValueOnce({ StorageLensConfigurationList: [exampleStorageLensConfig] });
    await fireEvent.click(screen.getByText("Storage Lens"));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "example-config" })).toBeInTheDocument();
    });
  });

  it("updates a Storage Lens configuration via the edit modal", async () => {
    // Start from a populated (not empty) access points list and
    // synchronize on its cell before switching tabs. Waiting on the empty
    // state's text instead would be an unreliable sync point here: `rows`
    // starts as `[]` before the initial fetch even resolves, so "No access
    // points found" can render on the very first paint, racing against the
    // mount effect's still-in-flight ListAccessPoints call and letting the
    // Storage Lens tab's ListStorageLensConfigurations mock get consumed
    // out of order.
    mockSend.mockResolvedValueOnce({ AccessPointList: [exampleAccessPoint] });
    render(S3ControlPage);
    await waitFor(() => screen.getByRole("cell", { name: "example-ap" }));

    mockSend.mockResolvedValueOnce({ StorageLensConfigurationList: [exampleStorageLensConfig] });
    await fireEvent.click(screen.getByText("Storage Lens"));
    await waitFor(() => screen.getByRole("cell", { name: "example-config" }));

    await fireEvent.click(screen.getByTitle("Edit"));
    expect(screen.getByText("Edit Storage Lens Configuration")).toBeInTheDocument();

    const disabledConfig = { ...exampleStorageLensConfig, IsEnabled: false };
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ StorageLensConfigurationList: [disabledConfig] });

    await fireEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => {
      expect(screen.getByText("Disabled")).toBeInTheDocument();
    });
  });
});
