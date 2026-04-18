import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/svelte";
import S3ControlPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getS3ControlClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("S3 Control Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({
      AccessPointList: [],
      AccessPoints: [],
      StorageLensConfigurationList: [],
    });
    render(S3ControlPage);
    expect(screen.getAllByText("Amazon S3 Control")[0]).toBeInTheDocument();
  });

  it("shows stat cards", () => {
    mockSend.mockResolvedValue({ AccessPointList: [] });
    render(S3ControlPage);
    expect(screen.getAllByText("Access Points")[0]).toBeInTheDocument();
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ AccessPointList: [] });
    render(S3ControlPage);
    expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
  });

  it("shows empty state", async () => {
    mockSend.mockResolvedValue({
      AccessPointList: [],
      AccessPoints: [],
      StorageLensConfigurationList: [],
    });
    render(S3ControlPage);
    await waitFor(
      () => {
        expect(screen.getAllByText(/no access points/i)[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded access points", async () => {
    mockSend.mockResolvedValue({
      AccessPointList: [
        { Name: "my-access-point", Bucket: "my-bucket", NetworkOrigin: "Internet" },
      ],
      AccessPoints: [],
      StorageLensConfigurationList: [],
    });
    render(S3ControlPage);
    await waitFor(
      () => {
        expect(screen.getAllByText("my-access-point")[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows refresh button", () => {
    mockSend.mockResolvedValue({ AccessPointList: [] });
    render(S3ControlPage);
    expect(screen.getByTitle("Refresh")).toBeInTheDocument();
  });

  it("shows Multi-Region APs tab", () => {
    mockSend.mockResolvedValue({ AccessPointList: [] });
    render(S3ControlPage);
    expect(screen.getAllByText("Multi-Region APs")[0]).toBeInTheDocument();
  });

  it("shows Storage Lens tab", () => {
    mockSend.mockResolvedValue({ AccessPointList: [] });
    render(S3ControlPage);
    expect(screen.getAllByText("Storage Lens")[0]).toBeInTheDocument();
  });
});
