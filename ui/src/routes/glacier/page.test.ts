import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/svelte";
import GlacierPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getGlacierClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: vi.fn().mockResolvedValue(false),
}));

describe("Glacier Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockResolvedValue({ VaultList: [] });
  });

  it("renders page title", () => {
    render(GlacierPage);
    expect(screen.getByText("Amazon S3 Glacier")).toBeInTheDocument();
  });

  it("shows info banner", () => {
    render(GlacierPage);
    expect(screen.getByText("Managed via Amazon S3")).toBeInTheDocument();
  });

  it("shows storage classes", () => {
    render(GlacierPage);
    expect(screen.getByText("Glacier Storage Classes")).toBeInTheDocument();
  });

  it("shows instant retrieval class", () => {
    render(GlacierPage);
    expect(screen.getByText("S3 Glacier Instant Retrieval")).toBeInTheDocument();
  });

  it("shows flexible retrieval class", () => {
    render(GlacierPage);
    expect(screen.getByText("S3 Glacier Flexible Retrieval")).toBeInTheDocument();
  });

  it("shows deep archive class", () => {
    render(GlacierPage);
    expect(screen.getByText("S3 Glacier Deep Archive")).toBeInTheDocument();
  });

  it("shows create vault button", () => {
    render(GlacierPage);
    expect(screen.getByText("Create Vault")).toBeInTheDocument();
  });
});
