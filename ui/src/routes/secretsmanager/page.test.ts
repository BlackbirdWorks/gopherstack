import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import SecretsManagerPage from "./+page.svelte";

const mockSend = vi.fn();
vi.mock("$lib/aws-client", () => ({ getSecretsManagerClient: () => ({ send: mockSend }) }));
vi.mock("svelte-sonner", () => ({ toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() } }));

const mockSecret = {
  Name: "prod/db/password",
  ARN: "arn:aws:secretsmanager:us-east-1:123:secret:prod/db/password-abc",
  Description: "Production DB password",
  LastChangedDate: new Date("2024-03-01"),
  RotationEnabled: true,
};

describe("Secrets Manager Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ SecretList: [] });
    render(SecretsManagerPage);
    expect(screen.getByText("Secrets Manager")).toBeInTheDocument();
  });

  it("shows Create Secret button", () => {
    mockSend.mockResolvedValue({ SecretList: [] });
    render(SecretsManagerPage);
    expect(screen.getByText("Create Secret")).toBeInTheDocument();
  });

  it("shows empty state", async () => {
    mockSend.mockResolvedValue({ SecretList: [] });
    render(SecretsManagerPage);
    await waitFor(
      () => {
        expect(screen.getByText("No secrets found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded secrets", async () => {
    mockSend.mockResolvedValueOnce({ SecretList: [mockSecret] });
    render(SecretsManagerPage);
    await waitFor(
      () => {
        expect(screen.getByText("prod/db/password")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows secret description", async () => {
    mockSend.mockResolvedValueOnce({ SecretList: [mockSecret] });
    render(SecretsManagerPage);
    await waitFor(
      () => {
        expect(screen.getByText("Production DB password")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("filters secrets by search", async () => {
    const secret2 = { Name: "dev/api/key", ARN: "arn:aws:...", Description: "Dev API key" };
    mockSend.mockResolvedValueOnce({ SecretList: [mockSecret, secret2] });
    render(SecretsManagerPage);
    await waitFor(() => expect(screen.getByText("prod/db/password")).toBeInTheDocument(), {
      timeout: 3000,
    });
    const searchInput = screen.getByPlaceholderText(/search/i);
    await fireEvent.input(searchInput, { target: { value: "dev" } });
    await waitFor(() => expect(screen.queryByText("prod/db/password")).not.toBeInTheDocument());
    expect(screen.getByText("dev/api/key")).toBeInTheDocument();
  });

  it("shows rotation badge when enabled", async () => {
    mockSend.mockResolvedValueOnce({ SecretList: [mockSecret] });
    render(SecretsManagerPage);
    await waitFor(
      () => {
        expect(screen.getByText("Rotation On")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows error toast on failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("access denied"));
    render(SecretsManagerPage);
    const { toast } = await import("svelte-sonner");
    await waitFor(() => expect(vi.mocked(toast.error)).toHaveBeenCalled(), { timeout: 3000 });
  });

  it("shows count summary", async () => {
    mockSend.mockResolvedValueOnce({ SecretList: [mockSecret] });
    render(SecretsManagerPage);
    await waitFor(
      () => {
        expect(screen.getByText(/1 secret/i)).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });
});
