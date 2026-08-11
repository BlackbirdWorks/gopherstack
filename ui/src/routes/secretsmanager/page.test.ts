import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import SecretsManagerPage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();
vi.mock("$lib/aws-client", () => ({ getSecretsManagerClient: () => ({ send: mockSend }) }));
vi.mock("svelte-sonner", () => ({ toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() } }));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

function stubRegionsWithData(regions: string[]): void {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ regions }),
    }),
  );
}

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
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
    // Every test below predates "All" mode and assumes exactly one
    // ListSecrets call against a single region, so pin single-region mode
    // here; the "All regions mode" describe block below opts back in.
    setStoredRegion(DEFAULT_REGION);
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

  it("creates a secret from modal", async () => {
    mockSend
      .mockResolvedValueOnce({ SecretList: [] })
      .mockResolvedValueOnce({
        ARN: "arn:aws:secretsmanager:us-east-1:123:secret:new/secret-abc",
        Name: "new/secret",
      })
      .mockResolvedValueOnce({
        SecretList: [
          { Name: "new/secret", ARN: "arn:aws:secretsmanager:us-east-1:123:secret:new/secret-abc" },
        ],
      });

    render(SecretsManagerPage);

    await fireEvent.click(screen.getByRole("button", { name: /create secret/i }));
    await fireEvent.input(screen.getByLabelText("Secret Name"), {
      target: { value: "new/secret" },
    });
    await fireEvent.input(screen.getByLabelText("Secret Value"), {
      target: { value: "top-secret" },
    });
    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      const createCall = mockSend.mock.calls.find(
        ([command]) =>
          command?.constructor?.name === "CreateSecretCommand" &&
          command.input?.Name === "new/secret" &&
          command.input?.SecretString === "top-secret",
      );
      expect(createCall).toBeTruthy();
    });
  });

  it("validates required name before create", async () => {
    mockSend.mockResolvedValueOnce({ SecretList: [] });
    render(SecretsManagerPage);

    await fireEvent.click(screen.getByRole("button", { name: /create secret/i }));
    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    const { toast } = await import("svelte-sonner");
    expect(vi.mocked(toast.error)).toHaveBeenCalledWith("Secret name is required");
  });

  it("shows error toast when create fails", async () => {
    mockSend
      .mockResolvedValueOnce({ SecretList: [] })
      .mockRejectedValueOnce(new Error("create denied"));
    render(SecretsManagerPage);

    await fireEvent.click(screen.getByRole("button", { name: /create secret/i }));
    await fireEvent.input(screen.getByLabelText("Secret Name"), {
      target: { value: "new/secret" },
    });
    await fireEvent.input(screen.getByLabelText("Secret Value"), {
      target: { value: "top-secret" },
    });
    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    const { toast } = await import("svelte-sonner");
    await waitFor(() => expect(vi.mocked(toast.error)).toHaveBeenCalledWith("create denied"));
  });

  describe("All regions mode", () => {
    it("fans ListSecrets out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ SecretList: [{ Name: "prod/db/password" }] });
      mockSend.mockResolvedValueOnce({ SecretList: [{ Name: "eu/db/password" }] });

      render(SecretsManagerPage);

      await waitFor(() => expect(screen.getByText("prod/db/password")).toBeInTheDocument());
      expect(screen.getByText("eu/db/password")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(2);

      vi.unstubAllGlobals();
    });

    it("falls back to just the default region when no region has data", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData([]);
      mockSend.mockResolvedValueOnce({ SecretList: [mockSecret] });

      render(SecretsManagerPage);

      await waitFor(() => expect(screen.getByText("prod/db/password")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(1);

      vi.unstubAllGlobals();
    });

    it("issues exactly one ListSecrets call in single-region mode", async () => {
      mockSend.mockResolvedValueOnce({ SecretList: [mockSecret] });
      render(SecretsManagerPage);
      await waitFor(() => expect(screen.getByText("prod/db/password")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(1);
    });

    it("renders the same secret name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ SecretList: [{ Name: "shared-name" }] });
      mockSend.mockResolvedValueOnce({ SecretList: [{ Name: "shared-name" }] });

      render(SecretsManagerPage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("shared-name");
        expect(found).toHaveLength(2);
        return found;
      });
      const chips = rows.map(
        (r) =>
          within(r.closest(".rounded-lg") as HTMLElement).getByTestId("region-chip").textContent,
      );
      expect(chips.toSorted()).toEqual(["eu-west-1", "us-east-1"]);

      vi.unstubAllGlobals();
    });

    it("deletes the row's own region, not the picker's, when two regions share a secret name", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ SecretList: [{ Name: "shared-name" }] });
      mockSend.mockResolvedValueOnce({ SecretList: [{ Name: "shared-name" }] });

      render(SecretsManagerPage);
      await waitFor(() => expect(screen.getAllByText("shared-name")).toHaveLength(2));

      // Deleting the eu-west-1 row, then reloading, leaves only us-east-1's
      // secret -- if the delete had gone to the wrong region (or a shared
      // client had its signing region frozen on the first region), the
      // reload below wouldn't reflect this split.
      mockSend.mockResolvedValueOnce({});
      mockSend.mockResolvedValueOnce({ SecretList: [{ Name: "shared-name" }] });
      mockSend.mockResolvedValueOnce({ SecretList: [] });

      const rows = screen.getAllByText("shared-name");
      const euRow = rows
        .map((r) => r.closest(".rounded-lg") as HTMLElement)
        .find((r) => within(r).getByTestId("region-chip").textContent === "eu-west-1")!;
      await fireEvent.click(within(euRow).getByRole("button", { name: /delete/i }));

      await waitFor(() => {
        const remaining = screen.getAllByText("shared-name");
        expect(remaining).toHaveLength(1);
        expect(
          within(remaining[0].closest(".rounded-lg") as HTMLElement).getByTestId("region-chip")
            .textContent,
        ).toBe("us-east-1");
      });

      vi.unstubAllGlobals();
    });
  });
});
