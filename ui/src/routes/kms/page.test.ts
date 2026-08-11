import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import KMSPage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();
vi.mock("$lib/aws-client", () => ({ getKMSClient: () => ({ send: mockSend }) }));
vi.mock("svelte-sonner", () => ({ toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() } }));

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

const mockKey = { KeyId: "aaaa-bbbb-cccc", KeyArn: "arn:aws:kms:us-east-1:123:key/aaaa-bbbb-cccc" };
const mockKeyMeta = {
  KeyId: "aaaa-bbbb-cccc",
  Description: "My key",
  KeyState: "Enabled",
  KeyUsage: "ENCRYPT_DECRYPT",
  CreationDate: new Date(),
};

describe("KMS Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ Keys: [] });
    render(KMSPage);
    expect(screen.getByText("KMS Keys")).toBeInTheDocument();
  });

  it("shows Create Key button", () => {
    mockSend.mockResolvedValue({ Keys: [] });
    render(KMSPage);
    expect(screen.getByText("Create Key")).toBeInTheDocument();
  });

  it("shows empty state", async () => {
    mockSend.mockResolvedValue({ Keys: [] });
    render(KMSPage);
    await waitFor(
      () => {
        expect(screen.getByText("No KMS keys found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("loads and displays keys", async () => {
    mockSend.mockResolvedValueOnce({ Keys: [mockKey] });
    mockSend.mockResolvedValueOnce({ KeyMetadata: mockKeyMeta });
    render(KMSPage);
    await waitFor(
      () => {
        expect(screen.getByText("My key")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows key state badge", async () => {
    mockSend.mockResolvedValueOnce({ Keys: [mockKey] });
    mockSend.mockResolvedValueOnce({ KeyMetadata: mockKeyMeta });
    render(KMSPage);
    await waitFor(
      () => {
        expect(screen.getByText("Enabled")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("filters keys by search", async () => {
    const key2 = { KeyId: "xxxx", KeyArn: "arn:aws:kms:us-east-1:123:key/xxxx" };
    const meta2 = {
      KeyId: "xxxx",
      Description: "Other key",
      KeyState: "Enabled",
      KeyUsage: "SIGN_VERIFY",
    };
    mockSend.mockResolvedValueOnce({ Keys: [mockKey, key2] });
    mockSend.mockResolvedValueOnce({ KeyMetadata: mockKeyMeta });
    mockSend.mockResolvedValueOnce({ KeyMetadata: meta2 });
    render(KMSPage);
    await waitFor(() => expect(screen.getByText("My key")).toBeInTheDocument(), { timeout: 3000 });
    const searchInput = screen.getByPlaceholderText(/search/i);
    await fireEvent.input(searchInput, { target: { value: "Other" } });
    await waitFor(() => expect(screen.queryByText("My key")).not.toBeInTheDocument());
    expect(screen.getByText("Other key")).toBeInTheDocument();
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("access denied"));
    render(KMSPage);
    const { toast } = await import("svelte-sonner");
    await waitFor(() => expect(vi.mocked(toast.error)).toHaveBeenCalled(), { timeout: 3000 });
  });

  it("shows key usage type", async () => {
    mockSend.mockResolvedValueOnce({ Keys: [mockKey] });
    mockSend.mockResolvedValueOnce({ KeyMetadata: mockKeyMeta });
    render(KMSPage);
    await waitFor(
      () => {
        expect(screen.getByText("ENCRYPT_DECRYPT")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows Aliases tab and loads aliases", async () => {
    // Initial load: ListKeys returns empty
    mockSend.mockResolvedValueOnce({ Keys: [] });
    // ListAliases called when Aliases tab clicked
    mockSend.mockResolvedValueOnce({
      Aliases: [
        { AliasName: "alias/my-app-key", TargetKeyId: "aaaa-bbbb-cccc" },
        { AliasName: "alias/aws/s3", TargetKeyId: "aws-managed-key" },
      ],
    });
    render(KMSPage);
    await waitFor(() => screen.getByText("No KMS keys found"), { timeout: 3000 });
    await fireEvent.click(screen.getByText("Aliases"));
    await waitFor(
      () => {
        expect(screen.getByText("alias/my-app-key")).toBeInTheDocument();
        expect(screen.getByText("alias/aws/s3")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows Keys and Aliases tab buttons", () => {
    mockSend.mockResolvedValue({ Keys: [] });
    render(KMSPage);
    const buttons = screen.getAllByRole("button");
    const btnTexts = buttons.map((b) => b.textContent?.trim());
    expect(btnTexts.some((t) => t?.includes("Keys"))).toBe(true);
    expect(btnTexts.some((t) => t?.includes("Aliases"))).toBe(true);
  });

  describe("All regions mode", () => {
    it("fans ListKeys out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      // multiRegionList fires every region's ListKeys concurrently, so both
      // ListKeys calls resolve before either region's DescribeKey call --
      // mock responses must be queued in that interleaved order, not
      // per-region: us-east-1 ListKeys, eu-west-1 ListKeys, us-east-1
      // DescribeKey, eu-west-1 DescribeKey.
      mockSend.mockResolvedValueOnce({ Keys: [mockKey] });
      mockSend.mockResolvedValueOnce({ Keys: [{ ...mockKey, KeyId: "eu-key" }] });
      mockSend.mockResolvedValueOnce({ KeyMetadata: mockKeyMeta });
      mockSend.mockResolvedValueOnce({
        KeyMetadata: { ...mockKeyMeta, KeyId: "eu-key", Description: "EU key" },
      });

      render(KMSPage);

      await waitFor(() => expect(screen.getByText("My key")).toBeInTheDocument());
      expect(screen.getByText("EU key")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(4);

      vi.unstubAllGlobals();
    });

    it("issues exactly one ListKeys call in single-region mode", async () => {
      mockSend.mockResolvedValueOnce({ Keys: [mockKey] });
      mockSend.mockResolvedValueOnce({ KeyMetadata: mockKeyMeta });
      render(KMSPage);
      await waitFor(() => expect(screen.getByText("My key")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(2);
    });

    it("renders the same key id from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      // Interleaved order: us-east-1 ListKeys, eu-west-1 ListKeys, us-east-1
      // DescribeKey, eu-west-1 DescribeKey.
      mockSend.mockResolvedValueOnce({ Keys: [mockKey] });
      mockSend.mockResolvedValueOnce({ Keys: [mockKey] });
      mockSend.mockResolvedValueOnce({ KeyMetadata: mockKeyMeta });
      mockSend.mockResolvedValueOnce({ KeyMetadata: mockKeyMeta });

      render(KMSPage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("My key");
        expect(found).toHaveLength(2);
        return found;
      });
      const chips = rows.map(
        (r) =>
          within(r.closest(".rounded-xl") as HTMLElement).getByTestId("region-chip").textContent,
      );
      expect(chips.toSorted()).toEqual(["eu-west-1", "us-east-1"]);

      vi.unstubAllGlobals();
    });

    it("disables the row's own region, not the picker's, when two regions share a key id", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      // Interleaved order: us-east-1 ListKeys, eu-west-1 ListKeys, us-east-1
      // DescribeKey, eu-west-1 DescribeKey.
      mockSend.mockResolvedValueOnce({ Keys: [mockKey] });
      mockSend.mockResolvedValueOnce({ Keys: [mockKey] });
      mockSend.mockResolvedValueOnce({ KeyMetadata: mockKeyMeta });
      mockSend.mockResolvedValueOnce({ KeyMetadata: mockKeyMeta });

      render(KMSPage);
      await waitFor(() => expect(screen.getAllByText("My key")).toHaveLength(2));

      // DisableKey, then the same interleaved reload order as above, with
      // eu-west-1's DescribeKey now reporting Disabled.
      mockSend.mockResolvedValueOnce({});
      mockSend.mockResolvedValueOnce({ Keys: [mockKey] });
      mockSend.mockResolvedValueOnce({ Keys: [mockKey] });
      mockSend.mockResolvedValueOnce({ KeyMetadata: mockKeyMeta });
      mockSend.mockResolvedValueOnce({ KeyMetadata: { ...mockKeyMeta, KeyState: "Disabled" } });

      const rows = screen.getAllByText("My key");
      const euRow = rows
        .map((r) => r.closest(".rounded-xl") as HTMLElement)
        .find((r) => within(r).getByTestId("region-chip").textContent === "eu-west-1")!;
      await fireEvent.click(within(euRow).getByRole("button", { name: /disable/i }));

      await waitFor(() => {
        const remaining = screen.getAllByText("My key");
        const stillEu = remaining
          .map((r) => r.closest(".rounded-xl") as HTMLElement)
          .find((r) => within(r).getByTestId("region-chip").textContent === "eu-west-1")!;
        expect(within(stillEu).getByText("Disabled")).toBeInTheDocument();
      });

      vi.unstubAllGlobals();
    });
  });
});
