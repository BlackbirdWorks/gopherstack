import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import ElastiCachePage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getElastiCacheClient: () => ({ send: mockSend }),
}));

vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: vi.fn().mockResolvedValue(true),
}));

vi.mock("svelte-sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
  },
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

describe("ElastiCache Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page header with ElastiCache title", () => {
    mockSend.mockResolvedValueOnce({ CacheClusters: [] });

    render(ElastiCachePage);

    expect(screen.getByText("ElastiCache")).toBeInTheDocument();
  });

  it("renders Seed Demo button", () => {
    mockSend.mockResolvedValueOnce({ CacheClusters: [] });

    render(ElastiCachePage);

    expect(screen.getByText("Seed Demo")).toBeInTheDocument();
  });

  it("renders Create Cluster button on clusters tab", () => {
    mockSend.mockResolvedValueOnce({ CacheClusters: [] });

    render(ElastiCachePage);

    expect(screen.getByText("Create Cluster")).toBeInTheDocument();
  });

  it("displays loaded clusters", async () => {
    mockSend.mockResolvedValueOnce({
      CacheClusters: [
        { CacheClusterId: "session-cache", CacheClusterStatus: "available", Engine: "redis" },
        { CacheClusterId: "api-cache", CacheClusterStatus: "available", Engine: "memcached" },
      ],
    });

    render(ElastiCachePage);

    await waitFor(
      () => {
        expect(screen.getByText("session-cache")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("api-cache")).toBeInTheDocument();
  });

  it("filters clusters via search input", async () => {
    mockSend.mockResolvedValueOnce({
      CacheClusters: [
        { CacheClusterId: "session-cache", CacheClusterStatus: "available", Engine: "redis" },
        { CacheClusterId: "product-cache", CacheClusterStatus: "available", Engine: "redis" },
        { CacheClusterId: "api-cache", CacheClusterStatus: "available", Engine: "memcached" },
      ],
    });

    render(ElastiCachePage);

    await waitFor(
      () => {
        expect(screen.getByText("session-cache")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    const searchInput = screen.getByPlaceholderText("Search…");
    await fireEvent.input(searchInput, { target: { value: "api" } });

    await waitFor(() => {
      expect(screen.queryByText("session-cache")).not.toBeInTheDocument();
    });
    expect(screen.getByText("api-cache")).toBeInTheDocument();
    expect(screen.queryByText("product-cache")).not.toBeInTheDocument();
  });

  it("opens create cluster modal when button is clicked", async () => {
    mockSend.mockResolvedValueOnce({ CacheClusters: [] });

    render(ElastiCachePage);

    await fireEvent.click(screen.getByText("Create Cluster"));

    expect(screen.getByText("Create Cache Cluster")).toBeInTheDocument();
    expect(screen.getByPlaceholderText("e.g. session-cache")).toBeInTheDocument();
  });

  it("closes create cluster modal on cancel click", async () => {
    mockSend.mockResolvedValueOnce({ CacheClusters: [] });

    render(ElastiCachePage);

    await fireEvent.click(screen.getByText("Create Cluster"));
    expect(screen.getByText("Create Cache Cluster")).toBeInTheDocument();

    await fireEvent.click(screen.getByText("Cancel"));

    await waitFor(() => {
      expect(screen.queryByText("Create Cache Cluster")).not.toBeInTheDocument();
    });
  });

  it("creates a cluster via the modal form", async () => {
    mockSend.mockResolvedValueOnce({ CacheClusters: [] });
    mockSend.mockResolvedValueOnce({ CacheCluster: { CacheClusterId: "new-cache" } });
    mockSend.mockResolvedValueOnce({
      CacheClusters: [
        { CacheClusterId: "new-cache", CacheClusterStatus: "creating", Engine: "redis" },
      ],
    });

    render(ElastiCachePage);

    await fireEvent.click(screen.getByText("Create Cluster"));

    const nameInput = screen.getByPlaceholderText("e.g. session-cache");
    await fireEvent.input(nameInput, { target: { value: "new-cache" } });

    const form = nameInput.closest("form")!;
    await fireEvent.submit(form);

    await waitFor(
      () => {
        expect(mockSend).toHaveBeenCalledTimes(3);
      },
      { timeout: 3000 },
    );

    const { toast } = await import("svelte-sonner");
    expect(toast.success).toHaveBeenCalled();
  });

  it("selects a cluster to view details", async () => {
    mockSend.mockResolvedValueOnce({
      CacheClusters: [
        {
          CacheClusterId: "prod-cache",
          CacheClusterStatus: "available",
          Engine: "redis",
          NumCacheNodes: 1,
        },
      ],
    });

    render(ElastiCachePage);

    await waitFor(
      () => {
        expect(screen.getByText("prod-cache")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    await fireEvent.click(screen.getByText("prod-cache"));

    await waitFor(
      () => {
        expect(screen.getAllByText("prod-cache").length).toBeGreaterThan(1);
      },
      { timeout: 3000 },
    );
  });

  it("shows empty state when no clusters exist", async () => {
    mockSend.mockResolvedValueOnce({ CacheClusters: [] });

    render(ElastiCachePage);

    await waitFor(
      () => {
        expect(screen.getByText("No clusters found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("access denied"));

    render(ElastiCachePage);

    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });

  it("renders all tab buttons", () => {
    mockSend.mockResolvedValueOnce({ CacheClusters: [] });

    render(ElastiCachePage);

    expect(screen.getByText("Clusters")).toBeInTheDocument();
    expect(screen.getByText("Replication Groups")).toBeInTheDocument();
    expect(screen.getByText("Parameter Groups")).toBeInTheDocument();
    expect(screen.getByText("Subnet Groups")).toBeInTheDocument();
    expect(screen.getByText("Snapshots")).toBeInTheDocument();
    expect(screen.getByText("Users")).toBeInTheDocument();
    expect(screen.getByText("Serverless")).toBeInTheDocument();
    expect(screen.getByText("Metrics")).toBeInTheDocument();
    expect(screen.getByText("Docs")).toBeInTheDocument();
  });

  it("switches to replication groups tab", async () => {
    mockSend.mockResolvedValueOnce({ CacheClusters: [] });
    mockSend.mockResolvedValueOnce({
      ReplicationGroups: [{ ReplicationGroupId: "my-rg", Status: "available" }],
    });

    render(ElastiCachePage);

    await fireEvent.click(screen.getByText("Replication Groups"));

    await waitFor(
      () => {
        expect(screen.getByText("Create Group")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows replication groups after loading", async () => {
    mockSend.mockResolvedValueOnce({ CacheClusters: [] });
    mockSend.mockResolvedValueOnce({
      ReplicationGroups: [
        { ReplicationGroupId: "test-rg", Status: "available", Description: "Test group" },
      ],
    });

    render(ElastiCachePage);

    await fireEvent.click(screen.getByText("Replication Groups"));

    await waitFor(
      () => {
        expect(screen.getByText("test-rg")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("switches to parameter groups tab", async () => {
    mockSend.mockResolvedValueOnce({ CacheClusters: [] });
    mockSend.mockResolvedValueOnce({ CacheParameterGroups: [] });

    render(ElastiCachePage);

    await fireEvent.click(screen.getByText("Parameter Groups"));

    await waitFor(
      () => {
        expect(screen.getByText("No parameter groups found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("switches to snapshots tab and shows empty state", async () => {
    mockSend.mockResolvedValueOnce({ CacheClusters: [] });
    mockSend.mockResolvedValueOnce({ Snapshots: [] });

    render(ElastiCachePage);

    await fireEvent.click(screen.getByText("Snapshots"));

    await waitFor(
      () => {
        expect(screen.getByText("No snapshots found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("switches to users tab and shows empty state", async () => {
    mockSend.mockResolvedValueOnce({ CacheClusters: [] });
    mockSend.mockResolvedValueOnce({ Users: [] });

    render(ElastiCachePage);

    await fireEvent.click(screen.getByText("Users"));

    await waitFor(
      () => {
        expect(screen.getByText("No users found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("switches to serverless tab and shows empty state", async () => {
    mockSend.mockResolvedValueOnce({ CacheClusters: [] });
    mockSend.mockResolvedValueOnce({ ServerlessCaches: [] });

    render(ElastiCachePage);

    await fireEvent.click(screen.getByText("Serverless"));

    await waitFor(
      () => {
        expect(screen.getByText("No serverless caches found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("switches to metrics tab and shows resource overview", async () => {
    mockSend.mockResolvedValueOnce({ CacheClusters: [] });
    mockSend.mockResolvedValue({
      CacheClusters: [],
      ReplicationGroups: [],
      CacheParameterGroups: [],
      CacheSubnetGroups: [],
      Snapshots: [],
      Users: [],
      ServerlessCaches: [],
    });

    render(ElastiCachePage);

    await fireEvent.click(screen.getByText("Metrics"));

    await waitFor(
      () => {
        expect(screen.getByText("Resource Overview")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("switches to docs tab and shows operation documentation", async () => {
    mockSend.mockResolvedValueOnce({ CacheClusters: [] });

    render(ElastiCachePage);

    await fireEvent.click(screen.getByText("Docs"));

    await waitFor(
      () => {
        expect(screen.getByText("ElastiCache Operations")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  describe("All regions mode", () => {
    it("fans DescribeCacheClusters out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend
        .mockResolvedValueOnce({ CacheClusters: [{ CacheClusterId: "us-cache" }] })
        .mockResolvedValueOnce({ CacheClusters: [{ CacheClusterId: "eu-cache" }] });

      render(ElastiCachePage);

      await waitFor(() => expect(screen.getByText("us-cache")).toBeInTheDocument());
      expect(screen.getByText("eu-cache")).toBeInTheDocument();

      vi.unstubAllGlobals();
    });

    it("issues exactly one DescribeCacheClusters call in single-region mode", async () => {
      mockSend.mockResolvedValueOnce({ CacheClusters: [{ CacheClusterId: "my-cache" }] });
      render(ElastiCachePage);
      await waitFor(() => expect(screen.getByText("my-cache")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(1);
    });

    it("renders the same cluster id from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend
        .mockResolvedValueOnce({ CacheClusters: [{ CacheClusterId: "shared-cache" }] })
        .mockResolvedValueOnce({ CacheClusters: [{ CacheClusterId: "shared-cache" }] });

      render(ElastiCachePage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("shared-cache");
        expect(found).toHaveLength(2);
        return found;
      });
      const chips = rows.map(
        (r) => within(r.closest("tr") as HTMLElement).getByTestId("region-chip").textContent,
      );
      expect(chips.toSorted()).toEqual(["eu-west-1", "us-east-1"]);

      vi.unstubAllGlobals();
    });
  });
});
