import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import APIGatewayPage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getAPIGatewayClient: () => ({ send: mockSend }),
}));

vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: vi.fn().mockResolvedValue(true),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
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

describe("API Gateway Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    // Every test below predates "All" mode and assumes exactly one
    // Get*Command call per action against a single region, so pin
    // single-region mode here; the "All regions mode" describe block below
    // opts back in.
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ items: [] });
    render(APIGatewayPage);
    expect(screen.getByText("API Gateway")).toBeInTheDocument();
  });

  it("shows all top tabs", () => {
    mockSend.mockResolvedValue({ items: [] });
    render(APIGatewayPage);
    expect(screen.getByText("REST APIs")).toBeInTheDocument();
    expect(screen.getByText("API Keys")).toBeInTheDocument();
    expect(screen.getByText("Usage Plans")).toBeInTheDocument();
    expect(screen.getByText("Domain Names")).toBeInTheDocument();
    expect(screen.getByText("Metrics")).toBeInTheDocument();
    expect(screen.getByText("Docs")).toBeInTheDocument();
  });

  it("displays REST APIs", async () => {
    mockSend.mockResolvedValueOnce({
      items: [
        {
          id: "api123",
          name: "my-rest-api",
          description: "My test API",
          endpointConfiguration: { types: ["REGIONAL"] },
          createdDate: new Date("2024-01-01"),
        },
      ],
    });

    render(APIGatewayPage);

    await waitFor(() => {
      expect(screen.getByText("my-rest-api")).toBeInTheDocument();
    });
  });

  it("shows create API modal", async () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(APIGatewayPage);
    await fireEvent.click(screen.getByText("Create API"));
    expect(screen.getByText("Create REST API")).toBeInTheDocument();
  });

  it("switches to API keys tab", async () => {
    mockSend.mockResolvedValue({ items: [] });
    render(APIGatewayPage);
    await fireEvent.click(screen.getByText("API Keys"));
    await waitFor(() => {
      expect(screen.getByText("No API keys found")).toBeInTheDocument();
    });
  });

  it("shows create API key button on API keys tab", async () => {
    mockSend.mockResolvedValue({ items: [] });
    render(APIGatewayPage);
    await fireEvent.click(screen.getByText("API Keys"));
    await waitFor(() => {
      expect(screen.getByText("Create Key")).toBeInTheDocument();
    });
  });

  it("shows create key modal", async () => {
    mockSend.mockResolvedValue({ items: [] });
    render(APIGatewayPage);
    await fireEvent.click(screen.getByText("API Keys"));
    await waitFor(() => screen.getByText("Create Key"));
    await fireEvent.click(screen.getByText("Create Key"));
    expect(screen.getByText("Create API Key")).toBeInTheDocument();
  });

  it("switches to Usage Plans tab", async () => {
    mockSend.mockResolvedValue({ items: [] });
    render(APIGatewayPage);
    await fireEvent.click(screen.getByText("Usage Plans"));
    await waitFor(() => {
      expect(screen.getByText("No usage plans found")).toBeInTheDocument();
    });
  });

  it("switches to Domain Names tab", async () => {
    mockSend.mockResolvedValue({ items: [] });
    render(APIGatewayPage);
    await fireEvent.click(screen.getByText("Domain Names"));
    await waitFor(() => {
      expect(screen.getByText("No custom domain names")).toBeInTheDocument();
    });
  });

  it("shows Metrics tab with stats", async () => {
    mockSend.mockResolvedValue({ items: [] });
    render(APIGatewayPage);
    await fireEvent.click(screen.getByText("Metrics"));
    expect(screen.getByText("Service Metrics")).toBeInTheDocument();
    // The metric card shows "REST APIs" as a category label
    expect(screen.getAllByText("REST APIs").length).toBeGreaterThan(0);
  });

  it("shows Docs tab with supported operations", async () => {
    mockSend.mockResolvedValue({ items: [] });
    render(APIGatewayPage);
    await fireEvent.click(screen.getByText("Docs"));
    expect(screen.getByText(/Supported Operations/)).toBeInTheDocument();
    expect(screen.getByText("CreateRestApi")).toBeInTheDocument();
  });

  it("shows Demo Data button", () => {
    mockSend.mockResolvedValue({ items: [] });
    render(APIGatewayPage);
    expect(screen.getByText("Demo Data")).toBeInTheDocument();
  });

  it("displays API keys with toggle", async () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    mockSend.mockResolvedValueOnce({
      items: [
        {
          id: "key123",
          name: "test-key",
          value: "abcdefghij1234567890",
          enabled: true,
          createdDate: new Date("2024-01-01"),
        },
      ],
    });

    render(APIGatewayPage);
    await fireEvent.click(screen.getByText("API Keys"));
    await waitFor(() => {
      expect(screen.getByText("test-key")).toBeInTheDocument();
    });
  });

  it("shows create usage plan modal", async () => {
    mockSend.mockResolvedValue({ items: [] });
    render(APIGatewayPage);
    await fireEvent.click(screen.getByText("Usage Plans"));
    await waitFor(() => screen.getByText("Create Plan"));
    await fireEvent.click(screen.getByText("Create Plan"));
    expect(screen.getByText("Create Usage Plan")).toBeInTheDocument();
  });

  it("shows create domain modal", async () => {
    mockSend.mockResolvedValue({ items: [] });
    render(APIGatewayPage);
    await fireEvent.click(screen.getByText("Domain Names"));
    await waitFor(() => screen.getByText("Create Domain"));
    await fireEvent.click(screen.getByText("Create Domain"));
    expect(screen.getByText("Create Custom Domain")).toBeInTheDocument();
  });

  describe("All regions mode", () => {
    const api = {
      id: "api123",
      name: "shared-api",
      description: "shared",
      endpointConfiguration: { types: ["REGIONAL"] },
    };

    it("fans GetRestApis out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ items: [api] });
      mockSend.mockResolvedValueOnce({ items: [{ ...api, id: "api-eu", name: "eu-api" }] });

      render(APIGatewayPage);

      await waitFor(() => expect(screen.getByText("shared-api")).toBeInTheDocument());
      expect(screen.getByText("eu-api")).toBeInTheDocument();

      vi.unstubAllGlobals();
    });

    it("issues exactly one GetRestApis call in single-region mode", async () => {
      mockSend.mockResolvedValueOnce({ items: [api] });
      render(APIGatewayPage);
      await waitFor(() => expect(screen.getByText("shared-api")).toBeInTheDocument());
      const calls = mockSend.mock.calls.filter(
        ([cmd]) => cmd?.constructor?.name === "GetRestApisCommand",
      );
      expect(calls).toHaveLength(1);
    });

    it("renders the same API name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ items: [api] });
      mockSend.mockResolvedValueOnce({ items: [api] });

      render(APIGatewayPage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("shared-api");
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
