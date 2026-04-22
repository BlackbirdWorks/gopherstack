import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import APIGatewayPage from "./+page.svelte";

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

describe("API Gateway Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
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
});
