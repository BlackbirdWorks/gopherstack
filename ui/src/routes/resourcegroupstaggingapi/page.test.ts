import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import ResourceGroupsTaggingAPIPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getResourceGroupsTaggingAPIClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleResource = {
  ResourceARN: "arn:aws:dynamodb:us-east-1:123456789012:table/Orders",
  Tags: [{ Key: "env", Value: "prod" }],
};

describe("Resource Groups Tagging API Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ ResourceTagMappingList: [exampleResource] });
    render(ResourceGroupsTaggingAPIPage);
    expect(screen.getByText("Resource Groups Tagging API")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ ResourceTagMappingList: [exampleResource] });
    render(ResourceGroupsTaggingAPIPage);
    expect(screen.getByText("Tagged Resources")).toBeInTheDocument();
    expect(screen.getByText("Tag Keys")).toBeInTheDocument();
    expect(screen.getByText("Compliance")).toBeInTheDocument();
    expect(screen.getByText("Reports")).toBeInTheDocument();
  });

  it("lists tagged resources", async () => {
    mockSend.mockResolvedValueOnce({ ResourceTagMappingList: [exampleResource] });
    render(ResourceGroupsTaggingAPIPage);
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: exampleResource.ResourceARN })).toBeInTheDocument();
    });
  });

  it("shows the cross-service coverage disclosure", async () => {
    mockSend.mockResolvedValueOnce({ ResourceTagMappingList: [exampleResource] });
    render(ResourceGroupsTaggingAPIPage);
    await waitFor(() => screen.getByRole("cell", { name: exampleResource.ResourceARN }));
    expect(screen.getByText(/11 of ~90 gopherstack services/)).toBeInTheDocument();
  });

  it("tags a resource via the modal", async () => {
    mockSend.mockResolvedValueOnce({ ResourceTagMappingList: [exampleResource] });
    render(ResourceGroupsTaggingAPIPage);
    await waitFor(() => screen.getByRole("cell", { name: exampleResource.ResourceARN }));

    await fireEvent.click(screen.getByText("Tag a resource"));
    expect(screen.getByText("Tag a Resource")).toBeInTheDocument();

    await fireEvent.input(screen.getByLabelText("Resource ARN"), {
      target: { value: "arn:aws:sqs:us-east-1:123456789012:queue/my-queue" },
    });
    await fireEvent.input(screen.getByLabelText("Tag Key"), { target: { value: "team" } });
    await fireEvent.input(screen.getByLabelText("Tag Value"), { target: { value: "payments" } });

    mockSend.mockResolvedValueOnce({ FailedResourcesMap: {} });
    mockSend.mockResolvedValueOnce({
      ResourceTagMappingList: [
        exampleResource,
        {
          ResourceARN: "arn:aws:sqs:us-east-1:123456789012:queue/my-queue",
          Tags: [{ Key: "team", Value: "payments" }],
        },
      ],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Apply Tag" }));

    await waitFor(() => {
      expect(
        screen.getByRole("cell", { name: "arn:aws:sqs:us-east-1:123456789012:queue/my-queue" }),
      ).toBeInTheDocument();
    });
    // GetResources (initial) + TagResources + GetResources (refresh).
    expect(mockSend).toHaveBeenCalledTimes(3);
  });

  it("views a resource's detail with compliance details", async () => {
    mockSend.mockResolvedValueOnce({ ResourceTagMappingList: [exampleResource] });
    render(ResourceGroupsTaggingAPIPage);
    await waitFor(() => screen.getByRole("cell", { name: exampleResource.ResourceARN }));

    mockSend.mockResolvedValueOnce({
      ResourceTagMappingList: [
        {
          ...exampleResource,
          ComplianceDetails: { ComplianceStatus: false, MissingTagKeys: ["cost-center"] },
        },
      ],
    });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getByText(/Non-compliant/)).toBeInTheDocument();
      expect(screen.getByText(/cost-center/)).toBeInTheDocument();
    });
  });

  it("removes a tag from a resource via the detail modal", async () => {
    mockSend.mockResolvedValueOnce({ ResourceTagMappingList: [exampleResource] });
    render(ResourceGroupsTaggingAPIPage);
    await waitFor(() => screen.getByRole("cell", { name: exampleResource.ResourceARN }));

    mockSend.mockResolvedValueOnce({ ResourceTagMappingList: [exampleResource] });
    await fireEvent.click(screen.getByTitle("View"));
    await waitFor(() => screen.getByLabelText("Remove tag env"));

    mockSend.mockResolvedValueOnce({ FailedResourcesMap: {} });
    mockSend.mockResolvedValueOnce({
      ResourceTagMappingList: [{ ResourceARN: exampleResource.ResourceARN, Tags: [] }],
    });

    await fireEvent.click(screen.getByLabelText("Remove tag env"));

    await waitFor(() => {
      expect(screen.getAllByText("No tags").length).toBeGreaterThan(0);
    });
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), {
      name: "ThrottledException",
      $metadata: { httpStatusCode: 429 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(ResourceGroupsTaggingAPIPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(screen.getByText("ThrottledException (HTTP 429): Rate exceeded.")).toBeInTheDocument();
    });
  });

  it("switches to the tag keys tab, lists keys, and views values", async () => {
    mockSend.mockResolvedValueOnce({ ResourceTagMappingList: [exampleResource] });
    render(ResourceGroupsTaggingAPIPage);
    await waitFor(() => screen.getByRole("cell", { name: exampleResource.ResourceARN }));

    mockSend.mockResolvedValueOnce({ TagKeys: ["env", "team"] });
    await fireEvent.click(screen.getByText("Tag Keys"));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "env" })).toBeInTheDocument();
    });

    mockSend.mockResolvedValueOnce({ TagValues: ["prod", "staging"] });
    const viewButtons = screen.getAllByTitle("View values");
    await fireEvent.click(viewButtons[0]);

    await waitFor(() => {
      expect(screen.getByText("prod")).toBeInTheDocument();
      expect(screen.getByText("staging")).toBeInTheDocument();
    });
  });

  it("switches to the compliance tab and lists summary rows", async () => {
    mockSend.mockResolvedValueOnce({ ResourceTagMappingList: [exampleResource] });
    render(ResourceGroupsTaggingAPIPage);
    await waitFor(() => screen.getByRole("cell", { name: exampleResource.ResourceARN }));

    mockSend.mockResolvedValueOnce({
      SummaryList: [
        { Region: "us-east-1", ResourceType: "dynamodb:table", NonCompliantResources: 0 },
      ],
    });
    await fireEvent.click(screen.getByText("Compliance"));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "dynamodb:table" })).toBeInTheDocument();
    });
    expect(screen.getByText(/aggregates across every member account/)).toBeInTheDocument();
  });

  it("loads required tags on the compliance tab and reports the empty state honestly", async () => {
    mockSend.mockResolvedValueOnce({ ResourceTagMappingList: [exampleResource] });
    render(ResourceGroupsTaggingAPIPage);
    await waitFor(() => screen.getByRole("cell", { name: exampleResource.ResourceARN }));

    mockSend.mockResolvedValueOnce({ SummaryList: [] });
    await fireEvent.click(screen.getByText("Compliance"));
    await waitFor(() => screen.getByText("Load required tags"));

    mockSend.mockResolvedValueOnce({ RequiredTags: [] });
    await fireEvent.click(screen.getByText("Load required tags"));

    await waitFor(() => {
      expect(screen.getByText(/no attached tag policy/)).toBeInTheDocument();
    });
  });

  it("switches to the reports tab, shows status, and starts a report", async () => {
    mockSend.mockResolvedValueOnce({ ResourceTagMappingList: [exampleResource] });
    render(ResourceGroupsTaggingAPIPage);
    await waitFor(() => screen.getByRole("cell", { name: exampleResource.ResourceARN }));

    mockSend.mockResolvedValueOnce({ Status: "NO REPORT" });
    await fireEvent.click(screen.getByText("Reports"));

    await waitFor(() => {
      expect(screen.getByText("NO REPORT")).toBeInTheDocument();
    });

    await fireEvent.input(screen.getByPlaceholderText("my-tagging-report-bucket"), {
      target: { value: "my-bucket" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Status: "RUNNING", StartDate: "2026-07-30T00:00:00Z" });

    await fireEvent.click(screen.getByRole("button", { name: "Start report" }));

    await waitFor(() => {
      expect(screen.getByText("RUNNING")).toBeInTheDocument();
    });
  });
});
