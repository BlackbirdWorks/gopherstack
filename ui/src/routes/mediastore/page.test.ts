import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import MediaStorePage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getMediaStoreClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleContainer = {
  Name: "my-container",
  ARN: "arn:aws:mediastore:us-east-1:123456789012:container/my-container",
  Status: "ACTIVE",
  Endpoint: "https://abc123.data.mediastore.us-east-1.amazonaws.com",
  CreationTime: new Date("2024-01-01T00:00:00Z"),
  AccessLoggingEnabled: false,
};

async function renderWithContainer() {
  mockSend.mockResolvedValueOnce({ Containers: [exampleContainer] });
  render(MediaStorePage);
  await waitFor(() => screen.getByText("my-container"));
}

async function selectContainer() {
  mockSend.mockResolvedValueOnce({ Container: exampleContainer });
  await fireEvent.click(screen.getByText("my-container"));
  await waitFor(() => screen.getByText(exampleContainer.ARN));
}

describe("MediaStore Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ Containers: [] });
    render(MediaStorePage);
    expect(screen.getByText("MediaStore")).toBeInTheDocument();
  });

  it("lists containers with no pagination token sent", async () => {
    await renderWithContainer();
    expect(mockSend).toHaveBeenCalledWith(expect.objectContaining({ input: { MaxResults: 100 } }));
    expect(screen.getByText("my-container")).toBeInTheDocument();
  });

  it("shows the AWS error code in the toast when loading containers fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), {
      name: "ThrottlingException",
      $metadata: { httpStatusCode: 429 },
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    render(MediaStorePage);

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        expect.stringContaining("ThrottlingException: Rate exceeded."),
      );
    });
  });

  it("creates a container via the inline modal (placeholder-only input, no label)", async () => {
    mockSend.mockResolvedValueOnce({ Containers: [] });
    render(MediaStorePage);
    await waitFor(() => screen.getByText("No containers found."));

    await fireEvent.click(screen.getByText("New Container"));
    await fireEvent.input(screen.getByPlaceholderText("Container name"), {
      target: { value: "my-container" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Containers: [exampleContainer] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => screen.getByText("my-container"));
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ input: { ContainerName: "my-container" } }),
    );
  });

  it("shows the AWS error code in the toast when creating a container fails", async () => {
    mockSend.mockResolvedValueOnce({ Containers: [] });
    render(MediaStorePage);
    await waitFor(() => screen.getByText("No containers found."));

    await fireEvent.click(screen.getByText("New Container"));
    await fireEvent.input(screen.getByPlaceholderText("Container name"), {
      target: { value: "my-container" },
    });

    const error = Object.assign(new Error("Container already exists."), {
      name: "ContainerInUseException",
      $metadata: { httpStatusCode: 409 },
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        expect.stringContaining("ContainerInUseException: Container already exists."),
      );
    });
  });

  it("selects a container and shows its ARN/Endpoint/Status in the overview tab", async () => {
    await renderWithContainer();
    await selectContainer();

    expect(screen.getByText(exampleContainer.Endpoint)).toBeInTheDocument();
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ input: { ContainerName: "my-container" } }),
    );
  });

  it("deletes the selected container after confirming", async () => {
    await renderWithContainer();
    await selectContainer();

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Containers: [] });

    await fireEvent.click(screen.getByTitle("Delete Container"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({ input: { ContainerName: "my-container" } }),
      );
    });
    await waitFor(() => screen.getByText("No containers found."));
  });

  it("does not delete the container when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    await renderWithContainer();
    await selectContainer();

    await fireEvent.click(screen.getByTitle("Delete Container"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only List + Describe so far -- no Delete, no reload.
    expect(mockSend).toHaveBeenCalledTimes(2);
    expect(screen.getByText(exampleContainer.ARN)).toBeInTheDocument();
  });

  it("loads and saves a container access policy, sending the exact ContainerName/Policy shape", async () => {
    await renderWithContainer();
    await selectContainer();

    mockSend.mockResolvedValueOnce({ Policy: '{"Version":"2012-10-17","Statement":[]}' });
    await fireEvent.click(screen.getByText("Access Policy"));
    await waitFor(() => screen.getByText("Edit"));
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({ input: { ContainerName: "my-container" } }),
    );

    await fireEvent.click(screen.getByText("Edit"));
    const newPolicy = '{"Version":"2012-10-17","Statement":[{"Sid":"1"}]}';
    // getByRole("textbox") is ambiguous with the sidebar's container-search
    // input -- target the policy textarea by its placeholder instead.
    await fireEvent.input(screen.getByPlaceholderText('{"Version":"2012-10-17","Statement":[]}'), {
      target: { value: newPolicy },
    });

    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByText("Save Policy"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        4,
        expect.objectContaining({
          input: { ContainerName: "my-container", Policy: newPolicy },
        }),
      );
    });
  });

  it("suppresses the toast and shows the empty state when the container has no policy (PolicyNotFoundException)", async () => {
    await renderWithContainer();
    await selectContainer();

    const error = Object.assign(
      new Error("No policy is associated with the specified container."),
      {
        name: "PolicyNotFoundException",
      },
    );
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByText("Access Policy"));

    await waitFor(() => screen.getByText("No container policy set"));
    expect(toast.error).not.toHaveBeenCalled();
  });

  it("shows the AWS error code in the toast when loading the policy fails for another reason", async () => {
    await renderWithContainer();
    await selectContainer();

    const error = Object.assign(new Error("Access denied."), {
      name: "AccessDeniedException",
      $metadata: { httpStatusCode: 403 },
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByText("Access Policy"));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        expect.stringContaining("AccessDeniedException: Access denied."),
      );
    });
  });

  it("deletes the container policy after confirming", async () => {
    await renderWithContainer();
    await selectContainer();

    mockSend.mockResolvedValueOnce({ Policy: '{"Version":"2012-10-17","Statement":[]}' });
    await fireEvent.click(screen.getByText("Access Policy"));
    await waitFor(() => screen.getByText("Delete"));

    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByText("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        4,
        expect.objectContaining({ input: { ContainerName: "my-container" } }),
      );
    });
    await waitFor(() => screen.getByText("No container policy set"));
  });

  it("saves a CORS policy parsed from the textarea as an exact CorsPolicy array", async () => {
    await renderWithContainer();
    await selectContainer();

    const error = Object.assign(new Error("No CORS policy found."), {
      name: "CorsPolicyNotFoundException",
    });
    mockSend.mockRejectedValueOnce(error);
    await fireEvent.click(screen.getByText("CORS"));
    await waitFor(() => screen.getByText("Add CORS Policy"));

    await fireEvent.click(screen.getByText("Add CORS Policy"));
    const corsJson =
      '[{"AllowedOrigins":["*"],"AllowedMethods":["GET"],"AllowedHeaders":["*"],"MaxAgeSeconds":3000}]';
    await fireEvent.input(
      screen.getByPlaceholderText(
        '[{"AllowedOrigins":["*"],"AllowedMethods":["GET"],"AllowedHeaders":["*"],"MaxAgeSeconds":3000}]',
      ),
      { target: { value: corsJson } },
    );

    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByText("Save CORS"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        4,
        expect.objectContaining({
          input: {
            ContainerName: "my-container",
            CorsPolicy: [
              {
                AllowedOrigins: ["*"],
                AllowedMethods: ["GET"],
                AllowedHeaders: ["*"],
                MaxAgeSeconds: 3000,
              },
            ],
          },
        }),
      );
    });
    expect(screen.getByText("GET")).toBeInTheDocument();
  });

  it("renders lifecycle policy JSON once loaded", async () => {
    await renderWithContainer();
    await selectContainer();

    mockSend.mockResolvedValueOnce({
      LifecyclePolicy: '{"rules":[{"definition":{},"action":"EXPIRE"}]}',
    });
    await fireEvent.click(screen.getByText("Lifecycle"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({ input: { ContainerName: "my-container" } }),
      );
    });
    expect(screen.getByText(/"action": "EXPIRE"/)).toBeInTheDocument();
  });

  it("saves a metric policy parsed from the textarea", async () => {
    await renderWithContainer();
    await selectContainer();

    mockSend.mockRejectedValueOnce(
      Object.assign(new Error("No metric policy found."), { name: "PolicyNotFoundException" }),
    );
    await fireEvent.click(screen.getByText("Metrics"));
    await waitFor(() => screen.getByText("Add Metric Policy"));

    await fireEvent.click(screen.getByText("Add Metric Policy"));
    const metricsJson = '{"ContainerLevelMetrics":"ENABLED","MetricPolicyRules":[]}';
    await fireEvent.input(
      screen.getByPlaceholderText('{"ContainerLevelMetrics":"ENABLED","MetricPolicyRules":[]}'),
      { target: { value: metricsJson } },
    );

    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByText("Save Metric Policy"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        4,
        expect.objectContaining({
          input: {
            ContainerName: "my-container",
            MetricPolicy: { ContainerLevelMetrics: "ENABLED", MetricPolicyRules: [] },
          },
        }),
      );
    });
    expect(screen.getByText("ENABLED")).toBeInTheDocument();
  });

  it("toggles access logging on and off with StartAccessLogging/StopAccessLogging", async () => {
    await renderWithContainer();
    await selectContainer();

    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByRole("button", { name: "Enable" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({ input: { ContainerName: "my-container" } }),
      );
    });
    await waitFor(() => screen.getByRole("button", { name: "Disable" }));

    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByRole("button", { name: "Disable" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        4,
        expect.objectContaining({ input: { ContainerName: "my-container" } }),
      );
    });
  });

  it("loads tags, adds a tag with the exact Resource/Tags shape, and removes it", async () => {
    await renderWithContainer();
    await selectContainer();

    mockSend.mockResolvedValueOnce({ Tags: [{ Key: "env", Value: "prod" }] });
    await fireEvent.click(screen.getByText("Tags"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({ input: { Resource: exampleContainer.ARN } }),
      );
    });
    expect(screen.getByText("env")).toBeInTheDocument();

    await fireEvent.input(screen.getByPlaceholderText("Key"), { target: { value: "team" } });
    await fireEvent.input(screen.getByPlaceholderText("Value"), { target: { value: "platform" } });

    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByRole("button", { name: "Add" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        4,
        expect.objectContaining({
          input: {
            Resource: exampleContainer.ARN,
            Tags: [{ Key: "team", Value: "platform" }],
          },
        }),
      );
    });
    await waitFor(() => screen.getByText("team"));

    mockSend.mockResolvedValueOnce({});
    // "env"'s immediate parent is just the label wrapper `<div class="text-sm">`
    // -- the remove button is a sibling of THAT div, one level up.
    const envRow = screen.getByText("env").closest("div.flex") as HTMLElement;
    await fireEvent.click(within(envRow).getByTitle("Remove tag"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        5,
        expect.objectContaining({
          input: { Resource: exampleContainer.ARN, TagKeys: ["env"] },
        }),
      );
    });
    await waitFor(() => expect(screen.queryByText("env")).not.toBeInTheDocument());
  });
});
