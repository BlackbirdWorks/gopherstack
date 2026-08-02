import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import KinesisAnalyticsPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getKinesisAnalyticsClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleApp = {
  ApplicationName: "my-sql-app",
  ApplicationStatus: "READY",
};

const createTimestamp = new Date("2024-01-01T00:00:00Z");

describe("Kinesis Analytics (v1) Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [] });
    render(KinesisAnalyticsPage);
    expect(screen.getByText("Kinesis Data Analytics (SQL / v1)")).toBeInTheDocument();
  });

  it("lists applications, rendering name and status cells", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsPage);
    await waitFor(() => {
      expect(screen.getByText("my-sql-app")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(expect.objectContaining({ input: {} }));
    expect(screen.getByRole("cell", { name: "READY" })).toBeInTheDocument();
  });

  it("shows empty state when there are no applications", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [] });
    render(KinesisAnalyticsPage);
    await waitFor(() => {
      expect(screen.getByText("No applications found")).toBeInTheDocument();
    });
  });

  it("shows a toast error including the AWS error code when loading applications fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), { name: "ThrottlingException" });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    render(KinesisAnalyticsPage);

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        "Failed to load applications: ThrottlingException: Rate exceeded.",
      );
    });
  });

  it("creates an application via the form, omitting blank code and description", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [] });
    render(KinesisAnalyticsPage);
    await waitFor(() => screen.getByText("No applications found"));

    await fireEvent.click(screen.getByText("Create Application"));
    await fireEvent.input(screen.getByPlaceholderText("my-sql-app"), {
      target: { value: "new-app" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      ApplicationSummaries: [{ ...exampleApp, ApplicationName: "new-app" }],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("new-app")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        input: {
          ApplicationName: "new-app",
          ApplicationCode: undefined,
          ApplicationDescription: undefined,
        },
      }),
    );
  });

  it("refuses to create an application with a blank name", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [] });
    render(KinesisAnalyticsPage);
    await waitFor(() => screen.getByText("No applications found"));
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByText("Create Application"));
    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(toast.error).toHaveBeenCalledWith("Application name is required");
    expect(mockSend).toHaveBeenCalledTimes(1);
  });

  it("shows a toast error including the AWS error code when creating an application fails", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [] });
    render(KinesisAnalyticsPage);
    await waitFor(() => screen.getByText("No applications found"));

    await fireEvent.click(screen.getByText("Create Application"));
    await fireEvent.input(screen.getByPlaceholderText("my-sql-app"), {
      target: { value: "new-app" },
    });

    const error = Object.assign(new Error("Limit exceeded."), { name: "LimitExceededException" });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        "Failed to create application: LimitExceededException: Limit exceeded.",
      );
    });
  });

  it("deletes an application immediately, with no confirmation dialog", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsPage);
    await waitFor(() => screen.getByText("my-sql-app"));

    mockSend.mockResolvedValueOnce({
      ApplicationDetail: { ApplicationName: "my-sql-app", CreateTimestamp: createTimestamp },
    });
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({
          input: { ApplicationName: "my-sql-app", CreateTimestamp: createTimestamp },
        }),
      );
    });
    await waitFor(() => {
      expect(screen.getByText("No applications found")).toBeInTheDocument();
    });
  });

  it("shows a toast error including the AWS error code when deleting an application fails", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsPage);
    await waitFor(() => screen.getByText("my-sql-app"));

    const error = Object.assign(new Error("Application not found."), {
      name: "ResourceNotFoundException",
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByTitle("Delete"));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        "Failed to delete application: ResourceNotFoundException: Application not found.",
      );
    });
  });

  it("starts a READY application with an empty InputConfigurations array", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsPage);
    await waitFor(() => screen.getByText("my-sql-app"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      ApplicationSummaries: [{ ...exampleApp, ApplicationStatus: "RUNNING" }],
    });

    await fireEvent.click(screen.getByTitle("Start"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({
          input: { ApplicationName: "my-sql-app", InputConfigurations: [] },
        }),
      );
    });
  });

  it("stops a RUNNING application", async () => {
    mockSend.mockResolvedValueOnce({
      ApplicationSummaries: [{ ...exampleApp, ApplicationStatus: "RUNNING" }],
    });
    render(KinesisAnalyticsPage);
    await waitFor(() => screen.getByText("my-sql-app"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      ApplicationSummaries: [{ ...exampleApp, ApplicationStatus: "READY" }],
    });

    await fireEvent.click(screen.getByTitle("Stop"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({ input: { ApplicationName: "my-sql-app" } }),
      );
    });
  });

  it("expands an application's detail panel, rendering its ARN, version, and description", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsPage);
    await waitFor(() => screen.getByText("my-sql-app"));

    mockSend.mockResolvedValueOnce({
      ApplicationDetail: {
        ApplicationARN: "arn:aws:kinesisanalytics:us-east-1:123456789012:application/my-sql-app",
        ApplicationVersionId: 2,
        ApplicationDescription: "my description",
        InputDescriptions: [],
        OutputDescriptions: [],
        CloudWatchLoggingOptionDescriptions: [],
      },
    });
    await fireEvent.click(screen.getByText("my-sql-app"));

    await waitFor(() => {
      expect(
        screen.getByText("arn:aws:kinesisanalytics:us-east-1:123456789012:application/my-sql-app"),
      ).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ input: { ApplicationName: "my-sql-app" } }),
    );
    expect(screen.getByText("v2")).toBeInTheDocument();
    expect(screen.getByText("my description")).toBeInTheDocument();
    expect(screen.getByText("No inputs configured")).toBeInTheDocument();
  });

  it("adds an input, including the default JSON schema and omitting KinesisStreamsInput when the ARN is blank", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsPage);
    await waitFor(() => screen.getByText("my-sql-app"));

    mockSend.mockResolvedValueOnce({
      ApplicationDetail: {
        ApplicationVersionId: 1,
        InputDescriptions: [],
        OutputDescriptions: [],
        CloudWatchLoggingOptionDescriptions: [],
      },
    });
    await fireEvent.click(screen.getByText("my-sql-app"));
    await waitFor(() => screen.getByText("No inputs configured"));

    await fireEvent.click(screen.getByText("Add Input"));
    const inputPrefixField = screen.getByPlaceholderText("Name prefix *");
    await fireEvent.input(inputPrefixField, {
      target: { value: "SOURCE_SQL_STREAM" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      ApplicationDetail: {
        ApplicationVersionId: 2,
        InputDescriptions: [{ InputId: "1.1", NamePrefix: "SOURCE_SQL_STREAM" }],
        OutputDescriptions: [],
        CloudWatchLoggingOptionDescriptions: [],
      },
    });

    const inputPanel = inputPrefixField.closest("div.space-y-2") as HTMLElement;
    await fireEvent.click(within(inputPanel).getByRole("button", { name: "Add" }));

    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: {
          ApplicationName: "my-sql-app",
          CurrentApplicationVersionId: 1,
          Input: {
            NamePrefix: "SOURCE_SQL_STREAM",
            KinesisStreamsInput: undefined,
            InputSchema: {
              RecordFormat: {
                RecordFormatType: "JSON",
                MappingParameters: { JSONMappingParameters: { RecordRowPath: "$" } },
              },
              RecordColumns: [{ Name: "COL_1", SqlType: "VARCHAR(4)", Mapping: "$.col1" }],
            },
          },
        },
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("SOURCE_SQL_STREAM")).toBeInTheDocument();
    });
  });

  it("refuses to add an input with a blank name prefix", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsPage);
    await waitFor(() => screen.getByText("my-sql-app"));

    mockSend.mockResolvedValueOnce({
      ApplicationDetail: {
        InputDescriptions: [],
        OutputDescriptions: [],
        CloudWatchLoggingOptionDescriptions: [],
      },
    });
    await fireEvent.click(screen.getByText("my-sql-app"));
    await waitFor(() => screen.getByText("No inputs configured"));
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByText("Add Input"));
    const inputPanel = screen
      .getByPlaceholderText("Name prefix *")
      .closest("div.space-y-2") as HTMLElement;
    await fireEvent.click(within(inputPanel).getByRole("button", { name: "Add" }));

    expect(toast.error).toHaveBeenCalledWith("Name prefix is required");
    expect(mockSend).toHaveBeenCalledTimes(2);
  });

  it("adds an output, omitting KinesisStreamsOutput when the ARN is blank", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsPage);
    await waitFor(() => screen.getByText("my-sql-app"));

    mockSend.mockResolvedValueOnce({
      ApplicationDetail: {
        ApplicationVersionId: 1,
        InputDescriptions: [],
        OutputDescriptions: [],
        CloudWatchLoggingOptionDescriptions: [],
      },
    });
    await fireEvent.click(screen.getByText("my-sql-app"));
    await waitFor(() => screen.getByText("No outputs configured"));

    await fireEvent.click(screen.getByText("Add Output"));
    const outputNameField = screen.getByPlaceholderText("Output name *");
    await fireEvent.input(outputNameField, {
      target: { value: "DESTINATION_SQL_STREAM" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      ApplicationDetail: {
        ApplicationVersionId: 2,
        InputDescriptions: [],
        OutputDescriptions: [{ OutputId: "2.1", Name: "DESTINATION_SQL_STREAM" }],
        CloudWatchLoggingOptionDescriptions: [],
      },
    });

    const outputPanel = outputNameField.closest("div.space-y-2") as HTMLElement;
    await fireEvent.click(within(outputPanel).getByRole("button", { name: "Add" }));

    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: {
          ApplicationName: "my-sql-app",
          CurrentApplicationVersionId: 1,
          Output: {
            Name: "DESTINATION_SQL_STREAM",
            KinesisStreamsOutput: undefined,
            DestinationSchema: { RecordFormatType: "JSON" },
          },
        },
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("DESTINATION_SQL_STREAM")).toBeInTheDocument();
    });
  });

  it("deletes an output", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsPage);
    await waitFor(() => screen.getByText("my-sql-app"));

    mockSend.mockResolvedValueOnce({
      ApplicationDetail: {
        ApplicationVersionId: 2,
        InputDescriptions: [],
        OutputDescriptions: [{ OutputId: "2.1", Name: "DESTINATION_SQL_STREAM" }],
        CloudWatchLoggingOptionDescriptions: [],
      },
    });
    await fireEvent.click(screen.getByText("my-sql-app"));
    await waitFor(() => screen.getByText("DESTINATION_SQL_STREAM"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      ApplicationDetail: {
        ApplicationVersionId: 3,
        InputDescriptions: [],
        OutputDescriptions: [],
        CloudWatchLoggingOptionDescriptions: [],
      },
    });

    const row = screen.getByText("DESTINATION_SQL_STREAM").closest("div.flex") as HTMLElement;
    await fireEvent.click(within(row).getByRole("button"));

    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: { ApplicationName: "my-sql-app", CurrentApplicationVersionId: 2, OutputId: "2.1" },
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("No outputs configured")).toBeInTheDocument();
    });
  });

  it("adds CloudWatch logging", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsPage);
    await waitFor(() => screen.getByText("my-sql-app"));

    mockSend.mockResolvedValueOnce({
      ApplicationDetail: {
        ApplicationVersionId: 1,
        InputDescriptions: [],
        OutputDescriptions: [],
        CloudWatchLoggingOptionDescriptions: [],
      },
    });
    await fireEvent.click(screen.getByText("my-sql-app"));
    await waitFor(() => screen.getByText("No CloudWatch logging configured"));

    await fireEvent.click(screen.getByText("Add", { selector: "button" }));
    const cwlArnField = screen.getByPlaceholderText("Log Stream ARN *");
    await fireEvent.input(cwlArnField, {
      target: { value: "arn:aws:logs:us-east-1:123456789012:log-group:g:log-stream:s" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      ApplicationDetail: {
        ApplicationVersionId: 2,
        InputDescriptions: [],
        OutputDescriptions: [],
        CloudWatchLoggingOptionDescriptions: [
          {
            CloudWatchLoggingOptionId: "1.1",
            LogStreamARN: "arn:aws:logs:us-east-1:123456789012:log-group:g:log-stream:s",
          },
        ],
      },
    });

    const cwlPanel = cwlArnField.closest("div.space-y-2") as HTMLElement;
    await fireEvent.click(within(cwlPanel).getByRole("button", { name: "Add" }));

    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: {
          ApplicationName: "my-sql-app",
          CurrentApplicationVersionId: 1,
          CloudWatchLoggingOption: {
            LogStreamARN: "arn:aws:logs:us-east-1:123456789012:log-group:g:log-stream:s",
            RoleARN: "",
          },
        },
      }),
    );
  });

  it("refuses to add CloudWatch logging with a blank log stream ARN", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsPage);
    await waitFor(() => screen.getByText("my-sql-app"));

    mockSend.mockResolvedValueOnce({
      ApplicationDetail: {
        InputDescriptions: [],
        OutputDescriptions: [],
        CloudWatchLoggingOptionDescriptions: [],
      },
    });
    await fireEvent.click(screen.getByText("my-sql-app"));
    await waitFor(() => screen.getByText("No CloudWatch logging configured"));
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByText("Add", { selector: "button" }));
    const cwlPanel = screen
      .getByPlaceholderText("Log Stream ARN *")
      .closest("div.space-y-2") as HTMLElement;
    await fireEvent.click(within(cwlPanel).getByRole("button", { name: "Add" }));

    expect(toast.error).toHaveBeenCalledWith("Log stream ARN is required");
    expect(mockSend).toHaveBeenCalledTimes(2);
  });

  it("discovers an input schema, including an optional role ARN", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [] });
    render(KinesisAnalyticsPage);
    await waitFor(() => screen.getByText("No applications found"));

    await fireEvent.click(screen.getByText("Discover Schema"));
    await fireEvent.input(screen.getByPlaceholderText("Kinesis Stream ARN"), {
      target: { value: "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream" },
    });
    await fireEvent.input(screen.getByPlaceholderText("Role ARN (optional)"), {
      target: { value: "arn:aws:iam::123456789012:role/discover-role" },
    });

    mockSend.mockResolvedValueOnce({ InputSchema: { RecordFormat: { RecordFormatType: "JSON" } } });

    await fireEvent.click(screen.getByRole("button", { name: "Discover" }));

    await waitFor(() => {
      expect(screen.getByText(/"RecordFormatType": "JSON"/)).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        input: {
          ResourceARN: "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
          RoleARN: "arn:aws:iam::123456789012:role/discover-role",
          InputStartingPositionConfiguration: { InputStartingPosition: "NOW" },
        },
      }),
    );
  });

  it("refuses to discover a schema with a blank resource ARN", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [] });
    render(KinesisAnalyticsPage);
    await waitFor(() => screen.getByText("No applications found"));
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByText("Discover Schema"));
    await fireEvent.click(screen.getByRole("button", { name: "Discover" }));

    expect(toast.error).toHaveBeenCalledWith("Resource ARN is required");
    expect(mockSend).toHaveBeenCalledTimes(1);
  });
});
