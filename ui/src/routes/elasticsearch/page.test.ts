import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import ElasticsearchPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getElasticsearchServiceClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleDomainStatus = {
  DomainName: "my-domain",
  ARN: "arn:aws:es:us-east-1:123456789012:domain/my-domain",
  ElasticsearchVersion: "7.10",
  ElasticsearchClusterConfig: { InstanceType: "t2.small.elasticsearch", InstanceCount: 1 },
  EBSOptions: { VolumeSize: 10 },
  Endpoint: "search-my-domain.us-east-1.es.amazonaws.com",
  Processing: false,
};

function domainCard(name: string): HTMLElement {
  return screen.getByText(name).closest('[role="button"]') as HTMLElement;
}

describe("Elasticsearch Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ DomainNames: [] });
    render(ElasticsearchPage);
    expect(screen.getByText("Amazon Elasticsearch Service")).toBeInTheDocument();
  });

  it("lists domain names", async () => {
    mockSend.mockResolvedValueOnce({ DomainNames: [{ DomainName: "my-domain" }] });
    render(ElasticsearchPage);
    await waitFor(() => {
      expect(screen.getByText("my-domain")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(expect.objectContaining({ input: {} }));
  });

  it("shows empty state when there are no domains", async () => {
    mockSend.mockResolvedValueOnce({ DomainNames: [] });
    render(ElasticsearchPage);
    await waitFor(() => {
      expect(screen.getByText("No domains found")).toBeInTheDocument();
    });
  });

  it("shows a toast error including the AWS error code when loading domains fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), { name: "ThrottlingException" });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    render(ElasticsearchPage);

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        "Failed to load domains: ThrottlingException: Rate exceeded.",
      );
    });
  });

  it("creates a domain via the modal with the right cluster config", async () => {
    mockSend.mockResolvedValueOnce({ DomainNames: [] });
    render(ElasticsearchPage);
    await waitFor(() => screen.getByText("No domains found"));

    await fireEvent.click(screen.getByText("Create Domain"));
    expect(screen.getByText("Create Elasticsearch Domain")).toBeInTheDocument();

    await fireEvent.input(screen.getByLabelText("Domain Name"), {
      target: { value: "new-domain" },
    });

    mockSend.mockResolvedValueOnce({ DomainStatus: exampleDomainStatus });
    mockSend.mockResolvedValueOnce({ DomainNames: [{ DomainName: "new-domain" }] });

    const modal = screen
      .getByText("Create Elasticsearch Domain")
      .closest("div.space-y-4") as HTMLElement;
    await fireEvent.click(within(modal).getByRole("button", { name: "Create Domain" }));

    await waitFor(() => {
      expect(screen.getByText("new-domain")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        input: {
          DomainName: "new-domain",
          ElasticsearchVersion: "7.10",
          ElasticsearchClusterConfig: { InstanceType: "t2.small.elasticsearch", InstanceCount: 1 },
          EBSOptions: { EBSEnabled: true, VolumeSize: 10, VolumeType: "gp2" },
        },
      }),
    );
  });

  it("shows a toast error including the AWS error code when creating a domain fails", async () => {
    mockSend.mockResolvedValueOnce({ DomainNames: [] });
    render(ElasticsearchPage);
    await waitFor(() => screen.getByText("No domains found"));

    await fireEvent.click(screen.getByText("Create Domain"));
    await fireEvent.input(screen.getByLabelText("Domain Name"), {
      target: { value: "new-domain" },
    });

    const error = Object.assign(new Error("Domain already exists."), {
      name: "ResourceAlreadyExistsException",
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    const modal = screen
      .getByText("Create Elasticsearch Domain")
      .closest("div.space-y-4") as HTMLElement;
    await fireEvent.click(within(modal).getByRole("button", { name: "Create Domain" }));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        "Failed to create domain: ResourceAlreadyExistsException: Domain already exists.",
      );
    });
  });

  it("deletes a domain from the list view after confirming", async () => {
    mockSend.mockResolvedValueOnce({ DomainNames: [{ DomainName: "my-domain" }] });
    render(ElasticsearchPage);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ DomainNames: [] });

    const card = domainCard("my-domain");
    await fireEvent.click(card.querySelector("button") as HTMLElement);

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({ input: { DomainName: "my-domain" } }),
      );
    });
    await waitFor(() => {
      expect(screen.getByText("No domains found")).toBeInTheDocument();
    });
  });

  it("does not delete a domain when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ DomainNames: [{ DomainName: "my-domain" }] });
    render(ElasticsearchPage);
    await waitFor(() => screen.getByText("my-domain"));

    const card = domainCard("my-domain");
    await fireEvent.click(card.querySelector("button") as HTMLElement);

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial ListDomainNames call -- no DeleteElasticsearchDomain, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByText("my-domain")).toBeInTheDocument();
  });

  it("opens a domain's detail view showing ARN, version, and endpoint", async () => {
    mockSend.mockResolvedValueOnce({ DomainNames: [{ DomainName: "my-domain" }] });
    render(ElasticsearchPage);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({ DomainStatus: exampleDomainStatus });
    await fireEvent.click(screen.getByText("my-domain"));

    await waitFor(() => {
      expect(screen.getByText(exampleDomainStatus.ARN)).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ input: { DomainName: "my-domain" } }),
    );
    expect(screen.getByText("7.10")).toBeInTheDocument();
    expect(screen.getByText(exampleDomainStatus.Endpoint)).toBeInTheDocument();
  });

  it("shows a toast error including the AWS error code when loading domain details fails", async () => {
    mockSend.mockResolvedValueOnce({ DomainNames: [{ DomainName: "my-domain" }] });
    render(ElasticsearchPage);
    await waitFor(() => screen.getByText("my-domain"));

    const error = Object.assign(new Error("Domain not found."), {
      name: "ResourceNotFoundException",
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByText("my-domain"));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        "Failed to load domain details: ResourceNotFoundException: Domain not found.",
      );
    });
  });

  it("switches to the Tags tab, loads tags, and adds a tag via the modal", async () => {
    mockSend.mockResolvedValueOnce({ DomainNames: [{ DomainName: "my-domain" }] });
    render(ElasticsearchPage);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({ DomainStatus: exampleDomainStatus });
    await fireEvent.click(screen.getByText("my-domain"));
    await waitFor(() => screen.getByText(exampleDomainStatus.ARN));

    mockSend.mockResolvedValueOnce({ TagList: [{ Key: "Environment", Value: "prod" }] });
    await fireEvent.click(screen.getByText("Tags"));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "Environment" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({ input: { ARN: exampleDomainStatus.ARN } }),
    );

    await fireEvent.click(screen.getByText("Add Tag"));
    await fireEvent.input(screen.getByLabelText("Key"), { target: { value: "Team" } });
    await fireEvent.input(screen.getByLabelText("Value"), { target: { value: "platform" } });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      TagList: [
        { Key: "Environment", Value: "prod" },
        { Key: "Team", Value: "platform" },
      ],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Add" }));

    expect(mockSend).toHaveBeenNthCalledWith(
      4,
      expect.objectContaining({
        input: {
          ARN: exampleDomainStatus.ARN,
          TagList: [{ Key: "Team", Value: "platform" }],
        },
      }),
    );
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "Team" })).toBeInTheDocument();
    });
  });

  it("switches to the Packages tab, creates, associates, dissociates, and deletes a package", async () => {
    mockSend.mockResolvedValueOnce({ DomainNames: [{ DomainName: "my-domain" }] });
    render(ElasticsearchPage);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({ DomainStatus: exampleDomainStatus });
    await fireEvent.click(screen.getByText("my-domain"));
    await waitFor(() => screen.getByText(exampleDomainStatus.ARN));

    mockSend.mockResolvedValueOnce({ PackageDetailsList: [] });
    await fireEvent.click(screen.getByText("Packages"));
    await waitFor(() => screen.getByText("No packages"));

    await fireEvent.click(screen.getByText("Create Package"));
    await fireEvent.input(screen.getByLabelText("Package Name"), {
      target: { value: "my-dictionary" },
    });

    const pkg = {
      PackageID: "pkg-1",
      PackageName: "my-dictionary",
      PackageType: "TXT-DICTIONARY",
      PackageDescription: "",
      PackageStatus: "AVAILABLE",
    };
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ PackageDetailsList: [pkg] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "my-dictionary" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      4,
      expect.objectContaining({
        input: {
          PackageName: "my-dictionary",
          PackageType: "TXT-DICTIONARY",
          PackageDescription: "",
          PackageSource: { S3BucketName: "gopherstack-packages", S3Key: "my-dictionary.txt" },
        },
      }),
    );

    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByText("Associate"));
    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        6,
        expect.objectContaining({
          input: { PackageID: "pkg-1", DomainName: "my-domain" },
        }),
      );
    });

    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByText("Dissociate"));
    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        7,
        expect.objectContaining({
          input: { PackageID: "pkg-1", DomainName: "my-domain" },
        }),
      );
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ PackageDetailsList: [] });
    const packageRow = screen
      .getByRole("cell", { name: "my-dictionary" })
      .closest("tr") as HTMLElement;
    await fireEvent.click(within(packageRow).getByText("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No packages")).toBeInTheDocument();
    });
  });

  it("updates a domain's configuration via the modal", async () => {
    mockSend.mockResolvedValueOnce({ DomainNames: [{ DomainName: "my-domain" }] });
    render(ElasticsearchPage);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({ DomainStatus: exampleDomainStatus });
    await fireEvent.click(screen.getByText("my-domain"));
    await waitFor(() => screen.getByText(exampleDomainStatus.ARN));

    await fireEvent.click(screen.getByText("Update Config"));
    await fireEvent.input(screen.getByLabelText("Instance Count"), { target: { value: "3" } });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      DomainStatus: {
        ...exampleDomainStatus,
        ElasticsearchClusterConfig: { InstanceType: "t2.small.elasticsearch", InstanceCount: 3 },
      },
    });

    await fireEvent.click(screen.getByRole("button", { name: "Update" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({
          input: {
            DomainName: "my-domain",
            ElasticsearchClusterConfig: {
              InstanceType: "t2.small.elasticsearch",
              InstanceCount: 3,
            },
          },
        }),
      );
    });
  });

  it("deletes a domain from the detail view after confirming", async () => {
    mockSend.mockResolvedValueOnce({ DomainNames: [{ DomainName: "my-domain" }] });
    render(ElasticsearchPage);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({ DomainStatus: exampleDomainStatus });
    await fireEvent.click(screen.getByText("my-domain"));
    await waitFor(() => screen.getByText(exampleDomainStatus.ARN));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ DomainNames: [] });

    await fireEvent.click(screen.getByText("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({ input: { DomainName: "my-domain" } }),
      );
    });
    await waitFor(() => {
      expect(screen.getByText("No domains found")).toBeInTheDocument();
    });
  });
});
