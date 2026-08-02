import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import CodeArtifactPage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getCodeArtifactClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleDomain = { name: "my-domain", owner: "123456789012", status: "Active" };
const exampleRepo = { name: "my-repo", domainName: "my-domain", description: "A repo" };

describe("CodeArtifact Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ domains: [] });
    render(CodeArtifactPage);
    expect(screen.getByText("AWS CodeArtifact")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No domains found"));
  });

  it("lists domains and shows empty state", async () => {
    mockSend.mockResolvedValueOnce({ domains: [] });
    render(CodeArtifactPage);
    await waitFor(() => {
      expect(screen.getByText("No domains found")).toBeInTheDocument();
    });
  });

  it("displays loaded domains", async () => {
    mockSend.mockResolvedValueOnce({ domains: [exampleDomain] });
    render(CodeArtifactPage);
    await waitFor(() => {
      expect(screen.getByText("my-domain")).toBeInTheDocument();
    });
  });

  it("creates a domain via the modal with the correct input", async () => {
    mockSend.mockResolvedValueOnce({ domains: [] });
    render(CodeArtifactPage);
    await waitFor(() => screen.getByText("No domains found"));

    await fireEvent.click(screen.getByText("Create domain"));
    await fireEvent.input(within(openDialog()).getByLabelText("Domain name"), {
      target: { value: "my-domain" },
    });

    mockSend.mockResolvedValueOnce({ domain: exampleDomain });
    mockSend.mockResolvedValueOnce({ domains: [exampleDomain] });

    await fireEvent.click(within(openDialog()).getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("my-domain")).toBeInTheDocument();
    });

    const createCall = mockSend.mock.calls[1][0];
    expect(createCall.input).toEqual({ domain: "my-domain" });
  });

  it("deletes a domain after confirming", async () => {
    mockSend.mockResolvedValueOnce({ domains: [exampleDomain] });
    render(CodeArtifactPage);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ domains: [] });

    await fireEvent.click(screen.getByTitle("Delete"));
    expect(confirmDestructive).toHaveBeenCalled();

    await waitFor(() => {
      expect(screen.getByText("No domains found")).toBeInTheDocument();
    });

    const deleteCall = mockSend.mock.calls[1][0];
    expect(deleteCall.input).toEqual({ domain: "my-domain" });
  });

  it("does not delete a domain when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ domains: [exampleDomain] });
    render(CodeArtifactPage);
    await waitFor(() => screen.getByText("my-domain"));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByText("my-domain")).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Domain does not exist."), {
      name: "ResourceNotFoundException",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(CodeArtifactPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ResourceNotFoundException (HTTP 404): Domain does not exist."),
      ).toBeInTheDocument();
    });
  });

  it("scopes repositories to the selected domain and creates one", async () => {
    mockSend.mockResolvedValueOnce({ domains: [exampleDomain] });
    render(CodeArtifactPage);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({ repositories: [] });
    await fireEvent.click(screen.getByRole("tab", { name: "Repositories" }));
    await waitFor(() => screen.getByText("No repositories found"));

    const listCall = mockSend.mock.calls[1][0];
    expect(listCall.input).toEqual({ domain: "my-domain" });

    await fireEvent.click(screen.getByText("Create repository"));
    await fireEvent.input(within(openDialog()).getByLabelText("Repository name"), {
      target: { value: "my-repo" },
    });

    mockSend.mockResolvedValueOnce({ repository: exampleRepo });
    mockSend.mockResolvedValueOnce({ repositories: [exampleRepo] });

    await fireEvent.click(within(openDialog()).getByRole("button", { name: "Create" }));
    await waitFor(() => screen.getByText("my-repo"));

    const createCall = mockSend.mock.calls[2][0];
    expect(createCall.input).toEqual({
      domain: "my-domain",
      repository: "my-repo",
      description: undefined,
    });
  });

  it("deletes a repository after confirming", async () => {
    mockSend.mockResolvedValueOnce({ domains: [exampleDomain] });
    render(CodeArtifactPage);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({ repositories: [exampleRepo] });
    await fireEvent.click(screen.getByRole("tab", { name: "Repositories" }));
    await waitFor(() => screen.getByText("my-repo"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ repositories: [] });

    await fireEvent.click(screen.getByTitle("Delete"));
    expect(confirmDestructive).toHaveBeenCalled();

    await waitFor(() => {
      expect(screen.getByText("No repositories found")).toBeInTheDocument();
    });

    const deleteCall = mockSend.mock.calls[2][0];
    expect(deleteCall.input).toEqual({ domain: "my-domain", repository: "my-repo" });
  });

  it("browses from a repository into its packages", async () => {
    mockSend.mockResolvedValueOnce({ domains: [exampleDomain] });
    render(CodeArtifactPage);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({ repositories: [exampleRepo] });
    await fireEvent.click(screen.getByRole("tab", { name: "Repositories" }));
    await waitFor(() => screen.getByText("my-repo"));

    mockSend.mockResolvedValueOnce({ packages: [{ format: "npm", package: "left-pad" }] });
    await fireEvent.click(screen.getByText("my-repo"));

    await waitFor(() => screen.getByText("left-pad"));

    const listCall = mockSend.mock.calls[2][0];
    expect(listCall.input).toEqual({ domain: "my-domain", repository: "my-repo" });
  });

  it("shows package groups scoped to the selected domain", async () => {
    mockSend.mockResolvedValueOnce({ domains: [exampleDomain] });
    render(CodeArtifactPage);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({
      packageGroups: [{ pattern: "npm/*", description: "All npm" }],
    });
    await fireEvent.click(screen.getByRole("tab", { name: "Pkg Groups" }));

    await waitFor(() => screen.getByText("npm/*"));

    const listCall = mockSend.mock.calls[1][0];
    expect(listCall.input).toEqual({ domain: "my-domain" });
  });

  it("publishes a package version with a computed asset SHA256", async () => {
    mockSend.mockResolvedValueOnce({ domains: [exampleDomain] });
    render(CodeArtifactPage);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({ repositories: [exampleRepo] });
    await fireEvent.click(screen.getByRole("tab", { name: "Repositories" }));
    await waitFor(() => screen.getByText("my-repo"));

    mockSend.mockResolvedValueOnce({ packages: [{ format: "npm", package: "left-pad" }] });
    await fireEvent.click(screen.getByText("my-repo"));
    await waitFor(() => screen.getByText("left-pad"));

    mockSend.mockResolvedValueOnce({ versions: [] });
    await fireEvent.click(screen.getByText("left-pad"));
    await waitFor(() => screen.getByText("No versions found"));

    await fireEvent.click(screen.getByText("Publish version"));
    await fireEvent.input(within(openDialog()).getByLabelText("Version"), {
      target: { value: "1.0.0" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Asset name"), {
      target: { value: "package.json" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Asset content"), {
      target: { value: '{"name":"left-pad"}' },
    });

    mockSend.mockResolvedValueOnce({ format: "npm", package: "left-pad", version: "1.0.0" });
    mockSend.mockResolvedValueOnce({ versions: [{ version: "1.0.0", status: "Published" }] });

    await fireEvent.click(within(openDialog()).getByRole("button", { name: "Publish" }));
    await waitFor(() => screen.getByText("1.0.0"));

    const publishCall = mockSend.mock.calls[4][0];
    expect(publishCall.input).toMatchObject({
      domain: "my-domain",
      repository: "my-repo",
      format: "npm",
      package: "left-pad",
      packageVersion: "1.0.0",
      assetName: "package.json",
    });
    expect(publishCall.input.assetSHA256).toMatch(/^[0-9a-f]{64}$/);
  });
});
