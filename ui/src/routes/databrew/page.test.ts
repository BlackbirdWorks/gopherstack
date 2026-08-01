import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import DataBrewPage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialogs = Array.from(document.querySelectorAll("dialog[open]"));
  const dialog = dialogs.at(-1);
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getDataBrewClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleDataset = {
  Name: "my-dataset",
  Format: "CSV",
  Input: { S3InputDefinition: { Bucket: "my-bucket", Key: "data.csv" } },
};

describe("DataBrew Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ Datasets: [] });
    render(DataBrewPage);
    expect(screen.getByText("AWS Glue DataBrew")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No datasets found"));
  });

  it("shows empty state when no datasets", async () => {
    mockSend.mockResolvedValueOnce({ Datasets: [] });
    render(DataBrewPage);
    await waitFor(() => {
      expect(screen.getByText("No datasets found")).toBeInTheDocument();
    });
  });

  it("lists datasets", async () => {
    mockSend.mockResolvedValueOnce({ Datasets: [exampleDataset] });
    render(DataBrewPage);
    await waitFor(() => {
      expect(within(screen.getByRole("table")).getByText("my-dataset")).toBeInTheDocument();
    });
  });

  it("creates a dataset via the modal with exact command input", async () => {
    mockSend.mockResolvedValueOnce({ Datasets: [] });
    render(DataBrewPage);
    await waitFor(() => screen.getByText("No datasets found"));

    await fireEvent.click(screen.getByText("Create dataset"));
    expect(screen.getByText("Create Dataset")).toBeInTheDocument();

    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "my-dataset" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("S3 bucket"), {
      target: { value: "my-bucket" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("S3 key (optional)"), {
      target: { value: "data.csv" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Datasets: [exampleDataset] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(within(screen.getByRole("table")).getByText("my-dataset")).toBeInTheDocument();
    });

    expect(mockSend.mock.calls[1][0].input).toEqual({
      Name: "my-dataset",
      Format: "CSV",
      Input: { S3InputDefinition: { Bucket: "my-bucket", Key: "data.csv" } },
    });
  });

  it("deletes a dataset after confirming", async () => {
    mockSend.mockResolvedValueOnce({ Datasets: [exampleDataset] });
    render(DataBrewPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-dataset"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Datasets: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No datasets found")).toBeInTheDocument();
    });
    expect(mockSend.mock.calls[1][0].input).toEqual({ Name: "my-dataset" });
  });

  it("does not delete a dataset when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ Datasets: [exampleDataset] });
    render(DataBrewPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-dataset"));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(within(screen.getByRole("table")).getByText("my-dataset")).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Dataset not found."), {
      name: "ResourceNotFoundException",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(DataBrewPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ResourceNotFoundException (HTTP 404): Dataset not found."),
      ).toBeInTheDocument();
    });
  });

  it("opens a dataset's detail view", async () => {
    mockSend.mockResolvedValueOnce({ Datasets: [exampleDataset] });
    render(DataBrewPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-dataset"));

    mockSend.mockResolvedValueOnce({ ...exampleDataset });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(within(openDialog()).getByText("s3://my-bucket/data.csv")).toBeInTheDocument();
    });
  });

  it("deletes a recipe through the version manager, not a DeleteRecipe call", async () => {
    mockSend.mockResolvedValueOnce({ Datasets: [] });
    render(DataBrewPage);
    await waitFor(() => screen.getByText("No datasets found"));

    const exampleRecipe = { Name: "my-recipe", RecipeVersion: "LATEST_WORKING" };
    mockSend.mockResolvedValueOnce({ Recipes: [exampleRecipe] });
    await fireEvent.click(screen.getByRole("tab", { name: "Recipes" }));
    await waitFor(() => within(screen.getByRole("table")).getByText("my-recipe"));

    mockSend.mockResolvedValueOnce({ Recipes: [{ Name: "my-recipe", RecipeVersion: "1.0" }] });
    await fireEvent.click(screen.getByTitle("Delete (manage versions)"));

    await waitFor(() => {
      expect(within(openDialog()).getByText("1.0")).toBeInTheDocument();
    });

    mockSend.mockResolvedValueOnce({ Errors: [] });
    await fireEvent.click(within(openDialog()).getByText("Delete entire recipe"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(mockSend.mock.calls[3][0].constructor.name).toBe("BatchDeleteRecipeVersionCommand");
    });
    expect(mockSend.mock.calls[3][0].input).toEqual({
      Name: "my-recipe",
      RecipeVersions: ["1.0", "LATEST_WORKING"],
    });
  });

  it("shows a Publish action for recipes", async () => {
    mockSend.mockResolvedValueOnce({ Datasets: [] });
    render(DataBrewPage);
    await waitFor(() => screen.getByText("No datasets found"));

    const exampleRecipe = { Name: "my-recipe", RecipeVersion: "LATEST_WORKING" };
    mockSend.mockResolvedValueOnce({ Recipes: [exampleRecipe] });
    await fireEvent.click(screen.getByRole("tab", { name: "Recipes" }));
    await waitFor(() => within(screen.getByRole("table")).getByText("my-recipe"));

    mockSend.mockResolvedValueOnce({ Name: "my-recipe", RecipeVersion: "1.0" });
    mockSend.mockResolvedValueOnce({ Recipes: [{ Name: "my-recipe", RecipeVersion: "1.0" }] });

    await fireEvent.click(screen.getByTitle("Publish"));

    await waitFor(() => {
      expect(mockSend.mock.calls[2][0].input).toEqual({ Name: "my-recipe" });
    });
  });
});
