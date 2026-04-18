import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/svelte";
import TranslatePage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getTranslateClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("Translate Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({
      TerminologyPropertiesList: [],
      ParallelDataPropertiesList: [],
      TextTranslationJobPropertiesList: [],
    });
    render(TranslatePage);
    expect(screen.getAllByText("Amazon Translate")[0]).toBeInTheDocument();
  });

  it("shows stat cards", () => {
    mockSend.mockResolvedValue({ TerminologyPropertiesList: [] });
    render(TranslatePage);
    expect(screen.getAllByText("Terminologies")[0]).toBeInTheDocument();
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ TerminologyPropertiesList: [] });
    render(TranslatePage);
    expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
  });

  it("shows empty state when no terminologies", async () => {
    mockSend.mockResolvedValue({
      TerminologyPropertiesList: [],
      ParallelDataPropertiesList: [],
      TextTranslationJobPropertiesList: [],
    });
    render(TranslatePage);
    await waitFor(
      () => {
        expect(screen.getAllByText(/no terminologies/i)[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded terminologies", async () => {
    mockSend.mockResolvedValue({
      TerminologyPropertiesList: [
        { Name: "my-glossary", SourceLanguageCode: "en", TargetLanguageCodes: ["es", "fr"] },
      ],
      ParallelDataPropertiesList: [],
      TextTranslationJobPropertiesList: [],
    });
    render(TranslatePage);
    await waitFor(
      () => {
        expect(screen.getAllByText("my-glossary")[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows refresh button", () => {
    mockSend.mockResolvedValue({ TerminologyPropertiesList: [] });
    render(TranslatePage);
    expect(screen.getByTitle("Refresh")).toBeInTheDocument();
  });

  it("shows Parallel Data tab", () => {
    mockSend.mockResolvedValue({ TerminologyPropertiesList: [] });
    render(TranslatePage);
    expect(screen.getAllByText("Parallel Data")[0]).toBeInTheDocument();
  });

  it("shows Translation Jobs tab", () => {
    mockSend.mockResolvedValue({ TerminologyPropertiesList: [] });
    render(TranslatePage);
    expect(screen.getAllByText("Translation Jobs")[0]).toBeInTheDocument();
  });
});
