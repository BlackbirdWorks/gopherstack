import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import RDSDataPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getRDSDataClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
    warning: vi.fn(),
    info: vi.fn(),
  },
}));

describe("RDSData Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    mockSend.mockResolvedValue({});
  });

  it("renders page title", () => {
    render(RDSDataPage);
    expect(screen.getByText("RDS Data")).toBeInTheDocument();
  });

  it("shows SQL Runner tab by default", () => {
    render(RDSDataPage);
    expect(screen.getByText("SQL Runner")).toBeInTheDocument();
    expect(screen.getByText("Run Query")).toBeInTheDocument();
  });

  it("shows all four tabs", () => {
    render(RDSDataPage);
    expect(screen.getByText("SQL Runner")).toBeInTheDocument();
    expect(screen.getByText("Transaction Browser")).toBeInTheDocument();
    expect(screen.getByText("Statement History")).toBeInTheDocument();
    expect(screen.getByText("Docs")).toBeInTheDocument();
  });

  it("shows SQL textarea with aria-label", () => {
    render(RDSDataPage);
    expect(screen.getByRole("textbox", { name: "SQL Query" })).toBeInTheDocument();
  });

  it("Run Query button is disabled when resource ARN is empty", async () => {
    render(RDSDataPage);
    const btn = screen.getByText("Run Query").closest("button")!;
    expect(btn).toBeDisabled();
  });

  it("Run Query button is enabled when resource ARN is provided", async () => {
    render(RDSDataPage);
    const arnInput = screen.getByLabelText("Resource ARN");
    await fireEvent.input(arnInput, {
      target: { value: "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster" },
    });
    const btn = screen.getByText("Run Query").closest("button")!;
    expect(btn).not.toBeDisabled();
  });

  it("shows connection fields", () => {
    render(RDSDataPage);
    expect(screen.getByLabelText("Resource ARN")).toBeInTheDocument();
    expect(screen.getByLabelText(/Secret ARN/)).toBeInTheDocument();
    expect(screen.getByLabelText(/Database/)).toBeInTheDocument();
  });

  it("shows batch mode toggle button", () => {
    render(RDSDataPage);
    expect(screen.getByRole("button", { name: "Toggle batch mode" })).toBeInTheDocument();
  });

  it("toggles to batch mode when Batch OFF button is clicked", async () => {
    render(RDSDataPage);
    const batchBtn = screen.getByRole("button", { name: "Toggle batch mode" });
    expect(batchBtn).toHaveTextContent("Batch OFF");
    await fireEvent.click(batchBtn);
    expect(batchBtn).toHaveTextContent("Batch ON");
  });

  it("shows Batch SQL textarea when batch mode is active", async () => {
    render(RDSDataPage);
    const batchBtn = screen.getByRole("button", { name: "Toggle batch mode" });
    await fireEvent.click(batchBtn);
    expect(screen.getByRole("textbox", { name: "Batch SQL" })).toBeInTheDocument();
  });

  it("executes statement and shows result", async () => {
    mockSend.mockResolvedValue({ numberOfRecordsUpdated: 1 });
    render(RDSDataPage);
    const arnInput = screen.getByLabelText("Resource ARN");
    await fireEvent.input(arnInput, {
      target: { value: "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster" },
    });
    const btn = screen.getByText("Run Query").closest("button")!;
    await fireEvent.click(btn);
    await waitFor(
      () => {
        expect(screen.getByText("Result")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows error on failed execution", async () => {
    mockSend.mockRejectedValue(new Error("ResourceNotFoundException: cluster not found"));
    render(RDSDataPage);
    const arnInput = screen.getByLabelText("Resource ARN");
    await fireEvent.input(arnInput, {
      target: { value: "arn:aws:rds:us-east-1:123456789012:cluster:missing" },
    });
    const btn = screen.getByText("Run Query").closest("button")!;
    await fireEvent.click(btn);
    await waitFor(
      () => {
        expect(screen.getByText(/ResourceNotFoundException/)).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("switches to Transaction Browser tab", async () => {
    render(RDSDataPage);
    await fireEvent.click(screen.getByText("Transaction Browser"));
    await waitFor(() => {
      expect(screen.getByText("Active Transactions")).toBeInTheDocument();
    });
  });

  it("shows Begin Transaction button in transaction tab", async () => {
    render(RDSDataPage);
    await fireEvent.click(screen.getByText("Transaction Browser"));
    await waitFor(() => {
      expect(screen.getByText("Begin Transaction")).toBeInTheDocument();
    });
  });

  it("Begin Transaction is disabled when resource ARN is empty", async () => {
    render(RDSDataPage);
    await fireEvent.click(screen.getByText("Transaction Browser"));
    await waitFor(() => {
      const btn = screen.getByText("Begin Transaction").closest("button")!;
      expect(btn).toBeDisabled();
    });
  });

  it("begins transaction and shows it in browser", async () => {
    mockSend.mockResolvedValue({ transactionId: "txn-000001" });
    render(RDSDataPage);
    const arnInput = screen.getByLabelText("Resource ARN");
    await fireEvent.input(arnInput, {
      target: { value: "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster" },
    });
    await fireEvent.click(screen.getByText("Transaction Browser"));
    await waitFor(() => {
      expect(screen.getByText("Begin Transaction")).toBeInTheDocument();
    });
    const beginBtn = screen.getByText("Begin Transaction").closest("button")!;
    await fireEvent.click(beginBtn);
    await waitFor(
      () => {
        expect(screen.getByText("txn-000001")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("switches to Statement History tab", async () => {
    render(RDSDataPage);
    await fireEvent.click(screen.getByText("Statement History"));
    await waitFor(() => {
      expect(screen.getByText("No statements executed yet")).toBeInTheDocument();
    });
  });

  it("shows Seed Demo button in history tab", async () => {
    render(RDSDataPage);
    await fireEvent.click(screen.getByText("Statement History"));
    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Seed demo history" })).toBeInTheDocument();
    });
  });

  it("seeds demo history", async () => {
    render(RDSDataPage);
    await fireEvent.click(screen.getByText("Statement History"));
    const seedBtn = await screen.findByRole("button", { name: "Seed demo history" });
    await fireEvent.click(seedBtn);
    await waitFor(
      () => {
        expect(screen.getByText(/CREATE TABLE users/)).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows Docs tab content", async () => {
    render(RDSDataPage);
    await fireEvent.click(screen.getByText("Docs"));
    await waitFor(() => {
      expect(screen.getByText("RDS Data API Reference")).toBeInTheDocument();
    });
  });

  it("shows keyboard shortcut hint", () => {
    render(RDSDataPage);
    expect(screen.getByText(/Ctrl\+Enter/)).toBeInTheDocument();
  });

  it("shows Raw JSON toggle button after results loaded", async () => {
    mockSend.mockResolvedValue({ numberOfRecordsUpdated: 0 });
    render(RDSDataPage);
    const arnInput = screen.getByLabelText("Resource ARN");
    await fireEvent.input(arnInput, {
      target: { value: "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster" },
    });
    await fireEvent.click(screen.getByText("Run Query").closest("button")!);
    await waitFor(
      () => {
        expect(screen.getByRole("button", { name: "Toggle raw JSON" })).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("history entry count badge appears after executing", async () => {
    mockSend.mockResolvedValue({});
    render(RDSDataPage);
    const arnInput = screen.getByLabelText("Resource ARN");
    await fireEvent.input(arnInput, {
      target: { value: "arn:aws:rds:us-east-1:123456789012:cluster:c" },
    });
    await fireEvent.click(screen.getByText("Run Query").closest("button")!);
    await waitFor(
      () => {
        expect(screen.getByText("1")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });
});
