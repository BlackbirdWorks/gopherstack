import { describe, it, expect, beforeEach, vi } from "vitest";
import { render, screen } from "@testing-library/svelte";
import ChaosPage from "./+page.svelte";

describe("Chaos Engineering Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.stubGlobal("fetch", vi.fn());
  });

  it("should render the chaos engineering page title", () => {
    render(ChaosPage);
    expect(screen.getByText("Chaos Engineering")).toBeInTheDocument();
  });

  it("should display fault rules section", () => {
    render(ChaosPage);
    expect(screen.getByText("Fault Rules")).toBeInTheDocument();
  });

  it("should display network effects section", () => {
    render(ChaosPage);
    expect(screen.getByText("Network Effects")).toBeInTheDocument();
  });

  it("should display activity log section", () => {
    render(ChaosPage);
    expect(screen.getByText("Activity Log")).toBeInTheDocument();
  });

  it("should have add fault button", () => {
    render(ChaosPage);
    const addButton = screen.getByText("Add Fault");
    expect(addButton).toBeInTheDocument();
  });
});
