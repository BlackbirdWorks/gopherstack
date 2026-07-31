import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/svelte";
import { Search } from "lucide-svelte";
import PageHeader from "./PageHeader.svelte";

describe("PageHeader", () => {
  it("defaults to the rose accent when no color is specified", () => {
    render(PageHeader, {
      props: { icon: Search, title: "Amazon Detective", description: "desc" },
    });
    const icon = screen
      .getByText("Amazon Detective")
      .parentElement?.parentElement?.querySelector("svg");
    expect(icon?.getAttribute("class")).toContain("text-rose-500");
  });

  it("renders the requested accent color", () => {
    render(PageHeader, {
      props: {
        icon: Search,
        title: "Data Lifecycle Manager",
        description: "desc",
        color: "teal",
      },
    });
    const icon = screen
      .getByText("Data Lifecycle Manager")
      .parentElement?.parentElement?.querySelector("svg");
    expect(icon?.getAttribute("class")).toContain("text-teal-500");
    expect(icon?.getAttribute("class")).not.toContain("text-rose-500");
  });

  it("still renders title and description", () => {
    render(PageHeader, {
      props: { icon: Search, title: "My Title", description: "My description" },
    });
    expect(screen.getByText("My Title")).toBeInTheDocument();
    expect(screen.getByText("My description")).toBeInTheDocument();
  });
});
