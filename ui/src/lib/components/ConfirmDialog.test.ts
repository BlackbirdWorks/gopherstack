import { describe, expect, it } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import ConfirmDialog from "./ConfirmDialog.svelte";

// HTMLDialogElement.showModal / close are polyfilled globally in vitest.setup.ts.

describe("ConfirmDialog", () => {
  it("resolves true when confirm button is clicked", async () => {
    const { component } = render(ConfirmDialog);
    const resultPromise = component.show({
      title: "Delete stream",
      message: 'Delete stream "orders"?',
    });

    await waitFor(() => expect(screen.getByRole("alertdialog")).toBeInTheDocument());
    await fireEvent.click(screen.getByRole("button", { name: "Delete" }));

    await expect(resultPromise).resolves.toBe(true);
  });

  it("resolves false when cancel button is clicked", async () => {
    const { component } = render(ConfirmDialog);
    const resultPromise = component.show({
      title: "Delete stream",
      message: 'Delete stream "orders"?',
    });

    await waitFor(() => expect(screen.getByRole("button", { name: "Cancel" })).toBeInTheDocument());
    await fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    await expect(resultPromise).resolves.toBe(false);
  });

  it("resolves true on Enter key", async () => {
    const { component } = render(ConfirmDialog);
    const resultPromise = component.show({
      title: "Delete stream",
      message: 'Delete stream "orders"?',
    });

    await waitFor(() => expect(screen.getByRole("alertdialog")).toBeInTheDocument());
    const dialog = screen.getByRole("alertdialog");
    await fireEvent.keyDown(dialog, { key: "Enter" });
    await expect(resultPromise).resolves.toBe(true);
  });

  it("resolves false on Escape key", async () => {
    const { component } = render(ConfirmDialog);
    const resultPromise = component.show({
      title: "Delete stream",
      message: 'Delete stream "orders"?',
    });

    await waitFor(() => expect(screen.getByRole("alertdialog")).toBeInTheDocument());
    const dialog = screen.getByRole("alertdialog");
    await fireEvent.keyDown(dialog, { key: "Escape" });
    await expect(resultPromise).resolves.toBe(false);
  });

  it("renders provided title and message", async () => {
    const { component } = render(ConfirmDialog);
    component.show({ title: "Remove user", message: "Permanently delete alice?" });

    await waitFor(() => expect(screen.getByText("Remove user")).toBeInTheDocument());
    expect(screen.getByText("Permanently delete alice?")).toBeInTheDocument();
  });

  it("uses custom confirmLabel and cancelLabel", async () => {
    const { component } = render(ConfirmDialog);
    const resultPromise = component.show({
      title: "Purge queue",
      message: "Purge all messages?",
      confirmLabel: "Purge",
      cancelLabel: "Keep",
    });

    await waitFor(() => expect(screen.getByRole("button", { name: "Purge" })).toBeInTheDocument());
    expect(screen.getByRole("button", { name: "Keep" })).toBeInTheDocument();

    await fireEvent.click(screen.getByRole("button", { name: "Purge" }));
    await expect(resultPromise).resolves.toBe(true);
  });

  it("renders confirm button in indigo when dangerous is false", async () => {
    const { component } = render(ConfirmDialog);
    component.show({
      title: "Enable feature",
      message: "Turn on feature flag?",
      dangerous: false,
      confirmLabel: "Enable",
    });

    await waitFor(() => expect(screen.getByRole("button", { name: "Enable" })).toBeInTheDocument());
    const confirmBtn = screen.getByRole("button", { name: "Enable" });
    expect(confirmBtn.className).toContain("bg-indigo-600");
    expect(confirmBtn.className).not.toContain("bg-rose-600");
  });

  it("renders confirm button in rose by default (dangerous)", async () => {
    const { component } = render(ConfirmDialog);
    component.show({ title: "Delete", message: "Delete this?" });

    await waitFor(() => expect(screen.getByRole("button", { name: "Delete" })).toBeInTheDocument());
    const confirmBtn = screen.getByRole("button", { name: "Delete" });
    expect(confirmBtn.className).toContain("bg-rose-600");
  });

  it("defaults to 'Delete' confirm label and 'Cancel' cancel label", async () => {
    const { component } = render(ConfirmDialog);
    component.show({ title: "x", message: "y" });

    await waitFor(() => expect(screen.getByRole("button", { name: "Delete" })).toBeInTheDocument());
    expect(screen.getByRole("button", { name: "Cancel" })).toBeInTheDocument();
  });

  it("cancels first promise when show() is called again before it resolves", async () => {
    const { component } = render(ConfirmDialog);
    const first = component.show({ title: "First", message: "First dialog" });
    const second = component.show({ title: "Second", message: "Second dialog" });

    // The first call should be cancelled when a second show() pre-empts it.
    await expect(first).resolves.toBe(false);

    // Resolve the second by clicking confirm.
    await waitFor(() => expect(screen.getByRole("button", { name: "Delete" })).toBeInTheDocument());
    await fireEvent.click(screen.getByRole("button", { name: "Delete" }));
    await expect(second).resolves.toBe(true);
  });
});
