import { describe, expect, it } from "vitest";
import { fireEvent, render, screen } from "@testing-library/svelte";
import ConfirmDialog from "./ConfirmDialog.svelte";

describe("ConfirmDialog", () => {
  it("resolves true when confirm is clicked", async () => {
    const { component } = render(ConfirmDialog);
    const resultPromise = component.show({
      title: "Delete stream",
      message: 'Delete stream "orders"?',
    });

    expect(screen.getByRole("alertdialog")).toBeInTheDocument();
    await fireEvent.click(screen.getByRole("button", { name: "Delete" }));

    await expect(resultPromise).resolves.toBe(true);
  });

  it("resolves false when cancel is clicked", async () => {
    const { component } = render(ConfirmDialog);
    const resultPromise = component.show({
      title: "Delete stream",
      message: 'Delete stream "orders"?',
    });

    await fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    await expect(resultPromise).resolves.toBe(false);
  });

  it("supports escape and enter keyboard actions", async () => {
    const { component } = render(ConfirmDialog);

    const escapePromise = component.show({
      title: "Delete stream",
      message: 'Delete stream "orders"?',
    });
    const escapeDialog = screen.getByRole("alertdialog");
    await fireEvent.keyDown(escapeDialog, { key: "Escape" });
    await expect(escapePromise).resolves.toBe(false);

    const enterPromise = component.show({
      title: "Delete stream",
      message: 'Delete stream "orders"?',
    });
    const enterDialog = screen.getByRole("alertdialog");
    await fireEvent.keyDown(enterDialog, { key: "Enter" });
    await expect(enterPromise).resolves.toBe(true);
  });
});
