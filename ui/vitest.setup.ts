import "@testing-library/jest-dom/vitest";
import "@testing-library/svelte/vitest";
import { vi } from "vitest";

// jsdom does not implement the HTMLDialogElement modal API.  Define minimal
// stubs so components that call showModal() / close() work in tests.
if (!("showModal" in HTMLDialogElement.prototype)) {
  Object.assign(HTMLDialogElement.prototype, {
    showModal(this: HTMLDialogElement) {
      this.setAttribute("open", "");
    },
    close(this: HTMLDialogElement, returnValue = "") {
      this.returnValue = returnValue;
      this.removeAttribute("open");
      this.dispatchEvent(new Event("close"));
    },
  });
}

vi.mock("$app/state", () => {
  // A simple mock for $app/state. You can use object getters or explicit signals if using latest svelte.
  let currentVal = { params: { tableName: "test-table", bucketName: "test-bucket" } };
  return {
    page: currentVal,
  };
});

vi.mock("$app/navigation", () => {
  return {
    goto: vi.fn(),
    invalidateAll: vi.fn(),
  };
});

vi.mock("$app/environment", () => ({
  browser: true,
}));
