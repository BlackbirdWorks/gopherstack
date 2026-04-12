import { describe, expect, it } from "vitest";

import {
  applyTheme,
  initializeTheme,
  resolveTheme,
  themeStorageKey,
  toggleStoredTheme,
  toggleTheme,
} from "./theme";

function newMemoryStorage(): Storage {
  const store = new Map<string, string>();

  return {
    get length() {
      return store.size;
    },
    clear() {
      store.clear();
    },
    getItem(key: string) {
      return store.get(key) ?? null;
    },
    key(index: number) {
      return [...store.keys()][index] ?? null;
    },
    removeItem(key: string) {
      store.delete(key);
    },
    setItem(key: string, value: string) {
      store.set(key, value);
    },
  };
}

describe("theme helpers", () => {
  it("resolves saved theme first", () => {
    expect(resolveTheme("dark", false)).toBe("dark");
    expect(resolveTheme("light", true)).toBe("light");
  });

  it("resolves from prefers dark when no saved value", () => {
    expect(resolveTheme(null, true)).toBe("dark");
    expect(resolveTheme("", false)).toBe("light");
  });

  it("applies dark class based on mode", () => {
    applyTheme(document, "dark");
    expect(document.documentElement.classList.contains("dark")).toBe(true);

    applyTheme(document, "light");
    expect(document.documentElement.classList.contains("dark")).toBe(false);
  });

  it("initializes theme and applies class", () => {
    const storage = newMemoryStorage();
    storage.setItem(themeStorageKey, "dark");

    const initialized = initializeTheme(document, storage, false);

    expect(initialized).toBe("dark");
    expect(document.documentElement.classList.contains("dark")).toBe(true);
  });

  it("toggles both transient and persisted theme", () => {
    expect(toggleTheme("light")).toBe("dark");
    expect(toggleTheme("dark")).toBe("light");

    const storage = newMemoryStorage();

    const next = toggleStoredTheme(document, storage, "light");
    expect(next).toBe("dark");
    expect(storage.getItem(themeStorageKey)).toBe("dark");
  });
});
