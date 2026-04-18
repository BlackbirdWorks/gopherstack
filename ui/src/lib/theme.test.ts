import { describe, expect, it } from "vitest";

import {
  applyTheme,
  initializeTheme,
  isDarkTheme,
  isValidTheme,
  resolveTheme,
  setTheme,
  themeStorageKey,
  themes,
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
  it("themes list contains all configured themes", () => {
    expect(themes).toEqual([
      "light",
      "dark",
      "github",
      "github-light",
      "ocean",
      "cyberpunk-2077",
      "aurora",
      "solstice",
    ]);
  });

  it("isValidTheme accepts valid themes", () => {
    expect(isValidTheme("light")).toBe(true);
    expect(isValidTheme("dark")).toBe(true);
    expect(isValidTheme("github")).toBe(true);
    expect(isValidTheme("github-light")).toBe(true);
    expect(isValidTheme("ocean")).toBe(true);
    expect(isValidTheme("cyberpunk-2077")).toBe(true);
    expect(isValidTheme("aurora")).toBe(true);
    expect(isValidTheme("solstice")).toBe(true);
  });

  it("isValidTheme rejects invalid values", () => {
    expect(isValidTheme(null)).toBe(false);
    expect(isValidTheme("")).toBe(false);
    expect(isValidTheme("blue")).toBe(false);
  });

  it("isDarkTheme identifies dark themes", () => {
    expect(isDarkTheme("dark")).toBe(true);
    expect(isDarkTheme("ocean")).toBe(true);
    expect(isDarkTheme("github")).toBe(true);
    expect(isDarkTheme("cyberpunk-2077")).toBe(true);
    expect(isDarkTheme("aurora")).toBe(true);
    expect(isDarkTheme("light")).toBe(false);
    expect(isDarkTheme("github-light")).toBe(false);
    expect(isDarkTheme("solstice")).toBe(false);
  });

  it("resolves saved theme first", () => {
    expect(resolveTheme("dark", false)).toBe("dark");
    expect(resolveTheme("light", true)).toBe("light");
    expect(resolveTheme("github", false)).toBe("github");
    expect(resolveTheme("github-light", false)).toBe("github-light");
    expect(resolveTheme("ocean", true)).toBe("ocean");
    expect(resolveTheme("cyberpunk-2077", false)).toBe("cyberpunk-2077");
    expect(resolveTheme("aurora", false)).toBe("aurora");
    expect(resolveTheme("solstice", true)).toBe("solstice");
  });

  it("resolves from prefers dark when no saved value", () => {
    expect(resolveTheme(null, true)).toBe("dark");
    expect(resolveTheme("", false)).toBe("light");
  });

  it("applies dark class for dark themes", () => {
    applyTheme(document, "dark");
    expect(document.documentElement.classList.contains("dark")).toBe(true);
    expect(document.documentElement.classList.contains("theme-dark")).toBe(true);

    applyTheme(document, "ocean");
    expect(document.documentElement.classList.contains("dark")).toBe(true);
    expect(document.documentElement.classList.contains("theme-ocean")).toBe(true);
    expect(document.documentElement.classList.contains("theme-dark")).toBe(false);
  });

  it("removes dark class for light themes", () => {
    applyTheme(document, "light");
    expect(document.documentElement.classList.contains("dark")).toBe(false);
    expect(document.documentElement.classList.contains("theme-light")).toBe(true);
  });

  it("applies dark class for github theme", () => {
    applyTheme(document, "github");
    expect(document.documentElement.classList.contains("dark")).toBe(true);
    expect(document.documentElement.classList.contains("theme-github")).toBe(true);
    expect(document.documentElement.classList.contains("theme-light")).toBe(false);
  });

  it("does not apply dark class for github-light theme", () => {
    applyTheme(document, "github-light");
    expect(document.documentElement.classList.contains("dark")).toBe(false);
    expect(document.documentElement.classList.contains("theme-github-light")).toBe(true);
    expect(document.documentElement.classList.contains("theme-github")).toBe(false);
  });

  it("initializes theme and applies class", () => {
    const storage = newMemoryStorage();
    storage.setItem(themeStorageKey, "ocean");

    const initialized = initializeTheme(document, storage, false);

    expect(initialized).toBe("ocean");
    expect(document.documentElement.classList.contains("dark")).toBe(true);
    expect(document.documentElement.classList.contains("theme-ocean")).toBe(true);
  });

  it("setTheme persists and applies theme", () => {
    const storage = newMemoryStorage();

    const result = setTheme(document, storage, "github");
    expect(result).toBe("github");
    expect(storage.getItem(themeStorageKey)).toBe("github");
    expect(document.documentElement.classList.contains("theme-github")).toBe(true);
    expect(document.documentElement.classList.contains("dark")).toBe(true);
  });
});
