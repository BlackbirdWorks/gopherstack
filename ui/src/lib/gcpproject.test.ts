import { describe, expect, it } from "vitest";

import {
  DEFAULT_PROJECT,
  readStoredProject,
  validateProjectId,
  writeStoredProject,
} from "./gcpproject";

describe("project id", () => {
  it.each([
    ["", "Project is required"],
    ["Bad_ID", "6-30 characters: lowercase letters, digits and hyphens"],
    ["gopherstack-local", ""],
  ])("%j", (id, want) => expect(validateProjectId(id)).toBe(want));
});

describe("storage", () => {
  it("defaults, round-trips and survives broken storage", () => {
    const mem = new Map<string, string>();
    const store = {
      getItem: (k: string) => mem.get(k) ?? null,
      setItem: (k: string, v: string) => void mem.set(k, v),
    };
    expect(readStoredProject("k", store)).toBe(DEFAULT_PROJECT);
    writeStoredProject("k", "my-project", store);
    expect(readStoredProject("k", store)).toBe("my-project");
    const broken = {
      getItem: () => {
        throw new Error("blocked");
      },
      setItem: () => {
        throw new Error("blocked");
      },
    };
    expect(readStoredProject("k", broken)).toBe(DEFAULT_PROJECT);
    expect(() => writeStoredProject("k", "x", broken)).not.toThrow();
  });
});
