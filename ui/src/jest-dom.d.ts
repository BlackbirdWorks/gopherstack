import type { TestingLibraryMatchers } from "@testing-library/jest-dom/matchers";

// jest-dom 7 augments Assertion<T>; vitest 5 declares Assertion<R, T>, so its merge fails.
declare module "vitest" {
  interface Assertion<
    R extends void | Promise<void> = void,
    T = unknown,
  > extends TestingLibraryMatchers<unknown, T> {}
  interface AsymmetricMatchersContaining extends TestingLibraryMatchers<unknown, unknown> {}
}
