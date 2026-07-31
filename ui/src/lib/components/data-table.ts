import type { Snippet } from "svelte";

// Column type lives in a plain .ts module (rather than inside
// DataTable.svelte's generics script) so it can be imported and
// parameterized by callers without depending on Svelte's component-generics
// export mechanism.
export type Column<T> = {
  key: string;
  label: string;
  render?: Snippet<[T]>;
};
