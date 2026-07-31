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

// Build a column array with real per-element type checking.
//
// Do NOT write `[...] as Column<T>[]` at call sites -- `as` only checks
// "comparability", and for a union source type (which is exactly what a
// mixed array of "column with a render snippet" / "column without one"
// literals infers to) that check succeeds if ANY constituent of the union
// is comparable to the target, not all of them. Concretely: a page's column
// array typically mixes columns that omit `render` (comparable to
// `Column<T>`, since the field is optional) with columns that supply one.
// If even one column in the array is render-less, `as Column<T>[]` will
// silently accept a sibling column whose `render` is a plain
// value-returning function instead of a `{#snippet}` -- exactly the fis
// `render: (t) => formatDate(...)` bug that shipped: the arrow function
// returns a `string`, not a `Snippet`, but the cast let it through because
// the `{key,label}`-only columns next to it were individually comparable to
// `Column<T>`, and that was enough for the whole array's cast to succeed.
// `{@render column.render(row)}` then does nothing for a non-snippet value,
// so the cell renders blank -- with zero type error.
//
// Passing the array literal as a normal generic function argument (instead
// of asserting its type after the fact) makes TypeScript check every
// element against `Column<T>` for real, the same way passing it directly
// to a `columns` prop would. Verified: `defineColumns<Row>([{ render: (t)
// => someString }])` fails `tsc`/`svelte-check` with "Type '(t: Row) =>
// string' is not assignable to type 'Snippet<[Row]>'"; the equivalent `as
// Column<Row>[]` cast on the same mixed array does not.
export function defineColumns<T>(columns: Column<T>[]): Column<T>[] {
  return columns;
}
