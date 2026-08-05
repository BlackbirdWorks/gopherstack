import "@testing-library/jest-dom/vitest";
import "@testing-library/svelte/vitest";
import { beforeEach, vi } from "vitest";
import { resetMockPage } from "./src/lib/mock-page.svelte";

// Every test starts from a clean mock page.url (?no query params) so a
// urlState() write in one test can't leak into the next.
beforeEach(() => {
  resetMockPage();
});

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

vi.mock("$app/state", async () => {
  // `url`/`state` back `$lib/url-state.svelte`'s `urlState()` primitive
  // (used by page tab/filter/search state) — every page test that mounts a
  // component reading a urlState value needs both present and REACTIVE, or
  // a page's `.get()`-derived $state (e.g. `let searchQuery =
  // $derived(searchQueryParam.get())`) would render once and never update
  // when a test fires a `goto(..., { replaceState: true })` write below. See
  // mock-page.svelte.ts for why that requires a real rune, not a plain
  // mutable object. Individual test files that want finer control (see
  // url-state.test.ts) override this mock locally.
  const { getMockPage } = await import("./src/lib/mock-page.svelte");
  return {
    get page() {
      return getMockPage();
    },
  };
});

vi.mock("$app/navigation", async () => {
  const { setMockPageUrl, setMockPageState } = await import("./src/lib/mock-page.svelte");
  return {
    // Backs `urlState().set()` — actually writes through to the reactive
    // mock `page.url` above, so a page's tab/filter click is honored the
    // same way it would be against the real `@sveltejs/kit` client runtime.
    // This must NOT be done by the standalone `replaceState` mock below:
    // reading the real client.js confirms standalone `replaceState` updates
    // `history` and `page.state` only, never `page.url` — only navigation
    // (`goto`) does that (`current.url = page.url = url`). An earlier version
    // of this mock had `replaceState` write `page.url` "for convenience",
    // which let every urlState-driven component test pass while the real app
    // silently never re-rendered on a tab/filter click — caught by e2e, not
    // by these tests, until this mock was corrected to match reality.
    //
    // The write is deferred by one microtask (`Promise.resolve().then(...)`)
    // rather than applied synchronously here, because the real `goto()`
    // isn't synchronous either: reading `@sveltejs/kit`'s shipped
    // client.js, `goto()` reassigns `page.url` only after `await
    // navigate(...)` resolves, deep inside its async pipeline — never
    // before returning. A synchronous mock write is MORE convenient for
    // tests but was hiding a real bug: two `urlState.set()` calls issued
    // back to back with no `await` between them (e.g. a click handler
    // doing `tab.set('overview'); table.set(name)`) each compute their
    // next URL from `new URL(page.url)`. With a synchronous mock, the
    // first call's write lands before the second call's snapshot is
    // taken, so the two compose correctly — which is NOT what happens in
    // a real browser, where both snapshots are taken before either
    // `goto()` has had a chance to update anything, and the second
    // `goto()`'s write clobbers the first. Deferring here reproduces that
    // ordering so tests can catch it. Callers that need to change more
    // than one urlState value in one handler must use `setUrlParams()`
    // (see `url-state.svelte.ts`), which computes a single URL from a
    // single snapshot and fires one `goto()` — see
    // `routes/dynamodb/page.test.ts` for an end-to-end repro of the bug
    // this deferral catches.
    goto: vi.fn((url: string | URL, opts?: { state?: Record<string, unknown> }) => {
      return Promise.resolve().then(() => {
        setMockPageUrl(new URL(url));
        setMockPageState(opts?.state ?? {});
      });
    }),
    invalidateAll: vi.fn(),
    // Deliberately inert: does NOT touch the mock page, matching the real
    // `@sveltejs/kit` behavior described above.
    replaceState: vi.fn(),
  };
});

vi.mock("$app/environment", () => ({
  browser: true,
}));
