package terraform_test

import "testing"

// findBy returns the first item in items matching match, failing the test
// with a clear message if none matches. Fixtures share one emulator per CI
// shard, so unfiltered List/Describe results can contain other fixtures'
// resources; use this instead of indexing [0] or asserting an exact count.
func findBy[T any](t *testing.T, items []T, match func(T) bool, what string) T {
	t.Helper()

	idx := -1

	for i, item := range items {
		if match(item) {
			idx = i

			break
		}
	}

	if idx < 0 {
		t.Fatalf("did not find %s among %d items", what, len(items))
	}

	return items[idx]
}
