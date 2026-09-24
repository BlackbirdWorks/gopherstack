package terraform_test

import "testing"

// findBy returns the first item in items matching match, failing the test
// with a clear message if none matches. Fixtures share one emulator per CI
// shard, so unfiltered List/Describe results can contain other fixtures'
// resources; use this instead of indexing [0] or asserting an exact count.
func findBy[T any](t *testing.T, items []T, match func(T) bool, what string) T {
	t.Helper()
	for _, item := range items {
		if match(item) {
			return item
		}
	}
	//nolint:revive // Fatalf doesn't stop control flow at compile time; a return value is still required
	t.Fatalf("did not find %s among %d items", what, len(items))

	var zero T

	return zero
}
