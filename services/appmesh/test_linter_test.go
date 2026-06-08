package appmesh_test

import "testing"

func TestDummy(t *testing.T) {
	t.Parallel()
	tests := []struct{ name string }{{"A"}}
	for _, tc := range tests {
		func() {
			t.Logf("Subtest: %v", tc.name)
		}()
	}
}
