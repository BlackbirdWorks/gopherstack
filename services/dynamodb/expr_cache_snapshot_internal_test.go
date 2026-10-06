package dynamodb

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func snapshotFixture() map[string]any {
	return map[string]any{
		"pk":   map[string]any{"S": "k"},
		"a":    map[string]any{"N": "1"},
		"b":    map[string]any{"N": "2"},
		"m":    map[string]any{"M": map[string]any{"x": map[string]any{"N": "7"}}},
		"l":    map[string]any{"L": []any{map[string]any{"S": "e0"}}},
		"tags": map[string]any{"SS": []string{"p", "q"}},
	}
}

func TestApplyUpdate_SnapshotMatchesCopyAndStaysUntouched(t *testing.T) {
	t.Parallel()

	tests := []struct {
		names  map[string]string
		values map[string]any
		name   string
		expr   string
	}{
		{name: "swap_scalars", expr: "SET a = b, b = a"},
		{name: "copy_map", expr: "SET m2 = m"},
		{name: "copy_list_and_set", expr: "SET l2 = l, tags2 = tags"},
		{name: "arithmetic", expr: "SET a = a + :i", values: map[string]any{":i": map[string]any{"N": "3"}}},
		{name: "add_set", expr: "ADD tags :s", values: map[string]any{":s": map[string]any{"SS": []string{"r"}}}},
		{name: "delete_set", expr: "DELETE tags :s", values: map[string]any{":s": map[string]any{"SS": []string{"p"}}}},
		{name: "nested_set", expr: "SET m.x = :v", values: map[string]any{":v": map[string]any{"N": "8"}}},
		{name: "list_append", expr: "SET l = list_append(l, :e)", values: map[string]any{
			":e": map[string]any{"L": []any{map[string]any{"S": "e1"}}},
		}},
		{name: "remove", expr: "REMOVE b, m.x"},
		{name: "alias", expr: "SET #n = m", names: map[string]string{"#n": "copy"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			existing := snapshotFixture()
			viaSnapshot := deepCopyItem(existing)
			viaCopy := deepCopyItem(existing)

			_, err := applyUpdate(viaSnapshot, tc.expr, tc.names, tc.values, existing)
			require.NoError(t, err)

			_, err = applyUpdate(viaCopy, tc.expr, tc.names, tc.values, nil)
			require.NoError(t, err)

			assert.Equal(t, viaCopy, viaSnapshot)
			assert.Equal(t, snapshotFixture(), existing, "snapshot item must not be mutated")

			_, err = applyUpdate(viaSnapshot, "SET m.x = :z", nil, map[string]any{":z": map[string]any{"N": "0"}}, nil)
			if err == nil {
				assert.Equal(t, snapshotFixture(), existing, "result must not alias the snapshot item")
			}
		})
	}
}

func TestParsedExpressionCache_ConcurrentUse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func() error
		name string
	}{
		{name: "condition", run: func() error {
			_, err := EvaluateExpression("a > :n AND attribute_exists(pk)", snapshotFixture(),
				map[string]any{":n": map[string]any{"N": "0"}}, nil)

			return err
		}},
		{name: "update", run: func() error {
			_, err := applyUpdate(snapshotFixture(), "SET a = a + :i REMOVE b", nil,
				map[string]any{":i": map[string]any{"N": "1"}}, nil)

			return err
		}},
		{name: "bad_update", run: func() error {
			_, err := applyUpdate(snapshotFixture(), "SET =", nil, nil, nil)
			if err == nil {
				return ErrUnknownOperation
			}

			return nil
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var wg sync.WaitGroup

			errs := make(chan error, 64)

			for range 16 {
				wg.Go(func() {
					for range 50 {
						if err := tc.run(); err != nil {
							errs <- err

							return
						}
					}
				})
			}

			wg.Wait()
			close(errs)

			for err := range errs {
				assert.NoError(t, err)
			}
		})
	}
}
