package asl_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

// TestExecutor_ItemBatcherWireShape covers the ItemBatcher iteration-input
// shape (gopherstack-vkrn): AWS wraps each batch as {"Items": [...]}, plus a
// "BatchInput" field when ItemBatcher.BatchInput is set (AWS docs:
// input-output-itembatcher.html, "Batch input"). The executor previously
// passed each batch through as a bare array, and BatchInput had no field on
// the parser's ItemBatcher struct at all.
func TestExecutor_ItemBatcherWireShape(t *testing.T) {
	t.Parallel()

	t.Run("batch_wraps_items_in_items_field", func(t *testing.T) {
		t.Parallel()

		def := `{
			"StartAt": "M",
			"States": {
				"M": {
					"Type": "Map",
					"End": true,
					"ItemsPath": "$.items",
					"ItemBatcher": {"MaxItemsPerBatch": 2},
					"Iterator": {
						"StartAt": "Pass",
						"States": {"Pass": {"Type": "Pass", "End": true}}
					}
				}
			}
		}`

		sm, err := asl.Parse(def)
		require.NoError(t, err)

		exec := asl.NewExecutor(sm, nil, nil)
		result, execErr := exec.Execute(t.Context(), "test", `{"items": [1,2,3]}`)
		require.NoError(t, execErr)
		require.Empty(t, result.Error)

		arr, ok := result.Output.([]any)
		require.True(t, ok)
		require.Len(t, arr, 2)

		assert.Equal(t, map[string]any{"Items": []any{float64(1), float64(2)}}, arr[0])
		assert.Equal(t, map[string]any{"Items": []any{float64(3)}}, arr[1])
	})

	t.Run("batch_input_merged_into_every_batch", func(t *testing.T) {
		t.Parallel()

		def := `{
			"StartAt": "M",
			"States": {
				"M": {
					"Type": "Map",
					"End": true,
					"ItemsPath": "$.items",
					"ItemBatcher": {
						"MaxItemsPerBatch": 2,
						"BatchInput": {"factCheck": "December 2022"}
					},
					"Iterator": {
						"StartAt": "Pass",
						"States": {"Pass": {"Type": "Pass", "End": true}}
					}
				}
			}
		}`

		sm, err := asl.Parse(def)
		require.NoError(t, err)

		exec := asl.NewExecutor(sm, nil, nil)
		result, execErr := exec.Execute(t.Context(), "test", `{"items": [1,2,3]}`)
		require.NoError(t, execErr)
		require.Empty(t, result.Error)

		arr, ok := result.Output.([]any)
		require.True(t, ok)
		require.Len(t, arr, 2)

		wantBatchInput := map[string]any{"factCheck": "December 2022"}
		assert.Equal(t, map[string]any{
			"Items":      []any{float64(1), float64(2)},
			"BatchInput": wantBatchInput,
		}, arr[0])
		assert.Equal(t, map[string]any{
			"Items":      []any{float64(3)},
			"BatchInput": wantBatchInput,
		}, arr[1])
	})
}
