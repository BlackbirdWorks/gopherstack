package asl_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

// TestExecutor_ItemBatcherPathSettings drives real executions of a
// Distributed Map's ItemBatcher configured only via its *Path variants
// (gopherstack-48r4): MaxItemsPerBatchPath and MaxInputBytesPerBatchPath had
// no field on the parser's ItemBatcher struct, so encoding/json silently
// discarded them and batching had no effect.
func TestExecutor_ItemBatcherPathSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		itemBatcher string
		input       string
		wantErr     bool
		wantBatches int
	}{
		{
			name:        "max_items_per_batch_path_batches_items",
			itemBatcher: `{"MaxItemsPerBatchPath": "$.maxBatch"}`,
			input:       `{"items": [1,2,3,4,5], "maxBatch": 2}`,
			wantBatches: 3,
		},
		{
			name:        "max_input_bytes_per_batch_path_batches_items",
			itemBatcher: `{"MaxInputBytesPerBatchPath": "$.maxBytes"}`,
			// Each item marshals to 1 byte ("1".."5"); a 3-byte cap fits 3
			// items in the first batch, then 2 in the second.
			input:       `{"items": [1,2,3,4,5], "maxBytes": 3}`,
			wantBatches: 2,
		},
		{
			name:        "max_items_per_batch_path_invalid_value_errors",
			itemBatcher: `{"MaxItemsPerBatchPath": "$.maxBatch"}`,
			input:       `{"items": [1,2,3], "maxBatch": "oops"}`,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			def := `{
				"StartAt": "M",
				"States": {
					"M": {
						"Type": "Map",
						"End": true,
						"ItemsPath": "$.items",
						"ItemBatcher": ` + tt.itemBatcher + `,
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
			result, execErr := exec.Execute(t.Context(), "test", tt.input)

			if tt.wantErr {
				require.Error(t, execErr)

				return
			}

			require.NoError(t, execErr)
			require.Empty(t, result.Error)
			arr, ok := result.Output.([]any)
			require.True(t, ok)
			assert.Len(t, arr, tt.wantBatches)
		})
	}
}

// TestExecutor_ItemReaderMaxItemsPath covers ReaderConfig.MaxItemsPath, the
// other ItemBatcher/ItemReader *Path field gopherstack-48r4 flags as
// unmodelled.
func TestExecutor_ItemReaderMaxItemsPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mapInput  string
		wantErr   bool
		wantItems int
	}{
		{
			name:      "max_items_path_truncates_items",
			mapInput:  `{"maxItems": 2}`,
			wantItems: 2,
		},
		{
			name:     "max_items_path_invalid_value_errors",
			mapInput: `{"maxItems": "oops"}`,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			def := `{
				"StartAt": "M",
				"States": {
					"M": {
						"Type": "Map",
						"End": true,
						"ItemReader": {
							"Resource": "arn:aws:states:::s3:getObject",
							"Parameters": {"Bucket": "b", "Key": "k"},
							"ReaderConfig": {"InputType": "JSON", "MaxItemsPath": "$.maxItems"}
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
			exec.SetS3Reader(stubS3{data: []byte(`[1,2,3,4,5]`)})

			result, execErr := exec.Execute(t.Context(), "test", tt.mapInput)

			if tt.wantErr {
				require.Error(t, execErr)

				return
			}

			require.NoError(t, execErr)
			require.Empty(t, result.Error)
			arr, ok := result.Output.([]any)
			require.True(t, ok)
			assert.Len(t, arr, tt.wantItems)
		})
	}
}

// recordingMapRunNotifier captures the maxConcurrency value the executor
// resolved for the Map run, so MaxConcurrencyPath resolution is observable.
type recordingMapRunNotifier struct {
	gotMaxConcurrency int
}

func (r *recordingMapRunNotifier) OnMapRunStart(_, _ string, maxConcurrency, _ int) string {
	r.gotMaxConcurrency = maxConcurrency

	return "map-run-arn"
}

func (r *recordingMapRunNotifier) OnMapRunEnd(_, _ string, _, _, _, _ int) {}

// TestExecutor_MapMaxConcurrencyPath covers State.MaxConcurrencyPath, found
// during the gopherstack-48r4 audit alongside ItemBatcher/ItemReader's *Path
// fields: same asymmetry (MaxConcurrency modelled, MaxConcurrencyPath not),
// same resolution as ToleratedFailureCountPath.
func TestExecutor_MapMaxConcurrencyPath(t *testing.T) {
	t.Parallel()

	t.Run("max_concurrency_path_resolves_from_input", func(t *testing.T) {
		t.Parallel()

		def := `{
			"StartAt": "M",
			"States": {
				"M": {
					"Type": "Map",
					"End": true,
					"ItemsPath": "$.items",
					"MaxConcurrencyPath": "$.maxConcurrency",
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
		notifier := &recordingMapRunNotifier{}
		exec.SetMapRunNotifier(notifier)

		result, execErr := exec.Execute(t.Context(), "test",
			`{"items": [1,2,3], "maxConcurrency": 2}`)
		require.NoError(t, execErr)
		require.Empty(t, result.Error)
		assert.Equal(t, 2, notifier.gotMaxConcurrency)
	})

	t.Run("max_concurrency_path_invalid_value_errors", func(t *testing.T) {
		t.Parallel()

		def := `{
			"StartAt": "M",
			"States": {
				"M": {
					"Type": "Map",
					"End": true,
					"ItemsPath": "$.items",
					"MaxConcurrencyPath": "$.notANumber",
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
		_, execErr := exec.Execute(t.Context(), "test",
			`{"items": [1,2,3], "notANumber": "oops"}`)
		require.Error(t, execErr)
	})
}
