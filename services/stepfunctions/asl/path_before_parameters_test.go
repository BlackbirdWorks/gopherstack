package asl_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

// TestExecutor_Task_TimeoutSecondsPath_ResolvesBeforeParameters covers
// gopherstack-w0s3: TimeoutSecondsPath must resolve against the state's
// input after InputPath but before Parameters (ASL spec, "Using InputPath,
// Parameters, ResultSelector, ResultPath and OutputPath": Parameters' own
// input "is the result of applying the InputPath to the raw input"; AWS
// docs amazon-states-language-task-state.html's GlueJobTask example pairs
// a Parameters block that replaces the whole payload with a
// TimeoutSecondsPath that still resolves against the pre-Parameters field).
// Both "timeout" keys exist so a wrong-scope resolution silently picks the
// wrong value instead of erroring.
func TestExecutor_Task_TimeoutSecondsPath_ResolvesBeforeParameters(t *testing.T) {
	t.Parallel()

	t.Run("resolves_against_pre_parameters_input", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			def := `{
				"StartAt": "T",
				"States": {
					"T": {
						"Type": "Task",
						"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
						"Parameters": {"timeout.$": "$.decoy"},
						"TimeoutSecondsPath": "$.timeout",
						"Catch": [{"ErrorEquals": ["States.Timeout"], "Next": "TimedOut"}],
						"End": true
					},
					"TimedOut": {"Type": "Pass", "End": true, "Result": "timeout"}
				}
			}`

			sm, err := asl.Parse(def)
			require.NoError(t, err)

			lambda := &mockLambdaFnCtx{fn: func(ctx context.Context) ([]byte, int, error) {
				<-ctx.Done()

				return nil, 0, ctx.Err()
			}}

			exec := asl.NewExecutor(sm, lambda, nil)

			start := time.Now()
			result, execErr := exec.Execute(t.Context(), "test", `{"timeout": 5, "decoy": 1}`)
			elapsed := time.Since(start)

			require.NoError(t, execErr)
			require.Empty(t, result.Error)
			assert.Equal(t, "timeout", result.Output)
			assert.Equal(
				t,
				5*time.Second,
				elapsed,
				"TimeoutSecondsPath must resolve against the pre-Parameters input (timeout=5), "+
					"not the Parameters output (timeout=1 from decoy)",
			)
		})
	})
}

// TestExecutor_Map_ItemsPath_ResolvesBeforeParameters covers
// gopherstack-w0s3 for ItemsPath: the ASL Map State spec section resolves
// it against "the effective input" (post-InputPath, pre-Parameters), not
// the state's own Parameters field -- which in JSONPath Map states is the
// deprecated ItemSelector alias applied per item, never to the whole
// array-selection scope.
func TestExecutor_Map_ItemsPath_ResolvesBeforeParameters(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "M",
		"States": {
			"M": {
				"Type": "Map",
				"End": true,
				"Parameters": {"items.$": "$.decoyItems"},
				"ItemsPath": "$.items",
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

	// Pre-Parameters "items" has 5 elements; post-Parameters (bug) "items" is
	// overwritten with the 2-element "decoyItems".
	result, execErr := exec.Execute(t.Context(), "test",
		`{"items": [1,2,3,4,5], "decoyItems": [1,2]}`)
	require.NoError(t, execErr)
	require.Empty(t, result.Error)

	arr, ok := result.Output.([]any)
	require.True(t, ok)
	assert.Len(t, arr, 5, "ItemsPath must resolve against the pre-Parameters input")
}

// TestExecutor_Map_ItemBatcherMaxItemsPerBatchPath_ResolvesBeforeParameters
// covers gopherstack-w0s3 for ItemBatcher.MaxItemsPerBatchPath.
func TestExecutor_Map_ItemBatcherMaxItemsPerBatchPath_ResolvesBeforeParameters(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "M",
		"States": {
			"M": {
				"Type": "Map",
				"End": true,
				"Parameters": {"items.$": "$.items", "maxBatch.$": "$.decoy"},
				"ItemsPath": "$.items",
				"ItemBatcher": {"MaxItemsPerBatchPath": "$.maxBatch"},
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

	// Pre-Parameters "maxBatch" is 5 (one batch of 5 items); post-Parameters
	// (bug) "maxBatch" is overwritten with the decoy value 2 (three batches).
	result, execErr := exec.Execute(t.Context(), "test",
		`{"items": [1,2,3,4,5], "maxBatch": 5, "decoy": 2}`)
	require.NoError(t, execErr)
	require.Empty(t, result.Error)

	arr, ok := result.Output.([]any)
	require.True(t, ok)
	assert.Len(t, arr, 1,
		"MaxItemsPerBatchPath must resolve against the pre-Parameters input (5), "+
			"not the Parameters output (2)")
}

// TestExecutor_Map_ItemReaderMaxItemsPath_ResolvesBeforeParameters covers
// gopherstack-w0s3 for ReaderConfig.MaxItemsPath: the Map state's own
// Parameters field must not affect the input ItemReader's ReaderConfig
// resolves MaxItemsPath against.
func TestExecutor_Map_ItemReaderMaxItemsPath_ResolvesBeforeParameters(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "M",
		"States": {
			"M": {
				"Type": "Map",
				"End": true,
				"Parameters": {"maxItems.$": "$.decoy"},
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

	// Pre-Parameters "maxItems" is 5 (no truncation); post-Parameters (bug)
	// "maxItems" is overwritten with the decoy value 2.
	result, execErr := exec.Execute(t.Context(), "test", `{"maxItems": 5, "decoy": 2}`)
	require.NoError(t, execErr)
	require.Empty(t, result.Error)

	arr, ok := result.Output.([]any)
	require.True(t, ok)
	assert.Len(t, arr, 5,
		"MaxItemsPath must resolve against the pre-Parameters input (5), not the "+
			"Parameters output (2)")
}

// TestExecutor_Map_MaxConcurrencyPath_ResolvesBeforeParameters covers
// gopherstack-w0s3 for MaxConcurrencyPath: it must resolve against the
// Map state's pre-Parameters input, matching ItemsPath/ToleratedFailure*.
func TestExecutor_Map_MaxConcurrencyPath_ResolvesBeforeParameters(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "M",
		"States": {
			"M": {
				"Type": "Map",
				"End": true,
				"Parameters": {"items.$": "$.items", "maxConcurrency.$": "$.override"},
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

	// Pre-Parameters "maxConcurrency" is 3; post-Parameters (bug) it's
	// overwritten by "override" (1).
	result, execErr := exec.Execute(t.Context(), "test",
		`{"items": [1,2,3], "maxConcurrency": 3, "override": 1}`)
	require.NoError(t, execErr)
	require.Empty(t, result.Error)
	assert.Equal(t, 3, notifier.gotMaxConcurrency,
		"MaxConcurrencyPath must resolve against the pre-Parameters input (3), not "+
			"the Parameters output (1)")
}

// TestExecutor_Map_ToleratedFailureCountPath_ResolvesBeforeParameters covers
// gopherstack-w0s3 for ToleratedFailureCountPath: resolving it against the
// post-Parameters input can silently pick a stricter (or looser) threshold
// than the state definition intends.
func TestExecutor_Map_ToleratedFailureCountPath_ResolvesBeforeParameters(t *testing.T) {
	t.Parallel()

	def := `{
		"StartAt": "M",
		"States": {
			"M": {
				"Type": "Map",
				"End": true,
				"Parameters": {"items.$": "$.items", "tolerate.$": "$.override"},
				"ItemsPath": "$.items",
				"ToleratedFailureCountPath": "$.tolerate",
				"Iterator": {
					"StartAt": "Check",
					"States": {
						"Check": {
							"Type": "Choice",
							"Choices": [
								{"Variable": "$.fail", "BooleanEquals": true, "Next": "Boom"}
							],
							"Default": "OK"
						},
						"Boom": {"Type": "Fail", "Error": "Boom", "Cause": "boom"},
						"OK": {"Type": "Pass", "End": true}
					}
				}
			}
		}
	}`

	sm, err := asl.Parse(def)
	require.NoError(t, err)

	exec := asl.NewExecutor(sm, nil, nil)

	// Pre-Parameters "tolerate" is 2 (one failing item is within tolerance);
	// post-Parameters (bug) "tolerate" is overwritten by "override" (0).
	input := `{
		"items": [{"fail": false}, {"fail": true}, {"fail": false}, {"fail": false}],
		"tolerate": 2,
		"override": 0
	}`

	result, execErr := exec.Execute(t.Context(), "test", input)
	require.NoError(t, execErr)
	assert.Empty(t, result.Error,
		"ToleratedFailureCountPath must resolve against the pre-Parameters input (2), "+
			"not the Parameters output (0)")

	arr, ok := result.Output.([]any)
	require.True(t, ok)
	assert.Len(t, arr, 4)
}
