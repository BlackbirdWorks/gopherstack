package iotanalytics_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotanalytics"
)

// errFakeThingNotFound/errFakeShadowNotFound/errFakeFunctionNotFound are static test-double
// errors (err113: no dynamic fmt.Errorf/errors.New at call sites).
var (
	errFakeThingNotFound    = errors.New("thing not found")
	errFakeShadowNotFound   = errors.New("shadow not found")
	errFakeFunctionNotFound = errors.New("function not found")
)

// fakeLambdaInvoker is a test double for iotanalytics.LambdaInvoker.
type fakeLambdaInvoker struct {
	invoke func(ctx context.Context, name, invocationType string, payload []byte) ([]byte, int, error)
	calls  *[]string
}

func (f *fakeLambdaInvoker) InvokeFunction(
	ctx context.Context, name, invocationType string, payload []byte,
) ([]byte, int, error) {
	if f.calls != nil {
		*f.calls = append(*f.calls, string(payload))
	}

	return f.invoke(ctx, name, invocationType, payload)
}

// echoAddFieldLambda returns a fakeLambdaInvoker that decodes the incoming JSON object
// array, adds seen=true to each object, and returns the array.
func echoAddFieldLambda(calls *[]string) *fakeLambdaInvoker {
	return &fakeLambdaInvoker{
		calls: calls,
		invoke: func(_ context.Context, _, _ string, payload []byte) ([]byte, int, error) {
			var msgs []map[string]any
			if err := json.Unmarshal(payload, &msgs); err != nil {
				return nil, 0, err
			}

			for _, m := range msgs {
				m["seen"] = true
			}

			out, err := json.Marshal(msgs)

			return out, 200, err
		},
	}
}

type fakeThingRegistry struct {
	things map[string]map[string]any
}

func (f *fakeThingRegistry) DescribeThing(thingName string) (map[string]any, error) {
	t, ok := f.things[thingName]
	if !ok {
		return nil, errFakeThingNotFound
	}

	return t, nil
}

type fakeThingShadowStore struct {
	shadows map[string]map[string]any
}

func (f *fakeThingShadowStore) GetThingShadow(thingName string) (map[string]any, error) {
	s, ok := f.shadows[thingName]
	if !ok {
		return nil, errFakeShadowNotFound
	}

	return s, nil
}

// TestInMemoryBackend_RunPipelineActivity_Lambda covers the "lambda" activity once a
// LambdaInvoker is wired: batching by BatchSize, splicing the invoked function's JSON
// object array response back into place, invoke errors, and malformed responses.
func TestInMemoryBackend_RunPipelineActivity_Lambda(t *testing.T) {
	t.Parallel()

	t.Run("invokes_and_applies_result", func(t *testing.T) {
		t.Parallel()

		b := iotanalytics.NewInMemoryBackend()
		b.SetLambdaBackend(echoAddFieldLambda(nil))

		activity := iotanalytics.PipelineActivity{
			Lambda: &iotanalytics.PipelineLambdaActivity{Name: "l", LambdaName: "fn", BatchSize: 10},
		}

		out, err := b.RunPipelineActivity(t.Context(), activity, [][]byte{[]byte(`{"x":1}`)})
		require.NoError(t, err)
		require.Len(t, out, 1)

		var decoded map[string]any
		require.NoError(t, json.Unmarshal(out[0], &decoded))
		assert.Equal(t, true, decoded["seen"])
		assert.InDelta(t, 1.0, decoded["x"], 1e-9)
	})

	t.Run("splits_into_batches_of_batch_size", func(t *testing.T) {
		t.Parallel()

		var calls []string

		b := iotanalytics.NewInMemoryBackend()
		b.SetLambdaBackend(echoAddFieldLambda(&calls))

		activity := iotanalytics.PipelineActivity{
			Lambda: &iotanalytics.PipelineLambdaActivity{Name: "l", LambdaName: "fn", BatchSize: 2},
		}

		payloads := [][]byte{[]byte(`{"x":1}`), []byte(`{"x":2}`), []byte(`{"x":3}`)}

		out, err := b.RunPipelineActivity(t.Context(), activity, payloads)
		require.NoError(t, err)
		require.Len(t, out, 3)
		require.Len(t, calls, 2, "3 payloads at batchSize=2 must invoke twice (2 then 1)")

		var firstBatch []map[string]any
		require.NoError(t, json.Unmarshal([]byte(calls[0]), &firstBatch))
		assert.Len(t, firstBatch, 2)

		var secondBatch []map[string]any
		require.NoError(t, json.Unmarshal([]byte(calls[1]), &secondBatch))
		assert.Len(t, secondBatch, 1)
	})

	t.Run("non_json_payload_excluded_but_unchanged", func(t *testing.T) {
		t.Parallel()

		b := iotanalytics.NewInMemoryBackend()
		b.SetLambdaBackend(echoAddFieldLambda(nil))

		activity := iotanalytics.PipelineActivity{
			Lambda: &iotanalytics.PipelineLambdaActivity{Name: "l", LambdaName: "fn", BatchSize: 10},
		}

		out, err := b.RunPipelineActivity(t.Context(), activity, [][]byte{[]byte(`not json`)})
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.Equal(t, "not json", string(out[0]))
	})

	t.Run("invoke_error_fails_the_call", func(t *testing.T) {
		t.Parallel()

		b := iotanalytics.NewInMemoryBackend()
		b.SetLambdaBackend(&fakeLambdaInvoker{
			invoke: func(context.Context, string, string, []byte) ([]byte, int, error) {
				return nil, 0, errFakeFunctionNotFound
			},
		})

		activity := iotanalytics.PipelineActivity{
			Lambda: &iotanalytics.PipelineLambdaActivity{Name: "l", LambdaName: "missing", BatchSize: 10},
		}

		_, err := b.RunPipelineActivity(t.Context(), activity, [][]byte{[]byte(`{"x":1}`)})
		require.Error(t, err)
		assert.ErrorIs(t, err, iotanalytics.ErrPipelineActivityFailed)
	})

	t.Run("mismatched_response_length_fails_the_call", func(t *testing.T) {
		t.Parallel()

		b := iotanalytics.NewInMemoryBackend()
		b.SetLambdaBackend(&fakeLambdaInvoker{
			invoke: func(context.Context, string, string, []byte) ([]byte, int, error) {
				return []byte(`[]`), 200, nil
			},
		})

		activity := iotanalytics.PipelineActivity{
			Lambda: &iotanalytics.PipelineLambdaActivity{Name: "l", LambdaName: "fn", BatchSize: 10},
		}

		_, err := b.RunPipelineActivity(t.Context(), activity, [][]byte{[]byte(`{"x":1}`)})
		require.Error(t, err)
		assert.ErrorIs(t, err, iotanalytics.ErrPipelineActivityFailed)
	})
}

// TestInMemoryBackend_RunPipelineActivity_DeviceRegistryEnrich covers the
// "deviceRegistryEnrich" activity once a ThingRegistry is wired: successful enrichment and
// the not-found failure path.
func TestInMemoryBackend_RunPipelineActivity_DeviceRegistryEnrich(t *testing.T) {
	t.Parallel()

	t.Run("enriches_with_registry_data", func(t *testing.T) {
		t.Parallel()

		b := iotanalytics.NewInMemoryBackend()
		b.SetThingRegistry(&fakeThingRegistry{
			things: map[string]map[string]any{"my-thing": {"thingTypeName": "sensor"}},
		})

		activity := iotanalytics.PipelineActivity{
			DeviceRegistryEnrich: &iotanalytics.PipelineDeviceRegistryEnrichActivity{
				Name: "e", Attribute: "registry", ThingName: "my-thing",
			},
		}

		out, err := b.RunPipelineActivity(t.Context(), activity, [][]byte{[]byte(`{"x":1}`)})
		require.NoError(t, err)
		require.Len(t, out, 1)

		var decoded map[string]any
		require.NoError(t, json.Unmarshal(out[0], &decoded))

		registry, ok := decoded["registry"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "sensor", registry["thingTypeName"])
	})

	t.Run("unknown_thing_fails_the_call", func(t *testing.T) {
		t.Parallel()

		b := iotanalytics.NewInMemoryBackend()
		b.SetThingRegistry(&fakeThingRegistry{things: map[string]map[string]any{}})

		activity := iotanalytics.PipelineActivity{
			DeviceRegistryEnrich: &iotanalytics.PipelineDeviceRegistryEnrichActivity{
				Name: "e", Attribute: "registry", ThingName: "does-not-exist",
			},
		}

		_, err := b.RunPipelineActivity(t.Context(), activity, [][]byte{[]byte(`{"x":1}`)})
		require.Error(t, err)
		assert.ErrorIs(t, err, iotanalytics.ErrPipelineActivityFailed)
	})
}

// TestInMemoryBackend_RunPipelineActivity_DeviceShadowEnrich covers the
// "deviceShadowEnrich" activity once a ThingShadowStore is wired: successful enrichment and
// the not-found failure path.
func TestInMemoryBackend_RunPipelineActivity_DeviceShadowEnrich(t *testing.T) {
	t.Parallel()

	t.Run("enriches_with_shadow_data", func(t *testing.T) {
		t.Parallel()

		wantState := map[string]any{"reported": map[string]any{"on": true}}

		b := iotanalytics.NewInMemoryBackend()
		b.SetThingShadowStore(&fakeThingShadowStore{
			shadows: map[string]map[string]any{"my-thing": {"state": wantState}},
		})

		activity := iotanalytics.PipelineActivity{
			DeviceShadowEnrich: &iotanalytics.PipelineDeviceShadowEnrichActivity{
				Name: "e", Attribute: "shadow", ThingName: "my-thing",
			},
		}

		out, err := b.RunPipelineActivity(t.Context(), activity, [][]byte{[]byte(`{"x":1}`)})
		require.NoError(t, err)
		require.Len(t, out, 1)

		var decoded map[string]any
		require.NoError(t, json.Unmarshal(out[0], &decoded))

		shadow, ok := decoded["shadow"].(map[string]any)
		require.True(t, ok)

		state, ok := shadow["state"].(map[string]any)
		require.True(t, ok)

		reported, ok := state["reported"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, true, reported["on"])
	})

	t.Run("unknown_thing_fails_the_call", func(t *testing.T) {
		t.Parallel()

		b := iotanalytics.NewInMemoryBackend()
		b.SetThingShadowStore(&fakeThingShadowStore{shadows: map[string]map[string]any{}})

		activity := iotanalytics.PipelineActivity{
			DeviceShadowEnrich: &iotanalytics.PipelineDeviceShadowEnrichActivity{
				Name: "e", Attribute: "shadow", ThingName: "does-not-exist",
			},
		}

		_, err := b.RunPipelineActivity(t.Context(), activity, [][]byte{[]byte(`{"x":1}`)})
		require.Error(t, err)
		assert.ErrorIs(t, err, iotanalytics.ErrPipelineActivityFailed)
	})
}
