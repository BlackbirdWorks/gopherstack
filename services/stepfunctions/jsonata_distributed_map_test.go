package stepfunctions_test

import (
	"context"
	"encoding/json"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

const jsonataPassProcessor = `"ItemProcessor":{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`

func TestJSONata_SDK_MapObjectItems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		state      string
		input      string
		wantOutput string
		wantStatus string
		wantError  string
	}{
		{
			name:       "object_literal_pairs",
			state:      `"Items":{"b":2,"a":1},`,
			input:      `{}`,
			wantOutput: `[{"Key":"a","Value":1},{"Key":"b","Value":2}]`,
			wantStatus: "SUCCEEDED",
		},
		{
			name:       "object_expression",
			state:      `"Items":"{% $states.input.servers %}",`,
			input:      `{"servers":{"web":{"port":80}}}`,
			wantOutput: `[{"Key":"web","Value":{"port":80}}]`,
			wantStatus: "SUCCEEDED",
		},
		{
			name: "item_selector_context_key_value",
			state: `"Items":"{% $states.input %}","ItemSelector":{"k":"{% $states.context.Map.Item.Key %}",` +
				`"v":"{% $states.context.Map.Item.Value %}","i":"{% $states.context.Map.Item.Index %}"},`,
			input:      `{"x":10,"y":20}`,
			wantOutput: `[{"i":0,"k":"x","v":10},{"i":1,"k":"y","v":20}]`,
			wantStatus: "SUCCEEDED",
		},
		{
			name:       "array_unchanged",
			state:      `"Items":"{% $states.input %}",`,
			input:      `[1,2]`,
			wantOutput: `[1,2]`,
			wantStatus: "SUCCEEDED",
		},
		{
			name:       "scalar_rejected",
			state:      `"Items":"{% 5 %}",`,
			input:      `{}`,
			wantStatus: "FAILED",
			wantError:  "States.QueryEvaluationError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			def := `{"QueryLanguage":"JSONata","StartAt":"M","States":{"M":{"Type":"Map",` +
				tt.state + jsonataPassProcessor + `,"End":true}}}`
			got := runJSONataExpress(t, def, tt.input)

			assert.Equal(t, tt.wantStatus, got.status, "cause=%s", got.cause)
			assert.Equal(t, tt.wantError, got.errMsg)

			if tt.wantOutput != "" {
				assert.JSONEq(t, tt.wantOutput, got.output)
			}
		})
	}
}

func TestJSONata_MapObjectItemsRejectSetting(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		t.Helper()
		testRejectObjectItems(t)
	})
}

func testRejectObjectItems(t *testing.T) {
	t.Helper()

	b := stepfunctions.NewInMemoryBackend()
	b.SetSettings(stepfunctions.Settings{JSONataMapObjectItems: false})

	def := `{"QueryLanguage":"JSONata","StartAt":"M","States":{"M":{"Type":"Map","Items":{"a":1},` +
		jsonataPassProcessor + `,"End":true}}}`

	sm, err := b.CreateStateMachine(context.Background(), "reject-obj", def, validRoleARN, "STANDARD")
	require.NoError(t, err)

	exec, err := b.StartExecution(sm.StateMachineArn, "e", `{}`)
	require.NoError(t, err)

	d := waitForTerminalExecution(t, b, exec.ExecutionArn)
	assert.Equal(t, "FAILED", d.Status)
	assert.Equal(t, "States.QueryEvaluationError", d.Error)
}

func TestJSONata_SDK_ItemBatcherExpressions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		batcher    string
		wantOutput string
		wantStatus string
		wantError  string
	}{
		{
			name:       "max_items_expression",
			batcher:    `{"MaxItemsPerBatch":"{% $states.input.n %}"}`,
			wantOutput: `[{"Items":[1,2]},{"Items":[3]}]`,
			wantStatus: "SUCCEEDED",
		},
		{
			name: "batch_input_expression",
			batcher: `{"MaxItemsPerBatch":"{% 3 %}","BatchInput":{"count":"{% $count($states.input.items) %}",` +
				`"fixed":"x"}}`,
			wantOutput: `[{"BatchInput":{"count":3,"fixed":"x"},"Items":[1,2,3]}]`,
			wantStatus: "SUCCEEDED",
		},
		{
			name:       "batch_input_direct_expression",
			batcher:    `{"MaxItemsPerBatch":3,"BatchInput":"{% $states.input.items[0] %}"}`,
			wantOutput: `[{"BatchInput":1,"Items":[1,2,3]}]`,
			wantStatus: "SUCCEEDED",
		},
		{
			name:       "max_bytes_expression",
			batcher:    `{"MaxInputBytesPerBatch":"{% 2 %}"}`,
			wantOutput: `[{"Items":[1,2]},{"Items":[3]}]`,
			wantStatus: "SUCCEEDED",
		},
		{
			name:       "non_positive_limit_fails",
			batcher:    `{"MaxItemsPerBatch":"{% 0 %}"}`,
			wantStatus: "FAILED",
			wantError:  "States.QueryEvaluationError",
		},
		{
			name:       "non_integer_limit_fails",
			batcher:    `{"MaxItemsPerBatch":"{% 1.5 %}"}`,
			wantStatus: "FAILED",
			wantError:  "States.QueryEvaluationError",
		},
		{
			name:       "string_limit_fails",
			batcher:    `{"MaxItemsPerBatch":"{% 'x' %}"}`,
			wantStatus: "FAILED",
			wantError:  "States.QueryEvaluationError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			def := `{"QueryLanguage":"JSONata","StartAt":"M","States":{"M":{"Type":"Map",` +
				`"Items":"{% $states.input.items %}","ItemBatcher":` + tt.batcher + `,` +
				jsonataPassProcessor + `,"End":true}}}`
			got := runJSONataExpress(t, def, `{"items":[1,2,3],"n":2}`)

			assert.Equal(t, tt.wantStatus, got.status, "cause=%s", got.cause)
			assert.Equal(t, tt.wantError, got.errMsg)

			if tt.wantOutput != "" {
				assert.JSONEq(t, tt.wantOutput, got.output)
			}
		})
	}
}

func TestJSONata_DistributedMapS3Arguments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		reader     string
		writer     string
		input      string
		wantOutput string
		wantExport bool
	}{
		{
			name: "reader_arguments_expression",
			reader: `"ItemReader":{"Resource":"arn:aws:states:::s3:getObject","ReaderConfig":{"InputType":"JSON"},` +
				`"Arguments":{"Bucket":"data-bucket","Key":"{% $states.input.key %}"}},`,
			input:      `{"key":"items.json"}`,
			wantOutput: `[{"id":1},{"id":2}]`,
		},
		{
			name: "reader_arguments_whole_expression",
			reader: `"ItemReader":{"Resource":"arn:aws:states:::s3:getObject","ReaderConfig":{"InputType":"JSON"},` +
				`"Arguments":"{% {'Bucket':'data-bucket','Key':'items.json'} %}"},`,
			input:      `{}`,
			wantOutput: `[{"id":1},{"id":2}]`,
		},
		{
			name: "writer_arguments_expression",
			reader: `"ItemReader":{"Resource":"arn:aws:states:::s3:getObject","ReaderConfig":{"InputType":"JSON"},` +
				`"Arguments":{"Bucket":"data-bucket","Key":"items.json"}},`,
			writer: `"ResultWriter":{"Resource":"arn:aws:states:::s3:putObject",` +
				`"Arguments":{"Bucket":"{% $states.input.out %}","Prefix":"jobs"}},`,
			input:      `{"out":"data-bucket"}`,
			wantExport: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				runS3ArgumentsCase(t, tt.reader, tt.writer, tt.input, tt.wantOutput, tt.wantExport)
			})
		})
	}
}

func runS3ArgumentsCase(t *testing.T, reader, writer, input, wantOutput string, wantExport bool) {
	t.Helper()

	s3Bk := newBucketBackedS3(t, "data-bucket")
	putS3Object(t, s3Bk, "data-bucket", "items.json", []byte(`[{"id":1},{"id":2}]`))

	b := stepfunctions.NewInMemoryBackend()
	b.SetS3Reader(stepfunctions.NewS3Integration(s3Bk))
	b.SetS3ResultWriter(stepfunctions.NewS3ResultWriterIntegration(s3Bk))

	def := `{"QueryLanguage":"JSONata","StartAt":"M","States":{"M":{"Type":"Map",` +
		reader + writer + jsonataPassProcessor + `,"End":true}}}`

	sm, err := b.CreateStateMachine(context.Background(), "s3-args", def, validRoleARN, "STANDARD")
	require.NoError(t, err)

	exec, err := b.StartExecution(sm.StateMachineArn, "e", input)
	require.NoError(t, err)

	d := waitForTerminalExecution(t, b, exec.ExecutionArn)
	require.Equal(t, "SUCCEEDED", d.Status, "error=%s cause=%s", d.Error, d.Cause)

	if !wantExport {
		assert.JSONEq(t, wantOutput, d.Output)

		return
	}

	var out mapExportOutput
	require.NoError(t, json.Unmarshal([]byte(d.Output), &out))
	assert.Equal(t, "data-bucket", out.ResultWriterDetails.Bucket)
	assert.NotEmpty(t, getS3ObjectBytes(t, s3Bk, "data-bucket", out.ResultWriterDetails.Key))
}
