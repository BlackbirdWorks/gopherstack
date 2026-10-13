package lambda_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestESM_TuningValidation(t *testing.T) {
	t.Parallel()

	const (
		sqsARN     = `"arn:aws:sqs:us-east-1:000000000000:q"`
		kinesisARN = `"arn:aws:kinesis:us-east-1:000000000000:stream/s"`
		mskARN     = `"arn:aws:kafka:us-east-1:000000000000:cluster/c/uuid"`
	)

	tests := []struct {
		name     string
		source   string
		extra    string
		wantCode int
	}{
		{
			name:     "sqs_scaling_ok",
			source:   sqsARN,
			extra:    `"ScalingConfig":{"MaximumConcurrency":5}`,
			wantCode: http.StatusCreated,
		},
		{
			name:     "kinesis_scaling_rejected",
			source:   kinesisARN,
			extra:    `"ScalingConfig":{"MaximumConcurrency":5}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "event_count_metric_ok",
			source:   sqsARN,
			extra:    `"MetricsConfig":{"Metrics":["EventCount"]}`,
			wantCode: http.StatusCreated,
		},
		{
			name:     "error_count_on_sqs_rejected",
			source:   sqsARN,
			extra:    `"MetricsConfig":{"Metrics":["ErrorCount"]}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "error_count_on_msk_ok",
			source:   mskARN,
			extra:    `"MetricsConfig":{"Metrics":["ErrorCount"]}`,
			wantCode: http.StatusCreated,
		},
		{
			name:     "unknown_metric_rejected",
			source:   sqsARN,
			extra:    `"MetricsConfig":{"Metrics":["Bogus"]}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "sqs_pollers_ok",
			source:   sqsARN,
			extra:    `"ProvisionedPollerConfig":{"MinimumPollers":2,"MaximumPollers":50}`,
			wantCode: http.StatusCreated,
		},
		{
			name:     "sqs_min_pollers_too_low",
			source:   sqsARN,
			extra:    `"ProvisionedPollerConfig":{"MinimumPollers":1}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "sqs_max_pollers_too_high",
			source:   sqsARN,
			extra:    `"ProvisionedPollerConfig":{"MaximumPollers":10001}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "msk_max_pollers_too_high",
			source:   mskARN,
			extra:    `"ProvisionedPollerConfig":{"MaximumPollers":2001}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "poller_group_on_sqs_rejected",
			source:   sqsARN,
			extra:    `"ProvisionedPollerConfig":{"PollerGroupName":"g"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "poller_group_on_msk_ok",
			source:   mskARN,
			extra:    `"ProvisionedPollerConfig":{"PollerGroupName":"g"}`,
			wantCode: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			createFunctionForTest(t, h, "tune-fn")

			body := `{"FunctionName":"tune-fn","EventSourceArn":` + tt.source + `,"StartingPosition":"LATEST",` + tt.extra + `}`
			rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
		})
	}
}

func TestESM_UpdateTuningValidation(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "tune-upd-fn")

	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings",
		`{"FunctionName":"tune-upd-fn","EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/s",`+
			`"StartingPosition":"LATEST"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))

	uuid, _ := created["UUID"].(string)

	rec = callInMemoryHandler(t, h, http.MethodPut, "/2015-03-31/event-source-mappings/"+uuid,
		`{"ScalingConfig":{"MaximumConcurrency":5}}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
}
