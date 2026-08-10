package kinesisanalytics_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/kinesisanalytics"
)

func TestHandler_AddApplicationInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend)
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "adds kinesis streams input",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "input-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "input-app",
				"CurrentApplicationVersionId": 1,
				"Input": map[string]any{
					"NamePrefix": "SOURCE_SQL_STREAM",
					"KinesisStreamsInput": map[string]any{
						"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/test",
						"RoleARN":     "arn:aws:iam::000000000000:role/role",
					},
					"InputSchema": map[string]any{
						"RecordFormat":  map[string]any{"RecordFormatType": "JSON"},
						"RecordColumns": []map[string]any{{"Name": "COL1", "SqlType": "VARCHAR(4)"}},
					},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing application name",
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "app not found",
			input: map[string]any{
				"ApplicationName":             "ghost",
				"CurrentApplicationVersionId": 1,
				"Input": map[string]any{
					"NamePrefix": "SOURCE",
					"KinesisStreamsInput": map[string]any{
						"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/test",
						"RoleARN":     "arn:aws:iam::000000000000:role/r",
					},
					"InputSchema": map[string]any{
						"RecordFormat":  map[string]any{"RecordFormatType": "JSON"},
						"RecordColumns": []map[string]any{{"Name": "COL1", "SqlType": "VARCHAR(4)"}},
					},
				},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			// AWS models Input.InputSchema as a required member (validated server-side via
			// aws-sdk-go-v2/service/kinesisanalytics/validators.go's validateInput, which is
			// authoritative even though the field's doc comment alone doesn't say "required").
			name: "missing InputSchema is rejected",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "input-noschema-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "input-noschema-app",
				"CurrentApplicationVersionId": 1,
				"Input": map[string]any{
					"NamePrefix": "SOURCE",
					"KinesisStreamsInput": map[string]any{
						"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/test",
						"RoleARN":     "arn:aws:iam::000000000000:role/role",
					},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "version mismatch",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "input-ver-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "input-ver-app",
				"CurrentApplicationVersionId": 99,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			// AWS models Input.NamePrefix as a required member; a request missing it must be
			// rejected, not silently stored with an empty prefix (which also breaks
			// InAppStreamNames derivation).
			name: "missing NamePrefix is rejected",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "input-noprefix-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "input-noprefix-app",
				"CurrentApplicationVersionId": 1,
				"Input": map[string]any{
					"KinesisStreamsInput": map[string]any{
						"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/test",
						"RoleARN":     "arn:aws:iam::000000000000:role/role",
					},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, "AddApplicationInput", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				app, err := b.DescribeApplication(context.Background(), "input-app")
				require.NoError(t, err)
				assert.NotEmpty(t, app.Inputs)
			}
		})
	}
}

// TestAddApplicationInput_LimitErrorCode verifies that exceeding the per-application
// input cap surfaces as InvalidArgumentException, matching the modeled error set for
// AddApplicationInput (no LimitExceededException in its AWS API definition).
func TestAddApplicationInput_LimitErrorCode(t *testing.T) {
	t.Parallel()

	b := newBackend()
	app, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "in-cap-app", "", "", nil)
	require.NoError(t, err)

	require.NoError(t, b.AddApplicationInput(
		context.Background(), app.ApplicationName, app.ApplicationVersionID,
		kinesisanalytics.InputDescription{NamePrefix: "IN1"},
	))

	err = b.AddApplicationInput(
		context.Background(), app.ApplicationName, 2,
		kinesisanalytics.InputDescription{NamePrefix: "IN2"},
	)
	require.ErrorIs(t, err, awserr.ErrInvalidParameter)
	assert.NotErrorIs(t, err, awserr.ErrConflict)
}

func TestHandler_AddApplicationInputProcessingConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend) string
		input      func(inputID string) map[string]any
		name       string
		appName    string
		wantStatus int
	}{
		{
			name:    "adds processing config to existing input",
			appName: "proc-app",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "proc-app", "", "", nil)
				_ = b.AddApplicationInput(
					context.Background(),
					"proc-app",
					1,
					kinesisanalytics.InputDescription{NamePrefix: "PREFIX"},
				)
				app, _ := b.DescribeApplication(context.Background(), "proc-app")

				return app.Inputs[0].InputID
			},
			input: func(inputID string) map[string]any {
				return map[string]any{
					"ApplicationName":             "proc-app",
					"CurrentApplicationVersionId": 2,
					"InputId":                     inputID,
					"InputProcessingConfiguration": map[string]any{
						"InputLambdaProcessor": map[string]any{
							"ResourceARN": "arn:aws:lambda:us-east-1:000000000000:function:fn",
							"RoleARN":     "arn:aws:iam::000000000000:role/role",
						},
					},
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:  "missing application name",
			setup: func(_ *kinesisanalytics.InMemoryBackend) string { return "" },
			input: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "input id not found",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "proc-notfound-app", "", "", nil)

				return "nonexistent-id"
			},
			input: func(inputID string) map[string]any {
				return map[string]any{
					"ApplicationName":             "proc-notfound-app",
					"CurrentApplicationVersionId": 1,
					"InputId":                     inputID,
					"InputProcessingConfiguration": map[string]any{
						"InputLambdaProcessor": map[string]any{
							"ResourceARN": "arn:aws:lambda:us-east-1:000000000000:function:fn",
							"RoleARN":     "arn:aws:iam::000000000000:role/role",
						},
					},
				}
			},
			wantStatus: http.StatusNotFound,
		},
		{
			// AWS requires InputProcessingConfiguration.InputLambdaProcessor whenever
			// InputProcessingConfiguration itself is supplied.
			name:    "missing InputLambdaProcessor is rejected",
			appName: "proc-nolambda-app",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "proc-nolambda-app", "", "", nil)
				_ = b.AddApplicationInput(
					context.Background(),
					"proc-nolambda-app",
					1,
					kinesisanalytics.InputDescription{NamePrefix: "PREFIX"},
				)
				app, _ := b.DescribeApplication(context.Background(), "proc-nolambda-app")

				return app.Inputs[0].InputID
			},
			input: func(inputID string) map[string]any {
				return map[string]any{
					"ApplicationName":              "proc-nolambda-app",
					"CurrentApplicationVersionId":  2,
					"InputId":                      inputID,
					"InputProcessingConfiguration": map[string]any{},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			inputID := tt.setup(b)
			rec := doRequest(t, h, "AddApplicationInputProcessingConfiguration", tt.input(inputID))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				app, err := b.DescribeApplication(context.Background(), tt.appName)
				require.NoError(t, err)
				require.NotEmpty(t, app.Inputs)
				assert.NotNil(t, app.Inputs[0].InputProcessingConfigurationDescription)
			}
		})
	}
}

// TestVersionNotBumpedOnAddInputProcConfigNotFound verifies no bump when inputID missing.
func TestVersionNotBumpedOnAddInputProcConfigNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "ipc-bump", "", "", nil)

	err := b.AddApplicationInputProcessingConfiguration(
		context.Background(),
		"ipc-bump",
		1,
		"nonexistent-input-id",
		nil,
	)
	require.ErrorIs(t, err, kinesisanalytics.ErrNotFound)

	app, _ := b.DescribeApplication(context.Background(), "ipc-bump")
	assert.Equal(t, int64(1), app.ApplicationVersionID)
}

func TestHandler_DeleteApplicationInputProcessingConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend) string
		input      func(inputID string) map[string]any
		name       string
		appName    string
		wantStatus int
	}{
		{
			name:    "removes processing config from input",
			appName: "del-proc-app",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "del-proc-app", "", "", nil)
				_ = b.AddApplicationInput(
					context.Background(),
					"del-proc-app",
					1,
					kinesisanalytics.InputDescription{NamePrefix: "STREAM"},
				)
				app, _ := b.DescribeApplication(context.Background(), "del-proc-app")
				inputID := app.Inputs[0].InputID
				cfg := &kinesisanalytics.InputProcessingConfigurationDesc{
					InputLambdaProcessor: &kinesisanalytics.LambdaProcessorDesc{ResourceARN: "arn:aws:lambda::fn"},
				}
				_ = b.AddApplicationInputProcessingConfiguration(context.Background(), "del-proc-app", 2, inputID, cfg)

				return inputID
			},
			input: func(inputID string) map[string]any {
				return map[string]any{
					"ApplicationName":             "del-proc-app",
					"CurrentApplicationVersionId": 3,
					"InputId":                     inputID,
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:  "missing application name",
			setup: func(_ *kinesisanalytics.InMemoryBackend) string { return "" },
			input: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "input id not found",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "del-proc-notfound", "", "", nil)

				return "nonexistent"
			},
			input: func(inputID string) map[string]any {
				return map[string]any{
					"ApplicationName":             "del-proc-notfound",
					"CurrentApplicationVersionId": 1,
					"InputId":                     inputID,
				}
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			inputID := tt.setup(b)
			rec := doRequest(t, h, "DeleteApplicationInputProcessingConfiguration", tt.input(inputID))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				app, err := b.DescribeApplication(context.Background(), tt.appName)
				require.NoError(t, err)
				require.NotEmpty(t, app.Inputs)
				assert.Nil(t, app.Inputs[0].InputProcessingConfigurationDescription)
			}
		})
	}
}

// TestDeleteApplicationInputProcessingConfiguration_Validation verifies missing input ID returns 400.
func TestDeleteApplicationInputProcessingConfiguration_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DeleteApplicationInputProcessingConfiguration", map[string]any{"ApplicationName": "my-app"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDeepCopyInput verifies appCopy handles Inputs with pointer fields.
func TestDeepCopyInput(t *testing.T) {
	t.Parallel()

	b := newBackend()
	app, _ := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "dc-input-app", "", "", nil)

	// Add input.
	_ = b.AddApplicationInput(
		context.Background(),
		"dc-input-app",
		app.ApplicationVersionID,
		kinesisanalytics.InputDescription{
			NamePrefix: "src_",
			KinesisStreamsInputDescription: &kinesisanalytics.KinesisStreamsInputDesc{
				ResourceARN: "arn:aws:kinesis:us-east-1:000:stream/st",
			},
		},
	)

	// Add processing config to the input.
	app2, _ := b.DescribeApplication(context.Background(), "dc-input-app")
	inputID := app2.Inputs[0].InputID

	_ = b.AddApplicationInputProcessingConfiguration(
		context.Background(),
		"dc-input-app",
		app2.ApplicationVersionID,
		inputID,
		&kinesisanalytics.InputProcessingConfigurationDesc{
			InputLambdaProcessor: &kinesisanalytics.LambdaProcessorDesc{
				ResourceARN: "arn:aws:lambda:us-east-1:000:function:fn",
			},
		},
	)

	app3, _ := b.DescribeApplication(context.Background(), "dc-input-app")
	require.Len(t, app3.Inputs, 1)
	require.NotNil(t, app3.Inputs[0].InputProcessingConfigurationDescription)

	// Mutate the copy — original must be unaffected.
	app3.Inputs[0].InputProcessingConfigurationDescription.InputLambdaProcessor.ResourceARN = "mutated"

	app4, _ := b.DescribeApplication(context.Background(), "dc-input-app")
	assert.Equal(
		t,
		"arn:aws:lambda:us-east-1:000:function:fn",
		app4.Inputs[0].InputProcessingConfigurationDescription.InputLambdaProcessor.ResourceARN,
	)
}

func TestHandler_DiscoverInputSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input       map[string]any
		name        string
		wantErrCode string
		wantStatus  int
	}{
		{
			// No Kinesis backend is wired in this handler, so DiscoverInputSchema cannot reach
			// the streaming source and correctly reports UnableToDetectSchemaException instead
			// of fabricating a "successful" schema (see TestDiscoverInputSchema_Kinesis* in
			// discover_schema_test.go for the case where a reader IS wired).
			name: "streaming source with no Kinesis backend wired reports unable to detect schema",
			input: map[string]any{
				"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/test",
				"RoleARN":     "arn:aws:iam::000000000000:role/role",
			},
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "UnableToDetectSchemaException",
		},
		{
			// No S3 backend is wired in this handler either (see
			// TestDiscoverInputSchema_S3* in discover_schema_test.go for the wired case).
			name: "S3 source with no S3 backend wired reports unable to detect schema",
			input: map[string]any{
				"S3Configuration": map[string]any{
					"BucketARN": "arn:aws:s3:::bucket",
					"FileKey":   "key.json",
					"RoleARN":   "arn:aws:iam::000000000000:role/role",
				},
			},
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "UnableToDetectSchemaException",
		},
		{
			// Real AWS rejects a request with no streaming source and no S3Configuration --
			// there is nothing to discover a schema from.
			name:        "empty request is rejected",
			input:       map[string]any{},
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "InvalidArgumentException",
		},
		{
			// ResourceARN without RoleARN can never authenticate against the streaming source.
			name: "streaming source missing RoleARN is rejected",
			input: map[string]any{
				"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/test",
			},
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "InvalidArgumentException",
		},
		{
			// S3Configuration with a missing required sub-field is rejected.
			name: "S3 source missing FileKey is rejected",
			input: map[string]any{
				"S3Configuration": map[string]any{
					"BucketARN": "arn:aws:s3:::bucket",
					"RoleARN":   "arn:aws:iam::000000000000:role/role",
				},
			},
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "InvalidArgumentException",
		},
		{
			// Both a streaming source and an S3Configuration is over-specified and rejected.
			name: "both sources specified is rejected",
			input: map[string]any{
				"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/test",
				"RoleARN":     "arn:aws:iam::000000000000:role/role",
				"S3Configuration": map[string]any{
					"BucketARN": "arn:aws:s3:::bucket",
					"FileKey":   "key.json",
					"RoleARN":   "arn:aws:iam::000000000000:role/role",
				},
			},
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "InvalidArgumentException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DiscoverInputSchema", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantErrCode, resp["__type"])
		})
	}
}
