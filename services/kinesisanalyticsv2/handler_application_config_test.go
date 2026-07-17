package kinesisanalyticsv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

func TestKAV2_AddApplicationCloudWatchLoggingOption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *kinesisanalyticsv2.Handler)
		body       map[string]any
		name       string
		wantCWLLen int
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *kinesisanalyticsv2.Handler) {
				doKAV2Request(t, h, "CreateApplication", map[string]any{
					"ApplicationName":    "cwl-app",
					"RuntimeEnvironment": "SQL-1_0",
				})
			},
			body: map[string]any{
				"ApplicationName": "cwl-app",
				"CloudWatchLoggingOption": map[string]any{
					"LogStreamARN": "arn:aws:logs:us-east-1:000000000000:log-group:my-group:log-stream:my-stream",
					"RoleARN":      "arn:aws:iam::000000000000:role/my-role",
				},
			},
			wantStatus: http.StatusOK,
			wantCWLLen: 1,
		},
		{
			name: "app_not_found",
			body: map[string]any{
				"ApplicationName": "missing-app",
				"CloudWatchLoggingOption": map[string]any{
					"LogStreamARN": "arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s",
				},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_cwl_option",
			body: map[string]any{
				"ApplicationName": "cwl-app",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doKAV2Request(t, h, "AddApplicationCloudWatchLoggingOption", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				descriptions, ok := out["CloudWatchLoggingOptionDescriptions"].([]any)
				require.True(t, ok)
				assert.Len(t, descriptions, tt.wantCWLLen)
			}
		})
	}
}

func TestKAV2_AddApplicationInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *kinesisanalyticsv2.Handler)
		body       map[string]any
		name       string
		wantInputs int
		wantStatus int
	}{
		{
			name: "success_kinesis_streams",
			setup: func(h *kinesisanalyticsv2.Handler) {
				doKAV2Request(t, h, "CreateApplication", map[string]any{
					"ApplicationName":    "input-app",
					"RuntimeEnvironment": "SQL-1_0",
				})
			},
			body: map[string]any{
				"ApplicationName": "input-app",
				"Input": map[string]any{
					"NamePrefix": "SOURCE_SQL_STREAM",
					"KinesisStreamsInput": map[string]any{
						"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
					},
				},
			},
			wantStatus: http.StatusOK,
			wantInputs: 1,
		},
		{
			name: "app_not_found",
			body: map[string]any{
				"ApplicationName": "missing-app",
				"Input": map[string]any{
					"NamePrefix": "PREFIX",
				},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_input",
			body: map[string]any{
				"ApplicationName": "input-app",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doKAV2Request(t, h, "AddApplicationInput", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				inputs, ok := out["InputDescriptions"].([]any)
				require.True(t, ok)
				assert.Len(t, inputs, tt.wantInputs)
			}
		})
	}
}

// TestKAV2_AddApplicationInput_MissingInput verifies the standalone
// (non-table-row) not-yet-created-app variant of the "missing Input field"
// validation error, exercised directly against a fresh handler.
func TestKAV2_AddApplicationInput_MissingInput(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "input-missing-app",
		"RuntimeEnvironment": "SQL-1_0",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "AddApplicationInput", map[string]any{
		"ApplicationName": "input-missing-app",
		// Input field omitted
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestKAV2_AddApplicationInputProcessingConfiguration(t *testing.T) {
	t.Parallel()

	// Create an app and add an input, return the input ID.
	setupInputApp := func(h *kinesisanalyticsv2.Handler, appName string) string {
		doKAV2Request(t, h, "CreateApplication", map[string]any{
			"ApplicationName":    appName,
			"RuntimeEnvironment": "SQL-1_0",
		})
		rec := doKAV2Request(t, h, "AddApplicationInput", map[string]any{
			"ApplicationName": appName,
			"Input": map[string]any{
				"NamePrefix": "SOURCE_SQL_STREAM",
				"KinesisStreamsInput": map[string]any{
					"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
				},
			},
		})

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		inputs := out["InputDescriptions"].([]any)
		input := inputs[0].(map[string]any)

		return input["InputId"].(string)
	}

	tests := []struct {
		appName    string
		inputID    string
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			appName:    "proc-app",
			wantStatus: http.StatusOK,
		},
		{
			name:       "input_not_found",
			appName:    "proc-app2",
			inputID:    "nonexistent-input",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "app_not_found",
			appName:    "missing-app",
			inputID:    "any-id",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			inputID := tt.inputID

			switch tt.name {
			case "success":
				inputID = setupInputApp(h, tt.appName)
			case "input_not_found":
				doKAV2Request(t, h, "CreateApplication", map[string]any{
					"ApplicationName":    tt.appName,
					"RuntimeEnvironment": "SQL-1_0",
				})
			}

			rec := doKAV2Request(t, h, "AddApplicationInputProcessingConfiguration", map[string]any{
				"ApplicationName": tt.appName,
				"InputId":         inputID,
				"InputProcessingConfiguration": map[string]any{
					"InputLambdaProcessor": map[string]any{
						"ResourceARN": "arn:aws:lambda:us-east-1:000000000000:function:my-fn",
					},
				},
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestKAV2_AddApplicationOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(h *kinesisanalyticsv2.Handler)
		body        map[string]any
		name        string
		wantOutputs int
		wantStatus  int
	}{
		{
			name: "success_kinesis_streams",
			setup: func(h *kinesisanalyticsv2.Handler) {
				doKAV2Request(t, h, "CreateApplication", map[string]any{
					"ApplicationName":    "output-app",
					"RuntimeEnvironment": "SQL-1_0",
				})
			},
			body: map[string]any{
				"ApplicationName": "output-app",
				"Output": map[string]any{
					"Name": "DESTINATION_STREAM",
					"KinesisStreamsOutput": map[string]any{
						"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
					},
					"DestinationSchema": map[string]any{
						"RecordFormatType": "JSON",
					},
				},
			},
			wantStatus:  http.StatusOK,
			wantOutputs: 1,
		},
		{
			name: "app_not_found",
			body: map[string]any{
				"ApplicationName": "missing-app",
				"Output": map[string]any{
					"Name": "output",
				},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_output",
			body: map[string]any{
				"ApplicationName": "output-app",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doKAV2Request(t, h, "AddApplicationOutput", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				outputs, ok := out["OutputDescriptions"].([]any)
				require.True(t, ok)
				assert.Len(t, outputs, tt.wantOutputs)
			}
		})
	}
}

// TestKAV2_AddApplicationOutput_MissingOutput verifies the standalone
// missing-Output-field validation error.
func TestKAV2_AddApplicationOutput_MissingOutput(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "output-missing-app",
		"RuntimeEnvironment": "SQL-1_0",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "AddApplicationOutput", map[string]any{
		"ApplicationName": "output-missing-app",
		// Output field omitted
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestKAV2_AddApplicationReferenceDataSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *kinesisanalyticsv2.Handler)
		body       map[string]any
		name       string
		wantRefs   int
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *kinesisanalyticsv2.Handler) {
				doKAV2Request(t, h, "CreateApplication", map[string]any{
					"ApplicationName":    "ref-app",
					"RuntimeEnvironment": "SQL-1_0",
				})
			},
			body: map[string]any{
				"ApplicationName": "ref-app",
				"ReferenceDataSource": map[string]any{
					"TableName": "REFERENCE_TABLE",
					"S3ReferenceDataSource": map[string]any{
						"BucketARN": "arn:aws:s3:::my-bucket",
						"FileKey":   "data/reference.csv",
					},
				},
			},
			wantStatus: http.StatusOK,
			wantRefs:   1,
		},
		{
			name: "app_not_found",
			body: map[string]any{
				"ApplicationName": "missing-app",
				"ReferenceDataSource": map[string]any{
					"TableName": "TABLE",
				},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_ref_source",
			body: map[string]any{
				"ApplicationName": "ref-app",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doKAV2Request(t, h, "AddApplicationReferenceDataSource", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				refs, ok := out["ReferenceDataSourceDescriptions"].([]any)
				require.True(t, ok)
				assert.Len(t, refs, tt.wantRefs)
			}
		})
	}
}

// TestKAV2_AddApplicationRefDataSource_MissingSource verifies the standalone
// missing-ReferenceDataSource-field validation error.
func TestKAV2_AddApplicationRefDataSource_MissingSource(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "ref-missing-app",
		"RuntimeEnvironment": "SQL-1_0",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "AddApplicationReferenceDataSource", map[string]any{
		"ApplicationName": "ref-missing-app",
		// ReferenceDataSource field omitted
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestKAV2_AddApplicationVpcConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *kinesisanalyticsv2.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *kinesisanalyticsv2.Handler) {
				doKAV2Request(t, h, "CreateApplication", map[string]any{
					"ApplicationName":    "vpc-app",
					"RuntimeEnvironment": "FLINK-1_18",
				})
			},
			body: map[string]any{
				"ApplicationName": "vpc-app",
				"VpcConfiguration": map[string]any{
					"SubnetIds":        []string{"subnet-abc123", "subnet-def456"},
					"SecurityGroupIds": []string{"sg-abc123"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "app_not_found",
			body: map[string]any{
				"ApplicationName": "missing-app",
				"VpcConfiguration": map[string]any{
					"SubnetIds":        []string{"subnet-abc123"},
					"SecurityGroupIds": []string{"sg-abc123"},
				},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_vpc_config",
			body: map[string]any{
				"ApplicationName": "vpc-app",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doKAV2Request(t, h, "AddApplicationVpcConfiguration", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotNil(t, out["VpcConfigurationDescription"])
				vpcDesc, ok := out["VpcConfigurationDescription"].(map[string]any)
				require.True(t, ok)
				assert.NotEmpty(t, vpcDesc["VpcConfigurationId"])
			}
		})
	}
}

// TestKAV2_VpcConfiguration_NonNilSlices verifies SubnetIds/SecurityGroupIds
// come back as empty (non-nil) arrays, never JSON null, when omitted from
// the request.
func TestKAV2_VpcConfiguration_NonNilSlices(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "vpc-nil-app",
		"RuntimeEnvironment": "FLINK-1_18",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "AddApplicationVpcConfiguration", map[string]any{
		"ApplicationName":  "vpc-nil-app",
		"VpcConfiguration": map[string]any{
			// SubnetIds and SecurityGroupIds omitted (nil equivalent)
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	vpcDesc, ok := out["VpcConfigurationDescription"].(map[string]any)
	require.True(t, ok)

	subnets, ok := vpcDesc["SubnetIds"].([]any)
	require.True(t, ok)
	assert.NotNil(t, subnets)

	sgs, ok := vpcDesc["SecurityGroupIds"].([]any)
	require.True(t, ok)
	assert.NotNil(t, sgs)
}

// TestKAV2_AddOps_VersionBump verifies every Add* config op bumps
// ApplicationVersionId by exactly 1.
func TestKAV2_AddOps_VersionBump(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupAdd func(h *kinesisanalyticsv2.Handler, appName string) []byte
		name     string
	}{
		{
			name: "AddApplicationCloudWatchLoggingOption",
			setupAdd: func(h *kinesisanalyticsv2.Handler, appName string) []byte {
				rec := doKAV2Request(t, h, "AddApplicationCloudWatchLoggingOption", map[string]any{
					"ApplicationName": appName,
					"CloudWatchLoggingOption": map[string]any{
						"LogStreamARN": "arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s",
					},
				})

				return rec.Body.Bytes()
			},
		},
		{
			name: "AddApplicationVpcConfiguration",
			setupAdd: func(h *kinesisanalyticsv2.Handler, appName string) []byte {
				rec := doKAV2Request(t, h, "AddApplicationVpcConfiguration", map[string]any{
					"ApplicationName": appName,
					"VpcConfiguration": map[string]any{
						"SubnetIds":        []string{"subnet-1"},
						"SecurityGroupIds": []string{"sg-1"},
					},
				})

				return rec.Body.Bytes()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)
			appName := "version-bump-app-" + tt.name

			doKAV2Request(t, h, "CreateApplication", map[string]any{
				"ApplicationName":    appName,
				"RuntimeEnvironment": "FLINK-1_18",
			})

			respBody := tt.setupAdd(h, appName)

			var out map[string]any
			require.NoError(t, json.Unmarshal(respBody, &out))
			versionID, ok := out["ApplicationVersionId"].(float64)
			require.True(t, ok)
			assert.InEpsilon(t, 2.0, versionID, 1e-9)
		})
	}
}

// TestKAV2_ConcurrentModification_Returns400 verifies a stale
// CurrentApplicationVersionId on a config Add* op surfaces as HTTP 400 with
// __type ConcurrentModificationException (not the generic
// InvalidArgumentException) -- aws-sdk-go-v2 switches on __type to build
// *types.ConcurrentModificationException for caller retry logic.
func TestKAV2_ConcurrentModification_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "ver-app-http",
		"RuntimeEnvironment": "FLINK-1_18",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "AddApplicationCloudWatchLoggingOption", map[string]any{
		"ApplicationName":             "ver-app-http",
		"CurrentApplicationVersionId": 99,
		"CloudWatchLoggingOption": map[string]any{
			"LogStreamARN": "arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s",
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assertErrorType(t, rec.Body.Bytes(), "ConcurrentModificationException")
}

// TestKAV2_GetSupportedOperations_IncludesConfigOps verifies every
// Add*/Delete* config operation is present in GetSupportedOperations().
func TestKAV2_GetSupportedOperations_IncludesConfigOps(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)
	ops := h.GetSupportedOperations()

	configOps := []string{
		"AddApplicationCloudWatchLoggingOption",
		"AddApplicationInput",
		"AddApplicationInputProcessingConfiguration",
		"AddApplicationOutput",
		"AddApplicationReferenceDataSource",
		"AddApplicationVpcConfiguration",
		"DeleteApplicationCloudWatchLoggingOption",
		"DeleteApplicationInputProcessingConfiguration",
		"DeleteApplicationOutput",
	}

	for _, op := range configOps {
		assert.Contains(t, ops, op, "expected %q in GetSupportedOperations", op)
	}
}
