package lambda_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// TestBatch2Audit_UpdateFunctionCode_Publish verifies that UpdateFunctionCode with
// Publish=true publishes a new numbered version after updating the code, matching
// AWS Lambda behaviour. Previously the Publish field was absent from the input
// struct and the version was never published.
func TestBatch2Audit_UpdateFunctionCode_Publish(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantVersion   string
		publish       bool
		wantVersionIn bool
	}{
		{
			name:          "publish_true_returns_version_number",
			publish:       true,
			wantVersion:   "1",
			wantVersionIn: true,
		},
		{
			name:          "publish_false_no_version",
			publish:       false,
			wantVersion:   "",
			wantVersionIn: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			createFunctionForTest(t, h, "upd-code-pub-fn")

			publishStr := "false"
			if tt.publish {
				publishStr = "true"
			}

			body := fmt.Sprintf(
				`{"ImageUri":"ecr/new:v2","Publish":%s}`,
				publishStr,
			)
			rec := callInMemoryHandler(t, h, http.MethodPut,
				"/2015-03-31/functions/upd-code-pub-fn/code", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var fn lambda.FunctionConfiguration
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))

			if tt.wantVersionIn {
				assert.Equal(t, tt.wantVersion, fn.Version,
					"Version must be set when Publish=true")
			} else {
				assert.Empty(t, fn.Version,
					"Version must be empty when Publish=false")
			}
		})
	}
}

// TestBatch2Audit_UpdateFunctionURLConfig_CORS verifies that UpdateFunctionUrlConfig
// updates CORS fields, matching AWS Lambda behaviour. Previously only AuthType was
// passed to the backend; the Cors field was silently dropped.
func TestBatch2Audit_UpdateFunctionURLConfig_CORS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		body            string
		wantAllowOrigin string
		wantAuthType    string
	}{
		{
			name:            "cors_origins_updated",
			body:            `{"AuthType":"NONE","Cors":{"AllowOrigins":["https://example.com"],"AllowMethods":["GET","POST"]}}`,
			wantAllowOrigin: "https://example.com",
			wantAuthType:    "NONE",
		},
		{
			name:            "cors_cleared_when_empty_cors",
			body:            `{"AuthType":"AWS_IAM"}`,
			wantAllowOrigin: "",
			wantAuthType:    "AWS_IAM",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			createFunctionForTest(t, h, "url-cors-fn")

			// Create URL config first.
			createBody := `{"AuthType":"NONE"}`
			rec := callInMemoryHandler(t, h, http.MethodPost,
				"/2021-10-31/functions/url-cors-fn/url", createBody)
			require.Equal(t, http.StatusCreated, rec.Code)

			// Update URL config with the test body.
			rec2 := callInMemoryHandler(t, h, http.MethodPut,
				"/2021-10-31/functions/url-cors-fn/url", tt.body)
			require.Equal(t, http.StatusOK, rec2.Code)

			var cfg lambda.FunctionURLConfig
			require.NoError(t, json.NewDecoder(rec2.Body).Decode(&cfg))

			assert.Equal(t, tt.wantAuthType, cfg.AuthType)

			if tt.wantAllowOrigin != "" {
				require.NotNil(t, cfg.Cors, "Cors must be present when origins are set")
				require.NotEmpty(t, cfg.Cors.AllowOrigins)
				assert.Equal(t, tt.wantAllowOrigin, cfg.Cors.AllowOrigins[0],
					"first AllowOrigin must match what was sent")
			}
		})
	}
}

// TestBatch2Audit_CreateFunction_Tags verifies that tags supplied in CreateFunction
// are returned in the function configuration, matching AWS Lambda behaviour.
// Previously CreateFunctionInput lacked a Tags field and any tags were silently dropped.
func TestBatch2Audit_CreateFunction_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		wantTags map[string]string
		name     string
	}{
		{
			name:     "tags_set_at_creation",
			tags:     map[string]string{"env": "prod", "team": "platform"},
			wantTags: map[string]string{"env": "prod", "team": "platform"},
		},
		{
			name:     "no_tags_returns_empty_map",
			tags:     nil,
			wantTags: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)

			var tagsJSON string
			if tt.tags != nil {
				tagsJSON = `,"Tags":{"env":"prod","team":"platform"}`
			}

			const tagFnBase = `{"FunctionName":"tag-create-fn","PackageType":"Image",` +
				`"Code":{"ImageUri":"ecr/x:latest"},"Role":"arn:aws:iam:::role/r"`
			body := fmt.Sprintf("%s%s}", tagFnBase, tagsJSON)
			rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
			require.Equal(t, http.StatusCreated, rec.Code)

			var fn lambda.FunctionConfiguration
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))

			for k, v := range tt.wantTags {
				assert.Equal(t, v, fn.Tags[k],
					"tag %q must be present with value %q", k, v)
			}
		})
	}
}

// TestBatch2Audit_CreateFunction_MemorySizeNotDivisibleBy64 verifies that CreateFunction
// returns HTTP 400 InvalidParameterValueException when MemorySize is not divisible by 64,
// matching AWS Lambda behaviour. Previously the backend returned ErrInvalidParameterValue
// but the handler mapped it to HTTP 500 ServiceException.
func TestBatch2Audit_CreateFunction_MemorySizeNotDivisibleBy64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		memorySize int
		wantStatus int
	}{
		{
			name:       "130_not_divisible_by_64_returns_400",
			memorySize: 130,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "192_divisible_by_64_returns_201",
			memorySize: 192,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "256_divisible_by_64_returns_201",
			memorySize: 256,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "500_not_divisible_by_64_returns_400",
			memorySize: 500,
			wantStatus: http.StatusBadRequest,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)

			const memFnBase = `{"FunctionName":"mem-div-fn-%d","PackageType":"Image",` +
				`"Code":{"ImageUri":"ecr/x:latest"},"Role":"arn:aws:iam:::role/r","MemorySize":%d}`
			body := fmt.Sprintf(memFnBase, i, tt.memorySize)
			rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
			assert.Equal(t, tt.wantStatus, rec.Code,
				"MemorySize=%d must return HTTP %d", tt.memorySize, tt.wantStatus)

			if tt.wantStatus == http.StatusBadRequest {
				var errResp map[string]string
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
				assert.Equal(t, "InvalidParameterValueException", errResp["__type"],
					"error type must be InvalidParameterValueException, not ServiceException")
			}
		})
	}
}
