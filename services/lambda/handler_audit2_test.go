package lambda_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// TestAudit2_Version_AlwaysLatestInCreateFunction verifies that CreateFunction always
// returns "Version": "$LATEST" in the response, matching AWS Lambda behaviour.
// Previously Version was absent when Publish was not set.
func TestAudit2_Version_AlwaysLatestInCreateFunction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		body        string
		wantVersion string
	}{
		{
			name:        "no_publish_returns_latest",
			body:        `{"FunctionName":"audit2-create-fn","PackageType":"Image","Code":{"ImageUri":"x"},"Role":"arn"}`,
			wantVersion: "$LATEST",
		},
		{
			name: "with_publish_returns_numbered_version",
			body: `{"FunctionName":"audit2-create-pub-fn","PackageType":"Image",` +
				`"Code":{"ImageUri":"x"},"Role":"arn","Publish":true}`,
			wantVersion: "1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", tt.body)
			require.Equal(t, http.StatusCreated, rec.Code)

			var fn lambda.FunctionConfiguration
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
			assert.Equal(t, tt.wantVersion, fn.Version,
				"CreateFunction response must include Version=%q", tt.wantVersion)
		})
	}
}

// TestAudit2_Version_AlwaysLatestInGetFunctionConfiguration verifies that
// GetFunctionConfiguration always returns "Version": "$LATEST" for the live code,
// matching AWS Lambda behaviour. Previously Version was absent from the response.
func TestAudit2_Version_AlwaysLatestInGetFunctionConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "fresh_function"},
		{name: "after_config_update"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			createFunctionForTest(t, h, "audit2-getcfg-fn")

			if tt.name == "after_config_update" {
				rec := callInMemoryHandler(t, h, http.MethodPut,
					"/2015-03-31/functions/audit2-getcfg-fn/configuration",
					`{"Description":"updated"}`)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := callInMemoryHandler(t, h, http.MethodGet,
				"/2015-03-31/functions/audit2-getcfg-fn/configuration", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var fn lambda.FunctionConfiguration
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
			assert.Equal(t, "$LATEST", fn.Version,
				"GetFunctionConfiguration must always return Version=$LATEST for live code")
		})
	}
}

// TestAudit2_Version_LiveFunctionStaysLatestAfterPublish verifies that after publishing
// a numbered version (via PublishVersion or UpdateFunctionCode with Publish=true), the
// live function's GetFunctionConfiguration still returns "Version": "$LATEST".
// Previously maybePublishVersion mutated the stored live fn.Version to the numbered
// version, causing subsequent GetFunctionConfiguration to return "1" instead of "$LATEST".
func TestAudit2_Version_LiveFunctionStaysLatestAfterPublish(t *testing.T) {
	t.Parallel()

	tests := []struct {
		publish func(t *testing.T, h *lambda.Handler)
		name    string
	}{
		{
			name: "after_explicit_publish_version",
			publish: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				rec := callInMemoryHandler(t, h, http.MethodPost,
					"/2015-03-31/functions/audit2-live-fn/versions", `{"Description":"v1"}`)
				require.Equal(t, http.StatusCreated, rec.Code)
			},
		},
		{
			name: "after_update_function_code_publish_true",
			publish: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				rec := callInMemoryHandler(t, h, http.MethodPut,
					"/2015-03-31/functions/audit2-live-fn/code",
					`{"ImageUri":"x:v2","Publish":true}`)
				require.Equal(t, http.StatusOK, rec.Code)
				var upd lambda.FunctionConfiguration
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&upd))
				assert.Equal(t, "1", upd.Version,
					"UpdateFunctionCode Publish=true response must show numbered version")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			createFunctionForTest(t, h, "audit2-live-fn")

			tt.publish(t, h)

			rec := callInMemoryHandler(t, h, http.MethodGet,
				"/2015-03-31/functions/audit2-live-fn/configuration", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var fn lambda.FunctionConfiguration
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
			assert.Equal(t, "$LATEST", fn.Version,
				"GetFunctionConfiguration after publish must still return Version=$LATEST")
		})
	}
}
