package lambda_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublishVersion_PublishTo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantVer  string
		wantCode int
	}{
		{"latest_published", `{"PublishTo":"LATEST_PUBLISHED"}`, "$LATEST.PUBLISHED", http.StatusCreated},
		{"numbered", `{}`, "1", http.StatusCreated},
		{"bad_value", `{"PublishTo":"OTHER"}`, "", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			create := `{"FunctionName":"pub-fn","PackageType":"Image","Role":"arn:aws:iam::000000000000:role/r",` +
				`"Code":{"ImageUri":"x"}}`
			require.Equal(t, http.StatusCreated,
				callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", create).Code)

			rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/pub-fn/versions", tt.body)
			require.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

			if tt.wantCode != http.StatusCreated {
				return
			}

			var got map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
			assert.Equal(t, tt.wantVer, got["Version"])
			assert.Contains(t, got["FunctionArn"], ":"+tt.wantVer)

			rec = callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/pub-fn/versions", tt.body)
			require.Equal(t, http.StatusCreated, rec.Code)
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))

			if tt.wantVer == "$LATEST.PUBLISHED" {
				assert.Equal(t, tt.wantVer, got["Version"], "republish keeps the single version")
			} else {
				assert.Equal(t, "2", got["Version"])
			}

			rec = callInMemoryHandler(t, h, http.MethodGet,
				"/2015-03-31/functions/pub-fn/configuration?Qualifier="+tt.wantVer, "")
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

func TestInvoke_UnqualifiedResolvesLatestPublished(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		capacity    string
		wantVersion string
	}{
		{"capacity_provider", `,"CapacityProviderConfig":{"LambdaManagedInstancesCapacityProviderConfig":` +
			`{"CapacityProviderArn":"arn:aws:lambda:us-east-1:000000000000:capacity-provider:cp1"}}`, "$LATEST.PUBLISHED"},
		{"plain", "", "$LATEST"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			create := `{"FunctionName":"inv-fn","PackageType":"Image","Role":"arn:aws:iam::000000000000:role/r",` +
				`"Code":{"ImageUri":"x"}` + tt.capacity + `}`
			require.Equal(t, http.StatusCreated,
				callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", create).Code)
			require.Equal(t, http.StatusCreated, callInMemoryHandler(t, h, http.MethodPost,
				"/2015-03-31/functions/inv-fn/versions", `{"PublishTo":"LATEST_PUBLISHED"}`).Code)

			req := httptest.NewRequest(
				http.MethodPost,
				"/2015-03-31/functions/inv-fn/invocations",
				strings.NewReader("{}"),
			)
			req.Header.Set("X-Amz-Invocation-Type", "DryRun")
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
			require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
			assert.Equal(t, tt.wantVersion, rec.Header().Get("X-Amz-Executed-Version"))
		})
	}
}

func TestInvoke_TenantID(t *testing.T) {
	t.Parallel()

	const perTenant = `,"TenancyConfig":{"TenantIsolationMode":"PER_TENANT"}`

	tests := []struct {
		name     string
		tenancy  string
		tenantID string
		want     int
	}{
		{"per_tenant_missing", perTenant, "", http.StatusBadRequest},
		{"per_tenant_present", perTenant, "acme", http.StatusNoContent},
		{"plain_with_tenant", "", "acme", http.StatusBadRequest},
		{"plain_without", "", "", http.StatusNoContent},
		{"bad_pattern", perTenant, "bad#id", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			create := `{"FunctionName":"ten-fn","PackageType":"Image","Role":"arn:aws:iam::000000000000:role/r",` +
				`"Code":{"ImageUri":"x"}` + tt.tenancy + `}`
			require.Equal(t, http.StatusCreated,
				callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", create).Code)

			req := httptest.NewRequest(
				http.MethodPost,
				"/2015-03-31/functions/ten-fn/invocations",
				strings.NewReader("{}"),
			)
			req.Header.Set("X-Amz-Invocation-Type", "DryRun")

			if tt.tenantID != "" {
				req.Header.Set("X-Amz-Tenant-Id", tt.tenantID)
			}

			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
			assert.Equal(t, tt.want, rec.Code, rec.Body.String())
		})
	}
}
