package lambda_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateFunction_TenancyAndCapacityProviderConfig(t *testing.T) {
	t.Parallel()

	const cpARN = "arn:aws:lambda:us-east-1:000000000000:capacity-provider:cp1"

	tests := []struct {
		name     string
		extra    string
		wantCode int
	}{
		{name: "both", extra: `,"TenancyConfig":{"TenantIsolationMode":"PER_TENANT"},` +
			`"CapacityProviderConfig":{"LambdaManagedInstancesCapacityProviderConfig":` +
			`{"CapacityProviderArn":"` + cpARN + `","PerExecutionEnvironmentMaxConcurrency":4}}`, wantCode: http.StatusCreated},
		{
			name:     "bad_tenancy_mode",
			extra:    `,"TenancyConfig":{"TenantIsolationMode":"SHARED"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "capacity_without_arn",
			extra:    `,"CapacityProviderConfig":{"LambdaManagedInstancesCapacityProviderConfig":{}}`,
			wantCode: http.StatusBadRequest,
		},
		{name: "capacity_empty", extra: `,"CapacityProviderConfig":{}`, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)

			body := `{"FunctionName":"tenant-fn","PackageType":"Image","Role":"arn:aws:iam::000000000000:role/r",` +
				`"Code":{"ImageUri":"x"}` + tt.extra + `}`
			rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
			require.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

			if tt.wantCode != http.StatusCreated {
				return
			}

			var created map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
			assert.Equal(t, "PER_TENANT", created["TenancyConfig"].(map[string]any)["TenantIsolationMode"])

			rec = callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/tenant-fn/versions", `{}`)
			require.Equal(t, http.StatusCreated, rec.Code)

			rec = callInMemoryHandler(
				t,
				h,
				http.MethodGet,
				"/2015-03-31/functions/tenant-fn/configuration?Qualifier=1",
				"",
			)
			require.Equal(t, http.StatusOK, rec.Code)

			var got map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))

			cp, _ := got["CapacityProviderConfig"].(map[string]any)
			require.NotNil(t, cp, "published version keeps the capacity provider config")

			inner, _ := cp["LambdaManagedInstancesCapacityProviderConfig"].(map[string]any)
			assert.Equal(t, cpARN, inner["CapacityProviderArn"])
			assert.InDelta(t, 4, inner["PerExecutionEnvironmentMaxConcurrency"], 0)
			assert.Equal(t, "PER_TENANT", got["TenancyConfig"].(map[string]any)["TenantIsolationMode"])
		})
	}
}

func TestUpdateFunctionConfiguration_CapacityProviderConfig(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)
	createFunctionForTest(t, h, "cap-upd-fn")

	rec := callInMemoryHandler(t, h, http.MethodPut, "/2015-03-31/functions/cap-upd-fn/configuration",
		`{"CapacityProviderConfig":{"LambdaManagedInstancesCapacityProviderConfig":{}}}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec = callInMemoryHandler(t, h, http.MethodPut, "/2015-03-31/functions/cap-upd-fn/configuration",
		`{"CapacityProviderConfig":{"LambdaManagedInstancesCapacityProviderConfig":`+
			`{"CapacityProviderArn":"arn:aws:lambda:us-east-1:000000000000:capacity-provider:cp2"}}}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	fn, err := bk.GetFunction("cap-upd-fn")
	require.NoError(t, err)
	require.NotNil(t, fn.CapacityProviderConfig)
	assert.Contains(
		t,
		fn.CapacityProviderConfig.ManagedInstances.CapacityProviderArn,
		"cp2",
	)
}
