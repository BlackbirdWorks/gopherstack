package lambda_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// TestSnapStart_InvalidApplyOnRejected verifies CreateFunction rejects an
// out-of-enum SnapStart.ApplyOn value with InvalidParameterValueException, as
// AWS does, and accepts the valid enum values.
func TestSnapStart_InvalidApplyOnRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		applyOn    string
		wantStatus int
	}{
		{name: "invalid_value_rejected", applyOn: "Always", wantStatus: http.StatusBadRequest},
		{name: "lowercase_rejected", applyOn: "publishedversions", wantStatus: http.StatusBadRequest},
		{name: "published_versions_ok", applyOn: "PublishedVersions", wantStatus: http.StatusCreated},
		{name: "none_ok", applyOn: "None", wantStatus: http.StatusCreated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			body := `{"FunctionName":"snap-val-fn","PackageType":"Image","Code":{"ImageUri":"x"},` +
				`"Role":"arn:aws:iam:::role/r","SnapStart":{"ApplyOn":"` + tt.applyOn + `"}}`
			rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantStatus == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "InvalidParameterValueException")
			}
		})
	}
}

// TestSnapStart_ReportedOnPublishedVersion verifies that a published version
// carries the SnapStart configuration in its response.
func TestSnapStart_ReportedOnPublishedVersion(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{"FunctionName":"snap-pub-fn","PackageType":"Image","Code":{"ImageUri":"x"},` +
		`"Role":"arn:aws:iam:::role/r","SnapStart":{"ApplyOn":"PublishedVersions"}}`
	create := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, create.Code)

	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/snap-pub-fn/versions", `{"Description":"v1"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var ver lambda.FunctionVersion
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&ver))
	require.NotNil(t, ver.SnapStart)
	assert.Equal(t, "PublishedVersions", ver.SnapStart.ApplyOn)
	assert.Equal(t, "On", ver.SnapStart.OptimizationStatus)
}

// TestSnapStart_OmittedWhenUnset verifies a function created without SnapStart
// reports no SnapStart on its published version (field omitted).
func TestSnapStart_OmittedWhenUnset(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{"FunctionName":"snap-unset-fn","PackageType":"Image","Code":{"ImageUri":"x"},` +
		`"Role":"arn:aws:iam:::role/r"}`
	create := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, create.Code)

	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/snap-unset-fn/versions", "")
	require.Equal(t, http.StatusCreated, rec.Code)

	var ver lambda.FunctionVersion
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&ver))
	assert.Nil(t, ver.SnapStart)
}
