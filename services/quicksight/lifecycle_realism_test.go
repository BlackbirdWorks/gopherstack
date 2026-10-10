package quicksight_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

func createSPICEDataSet(t *testing.T, h *quicksight.Handler, id string) {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, accountPath("/data-sets"), map[string]any{
		"DataSetId": id, "Name": id, "ImportMode": "SPICE", "PhysicalTableMap": testPhysicalTableMap(),
	})
	require.Equal(t, http.StatusCreated, rec.Code)
}

func TestIngestion_StatusProgression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		delay      time.Duration
	}{
		{name: "still running", delay: time.Hour, wantStatus: "RUNNING"},
		{name: "settled", delay: 0, wantStatus: "COMPLETED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			b.SetIngestionDelay(tt.delay)
			h := quicksight.NewHandler(b)
			createSPICEDataSet(t, h, "ds")

			created := doRequest(t, h, http.MethodPut, accountPath("/data-sets/ds/ingestions/i1"), map[string]any{})
			require.Equal(t, http.StatusCreated, created.Code)
			assert.Equal(t, "RUNNING", parseBody(t, created)["IngestionStatus"])

			desc := doRequest(t, h, http.MethodGet, accountPath("/data-sets/ds/ingestions/i1"), nil)
			require.Equal(t, http.StatusOK, desc.Code)
			ing, ok := parseBody(t, desc)["Ingestion"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantStatus, ing["IngestionStatus"])

			cancel := doRequest(t, h, http.MethodDelete, accountPath("/data-sets/ds/ingestions/i1"), nil)
			if tt.wantStatus == "COMPLETED" {
				assert.Equal(t, http.StatusConflict, cancel.Code)
			} else {
				assert.Equal(t, http.StatusOK, cancel.Code)
			}
		})
	}
}

func TestIngestion_RequestRealism(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		path     string
		wantType string
		wantCode int
	}{
		{
			name: "bad id", method: http.MethodPut, path: "/data-sets/ds/ingestions/bad%20id",
			wantCode: http.StatusBadRequest, wantType: "InvalidParameterValueException",
		},
		{
			name: "duplicate", method: http.MethodPut, path: "/data-sets/ds/ingestions/dup",
			wantCode: http.StatusConflict, wantType: "ResourceExistsException",
		},
		{
			name: "bad token", method: http.MethodGet, path: "/data-sets/ds/ingestions?next-token=zzz",
			wantCode: http.StatusBadRequest, wantType: "InvalidNextTokenException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createSPICEDataSet(t, h, "ds")
			first := doRequest(t, h, http.MethodPut, accountPath("/data-sets/ds/ingestions/dup"), map[string]any{})
			require.Equal(t, http.StatusCreated, first.Code)

			rec := doRequest(t, h, tt.method, accountPath(tt.path), map[string]any{})
			require.Equal(t, tt.wantCode, rec.Code)

			body := parseBody(t, rec)
			assert.Equal(t, tt.wantType, body["Code"])
			assert.NotEqual(t, tt.wantType, body["Message"])
		})
	}
}

func TestDashboardAndDataSource_CreationStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantDescribe string
		delay        time.Duration
	}{
		{name: "in progress window", delay: time.Hour, wantDescribe: "CREATION_IN_PROGRESS"},
		{name: "settled", delay: 0, wantDescribe: "CREATION_SUCCESSFUL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			b.SetCreationDelay(tt.delay)

			d, err := b.CreateAnalysis(testAccountID, "an", "an", "", nil, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, "CREATION_IN_PROGRESS", d.Status)

			got, err := b.DescribeAnalysis(testAccountID, "an")
			require.NoError(t, err)
			assert.Equal(t, tt.wantDescribe, got.Status)
		})
	}
}
