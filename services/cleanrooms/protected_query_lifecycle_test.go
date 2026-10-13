package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cleanrooms"
)

func queryStatus(t *testing.T, e *echo.Echo, memID, id string) string {
	t.Helper()

	rec := doRequest(t, e, http.MethodGet, "/memberships/"+memID+"/protectedQueries/"+id, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["protectedQuery"]["status"].(string)
}

func TestProtectedQuery_StatusProgression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		want    string
		delay   time.Duration
		elapsed time.Duration
	}{
		{name: "default", want: "SUCCESS"},
		{name: "submitted", delay: time.Hour, elapsed: time.Minute, want: "SUBMITTED"},
		{name: "started", delay: time.Hour, elapsed: 40 * time.Minute, want: "STARTED"},
		{name: "success", delay: time.Hour, elapsed: 2 * time.Hour, want: "SUCCESS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				backend := cleanrooms.NewInMemoryBackend("123456789012", "us-east-1")
				backend.SetProtectedWorkDelay(tt.delay)
				e := echo.New()
				e.Any("/*", cleanrooms.NewHandler(backend).Handler())
				_, memID := setupTestEnvironment(t, e)

				id := createProtectedQuery(t, e, memID)
				time.Sleep(tt.elapsed)
				assert.Equal(t, tt.want, queryStatus(t, e, memID, id))

				rec := doRequest(t, e, http.MethodPatch, "/memberships/"+memID+"/protectedQueries/"+id,
					map[string]any{"targetStatus": "CANCELLED"})
				if tt.want == "SUCCESS" {
					assert.Equal(t, http.StatusConflict, rec.Code)
				} else {
					assert.Equal(t, http.StatusOK, rec.Code)
					assert.Equal(t, "CANCELLED", queryStatus(t, e, memID, id))
				}
			})
		})
	}
}

func TestStartProtectedQuery_InputValidation(t *testing.T) {
	t.Parallel()

	s3 := func(format, bucket string) map[string]any {
		return map[string]any{"outputConfiguration": map[string]any{
			"s3": map[string]any{"resultFormat": format, "bucket": bucket},
		}}
	}

	tests := []struct {
		body map[string]any
		name string
		ok   bool
	}{
		{name: "query", ok: true, body: map[string]any{"sqlParameters": map[string]any{"queryString": "select 1"}}},
		{name: "template", ok: true, body: map[string]any{"sqlParameters": map[string]any{
			"analysisTemplateArn": "arn:aws:cleanrooms:us-east-1:123456789012:membership/m-1/analysistemplate/t-1",
			"parameters":          map[string]any{"a": "b"},
		}}},
		{name: "empty params", body: map[string]any{"sqlParameters": map[string]any{}}},
		{name: "empty query", body: map[string]any{"sqlParameters": map[string]any{"queryString": ""}}},
		{name: "query too long", body: map[string]any{"sqlParameters": map[string]any{
			"queryString": strings.Repeat("a", 500001),
		}}},
		{name: "bad template arn", body: map[string]any{"sqlParameters": map[string]any{"analysisTemplateArn": "x"}}},
		{name: "bad format", body: map[string]any{
			"sqlParameters": map[string]any{"queryString": "select 1"}, "resultConfiguration": s3("XML", "bkt"),
		}},
		{name: "short bucket", body: map[string]any{
			"sqlParameters": map[string]any{"queryString": "select 1"}, "resultConfiguration": s3("CSV", "b"),
		}},
		{name: "good s3", ok: true, body: map[string]any{
			"sqlParameters": map[string]any{"queryString": "select 1"}, "resultConfiguration": s3("PARQUET", "bkt"),
		}},
		{name: "bad type", body: map[string]any{
			"type": "PYTHON", "sqlParameters": map[string]any{"queryString": "select 1"},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newTestServer(t)
			_, memID := setupTestEnvironment(t, e)
			rec := doRequest(t, e, http.MethodPost, "/memberships/"+memID+"/protectedQueries", tt.body)

			if tt.ok {
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				return
			}

			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			assert.NotContains(t, rec.Body.String(), "ValidationException:")
		})
	}
}

func TestStartProtectedQuery_KeepsSQLParameters(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	_, memID := setupTestEnvironment(t, e)
	tmpl := "arn:aws:cleanrooms:us-east-1:123456789012:membership/m-1/analysistemplate/t-1"

	rec := doRequest(t, e, http.MethodPost, "/memberships/"+memID+"/protectedQueries", map[string]any{
		"sqlParameters": map[string]any{"analysisTemplateArn": tmpl, "parameters": map[string]any{"k": "v"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	got := resp["protectedQuery"]["sqlParameters"].(map[string]any)
	assert.Equal(t, tmpl, got["analysisTemplateArn"])
	assert.Equal(t, map[string]any{"k": "v"}, got["parameters"])
}

func TestUpdateProtectedQuery_TargetStatus(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	_, memID := setupTestEnvironment(t, e)
	id := createProtectedQuery(t, e, memID)

	rec := doRequest(t, e, http.MethodPatch, "/memberships/"+memID+"/protectedQueries/"+id,
		map[string]any{"targetStatus": "SUCCESS"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateConfiguredTable_AnalysisMethodEnum(t *testing.T) {
	t.Parallel()

	tests := []struct {
		method string
		ok     bool
	}{
		{method: "DIRECT_QUERY", ok: true},
		{method: "DIRECT_JOB", ok: true},
		{method: "BOGUS"},
	}

	for _, tt := range tests {
		t.Run(strings.ToLower(tt.method), func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, newTestServer(t), http.MethodPost, "/configuredTables", map[string]any{
				"name":           "ct",
				"analysisMethod": tt.method,
				"allowedColumns": []string{"a"},
				"tableReference": map[string]any{"glue": map[string]any{"tableName": "t", "databaseName": "d"}},
			})

			if tt.ok {
				assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				return
			}

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestErrorMessages_NoCodePrefix(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestServer(t), http.MethodGet, "/collaborations/11111111-1111-1111-1111-111111111111", nil)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.NotContains(t, rec.Body.String(), "ValidationException:")
	assert.Contains(t, rec.Body.String(), "collaboration not found")
}
