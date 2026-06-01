package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandlerJobRuns_StartJobRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		jobName  string
		wantCode int
		wantErr  bool
	}{
		{
			name:     "success",
			jobName:  "my-job",
			wantCode: http.StatusOK,
		},
		{
			name:     "unknown_job",
			jobName:  "no-such-job",
			wantCode: http.StatusBadRequest,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.name == "success" {
				doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
				doGlueRequest(
					t,
					h,
					"CreateJob",
					map[string]any{
						"Name":    "my-job",
						"Role":    "arn:aws:iam::000000000000:role/r",
						"Command": map[string]any{"Name": "glueetl"},
					},
				)
			}

			rec := doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": tt.jobName})

			assert.Equal(t, tt.wantCode, rec.Code)
			if !tt.wantErr {
				var out map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out["JobRunId"])
			}
		})
	}
}

func TestHandlerJobRuns_GetJobRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		runID    string
		wantCode int
		wantErr  bool
	}{
		{
			name:     "success",
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			runID:    "no-such-run",
			wantCode: http.StatusBadRequest,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
			doGlueRequest(
				t,
				h,
				"CreateJob",
				map[string]any{
					"Name":    "my-job",
					"Role":    "arn:aws:iam::000000000000:role/r",
					"Command": map[string]any{"Name": "glueetl"},
				},
			)

			runID := tt.runID
			if runID == "" {
				rec := doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "my-job"})
				require.Equal(t, http.StatusOK, rec.Code)
				var out map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				runID = out["JobRunId"]
			}

			rec := doGlueRequest(t, h, "GetJobRun", map[string]any{"JobName": "my-job", "RunId": runID})

			assert.Equal(t, tt.wantCode, rec.Code)
			if !tt.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotNil(t, out["JobRun"])
			}
		})
	}
}

func TestHandlerJobRuns_GetJobRuns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		numRuns  int
		wantCode int
	}{
		{
			name:     "multiple_runs",
			numRuns:  3,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
			doGlueRequest(
				t,
				h,
				"CreateJob",
				map[string]any{
					"Name":    "my-job",
					"Role":    "arn:aws:iam::000000000000:role/r",
					"Command": map[string]any{"Name": "glueetl"},
				},
			)

			for range tt.numRuns {
				doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "my-job"})
			}

			rec := doGlueRequest(t, h, "GetJobRuns", map[string]any{"JobName": "my-job"})

			require.Equal(t, tt.wantCode, rec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			runs, ok := out["JobRuns"].([]any)
			require.True(t, ok)
			assert.Len(t, runs, tt.numRuns)
		})
	}
}

func TestHandlerJobRuns_BatchStopJobRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stopBogus  bool
		wantCode   int
		wantErrors int
	}{
		{
			name:       "success",
			wantCode:   http.StatusOK,
			wantErrors: 0,
		},
		{
			name:       "partial_errors",
			stopBogus:  true,
			wantCode:   http.StatusOK,
			wantErrors: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
			doGlueRequest(
				t,
				h,
				"CreateJob",
				map[string]any{
					"Name":    "my-job",
					"Role":    "arn:aws:iam::000000000000:role/r",
					"Command": map[string]any{"Name": "glueetl"},
				},
			)

			startRec := doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "my-job"})
			require.Equal(t, http.StatusOK, startRec.Code)
			var startOut map[string]string
			require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
			realRunID := startOut["JobRunId"]

			stopIDs := []string{realRunID}
			if tt.stopBogus {
				stopIDs = append(stopIDs, "bogus-run-id")
			}

			rec := doGlueRequest(t, h, "BatchStopJobRun", map[string]any{"JobName": "my-job", "JobRunIds": stopIDs})

			require.Equal(t, tt.wantCode, rec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			errs, _ := out["Errors"].([]any)
			assert.Len(t, errs, tt.wantErrors)
		})
	}
}

func TestHandlerJobRuns_GetJobBookmark(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		jobName  string
		wantCode int
	}{
		{
			name:     "success",
			jobName:  "my-job",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
			doGlueRequest(
				t,
				h,
				"CreateJob",
				map[string]any{
					"Name":    "my-job",
					"Role":    "arn:aws:iam::000000000000:role/r",
					"Command": map[string]any{"Name": "glueetl"},
				},
			)

			rec := doGlueRequest(t, h, "GetJobBookmark", map[string]any{"JobName": tt.jobName})

			assert.Equal(t, tt.wantCode, rec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.NotNil(t, out["JobBookmarkEntry"])
		})
	}
}

func TestHandlerJobRuns_ResetJobBookmark(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "success",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
			doGlueRequest(
				t,
				h,
				"CreateJob",
				map[string]any{
					"Name":    "my-job",
					"Role":    "arn:aws:iam::000000000000:role/r",
					"Command": map[string]any{"Name": "glueetl"},
				},
			)

			rec := doGlueRequest(t, h, "ResetJobBookmark", map[string]any{"JobName": "my-job"})

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
