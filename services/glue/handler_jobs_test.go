package glue_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func Test_StartTrigger_OnDemand_StaysCreatedAndFiresJobRun(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreateJob", map[string]any{
		"Name":    "ondemand-job",
		"Role":    "role1",
		"Command": map[string]any{"Name": "glueetl"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "CreateTrigger", map[string]any{
		"Name": "ondemand-trigger",
		"Type": "ON_DEMAND",
		"Actions": []map[string]any{
			{"JobName": "ondemand-job", "Arguments": map[string]any{"--x": "1"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "CREATED", triggerState(t, h, "ondemand-trigger"))

	rec = doGlueRequest(t, h, "StartTrigger", map[string]any{"Name": "ondemand-trigger"})
	require.Equal(t, http.StatusOK, rec.Code)

	// On-demand triggers never enter ACTIVATED — they remain CREATED.
	assert.Equal(t, "CREATED", triggerState(t, h, "ondemand-trigger"))

	// Firing the trigger must have started a run of the job it targets.
	rec = doGlueRequest(t, h, "GetJobRuns", map[string]any{"JobName": "ondemand-job"})
	require.Equal(t, http.StatusOK, rec.Code)

	var runsOut struct {
		JobRuns []struct {
			JobRunState string `json:"JobRunState"`
		} `json:"JobRuns"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &runsOut))
	require.Len(t, runsOut.JobRuns, 1)
	assert.NotEmpty(t, runsOut.JobRuns[0].JobRunState)
}

// Test_ErrValidation_WireCodeIsInvalidInputException verifies that hand-rolled
// validation failures surface as InvalidInputException, not ValidationException.
// aws-sdk-go-v2/service/glue's per-operation error deserializers (e.g.
// awsAwsjson11_deserializeOpErrorCreateDatabase/CreateTrigger/CreateBlueprint)
// list InvalidInputException as the documented hand-validation error; Glue's
// ValidationException type exists but is only declared by a handful of newer
// operations (e.g. DeleteConnectionType), not the general Create* family.
func Test_ErrValidation_WireCodeIsInvalidInputException(t *testing.T) {
	t.Parallel()

	cases := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "CreateDatabase empty name",
			action: "CreateDatabase",
			body:   map[string]any{"DatabaseInput": map[string]any{"Name": ""}},
		},
		{
			name:   "CreateJob missing role",
			action: "CreateJob",
			body: map[string]any{
				"Name":    "job1",
				"Command": map[string]any{"Name": "glueetl"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, tc.action, tc.body)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "InvalidInputException")
			assert.NotContains(t, rec.Body.String(), "\"__type\":\"ValidationException\"")
		})
	}
}

// Test_Job_MaxCapacity verifies that CreateJob/UpdateJob accept and persist
// MaxCapacity (the DPU-based capacity knob used by e.g. Python shell jobs),
// and reject a request that mixes MaxCapacity with WorkerType/NumberOfWorkers
// — AWS's documented mutual-exclusion rule for the two capacity models.
func Test_Job_MaxCapacity(t *testing.T) {
	t.Parallel()

	cases := []struct {
		body   map[string]any
		name   string
		wantOK bool
	}{
		{
			name: "max_capacity_alone_accepted",
			body: map[string]any{
				"Name":        "job-mc",
				"Role":        "role1",
				"Command":     map[string]any{"Name": "pythonshell"},
				"MaxCapacity": 0.0625,
			},
			wantOK: true,
		},
		{
			name: "max_capacity_with_worker_type_rejected",
			body: map[string]any{
				"Name":            "job-mc-conflict",
				"Role":            "role1",
				"Command":         map[string]any{"Name": "glueetl"},
				"MaxCapacity":     2.0,
				"WorkerType":      "G.1X",
				"NumberOfWorkers": 2,
			},
			wantOK: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "CreateJob", tc.body)

			if !tc.wantOK {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "InvalidInputException")

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			rec = doGlueRequest(t, h, "GetJob", map[string]any{"JobName": tc.body["Name"]})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Job struct {
					MaxCapacity float64 `json:"MaxCapacity"`
				} `json:"Job"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.InDelta(t, tc.body["MaxCapacity"], out.Job.MaxCapacity, 0.0001)
		})
	}
}

// TestCreateJob_CommandNameRequired tests that CreateJob requires Command.Name.
func TestCreateJob_CommandNameRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "missing_command_rejected",
			body:     map[string]any{"Name": "j1", "Role": "r"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_command_name_rejected",
			body:     map[string]any{"Name": "j2", "Role": "r", "Command": map[string]any{"Name": ""}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "valid_command_name_accepted",
			body:     map[string]any{"Name": "j3", "Role": "r", "Command": map[string]any{"Name": "glueetl"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "pythonshell_accepted",
			body:     map[string]any{"Name": "j4", "Role": "r", "Command": map[string]any{"Name": "pythonshell"}},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "CreateJob", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestCreateJob_MaxRetries tests MaxRetries bounds validation.
func TestCreateJob_MaxRetries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxRetries int
		wantCode   int
	}{
		{
			name:       "zero_retries_ok",
			maxRetries: 0,
			wantCode:   http.StatusOK,
		},
		{
			name:       "max_retries_ok",
			maxRetries: 10,
			wantCode:   http.StatusOK,
		},
		{
			name:       "eleven_retries_rejected",
			maxRetries: 11,
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "negative_retries_rejected",
			maxRetries: -1,
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "CreateJob", map[string]any{
				"Name":       "retry-job-" + tt.name,
				"Role":       "r",
				"Command":    map[string]any{"Name": "glueetl"},
				"MaxRetries": tt.maxRetries,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestUpdateJob_MaxRetries tests MaxRetries bounds on UpdateJob.
func TestUpdateJob_MaxRetries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxRetries int
		wantCode   int
	}{
		{
			name:       "valid_retries",
			maxRetries: 5,
			wantCode:   http.StatusOK,
		},
		{
			name:       "too_many_retries",
			maxRetries: 11,
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateJob", map[string]any{
				"Name":    "upjob-" + tt.name,
				"Role":    "r",
				"Command": map[string]any{"Name": "glueetl"},
			})

			rec := doGlueRequest(t, h, "UpdateJob", map[string]any{
				"JobName": "upjob-" + tt.name,
				"JobUpdate": map[string]any{
					"Role":       "r",
					"MaxRetries": tt.maxRetries,
				},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestCreateJob_TagValidation tests tag validation on CreateJob.
func TestCreateJob_TagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "valid_tags",
			tags:     map[string]string{"env": "prod"},
			wantCode: http.StatusOK,
		},
		{
			name:     "key_too_long",
			tags:     map[string]string{strings.Repeat("k", 129): "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "value_too_long",
			tags:     map[string]string{"k": strings.Repeat("v", 257)},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "CreateJob", map[string]any{
				"Name":    "tagged-job-" + tt.name,
				"Role":    "r",
				"Command": map[string]any{"Name": "glueetl"},
				"Tags":    tt.tags,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestJobLifecycle tests job lifecycle operations.
func TestJobLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*glue.Handler) string
		makeBody func(string) map[string]any
		check    func(*testing.T, map[string]any)
		name     string
		action   string
		wantCode int
	}{
		{
			name: "delete_cascades_runs_and_bookmarks",
			setup: func(h *glue.Handler) string {
				doGlueRequest(
					t,
					h,
					"CreateJob",
					map[string]any{"Name": "cascade-job", "Role": "r", "Command": map[string]any{"Name": "glueetl"}},
				)
				doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "cascade-job"})
				doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "cascade-job"})

				return "cascade-job"
			},
			action:   "DeleteJob",
			makeBody: func(name string) map[string]any { return map[string]any{"JobName": name} },
			wantCode: http.StatusOK,
		},
		{
			name: "bookmark_not_found_for_nonexistent_job",
			setup: func(_ *glue.Handler) string {
				return "no-job"
			},
			action:   "GetJobBookmark",
			makeBody: func(name string) map[string]any { return map[string]any{"JobName": name} },
			wantCode: http.StatusBadRequest,
		},
		{
			name: "reset_bookmark_returns_post_reset_state",
			setup: func(h *glue.Handler) string {
				doGlueRequest(
					t,
					h,
					"CreateJob",
					map[string]any{"Name": "j", "Role": "r", "Command": map[string]any{"Name": "glueetl"}},
				)
				doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "j"})

				return "j"
			},
			action:   "ResetJobBookmark",
			makeBody: func(name string) map[string]any { return map[string]any{"JobName": name} },
			wantCode: http.StatusOK,
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				bm, _ := out["JobBookmarkEntry"].(map[string]any)
				assert.Equal(t, "j", bm["JobName"])
			},
		},
		{
			name: "reset_bookmark_not_found",
			setup: func(_ *glue.Handler) string {
				return "no-job"
			},
			action:   "ResetJobBookmark",
			makeBody: func(name string) map[string]any { return map[string]any{"JobName": name} },
			wantCode: http.StatusBadRequest,
		},
		{
			name: "bookmark_active_run_tracking",
			setup: func(h *glue.Handler) string {
				doGlueRequest(
					t,
					h,
					"CreateJob",
					map[string]any{"Name": "j", "Role": "r", "Command": map[string]any{"Name": "glueetl"}},
				)
				doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "j"})

				return "j"
			},
			action:   "GetJobBookmark",
			makeBody: func(name string) map[string]any { return map[string]any{"JobName": name} },
			wantCode: http.StatusOK,
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				bm, _ := out["JobBookmarkEntry"].(map[string]any)
				assert.NotEmpty(t, bm["ActiveRun"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			var resourceName string
			if tt.setup != nil {
				resourceName = tt.setup(h)
			}

			rec := doGlueRequest(t, h, tt.action, tt.makeBody(resourceName))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil && rec.Code == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				tt.check(t, out)
			}
		})
	}
}

// TestMaxConcurrentRuns tests job run concurrency enforcement.
func TestMaxConcurrentRuns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		maxConcurrentRuns int
		startCount        int
		wantLastCode      int
	}{
		{
			name:              "limit_2_blocks_third",
			maxConcurrentRuns: 2,
			startCount:        3,
			wantLastCode:      http.StatusBadRequest,
		},
		{
			name:              "limit_1_blocks_second",
			maxConcurrentRuns: 1,
			startCount:        2,
			wantLastCode:      http.StatusBadRequest,
		},
		{
			name:              "unlimited_allows_many",
			maxConcurrentRuns: 0,
			startCount:        3,
			wantLastCode:      http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{"Name": "limited-job", "Role": "r", "Command": map[string]any{"Name": "glueetl"}}
			if tt.maxConcurrentRuns > 0 {
				body["ExecutionProperty"] = map[string]any{"MaxConcurrentRuns": tt.maxConcurrentRuns}
			}
			doGlueRequest(t, h, "CreateJob", body)

			var lastCode int
			for range tt.startCount {
				rec := doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "limited-job"})
				lastCode = rec.Code
			}
			assert.Equal(t, tt.wantLastCode, lastCode)
		})
	}
}

// TestBatchStopJobRun tests batch stop for non-stoppable runs.
func TestBatchStopJobRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErrors int
	}{
		{
			name:       "already_stopped_run_returns_error",
			wantErrors: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(
				t,
				h,
				"CreateJob",
				map[string]any{"Name": "j", "Role": "r", "Command": map[string]any{"Name": "glueetl"}},
			)
			runRec := doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "j"})

			var runOut map[string]any
			require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &runOut))
			runID := runOut["JobRunId"].(string)

			doGlueRequest(t, h, "BatchStopJobRun", map[string]any{
				"JobName": "j", "JobRunIds": []string{runID},
			})

			rec := doGlueRequest(t, h, "BatchStopJobRun", map[string]any{
				"JobName": "j", "JobRunIds": []string{runID},
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			errs, _ := out["Errors"].([]any)
			assert.Len(t, errs, tt.wantErrors)
		})
	}
}

// TestUpdateJobFromSourceControl_ValidatesJob verifies the job must exist and
// that a successful sync records real source-control state on the job.
func TestUpdateJobFromSourceControl_ValidatesJob(t *testing.T) {
	t.Parallel()

	validationTests := []struct {
		body map[string]any
		name string
	}{
		{name: "missing_job_name", body: map[string]any{}},
		{name: "unknown_job_name", body: map[string]any{"JobName": "no-such-job"}},
	}

	for _, tt := range validationTests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "UpdateJobFromSourceControl", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}

	h := newTestHandler(t)

	doGlueRequest(t, h, "CreateJob", map[string]any{
		"Name":    "my-job",
		"Command": map[string]any{"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
		"Role":    "arn:aws:iam::123456789012:role/GlueRole",
	})

	rec := doGlueRequest(t, h, "UpdateJobFromSourceControl", map[string]any{
		"JobName":         "my-job",
		"Provider":        "GITHUB",
		"RepositoryName":  "my-repo",
		"RepositoryOwner": "my-org",
		"BranchName":      "main",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetJob", map[string]any{"JobName": "my-job"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Job struct {
			SourceControlDetails struct {
				Provider   string `json:"Provider"`
				Repository string `json:"Repository"`
				Branch     string `json:"Branch"`
			} `json:"SourceControlDetails"`
		} `json:"Job"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "GITHUB", out.Job.SourceControlDetails.Provider)
	assert.Equal(t, "my-repo", out.Job.SourceControlDetails.Repository)
	assert.Equal(t, "main", out.Job.SourceControlDetails.Branch)
}

// TestUpdateSourceControlFromJob_ValidatesJob mirrors
// TestUpdateJobFromSourceControl_ValidatesJob for the reverse-direction sync op.
func TestUpdateSourceControlFromJob_ValidatesJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "UpdateSourceControlFromJob", map[string]any{"JobName": "no-such-job"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	doGlueRequest(t, h, "CreateJob", map[string]any{
		"Name":    "my-job",
		"Command": map[string]any{"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
		"Role":    "arn:aws:iam::123456789012:role/GlueRole",
	})

	rec = doGlueRequest(t, h, "UpdateSourceControlFromJob", map[string]any{
		"JobName":  "my-job",
		"Provider": "GITHUB",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestGlue_ResetJobBookmark(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a job first
	doGlueRequest(t, h, "CreateJob", map[string]any{
		"Name":    "bm-job",
		"Command": map[string]any{"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
		"Role":    "arn:aws:iam::123456789012:role/GlueRole",
	})

	rec := doGlueRequest(t, h, "ResetJobBookmark", map[string]any{
		"JobName": "bm-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

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
