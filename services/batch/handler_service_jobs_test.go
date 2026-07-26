package batch_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

// createTestServiceJobQueue creates a MANAGED compute environment and an
// ENABLED job queue backed by it, returning the queue name. Real AWS Batch
// requires SubmitServiceJob's jobQueue to reference an existing queue (of
// type SAGEMAKER_TRAINING for real service jobs; this emulator doesn't
// enforce that cross-field constraint).
func createTestServiceJobQueue(t *testing.T, h *batch.Handler, suffix string) string {
	t.Helper()

	ceName := "sj-ce-" + suffix
	rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
		"computeEnvironmentName": ceName,
		"type":                   "MANAGED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	qName := "sj-queue-" + suffix
	rec = post(t, h, "/v1/createjobqueue", map[string]any{
		"jobQueueName": qName,
		"priority":     1,
		"computeEnvironmentOrder": []map[string]any{
			{"order": 1, "computeEnvironment": ceName},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	return qName
}

func TestHandler_SubmitServiceJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	qName := createTestServiceJobQueue(t, h, "submit")

	tests := []struct {
		input       map[string]any
		name        string
		wantJobName string
		wantStatus  int
	}{
		{
			name: "valid_submit",
			input: map[string]any{
				"jobName":               "my-sj",
				"jobQueue":              qName,
				"serviceJobType":        "SAGEMAKER_TRAINING",
				"serviceRequestPayload": `{"foo":"bar"}`,
			},
			wantStatus:  http.StatusOK,
			wantJobName: "my-sj",
		},
		{
			name: "missing_name",
			input: map[string]any{
				"jobQueue":              qName,
				"serviceJobType":        "SAGEMAKER_TRAINING",
				"serviceRequestPayload": `{"foo":"bar"}`,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_queue",
			input: map[string]any{
				"jobName":               "no-queue-sj",
				"serviceJobType":        "SAGEMAKER_TRAINING",
				"serviceRequestPayload": `{"foo":"bar"}`,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := post(t, h, "/v1/submitservicejob", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantJobName != "" {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.Equal(t, tt.wantJobName, out["jobName"])
				assert.NotEmpty(t, out["jobArn"])
				assert.NotEmpty(t, out["jobId"])
			}
		})
	}
}

func TestHandler_DescribeServiceJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		jobName    string
		wantStatus int
		wantFound  bool
	}{
		{name: "found", jobName: "my-sj", wantStatus: http.StatusOK, wantFound: true},
		{name: "missing_id", jobName: "", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var jobID string
			if tt.wantFound {
				qName := createTestServiceJobQueue(t, h, "describe")
				rec := post(t, h, "/v1/submitservicejob", map[string]any{
					"jobName":               tt.jobName,
					"jobQueue":              qName,
					"serviceJobType":        "SAGEMAKER_TRAINING",
					"serviceRequestPayload": `{"foo":"bar"}`,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out map[string]any
				mustUnmarshal(t, rec, &out)
				jobID = out["jobId"].(string)
			}

			rec2 := post(t, h, "/v1/describeservicejob", map[string]any{"jobId": jobID})
			assert.Equal(t, tt.wantStatus, rec2.Code)

			if tt.wantFound {
				var desc map[string]any
				mustUnmarshal(t, rec2, &desc)
				assert.Equal(t, tt.jobName, desc["jobName"])
				assert.NotEmpty(t, desc["jobQueue"])
				assert.Equal(t, "SAGEMAKER_TRAINING", desc["serviceJobType"])
			}
		})
	}
}

func TestHandler_ListServiceJobs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	qA := createTestServiceJobQueue(t, h, "list-a")
	qB := createTestServiceJobQueue(t, h, "list-b")

	for i, q := range []string{qA, qB} {
		rec := post(t, h, "/v1/submitservicejob", map[string]any{
			"jobName":               fmt.Sprintf("sj-%d", i),
			"jobQueue":              q,
			"serviceJobType":        "SAGEMAKER_TRAINING",
			"serviceRequestPayload": `{"foo":"bar"}`,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Real AWS Batch's ListServiceJobs defaults to only RUNNING jobs when no
	// jobStatus is given; newly-submitted jobs are SUBMITTED, so an explicit
	// status filter is required to see them here.
	rec := post(t, h, "/v1/listservicejobs", map[string]any{
		"jobQueue":  qA,
		"jobStatus": "SUBMITTED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	items := out["jobSummaryList"].([]any)
	assert.Len(t, items, 1)

	// Filtering by a status with no matches returns an empty (not null) list.
	recEmpty := post(t, h, "/v1/listservicejobs", map[string]any{
		"jobQueue":  qA,
		"jobStatus": "RUNNING",
	})
	require.Equal(t, http.StatusOK, recEmpty.Code)

	var outEmpty map[string]any
	mustUnmarshal(t, recEmpty, &outEmpty)
	assert.Empty(t, outEmpty["jobSummaryList"])
}

func TestHandler_TerminateServiceJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		jobID       string
		wantStatus  int
		submitFirst bool
	}{
		{
			name:        "terminate_existing",
			wantStatus:  http.StatusOK,
			submitFirst: true,
		},
		{
			name:       "missing_id",
			jobID:      "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			jobID:      "nonexistent-id",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			jobID := tt.jobID

			if tt.submitFirst {
				qName := createTestServiceJobQueue(t, h, "terminate")
				rec := post(t, h, "/v1/submitservicejob", map[string]any{
					"jobName":               "sj-term",
					"jobQueue":              qName,
					"serviceJobType":        "SAGEMAKER_TRAINING",
					"serviceRequestPayload": `{"foo":"bar"}`,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out map[string]any
				mustUnmarshal(t, rec, &out)
				jobID = out["jobId"].(string)
			}

			rec := post(t, h, "/v1/terminateservicejob", map[string]any{
				"jobId":  jobID,
				"reason": "test termination",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_UpdateServiceJob covers UpdateServiceJob's real-API contract:
// only schedulingPriority can be changed (see UpdateServiceJobInput, which
// has exactly jobId + schedulingPriority, both required), and a service job
// that has already reached a terminal status (SUCCEEDED/FAILED) rejects the
// update.
func TestHandler_UpdateServiceJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		schedulingPriority any
		name               string
		jobID              string
		wantStatus         int
		submitFirst        bool
		terminateFirst     bool
	}{
		{
			name:               "update_existing",
			wantStatus:         http.StatusOK,
			submitFirst:        true,
			schedulingPriority: 42,
		},
		{
			name:               "missing_id",
			jobID:              "",
			schedulingPriority: 5,
			wantStatus:         http.StatusBadRequest,
		},
		{
			name:               "not_found",
			jobID:              "nonexistent-id",
			schedulingPriority: 5,
			wantStatus:         http.StatusBadRequest,
		},
		{
			name:               "terminal_state_rejected",
			wantStatus:         http.StatusBadRequest,
			submitFirst:        true,
			terminateFirst:     true,
			schedulingPriority: 5,
		},
		{
			name:               "priority_out_of_range",
			wantStatus:         http.StatusBadRequest,
			submitFirst:        true,
			schedulingPriority: 10000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			jobID := tt.jobID

			if tt.submitFirst {
				qName := createTestServiceJobQueue(t, h, "update-"+tt.name)
				rec := post(t, h, "/v1/submitservicejob", map[string]any{
					"jobName":               "sj-update",
					"jobQueue":              qName,
					"serviceJobType":        "SAGEMAKER_TRAINING",
					"serviceRequestPayload": `{"foo":"bar"}`,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out map[string]any
				mustUnmarshal(t, rec, &out)
				jobID = out["jobId"].(string)
			}

			if tt.terminateFirst {
				rec := post(t, h, "/v1/terminateservicejob", map[string]any{
					"jobId":  jobID,
					"reason": "force terminal state",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := post(t, h, "/v1/updateservicejob", map[string]any{
				"jobId":              jobID,
				"schedulingPriority": tt.schedulingPriority,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.Equal(t, jobID, out["jobId"])
				assert.Equal(t, "sj-update", out["jobName"])
				assert.NotEmpty(t, out["jobArn"])

				// The update actually mutated the existing record, not a
				// fresh/parallel one -- prove it round-trips via Describe.
				rec2 := post(t, h, "/v1/describeservicejob", map[string]any{"jobId": jobID})
				require.Equal(t, http.StatusOK, rec2.Code)

				var desc map[string]any
				mustUnmarshal(t, rec2, &desc)
				assert.EqualValues(t, 42, desc["schedulingPriority"])
			}
		})
	}
}

// TestHandler_SubmitServiceJob_ARNLikeJobID exercises the ARN suffix a real
// SDK client would parse from jobArn (defensive against a jobArn wire-shape
// regression -- see TestHandler_DescribeServiceJob for the primary
// round-trip coverage).
func TestHandler_SubmitServiceJob_ARNLikeJobID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	qName := createTestServiceJobQueue(t, h, "arn-like")

	rec := post(t, h, "/v1/submitservicejob", map[string]any{
		"jobName":               "arn-check",
		"jobQueue":              qName,
		"serviceJobType":        "SAGEMAKER_TRAINING",
		"serviceRequestPayload": `{"foo":"bar"}`,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	arnStr := out["jobArn"].(string)
	parts := strings.Split(arnStr, "/")
	assert.Equal(t, out["jobId"], parts[len(parts)-1])
}
