package backup_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func TestScanJob(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")

	job := b.StartScanJob("arn:aws:backup:us-east-1:000000000000:backup-vault:test")
	assert.NotEmpty(t, job.ScanJobID)

	found, err := b.DescribeScanJob(job.ScanJobID)
	require.NoError(t, err)
	assert.Equal(t, "COMPLETED", found.Status)

	jobs := b.ListScanJobs()
	assert.NotEmpty(t, jobs)
}

func TestRestoreTestingPlanStartWindowHours(t *testing.T) {
	t.Parallel()

	tests := []struct {
		startWindowHours any
		name             string
		wantHours        float64
		wantInResponse   bool
	}{
		{
			name:             "start_window_hours_round_trips",
			startWindowHours: 4,
			wantHours:        4,
			wantInResponse:   true,
		},
		{
			name:             "zero_start_window_omitted",
			startWindowHours: 0,
			wantHours:        0,
			wantInResponse:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()

			rec := doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
				"RestoreTestingPlan": map[string]any{
					"RestoreTestingPlanName": "swh-plan",
					"ScheduleExpression":     "cron(0 1 * * ? *)",
					"StartWindowHours":       tc.startWindowHours,
				},
			})
			require.Equal(t, http.StatusCreated, rec.Code)

			rec2 := doREST(t, h, http.MethodGet, "/restore-testing/plans/swh-plan", nil)
			assert.Equal(t, http.StatusOK, rec2.Code)
			resp := parseResp(t, rec2)
			planDoc, _ := resp["RestoreTestingPlan"].(map[string]any)
			require.NotNil(t, planDoc)

			hours, present := planDoc["StartWindowHours"]
			assert.Equal(t, tc.wantInResponse, present)
			if tc.wantInResponse {
				assert.InDelta(t, tc.wantHours, hours, 0)
			}
		})
	}
}

// TestRestoreTestingPlanStartWindowHoursUpdate verifies UpdateRestoreTestingPlan
// preserves and updates StartWindowHours.
func TestRestoreTestingPlanStartWindowHoursUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		initialHours      any
		updatedHours      any
		name              string
		wantHoursAfterUpd float64
		wantPresent       bool
	}{
		{
			name:              "update_start_window_hours",
			initialHours:      2,
			updatedHours:      8,
			wantHoursAfterUpd: 8,
			wantPresent:       true,
		},
		{
			name:              "preserve_hours_when_not_in_update",
			initialHours:      3,
			updatedHours:      0,
			wantHoursAfterUpd: 3,
			wantPresent:       true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()

			doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
				"RestoreTestingPlan": map[string]any{
					"RestoreTestingPlanName": "swh-update-plan",
					"StartWindowHours":       tc.initialHours,
				},
			})

			doREST(t, h, http.MethodPut, "/restore-testing/plans/swh-update-plan",
				map[string]any{
					"RestoreTestingPlan": map[string]any{
						"StartWindowHours": tc.updatedHours,
					},
				})

			rec := doREST(t, h, http.MethodGet, "/restore-testing/plans/swh-update-plan", nil)
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := parseResp(t, rec)
			planDoc, _ := resp["RestoreTestingPlan"].(map[string]any)
			require.NotNil(t, planDoc)

			hours, present := planDoc["StartWindowHours"]
			assert.Equal(t, tc.wantPresent, present)
			if tc.wantPresent {
				assert.InDelta(t, tc.wantHoursAfterUpd, hours, 0)
			}
		})
	}
}

// TestRestoreTestingPlanListStartWindowHours verifies ListRestoreTestingPlans
// includes StartWindowHours when non-zero.
func TestRestoreTestingPlanListStartWindowHours(t *testing.T) {
	t.Parallel()

	tests := []struct {
		hours       any
		name        string
		wantHours   float64
		wantPresent bool
	}{
		{
			name:        "list_includes_start_window_hours",
			hours:       6,
			wantPresent: true,
			wantHours:   6,
		},
		{
			name:        "list_omits_zero_start_window_hours",
			hours:       0,
			wantPresent: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()

			doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
				"RestoreTestingPlan": map[string]any{
					"RestoreTestingPlanName": "swh-list-plan",
					"StartWindowHours":       tc.hours,
				},
			})

			rec := doREST(t, h, http.MethodGet, "/restore-testing/plans", nil)
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := parseResp(t, rec)
			plans, _ := resp["RestoreTestingPlans"].([]any)
			require.NotEmpty(t, plans)

			plan := plans[0].(map[string]any)
			hours, present := plan["StartWindowHours"]
			assert.Equal(t, tc.wantPresent, present)
			if tc.wantPresent {
				assert.InDelta(t, tc.wantHours, hours, 0)
			}
		})
	}
}

// ---- 9. StartCopyJob (backend) and HTTP DescribeCopyJob fields ----

// TestCreateRestoreTestingPlan exercises the restore testing plan creation.
func TestCreateRestoreTestingPlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *backup.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
					"RestoreTestingPlan": map[string]any{
						"RestoreTestingPlanName": "my-test-plan",
						"ScheduleExpression":     "cron(0 1 ? * * *)",
					},
				})
				require.Equal(t, http.StatusCreated, rec.Code)
				resp := parseResp(t, rec)
				assert.Equal(t, "my-test-plan", resp["RestoreTestingPlanName"])
				assert.NotEmpty(t, resp["RestoreTestingPlanArn"])
				assert.NotNil(t, resp["CreationTime"])
			},
		},
		{
			name: "duplicate_name",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
					"RestoreTestingPlan": map[string]any{"RestoreTestingPlanName": "dup-plan"},
				})
				rec := doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
					"RestoreTestingPlan": map[string]any{"RestoreTestingPlanName": "dup-plan"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "missing_plan_name",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
					"RestoreTestingPlan": map[string]any{},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

// TestCreateRestoreTestingSelection exercises restore testing selection creation.
func TestCreateRestoreTestingSelection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *backup.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
					"RestoreTestingPlan": map[string]any{"RestoreTestingPlanName": "my-plan"},
				})
				rec := doREST(t, h, http.MethodPut, "/restore-testing/plans/my-plan/selections", map[string]any{
					"RestoreTestingSelection": map[string]any{
						"RestoreTestingSelectionName": "my-selection",
						"ProtectedResourceType":       "EC2",
						"IamRoleArn":                  "arn:aws:iam::123456789012:role/restore-role",
						"ProtectedResourceArns":       []string{"*"},
						"ValidationWindowHours":       12,
					},
				})
				require.Equal(t, http.StatusCreated, rec.Code)
				resp := parseResp(t, rec)
				assert.Equal(t, "my-plan", resp["RestoreTestingPlanName"])
				assert.Equal(t, "my-selection", resp["RestoreTestingSelectionName"])
				assert.NotEmpty(t, resp["RestoreTestingPlanArn"])

				getRec := doREST(t, h, http.MethodGet, "/restore-testing/plans/my-plan/selections/my-selection", nil)
				require.Equal(t, http.StatusOK, getRec.Code)
				got := parseResp(t, getRec)
				sel, ok := got["RestoreTestingSelection"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "arn:aws:iam::123456789012:role/restore-role", sel["IamRoleArn"])
				assert.InEpsilon(t, float64(12), sel["ValidationWindowHours"], 0)
			},
		},
		{
			name: "plan_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/restore-testing/plans/missing-plan/selections", map[string]any{
					"RestoreTestingSelection": map[string]any{
						"RestoreTestingSelectionName": "sel",
						"ProtectedResourceType":       "EC2",
						"IamRoleArn":                  "arn:aws:iam::123456789012:role/restore-role",
					},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "duplicate_selection",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
					"RestoreTestingPlan": map[string]any{"RestoreTestingPlanName": "plan-b"},
				})
				selBody := map[string]any{
					"RestoreTestingSelection": map[string]any{
						"RestoreTestingSelectionName": "sel-b",
						"ProtectedResourceType":       "EC2",
						"IamRoleArn":                  "arn:aws:iam::123456789012:role/restore-role",
					},
				}
				firstRec := doREST(t, h, http.MethodPut, "/restore-testing/plans/plan-b/selections", selBody)
				require.Equal(t, http.StatusCreated, firstRec.Code)
				rec := doREST(t, h, http.MethodPut, "/restore-testing/plans/plan-b/selections", selBody)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "AlreadyExistsException")
			},
		},
		{
			name: "missing_selection_name",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
					"RestoreTestingPlan": map[string]any{"RestoreTestingPlanName": "plan-c"},
				})
				rec := doREST(t, h, http.MethodPut, "/restore-testing/plans/plan-c/selections", map[string]any{
					"RestoreTestingSelection": map[string]any{},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "missing_iam_role_arn",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
					"RestoreTestingPlan": map[string]any{"RestoreTestingPlanName": "plan-d"},
				})
				rec := doREST(t, h, http.MethodPut, "/restore-testing/plans/plan-d/selections", map[string]any{
					"RestoreTestingSelection": map[string]any{
						"RestoreTestingSelectionName": "sel-d",
						"ProtectedResourceType":       "EC2",
					},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "MissingParameterValueException")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

func TestRestoreTestingPlan_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()

	// Create (PUT /restore-testing/plans)
	rec := doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
		"RestoreTestingPlan": map[string]any{
			"RestoreTestingPlanName": "myrestoreplan",
			"ScheduleExpression":     "cron(0 1 * * ? *)",
			"StartWindowHours":       1,
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	resp := parseResp(t, rec)
	planName, ok := resp["RestoreTestingPlanName"].(string)
	require.True(t, ok)
	require.NotEmpty(t, planName)

	// Get
	rec2 := doREST(t, h, http.MethodGet, "/restore-testing/plans/"+planName, nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	// List
	rec3 := doREST(t, h, http.MethodGet, "/restore-testing/plans", nil)
	assert.Equal(t, http.StatusOK, rec3.Code)
	listResp := parseResp(t, rec3)
	plans, ok := listResp["RestoreTestingPlans"].([]any)
	assert.True(t, ok)
	assert.NotEmpty(t, plans)

	// Update
	rec4 := doREST(t, h, http.MethodPut, "/restore-testing/plans/"+planName, map[string]any{
		"RestoreTestingPlan": map[string]any{
			"StartWindowHours": 2,
		},
	})
	assert.Equal(t, http.StatusOK, rec4.Code)

	// Delete
	rec5 := doREST(t, h, http.MethodDelete, "/restore-testing/plans/"+planName, nil)
	assert.Equal(t, http.StatusNoContent, rec5.Code)
}
