package athena_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

func TestHandler_CreateCapacityReservation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*athena.Handler)
		name       string
		body       string
		wantStatus int
		wantErr    bool
	}{
		{
			name:       "success",
			body:       `{"Name":"res1","TargetDpus":24}`,
			wantStatus: http.StatusOK,
		},
		{
			name: "duplicate",
			setup: func(h *athena.Handler) {
				_ = doRequest(t, h, "CreateCapacityReservation", `{"Name":"res1","TargetDpus":24}`)
			},
			body:       `{"Name":"res1","TargetDpus":24}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "CreateCapacityReservation", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErr {
				var errResp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.NotEmpty(t, errResp["__type"])
			}
		})
	}
}

func TestHandler_CancelCapacityReservation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*athena.Handler)
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *athena.Handler) {
				_ = doRequest(t, h, "CreateCapacityReservation", `{"Name":"cancel-res","TargetDpus":24}`)
			},
			body:       `{"Name":"cancel-res"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			body:       `{"Name":"no-such-res"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "CancelCapacityReservation", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteCapacityReservation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*athena.Handler)
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *athena.Handler) {
				_ = doRequest(t, h, "CreateCapacityReservation", `{"Name":"del-res","TargetDpus":24}`)
				_ = doRequest(t, h, "CancelCapacityReservation", `{"Name":"del-res"}`)
			},
			body:       `{"Name":"del-res"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			body:       `{"Name":"no-such-res"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "DeleteCapacityReservation", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Notebook tests ---

func TestHandler_CreateCapacityReservation_WithTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "with_tags",
			body:       `{"Name":"tagged-res","TargetDpus":24,"Tags":[{"Key":"owner","Value":"platform"}]}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateCapacityReservation", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateCapacityReservation_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "missing_name",
			body:       `{"TargetDpus":24}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "zero_target_dpus",
			body:       `{"Name":"res1","TargetDpus":0}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "negative_target_dpus",
			body:       `{"Name":"res1","TargetDpus":-1}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateCapacityReservation", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteCapacityReservation_ActiveBlocked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "active_reservation_cannot_be_deleted",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_ = doRequest(t, h, "CreateCapacityReservation", `{"Name":"active-res","TargetDpus":24}`)

			rec := doRequest(t, h, "DeleteCapacityReservation", `{"Name":"active-res"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- GetPreparedStatement and ListPreparedStatements tests ---

func TestHandler_CapacityReservation_LastAllocationStruct(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_ = doRequest(t, h, "CreateCapacityReservation", `{"Name":"cap-test","TargetDpus":24}`)

	rec := doRequest(t, h, "GetCapacityReservation", `{"Name":"cap-test"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	cr := resp["CapacityReservation"].(map[string]any)

	assert.Equal(t, "ACTIVE", cr["Status"])
	assert.NotZero(t, cr["CreationTime"])
	assert.NotZero(t, cr["LastSuccessfulAllocationTime"])

	lastAlloc := cr["LastAllocation"].(map[string]any)
	assert.Equal(t, "SUCCEEDED", lastAlloc["Status"])
	assert.NotZero(t, lastAlloc["RequestTime"])
	assert.NotZero(t, lastAlloc["RequestCompletionTime"])
}

func TestHandler_CapacityReservation_UpdateLastAllocation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_ = doRequest(t, h, "CreateCapacityReservation", `{"Name":"cap-upd","TargetDpus":24}`)
	_ = doRequest(t, h, "UpdateCapacityReservation", `{"Name":"cap-upd","TargetDpus":30}`)

	rec := doRequest(t, h, "GetCapacityReservation", `{"Name":"cap-upd"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	cr := resp["CapacityReservation"].(map[string]any)

	assert.InDelta(t, float64(30), cr["TargetDpus"], 0.001)
	lastAlloc := cr["LastAllocation"].(map[string]any)
	assert.Equal(t, "SUCCEEDED", lastAlloc["Status"])
}

func capacityHandler(t *testing.T) *athena.Handler {
	t.Helper()

	h := newTestHandler(t)
	require.Equal(t, http.StatusOK,
		doRequest(t, h, "CreateCapacityReservation", `{"Name":"cap1","TargetDpus":24}`).Code)

	return h
}

func TestHandler_GetCapacityReservation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resName    string
		wantStatus int
	}{
		{
			name:       "success",
			resName:    "cap1",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			resName:    "missing",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := capacityHandler(t)
			rec := doRequest(t, h, "GetCapacityReservation", `{"Name":"`+tt.resName+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_GetCapacityReservation_NoInventedTagsField locks in that
// GetCapacityReservation's response CapacityReservation object never carries
// a "Tags" key -- AWS's real types.CapacityReservation has no such field.
// Tags set at creation are visible only through ListTagsForResource against
// the reservation's ARN.
func TestHandler_GetCapacityReservation_NoInventedTagsField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateCapacityReservation",
		`{"Name":"tagged-cr","TargetDpus":24,"Tags":[{"Key":"owner","Value":"platform"}]}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "GetCapacityReservation", `{"Name":"tagged-cr"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	_, hasTags := resp["CapacityReservation"]["Tags"]
	assert.False(t, hasTags, "CapacityReservation response must not carry an invented Tags field")

	const crARN = "arn:aws:athena:us-east-1:000000000000:capacity-reservation/tagged-cr"
	rec = doRequest(t, h, "ListTagsForResource", `{"ResourceARN":"`+crARN+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var tagsResp map[string][]map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))
	require.Len(t, tagsResp["Tags"], 1, "the tag set at creation must still be visible via ListTagsForResource")
	assert.Equal(t, "owner", tagsResp["Tags"][0]["Key"])
}

func TestHandler_ListCapacityReservations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantStatus   int
		wantNonEmpty bool
	}{
		{
			name:         "success",
			wantStatus:   http.StatusOK,
			wantNonEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := capacityHandler(t)
			rec := doRequest(t, h, "ListCapacityReservations", `{}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UpdateCapacityReservation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       `{"Name":"cap1","TargetDpus":30}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_dpus",
			body:       `{"Name":"cap1","TargetDpus":0}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			body:       `{"Name":"missing","TargetDpus":24}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := capacityHandler(t)
			rec := doRequest(t, h, "UpdateCapacityReservation", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_PutCapacityAssignmentConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains string
		wantStatus   int
	}{
		{
			name:       "success",
			body:       `{"CapacityReservationName":"cap1","CapacityAssignments":[{"WorkGroupNames":["primary"]}]}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "validation_no_name",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			body:       `{"CapacityReservationName":"missing"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := capacityHandler(t)
			rec := doRequest(t, h, "PutCapacityAssignmentConfiguration", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetCapacityAssignmentConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resName      string
		wantContains string
		wantStatus   int
		setup        bool
	}{
		{
			name:         "success",
			resName:      "cap1",
			setup:        true,
			wantStatus:   http.StatusOK,
			wantContains: "primary",
		},
		{
			name:       "not_found",
			resName:    "missing",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := capacityHandler(t)

			if tt.setup {
				doRequest(t, h, "PutCapacityAssignmentConfiguration",
					`{"CapacityReservationName":"cap1","CapacityAssignments":[{"WorkGroupNames":["primary"]}]}`)
			}

			rec := doRequest(t, h, "GetCapacityAssignmentConfiguration",
				`{"CapacityReservationName":"`+tt.resName+`"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContains)
			}
		})
	}
}

// --- Metadata tests ---

func TestCapacityReservation_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *athena.Handler)
		name string
	}{
		{
			name: "create_and_get_shape",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				rec := a1Do(t, h, "CreateCapacityReservation", `{"Name":"res1","TargetDpus":24}`)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Empty(t, a1Unmarshal(t, rec))

				rec = a1Do(t, h, "GetCapacityReservation", `{"Name":"res1"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1Unmarshal(t, rec)
				cr, ok := m["CapacityReservation"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "res1", cr["Name"])
				assert.Equal(t, "ACTIVE", cr["Status"])
				assert.NotZero(t, cr["CreationTime"])
				assert.NotZero(t, cr["LastSuccessfulAllocationTime"])
				lastAlloc, ok := cr["LastAllocation"].(map[string]any)
				require.True(t, ok, "LastAllocation must be present")
				assert.Equal(t, "SUCCEEDED", lastAlloc["Status"])
				assert.NotZero(t, lastAlloc["RequestTime"])
				assert.NotZero(t, lastAlloc["RequestCompletionTime"])
			},
		},
		{
			name: "cancel_sets_cancelling_status",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				a1Do(t, h, "CreateCapacityReservation", `{"Name":"res2","TargetDpus":24}`)
				rec := a1Do(t, h, "CancelCapacityReservation", `{"Name":"res2"}`)
				require.Equal(t, http.StatusOK, rec.Code)

				rec = a1Do(t, h, "GetCapacityReservation", `{"Name":"res2"}`)
				cr := a1Unmarshal(t, rec)["CapacityReservation"].(map[string]any)
				assert.Equal(t, "CANCELLING", cr["Status"])
			},
		},
		{
			name: "delete_after_cancel_succeeds",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				a1Do(t, h, "CreateCapacityReservation", `{"Name":"res3","TargetDpus":24}`)
				a1Do(t, h, "CancelCapacityReservation", `{"Name":"res3"}`)
				rec := a1Do(t, h, "DeleteCapacityReservation", `{"Name":"res3"}`)
				require.Equal(t, http.StatusOK, rec.Code)

				rec = a1Do(t, h, "GetCapacityReservation", `{"Name":"res3"}`)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "delete_active_returns_error",
			fn: func(t *testing.T, h *athena.Handler) {
				t.Helper()
				a1Do(t, h, "CreateCapacityReservation", `{"Name":"active-res","TargetDpus":24}`)
				rec := a1Do(t, h, "DeleteCapacityReservation", `{"Name":"active-res"}`)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.NotEmpty(t, a1Unmarshal(t, rec)["__type"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := a1Handler(t)
			tt.fn(t, h)
		})
	}
}

func TestCreateCapacityReservation_MinDPUs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "zero_dpus_rejected",
			body:       `{"Name":"r1","TargetDpus":0}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "below_minimum_rejected",
			body:       `{"Name":"r2","TargetDpus":23}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "exactly_24_accepted",
			body:       `{"Name":"r3","TargetDpus":24}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "above_minimum_accepted",
			body:       `{"Name":"r4","TargetDpus":48}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := athena.NewHandler(athena.NewInMemoryBackend("", ""))
			rec := athenaDoPass5(t, h, "CreateCapacityReservation", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestCancelCapacityReservation_SetsCancelling(t *testing.T) {
	t.Parallel()

	h := athena.NewHandler(athena.NewInMemoryBackend("", ""))
	rec := athenaDoPass5(t, h, "CreateCapacityReservation", `{"Name":"r","TargetDpus":24}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = athenaDoPass5(t, h, "CancelCapacityReservation", `{"Name":"r"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = athenaDoPass5(t, h, "GetCapacityReservation", `{"Name":"r"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	m := athenaUnmarshalPass5(t, rec)
	cr := m["CapacityReservation"].(map[string]any)
	assert.Equal(t, "CANCELLING", cr["Status"])
}

func TestDeleteCapacityReservation_AfterCancel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*athena.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "delete_after_cancel_succeeds",
			setup: func(h *athena.Handler) {
				athenaDoPass5(t, h, "CreateCapacityReservation", `{"Name":"r","TargetDpus":24}`)
				athenaDoPass5(t, h, "CancelCapacityReservation", `{"Name":"r"}`)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "delete_active_rejected",
			setup: func(h *athena.Handler) {
				athenaDoPass5(t, h, "CreateCapacityReservation", `{"Name":"r","TargetDpus":24}`)
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := athena.NewHandler(athena.NewInMemoryBackend("", ""))
			tt.setup(h)
			rec := athenaDoPass5(t, h, "DeleteCapacityReservation", `{"Name":"r"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
