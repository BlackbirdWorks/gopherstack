package appstream_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

// TestAppStream_Batch2Accuracy covers AWS-accuracy gaps fixed in batch-2:
//  1. ErrAlreadyExists __type: ResourceAlreadyExistsException (not InvalidParameterCombinationException)
//  2. ErrFleetNotStopped __type: InvalidAccountStatusException (not InvalidParameterCombinationException)
//  3. StartFleet on already-RUNNING fleet → InvalidAccountStatusException
//  4. StopFleet on already-STOPPED fleet → InvalidAccountStatusException
//  5. DeleteStack with associated fleets → ResourceInUseException
//  6. CreateFleet with invalid FleetType → InvalidParameterCombinationException
//  7. CreateFleet missing InstanceType → InvalidParameterCombinationException
func TestAppStream_Batch2Accuracy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		action   string
		wantCode int
		wantType string
	}{
		// Gap 1: CreateStack duplicate → ResourceAlreadyExistsException
		{
			name:   "CreateStack duplicate returns ResourceAlreadyExistsException",
			action: "CreateStack",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "dup-stack")
			},
			body:     map[string]any{"Name": "dup-stack"},
			wantCode: http.StatusBadRequest,
			wantType: "ResourceAlreadyExistsException",
		},
		// Gap 1b: CreateFleet duplicate → ResourceAlreadyExistsException
		{
			name:   "CreateFleet duplicate returns ResourceAlreadyExistsException",
			action: "CreateFleet",
			setup: func(h *appstream.Handler) {
				createFleet(t, h, "dup-fleet")
			},
			body: map[string]any{
				"Name":         "dup-fleet",
				"InstanceType": "stream.standard.medium",
			},
			wantCode: http.StatusBadRequest,
			wantType: "ResourceAlreadyExistsException",
		},
		// Gap 2: DeleteFleet on running fleet → InvalidAccountStatusException
		{
			name:   "DeleteFleet on running fleet returns InvalidAccountStatusException",
			action: "DeleteFleet",
			setup: func(h *appstream.Handler) {
				createFleet(t, h, "running-fleet")
				rec := doRequest(t, h, "StartFleet", map[string]any{"Name": "running-fleet"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"Name": "running-fleet"},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidAccountStatusException",
		},
		// Gap 3: StartFleet on already-RUNNING fleet → InvalidAccountStatusException
		{
			name:   "StartFleet on running fleet returns InvalidAccountStatusException",
			action: "StartFleet",
			setup: func(h *appstream.Handler) {
				createFleet(t, h, "running-fleet2")
				rec := doRequest(t, h, "StartFleet", map[string]any{"Name": "running-fleet2"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"Name": "running-fleet2"},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidAccountStatusException",
		},
		// Gap 4: StopFleet on already-STOPPED fleet → InvalidAccountStatusException
		{
			name:     "StopFleet on stopped fleet returns InvalidAccountStatusException",
			action:   "StopFleet",
			setup:    func(h *appstream.Handler) { createFleet(t, h, "stopped-fleet") },
			body:     map[string]any{"Name": "stopped-fleet"},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidAccountStatusException",
		},
		// Gap 5: DeleteStack with associated fleet → ResourceInUseException
		{
			name:   "DeleteStack with associated fleet returns ResourceInUseException",
			action: "DeleteStack",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "in-use-stack")
				createFleet(t, h, "in-use-fleet")
				rec := doRequest(t, h, "AssociateFleet", map[string]any{
					"FleetName": "in-use-fleet",
					"StackName": "in-use-stack",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"Name": "in-use-stack"},
			wantCode: http.StatusBadRequest,
			wantType: "ResourceInUseException",
		},
		// Gap 5b: DeleteStack succeeds after disassociation
		{
			name:   "DeleteStack succeeds after fleet disassociation",
			action: "DeleteStack",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "free-stack")
				createFleet(t, h, "free-fleet")
				rec := doRequest(t, h, "AssociateFleet", map[string]any{
					"FleetName": "free-fleet",
					"StackName": "free-stack",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				rec = doRequest(t, h, "DisassociateFleet", map[string]any{
					"FleetName": "free-fleet",
					"StackName": "free-stack",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"Name": "free-stack"},
			wantCode: http.StatusOK,
		},
		// Gap 6: CreateFleet invalid FleetType → InvalidParameterCombinationException
		{
			name:   "CreateFleet with invalid FleetType returns InvalidParameterCombinationException",
			action: "CreateFleet",
			body: map[string]any{
				"Name":         "bad-type-fleet",
				"InstanceType": "stream.standard.medium",
				"FleetType":    "INVALID_TYPE",
			},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidParameterCombinationException",
		},
		// Gap 6b: CreateFleet valid FleetType ALWAYS_ON succeeds
		{
			name:   "CreateFleet with ALWAYS_ON FleetType succeeds",
			action: "CreateFleet",
			body: map[string]any{
				"Name":         "always-on-fleet",
				"InstanceType": "stream.standard.medium",
				"FleetType":    "ALWAYS_ON",
			},
			wantCode: http.StatusOK,
		},
		// Gap 6c: CreateFleet valid FleetType ELASTIC succeeds
		{
			name:   "CreateFleet with ELASTIC FleetType succeeds",
			action: "CreateFleet",
			body: map[string]any{
				"Name":         "elastic-fleet",
				"InstanceType": "stream.standard.medium",
				"FleetType":    "ELASTIC",
			},
			wantCode: http.StatusOK,
		},
		// Gap 7: CreateFleet missing InstanceType → InvalidParameterCombinationException
		{
			name:   "CreateFleet missing InstanceType returns InvalidParameterCombinationException",
			action: "CreateFleet",
			body: map[string]any{
				"Name": "no-instance-fleet",
			},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidParameterCombinationException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}

			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.wantType != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tc.wantType, resp["__type"], "wrong __type in error response")
			}
		})
	}
}
