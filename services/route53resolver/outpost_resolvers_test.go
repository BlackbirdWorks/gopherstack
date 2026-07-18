package route53resolver_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutpostResolverCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "full_crud", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create.
			rec := doRequest(t, h, "CreateOutpostResolver", map[string]any{
				"Name":                  "op-res-1",
				"OutpostArn":            "arn:aws:outposts:us-east-1:000000000000:outpost/op-abc",
				"PreferredInstanceType": "m5.xlarge",
			})
			require.Equal(t, tt.wantCode, rec.Code)
			resp := decodeJSON(t, rec)
			r, ok := resp["OutpostResolver"].(map[string]any)
			require.True(t, ok)
			id := r["Id"].(string)

			// GetOutpostResolver.
			rec = doRequest(t, h, "GetOutpostResolver", map[string]any{"Id": id})
			assert.Equal(t, http.StatusOK, rec.Code)

			// ListOutpostResolvers.
			rec = doRequest(t, h, "ListOutpostResolvers", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)
			listResp := decodeJSON(t, rec)
			resolvers, _ := listResp["OutpostResolvers"].([]any)
			assert.Len(t, resolvers, 1)

			// UpdateOutpostResolver.
			rec = doRequest(t, h, "UpdateOutpostResolver", map[string]any{
				"Id":   id,
				"Name": "op-res-1-updated",
			})
			assert.Equal(t, http.StatusOK, rec.Code)
			updateResp := decodeJSON(t, rec)
			updated, _ := updateResp["OutpostResolver"].(map[string]any)
			assert.Equal(t, "op-res-1-updated", updated["Name"])

			// DeleteOutpostResolver.
			rec = doRequest(t, h, "DeleteOutpostResolver", map[string]any{"Id": id})
			assert.Equal(t, http.StatusOK, rec.Code)

			// Verify deleted.
			rec = doRequest(t, h, "GetOutpostResolver", map[string]any{"Id": id})
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestOutpostResolverErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "get_missing_id",
			action:   "GetOutpostResolver",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get_not_found",
			action:   "GetOutpostResolver",
			body:     map[string]any{"Id": "rslvr-op-notexist"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "delete_missing_id",
			action:   "DeleteOutpostResolver",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "delete_not_found",
			action:   "DeleteOutpostResolver",
			body:     map[string]any{"Id": "rslvr-op-notexist"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "update_missing_id",
			action:   "UpdateOutpostResolver",
			body:     map[string]any{"Name": "new"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "update_not_found",
			action:   "UpdateOutpostResolver",
			body:     map[string]any{"Id": "rslvr-op-notexist", "Name": "new"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// --- DeleteResolverQueryLogConfig / ListResolverQueryLogConfigs ---

func TestCreateOutpostResolver_DefaultInstanceCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateOutpostResolver", map[string]any{
		"Name":                  "my-outpost",
		"OutpostArn":            "arn:aws:outposts:us-east-1:000000000000:outpost/op-abc",
		"PreferredInstanceType": "m5.large",
		"InstanceCount":         0, // should default to 4
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	resolver, ok := resp["OutpostResolver"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, 4, resolver["InstanceCount"], 0)
}

// --- Persistence snapshot with all maps ---

func TestCreateOutpostResolver(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          any
		name          string
		wantName      string
		wantCode      int
		wantInstances int32
	}{
		{
			name: "success_default_instance_count",
			body: map[string]any{
				"Name":                  "my-outpost-resolver",
				"OutpostArn":            "arn:aws:outposts:us-east-1:000000000000:outpost/op-1234",
				"PreferredInstanceType": "m5.xlarge",
				"CreatorRequestId":      "req-op-1",
			},
			wantCode:      http.StatusOK,
			wantName:      "my-outpost-resolver",
			wantInstances: 4,
		},
		{
			name: "success_custom_instance_count",
			body: map[string]any{
				"Name":                  "my-outpost-resolver-6",
				"OutpostArn":            "arn:aws:outposts:us-east-1:000000000000:outpost/op-5678",
				"PreferredInstanceType": "m5.2xlarge",
				"InstanceCount":         6,
				"CreatorRequestId":      "req-op-2",
			},
			wantCode:      http.StatusOK,
			wantName:      "my-outpost-resolver-6",
			wantInstances: 6,
		},
		{
			name:     "missing_name",
			body:     map[string]any{"OutpostArn": "arn:aws:outposts:x", "PreferredInstanceType": "m5.xlarge"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_outpost_arn",
			body:     map[string]any{"Name": "resolver", "PreferredInstanceType": "m5.xlarge"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_instance_type",
			body:     map[string]any{"Name": "resolver", "OutpostArn": "arn:aws:outposts:x"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateOutpostResolver", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				r, ok := resp["OutpostResolver"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, r["Name"])
				assert.Equal(t, "OPERATIONAL", r["Status"])
				assert.Equal(t, tt.wantInstances, int32(r["InstanceCount"].(float64)))
				assert.NotEmpty(t, r["Id"])
				assert.Contains(t, r["Arn"].(string), "arn:aws:route53resolver:")
			}
		})
	}
}

// --- Persistence round-trip for new types ---

// TestParity_CreateOutpostResolver_RequiresFields verifies that
// CreateOutpostResolver rejects requests missing Name, OutpostArn, or
// PreferredInstanceType. Real AWS returns 400; the emulator had the validation
// but lacked handler-level tests.
func TestCreateOutpostResolver_RequiresFields(t *testing.T) {
	t.Parallel()

	const validOutpostArn = "arn:aws:outposts:us-east-1:000000000000:outpost/op-abc"
	const validInstanceType = "m5.xlarge"

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "missing_name_rejected",
			body: map[string]any{
				"OutpostArn":            validOutpostArn,
				"PreferredInstanceType": validInstanceType,
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_outpost_arn_rejected",
			body: map[string]any{
				"Name":                  "my-outpost-resolver",
				"PreferredInstanceType": validInstanceType,
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_preferred_instance_type_rejected",
			body: map[string]any{
				"Name":       "my-outpost-resolver",
				"OutpostArn": validOutpostArn,
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "valid_request_accepted",
			body: map[string]any{
				"Name":                  "my-outpost-resolver",
				"OutpostArn":            validOutpostArn,
				"PreferredInstanceType": validInstanceType,
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateOutpostResolver", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"CreateOutpostResolver status for case %q", tt.name)

			if tt.wantCode == http.StatusOK {
				require.Equal(t, http.StatusOK, rec.Code)
			}
		})
	}
}

// TestParity_ListOutpostResolvers_Pagination verifies NextToken/MaxResults on
// ListOutpostResolvers.
func TestListOutpostResolvers_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		rec := doRequest(t, h, "CreateOutpostResolver", map[string]any{
			"Name":                  fmt.Sprintf("op-res-%d", i),
			"OutpostArn":            fmt.Sprintf("arn:aws:outposts:us-east-1:000000000000:outpost/op-%d", i),
			"PreferredInstanceType": "m5.xlarge",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		body          map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			body:          map[string]any{},
			wantLen:       3,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			body:          map[string]any{"MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, "ListOutpostResolvers", tt.body)
			require.Equal(t, http.StatusOK, listRec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			resolvers, _ := out["OutpostResolvers"].([]any)
			assert.Len(t, resolvers, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}
