package ram_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

func TestHandler_AssociateResourceShare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "associate principal",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("assoc-share", true, nil, nil, nil)
				require.NoError(t, err)

				return rs.ARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return "arn:aws:ram:us-east-1:000000000000:resource-share/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			shareARN := tt.setup(t, h)

			rec := doRAMRequest(t, h, "/associateresourceshare", map[string]any{
				"resourceShareArn": shareARN,
				"principals":       []string{"123456789012"},
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DisassociateResourceShare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "disassociate principal",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("disassoc-share", true, nil, []string{"123456789012"}, nil)
				require.NoError(t, err)

				return rs.ARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return "arn:aws:ram:us-east-1:000000000000:resource-share/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			shareARN := tt.setup(t, h)

			rec := doRAMRequest(t, h, "/disassociateresourceshare", map[string]any{
				"resourceShareArn": shareARN,
				"principals":       []string{"123456789012"},
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetResourceShareAssociations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) string
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "list associations",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("assoc-list-share", true, nil, []string{"123456789012"}, nil)
				require.NoError(t, err)

				return rs.ARN
			},
			wantStatus: http.StatusOK,
			wantBody:   "123456789012",
		},
		{
			name: "empty",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return ""
			},
			wantStatus: http.StatusOK,
			wantBody:   "resourceShareAssociations",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			shareARN := tt.setup(t, h)

			body := map[string]any{"associationType": "PRINCIPAL"}
			if shareARN != "" {
				body["resourceShareArns"] = []string{shareARN}
			}

			rec := doRAMRequest(t, h, "/getresourceshareassociations", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestHandler_GetResourceShareAssociations_AssociationStatusFilter verifies that
// the associationStatus request field (supported by the real AWS API) filters
// results, so a caller polling for DISASSOCIATED entries doesn't see ASSOCIATED
// ones mixed in and vice versa.
func TestHandler_GetResourceShareAssociations_AssociationStatusFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// One share stays active (its principal association stays ASSOCIATED);
	// the other is deleted, which soft-deletes its association to DISASSOCIATED.
	activeShare, err := h.Backend.CreateResourceShare(
		"assoc-status-active", true, nil, []string{"111111111111"}, nil,
	)
	require.NoError(t, err)

	deletedShare, err := h.Backend.CreateResourceShare(
		"assoc-status-deleted", true, nil, []string{"222222222222"}, nil,
	)
	require.NoError(t, err)
	require.NoError(t, h.Backend.DeleteResourceShare(deletedShare.ARN))

	shareARNs := []string{activeShare.ARN, deletedShare.ARN}

	rec := doRAMRequest(t, h, "/getresourceshareassociations", map[string]any{
		"associationType":   "PRINCIPAL",
		"associationStatus": "ASSOCIATED",
		"resourceShareArns": shareARNs,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "111111111111")
	assert.NotContains(t, rec.Body.String(), "222222222222")

	rec = doRAMRequest(t, h, "/getresourceshareassociations", map[string]any{
		"associationType":   "PRINCIPAL",
		"associationStatus": "DISASSOCIATED",
		"resourceShareArns": shareARNs,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "222222222222")
	assert.NotContains(t, rec.Body.String(), "111111111111")
}

func TestHandler_AssociateResourceShare_MissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/associateresourceshare", map[string]any{
		"principals": []string{"123456789012"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DisassociateResourceShare_MissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/disassociateresourceshare", map[string]any{
		"principals": []string{"123456789012"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGetResourceShareAssociations_Filters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		associationType string
		setupPrincipals []string
		setupResources  []string
		wantCount       int
	}{
		{
			name:            "PRINCIPAL type returns only principals",
			associationType: "PRINCIPAL",
			setupPrincipals: []string{"111111111111", "222222222222"},
			setupResources:  []string{"arn:aws:ec2:us-east-1:123456789012:subnet/sub-1"},
			wantCount:       2,
		},
		{
			name:            "RESOURCE type returns only resources",
			associationType: "RESOURCE",
			setupPrincipals: []string{"111111111111"},
			setupResources: []string{
				"arn:aws:ec2:us-east-1:123456789012:subnet/sub-1",
				"arn:aws:ec2:us-east-1:123456789012:subnet/sub-2",
			},
			wantCount: 2,
		},
		{
			name:            "no type filter returns all",
			associationType: "",
			setupPrincipals: []string{"111111111111"},
			setupResources:  []string{"arn:aws:ec2:us-east-1:123456789012:subnet/sub-1"},
			wantCount:       2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rs, err := h.Backend.CreateResourceShare(
				"assoc-filter-share",
				true,
				nil,
				tt.setupPrincipals,
				tt.setupResources,
			)
			require.NoError(t, err)

			body := map[string]any{
				"resourceShareArns": []string{rs.ARN},
			}
			if tt.associationType != "" {
				body["associationType"] = tt.associationType
			}

			rec := doRAMRequest(t, h, "/getresourceshareassociations", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				ResourceShareAssociations []json.RawMessage `json:"resourceShareAssociations"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.ResourceShareAssociations, tt.wantCount)
		})
	}
}

func TestAssociationStatusMessage_InResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rs, err := h.Backend.CreateResourceShare("status-msg-share", false, nil, nil, nil)
	require.NoError(t, err)

	_, err = h.Backend.AssociateResourceShare(rs.ARN, nil,
		[]string{"arn:aws:ec2:us-east-1:000000000000:subnet/sub-1"})
	require.NoError(t, err)

	rec := doRAMRequest(t, h, "/getresourceshareassociations", map[string]any{
		"resourceShareArns": []string{rs.ARN},
		"associationType":   "RESOURCE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		ResourceShareAssociations []struct {
			Status        string `json:"status"`
			StatusMessage string `json:"statusMessage"`
		} `json:"resourceShareAssociations"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.NotEmpty(t, resp.ResourceShareAssociations)

	// statusMessage is optional; verify the field exists in JSON (can be empty).
	// We check by re-parsing as raw map.
	var raw struct {
		ResourceShareAssociations []map[string]json.RawMessage `json:"resourceShareAssociations"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	require.NotEmpty(t, raw.ResourceShareAssociations)
	// statusMessage should NOT appear when empty (omitempty).
	_, present := raw.ResourceShareAssociations[0]["statusMessage"]
	assert.False(t, present, "statusMessage should be omitted when empty")
}

// TestRAMPagination_GetResourceShareAssociations covers PRINCIPAL associations.
func TestGetResourceShareAssociations_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxResults *int32
		name       string
		wantError  bool
	}{
		{
			name:       "maxResults=1 pages through all associations",
			maxResults: ptr32(1),
		},
		{
			name:       "maxResults=0 returns error",
			maxResults: ptr32(0),
			wantError:  true,
		},
		{
			name:       "maxResults=100 returns all",
			maxResults: ptr32(100),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend("000000000000", "us-east-1")
			h := ram.NewHandler(b)

			// Create 3 shares, each with a PRINCIPAL association (same account = internal).
			shareARNs := makeNShares(t, b, 3)

			for _, shareARN := range shareARNs {
				body := map[string]any{
					"resourceShareArn": shareARN,
					"principals":       []string{"000000000000"},
				}
				rec := doRAMRequest(t, h, "/associateresourceshare", body)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			type reqBody struct {
				AssociationType string `json:"associationType"`
				MaxResults      *int32 `json:"maxResults,omitempty"`
				NextToken       string `json:"nextToken,omitempty"`
			}

			type respBody struct {
				NextToken                 string `json:"nextToken"`
				ResourceShareAssociations []any  `json:"resourceShareAssociations"`
			}

			nextToken := ""
			totalSeen := 0

			for {
				req := reqBody{
					AssociationType: "PRINCIPAL",
					MaxResults:      tc.maxResults,
					NextToken:       nextToken,
				}

				rec := doRAMRequest(t, h, "/getresourceshareassociations", req)

				if tc.wantError {
					assert.Equal(t, http.StatusBadRequest, rec.Code)

					var errResp map[string]string
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
					assert.Equal(t, "InvalidParameterException", errResp["__type"])

					return
				}

				require.Equal(t, http.StatusOK, rec.Code)

				var resp respBody
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				totalSeen += len(resp.ResourceShareAssociations)
				nextToken = resp.NextToken

				if nextToken == "" {
					break
				}
			}

			assert.Equal(t, 3, totalSeen)
		})
	}
}
