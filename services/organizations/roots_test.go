package organizations_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_ListRoots tests the HTTP handler for ListRoots.
func TestHandler_ListRoots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		wantRoots  bool
	}{
		{
			name:       "lists_roots_after_org_creation",
			wantStatus: http.StatusOK,
			wantRoots:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

			rec := doRequest(t, h, "ListRoots", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantRoots {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				roots, ok := resp["Roots"].([]any)
				require.True(t, ok, "response should contain Roots")
				assert.NotEmpty(t, roots)
			}
		})
	}
}

// TestListRoots_DeepCopy verifies ListRoots returns a copy, not a reference.
func TestListRoots_DeepCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "mutating_returned_root_does_not_affect_backend"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			createOrgOn(t, b)

			roots, err := b.ListRoots()
			require.NoError(t, err)
			require.Len(t, roots, 1)

			originalName := roots[0].Name
			roots[0].Name = "mutated"

			roots2, err := b.ListRoots()
			require.NoError(t, err)

			assert.Equal(t, originalName, roots2[0].Name, tt.name)
		})
	}
}
