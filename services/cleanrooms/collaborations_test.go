package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCollaborationHasBothIDKeys verifies that a Collaboration response
// includes both "id" (AWS canonical) and "collaborationIdentifier" (legacy).
func TestCollaborationHasBothIDKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{"CollaborationHasBothIDKeys"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newTestServer(t)
			rec := doRequest(t, e, "POST", "/collaborations", map[string]any{
				"name":                   "dual-key-collab",
				"creatorDisplayName":     "Alice",
				"creatorMemberAbilities": []string{"CAN_QUERY"},
				"members":                []any{},
				"queryLogStatus":         "ENABLED",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			collab := resp["collaboration"].(map[string]any)

			id, hasID := collab["id"]
			legacyID, hasLegacy := collab["collaborationIdentifier"]

			assert.True(t, hasID, "collaboration must have 'id' key (AWS canonical)")
			assert.True(t, hasLegacy, "collaboration must have 'collaborationIdentifier' key (backward compat)")
			assert.Equal(t, id, legacyID, "id and collaborationIdentifier must have the same value")
			assert.NotEmpty(t, id)
		})
	}
}

// TestCollaborationARNFormat verifies ARN format for collaborations.
func TestCollaborationARNFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{"CollaborationARNFormat"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newTestServer(t)
			rec := doRequest(t, e, "POST", "/collaborations", map[string]any{
				"name": "arn-test", "creatorDisplayName": "Grace",
				"creatorMemberAbilities": []string{"CAN_QUERY"},
				"members":                []any{}, "queryLogStatus": "ENABLED",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			collab := resp["collaboration"].(map[string]any)

			arn, ok := collab["arn"].(string)
			require.True(t, ok, "collaboration must have 'arn' field")
			assert.Contains(t, arn, "arn:aws:cleanrooms:")
			assert.Contains(t, arn, ":collaboration/")
		})
	}
}

// TestCollaborationQueryLogStatusRoundtrip verifies queryLogStatus roundtrip.
func TestCollaborationQueryLogStatusRoundtrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{"CollaborationQueryLogStatusRoundtrip"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newTestServer(t)
			rec := doRequest(t, e, "POST", "/collaborations", map[string]any{
				"name": "qls-test", "creatorDisplayName": "Iris",
				"creatorMemberAbilities": []string{"CAN_QUERY"},
				"members":                []any{}, "queryLogStatus": "ENABLED",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			collab := resp["collaboration"].(map[string]any)
			assert.Equal(t, "ENABLED", collab["queryLogStatus"])
		})
	}
}

// TestCollaborationCreatorMemberAbilities verifies creator abilities roundtrip.
func TestCollaborationCreatorMemberAbilities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{"CollaborationCreatorMemberAbilities"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newTestServer(t)
			rec := doRequest(t, e, "POST", "/collaborations", map[string]any{
				"name": "abilities-test", "creatorDisplayName": "Jake",
				"creatorMemberAbilities": []string{"CAN_QUERY", "CAN_RECEIVE_RESULTS"},
				"members":                []any{}, "queryLogStatus": "DISABLED",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			collab := resp["collaboration"].(map[string]any)
			abilities, ok := collab["memberAbilities"].([]any)
			assert.True(t, ok, "collaboration must have memberAbilities")
			assert.Len(t, abilities, 2)
		})
	}
}
