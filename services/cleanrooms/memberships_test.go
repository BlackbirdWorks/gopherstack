package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createMembership(t *testing.T, e *echo.Echo, colID string, queryLog string) (map[string]any, string) {
	t.Helper()
	rec := doRequest(t, e, "POST", "/memberships", map[string]any{
		"collaborationIdentifier": colID,
		"queryLogStatus":          queryLog,
		"memberAbilities":         []string{"CAN_QUERY", "CAN_RECEIVE_RESULTS"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	mem := resp["membership"].(map[string]any)

	return mem, mem["id"].(string)
}

func createBaseCollaboration(t *testing.T, e *echo.Echo, name string) string {
	t.Helper()
	doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name": name, "creatorDisplayName": "User",
		"creatorMemberAbilities": []string{"CAN_QUERY"},
		"members":                []any{}, "queryLogStatus": "DISABLED",
	})
	var colResp map[string]any
	rec := doRequest(t, e, "GET", "/collaborations", nil)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &colResp))

	return colResp["collaborationList"].([]any)[0].(map[string]any)["id"].(string)
}

func TestMemberships_Create(t *testing.T) {
	t.Parallel()

	type args struct {
		queryLogStatus string
	}
	type wants struct {
		status int
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name:  "valid_create_disabled",
			args:  args{queryLogStatus: "DISABLED"},
			wants: wants{status: http.StatusOK},
		},
		{
			name:  "valid_create_enabled",
			args:  args{queryLogStatus: "ENABLED"},
			wants: wants{status: http.StatusOK},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			colID := createBaseCollaboration(t, e, "m-collab")

			mem, id := createMembership(t, e, colID, tt.args.queryLogStatus)

			_, hasInventedID := mem["membershipIdentifier"]
			collabID, hasCollabID := mem["collaborationId"]
			_, hasInventedCollabID := mem["collaborationIdentifier"]

			assert.False(t, hasInventedID, "membership must not have invented 'membershipIdentifier' key")
			assert.True(t, hasCollabID, "membership must have 'collaborationId' key (AWS canonical)")
			assert.False(t, hasInventedCollabID, "membership must not have invented 'collaborationIdentifier' key")
			assert.Equal(t, colID, collabID)
			assert.NotEmpty(t, id)

			payCfg, hasPayCfg := mem["paymentConfiguration"]
			assert.True(t, hasPayCfg, "membership must have required 'paymentConfiguration' key")
			assert.NotNil(t, payCfg)

			abilities, ok := mem["memberAbilities"].([]any)
			assert.True(t, ok, "memberAbilities must be present")
			assert.Len(t, abilities, 2)

			arn, ok := mem["arn"].(string)
			require.True(t, ok)
			assert.Contains(t, arn, "arn:aws:cleanrooms:")
			assert.Contains(t, arn, ":membership/")
		})
	}
}

func TestMemberships_List(t *testing.T) {
	t.Parallel()

	type args struct{}
	type wants struct {
		status int
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name:  "list_all",
			args:  args{},
			wants: wants{status: http.StatusOK},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			colID := createBaseCollaboration(t, e, "list-m-collab")
			// CreateCollaboration auto-creates a membership for the creator (real
			// AWS behavior, matches Collaboration.membershipArn/membershipId), so
			// this explicit CreateMembership call adds a second one.
			createMembership(t, e, colID, "DISABLED")

			rec := doRequest(t, e, "GET", "/memberships", nil)
			require.Equal(t, tt.wants.status, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			summaries := listResp["membershipSummaries"].([]any)
			require.Len(t, summaries, 2)
			summary := summaries[0].(map[string]any)

			assert.Contains(t, summary, "id", "membership summary must have canonical 'id' key")
			assert.Contains(
				t,
				summary,
				"collaborationId",
				"membership summary must have canonical 'collaborationId' key",
			)
			assert.Equal(t, colID, summary["collaborationId"])
		})
	}
}

func TestMemberships_Update(t *testing.T) {
	t.Parallel()

	type args struct {
		body map[string]any
	}
	type wants struct {
		status int
	}

	tests := []struct {
		args  args
		name  string
		wants wants
	}{
		{
			name: "valid_update",
			args: args{
				body: map[string]any{
					"queryLogStatus": "ENABLED",
				},
			},
			wants: wants{status: http.StatusOK},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			colID := createBaseCollaboration(t, e, "up-m-collab")
			_, memID := createMembership(t, e, colID, "DISABLED")

			rec := doRequest(t, e, "PATCH", "/memberships/"+memID, tt.args.body)
			require.Equal(t, tt.wants.status, rec.Code)
		})
	}
}
