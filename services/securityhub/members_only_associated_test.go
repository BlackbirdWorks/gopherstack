package securityhub_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListMembers_OnlyAssociated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		accept    bool
		wantCount int
	}{
		{name: "default_hides_invited", query: "/members", wantCount: 0},
		{name: "false_lists_invited", query: "/members?OnlyAssociated=false", wantCount: 1},
		{name: "accepted_listed_by_default", query: "/members", accept: true, wantCount: 1},
		{name: "accepted_listed_when_true", query: "/members?OnlyAssociated=true", accept: true, wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			require.Equal(t, http.StatusOK, doRequest(t, h, http.MethodPost, "/members", map[string]any{
				"AccountDetails": []any{map[string]any{"AccountId": "222222222222", "Email": "m@example.com"}},
			}).Code)
			require.Equal(t, http.StatusOK, doRequest(t, h, http.MethodPost, "/members/invite", map[string]any{
				"AccountIds": []any{"222222222222"},
			}).Code)

			if tt.accept {
				rec := doRequest(t, h, http.MethodGet, "/invitations", nil)
				var inv struct {
					Invitations []struct {
						InvitationID string `json:"InvitationId"`
					} `json:"Invitations"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &inv))
				require.Len(t, inv.Invitations, 1)

				require.Equal(t, http.StatusOK, doRequest(t, h, http.MethodPost, "/administrator", map[string]any{
					"AdministratorId": "000000000000", "InvitationId": inv.Invitations[0].InvitationID,
				}).Code)
			}

			rec := doRequest(t, h, http.MethodGet, tt.query, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Members []struct {
					MemberStatus string `json:"MemberStatus"`
				} `json:"Members"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.Members, tt.wantCount)

			if tt.accept {
				assert.Equal(t, "Enabled", out.Members[0].MemberStatus)
			}
		})
	}
}
