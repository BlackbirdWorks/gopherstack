package workmail_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workmail"
)

// --- DescribeEntity ---

func TestWorkMail_DescribeEntity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *workmail.Handler)
		name string
	}{
		{
			name: "describe_user_entity_by_id",
			run: func(t *testing.T, h *workmail.Handler) {
				t.Helper()
				orgID := createTestOrg(t, h, "deentorg")
				userID := createTestUser(t, h, orgID, "entuser", "Ent User")
				rec := doOp(t, h, "DescribeEntity", fmt.Sprintf(
					`{"OrganizationId":%q,"Email":%q}`, orgID, userID,
				))
				require.Equal(t, http.StatusOK, rec.Code)
				m := decodeJSON(t, rec)
				assert.Equal(t, userID, m["EntityId"])
				assert.Equal(t, "entuser", m["Name"])
				assert.Equal(t, "USER", m["Type"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.run(t, h)
		})
	}
}
