package ram_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

func TestGetResourceShareInvitations_Status(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*testing.T, *ram.Handler) string
		wantStatus string
	}{
		{
			name: "pending invitation",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("inv-acc-share", true, nil, nil, nil)
				require.NoError(t, err)
				inv := ram.CreateInvitation(
					h.Backend.(*ram.InMemoryBackend),
					rs.ARN,
					"inv-acc-share",
					"111111111111",
					"222222222222",
				)

				return inv.InvitationARN
			},
			wantStatus: "PENDING",
		},
		{
			name: "accepted invitation",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("inv-acc2-share", true, nil, nil, nil)
				require.NoError(t, err)
				inv := ram.CreateInvitation(
					h.Backend.(*ram.InMemoryBackend),
					rs.ARN,
					"inv-acc2-share",
					"111111111111",
					"222222222222",
				)
				_, err = h.Backend.AcceptResourceShareInvitation(inv.InvitationARN)
				require.NoError(t, err)

				return inv.InvitationARN
			},
			wantStatus: "ACCEPTED",
		},
		{
			name: "rejected invitation",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("inv-rej-share", true, nil, nil, nil)
				require.NoError(t, err)
				inv := ram.CreateInvitation(
					h.Backend.(*ram.InMemoryBackend),
					rs.ARN,
					"inv-rej-share",
					"111111111111",
					"222222222222",
				)
				_, err = h.Backend.RejectResourceShareInvitation(inv.InvitationARN)
				require.NoError(t, err)

				return inv.InvitationARN
			},
			wantStatus: "REJECTED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			invARN := tt.setup(t, h)

			rec := doRAMRequest(t, h, "/getresourceshareinvitations", map[string]any{
				"resourceShareInvitationArns": []string{invARN},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				ResourceShareInvitations []struct {
					Status string `json:"status"`
				} `json:"resourceShareInvitations"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.ResourceShareInvitations, 1)
			assert.Equal(t, tt.wantStatus, resp.ResourceShareInvitations[0].Status)
		})
	}
}

func TestInvitationExpiredStatus_HTTPResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rs, err := h.Backend.CreateResourceShare("exp-share", true, nil, nil, nil)
	require.NoError(t, err)

	inv := ram.CreateInvitation(
		h.Backend.(*ram.InMemoryBackend),
		rs.ARN, "exp-share", "111111111111", "000000000000",
	)
	inv.Status = "EXPIRED"
	ram.AddInvitationInternal(h.Backend.(*ram.InMemoryBackend), &ram.ResourceShareInvitation{
		InvitationARN:     inv.InvitationARN + "-expired",
		ResourceShareARN:  rs.ARN,
		ResourceShareName: "exp-share",
		SenderAccountID:   "111111111111",
		ReceiverAccountID: "000000000000",
		Status:            "EXPIRED",
	})

	// Accept an expired invitation should return 400 with ExpiredException.
	expiredARN := inv.InvitationARN + "-expired"
	rec := doRAMRequest(t, h, "/acceptresourceshareinvitation", map[string]any{
		"resourceShareInvitationArn": expiredARN,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ResourceShareInvitationExpiredException")
}

func TestHandler_AcceptResourceShareInvitation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) string
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("invite-share", true, nil, nil, nil)
				require.NoError(t, err)
				inv := ram.CreateInvitation(
					h.Backend.(*ram.InMemoryBackend),
					rs.ARN,
					"invite-share",
					"111111111111",
					"222222222222",
				)

				return inv.InvitationARN
			},
			wantStatus: http.StatusOK,
			wantBody:   "ACCEPTED",
		},
		{
			name: "already accepted",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("accepted-share", true, nil, nil, nil)
				require.NoError(t, err)
				inv := ram.CreateInvitation(
					h.Backend.(*ram.InMemoryBackend),
					rs.ARN,
					"accepted-share",
					"111111111111",
					"222222222222",
				)
				_, err = h.Backend.AcceptResourceShareInvitation(inv.InvitationARN)
				require.NoError(t, err)

				return inv.InvitationARN
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not found",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return "arn:aws:ram:us-east-1:000000000000:resource-share-invitation/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing invitationArn",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return ""
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			invARN := tt.setup(t, h)

			body := map[string]any{}
			if invARN != "" {
				body["resourceShareInvitationArn"] = invARN
			}

			rec := doRAMRequest(t, h, "/acceptresourceshareinvitation", body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_GetResourceShareInvitations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) (string, string)
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "list all",
			setup: func(t *testing.T, h *ram.Handler) (string, string) {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("inv-share", true, nil, nil, nil)
				require.NoError(t, err)
				inv := ram.CreateInvitation(
					h.Backend.(*ram.InMemoryBackend),
					rs.ARN,
					"inv-share",
					"111111111111",
					"222222222222",
				)

				return inv.InvitationARN, rs.ARN
			},
			wantStatus: http.StatusOK,
			wantBody:   "resourceShareInvitations",
		},
		{
			name: "filter by invitationArn",
			setup: func(t *testing.T, h *ram.Handler) (string, string) {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("inv-share2", true, nil, nil, nil)
				require.NoError(t, err)
				inv := ram.CreateInvitation(
					h.Backend.(*ram.InMemoryBackend),
					rs.ARN,
					"inv-share2",
					"111111111111",
					"222222222222",
				)

				return inv.InvitationARN, ""
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "empty",
			setup: func(_ *testing.T, _ *ram.Handler) (string, string) {
				return "", ""
			},
			wantStatus: http.StatusOK,
			wantBody:   "resourceShareInvitations",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			invARN, shareARN := tt.setup(t, h)

			body := map[string]any{}
			if invARN != "" {
				body["resourceShareInvitationArns"] = []string{invARN}
			}

			if shareARN != "" {
				body["resourceShareArns"] = []string{shareARN}
			}

			rec := doRAMRequest(t, h, "/getresourceshareinvitations", body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestRAMPagination_GetResourceShareInvitations covers invitation pagination.
func TestGetResourceShareInvitations_Pagination(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(b)

	// Seed invitations.
	for i := range 5 {
		shareARN := fmt.Sprintf("arn:aws:ram:us-east-1:111111111111:resource-share/inv-share-%d", i)
		invARN := fmt.Sprintf("arn:aws:ram:us-east-1:111111111111:resource-share-invitation/inv-%d", i)
		ram.AddInvitationInternal(b, ram.NewTestInvitation(invARN, shareARN, fmt.Sprintf("share-%d", i)))
	}

	tests := []struct {
		maxResults *int32
		name       string
		wantTotal  int
		wantPages  int
		wantError  bool
	}{
		{
			name:       "maxResults=2 paginates 5 invitations",
			maxResults: ptr32(2),
			wantTotal:  5,
			wantPages:  3,
		},
		{
			name:       "maxResults=5 returns all in one page",
			maxResults: ptr32(5),
			wantTotal:  5,
			wantPages:  1,
		},
		{
			name:       "maxResults=0 returns error",
			maxResults: ptr32(0),
			wantError:  true,
		},
	}

	type reqBody struct {
		MaxResults *int32 `json:"maxResults,omitempty"`
		NextToken  string `json:"nextToken,omitempty"`
	}

	type respBody struct {
		NextToken                string `json:"nextToken"`
		ResourceShareInvitations []any  `json:"resourceShareInvitations"`
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			nextToken := ""
			totalSeen := 0
			pages := 0

			for {
				req := reqBody{MaxResults: tc.maxResults, NextToken: nextToken}
				rec := doRAMRequest(t, h, "/getresourceshareinvitations", req)

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

				pages++
				totalSeen += len(resp.ResourceShareInvitations)
				nextToken = resp.NextToken

				if nextToken == "" {
					break
				}
			}

			assert.Equal(t, tc.wantTotal, totalSeen)
			assert.Equal(t, tc.wantPages, pages)
		})
	}
}

// TestRefinement1_AcceptInvitation_HTTP verifies the full HTTP roundtrip.
func TestAcceptInvitation_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	shareARN := "arn:aws:ram:us-east-1:000000000000:resource-share/accept-test"
	invARN := "arn:aws:ram:us-east-1:000000000000:resource-share-invitation/inv-accept"
	ram.AddInvitationInternal(h.Backend.(*ram.InMemoryBackend), ram.NewTestInvitation(invARN, shareARN, "accept-share"))

	rec := doRAMRequest(t, h, "/acceptresourceshareinvitation", map[string]any{
		"resourceShareInvitationArn": invARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		ResourceShareInvitation struct {
			Status string `json:"status"`
		} `json:"resourceShareInvitation"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ACCEPTED", resp.ResourceShareInvitation.Status)

	// Double-accept should return AlreadyAccepted.
	rec2 := doRAMRequest(t, h, "/acceptresourceshareinvitation", map[string]any{
		"resourceShareInvitationArn": invARN,
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "AlreadyAccepted")
}

// TestRefinement1_ErrInvitationNotFound_HTTP verifies 400 for missing invitation.
func TestErrInvitationNotFound_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/acceptresourceshareinvitation", map[string]any{
		"resourceShareInvitationArn": "arn:aws:ram:us-east-1:000000000000:resource-share-invitation/missing",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ResourceShareInvitationArnNotFoundException")
}

func TestInvitationOps_Smoke(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateResourceShare("inv-share", true, nil, nil, nil)
	require.NoError(t, err)

	// RejectResourceShareInvitation (no invitations, exercises the code path)
	rec := doRAMRequest(t, h, "/rejectresourceshareinvitation", map[string]any{
		"resourceShareInvitationArn": "arn:aws:ram:us-east-1:123456789012:resource-share-invitation/nonexistent",
	})
	assert.True(t, rec.Code >= 200 && rec.Code < 300 || rec.Code == 400)
}
