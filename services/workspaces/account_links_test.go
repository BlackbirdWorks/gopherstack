package workspaces_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	wssdk "github.com/aws/aws-sdk-go-v2/service/workspaces"
	"github.com/stretchr/testify/require"
)

func TestAccountLinkLifecycle(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		name            string
		targetAccountID string
		action          string // "accept" or "reject"
	}{
		{name: "accept link", targetAccountID: "999988887777", action: "accept"},
		{name: "reject link", targetAccountID: "111100002222", action: "reject"},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newTestHandlerWithBackend(t)

			// Create
			rec := doTargetRequest(t, h, "CreateAccountLinkInvitation", map[string]any{
				"TargetAccountId": tc.targetAccountID,
				"ClientToken":     "tok-123",
			})
			if rec.Code != http.StatusOK {
				t.Fatalf("create: expected 200, got %d: %s", rec.Code, rec.Body)
			}

			var createOut struct {
				AccountLink map[string]string `json:"AccountLink"`
			}
			decodeJSON(t, rec.Body.Bytes(), &createOut)

			linkID := createOut.AccountLink["AccountLinkId"]
			if linkID == "" {
				t.Fatal("expected non-empty AccountLinkId")
			}

			if _, ok := createOut.AccountLink["LinkId"]; ok {
				t.Fatal("LinkId is not a field on the real AccountLink type")
			}

			if createOut.AccountLink["AccountLinkStatus"] != "PENDING_ACCEPTANCE_BY_TARGET_ACCOUNT" {
				t.Fatalf(
					"expected PENDING_ACCEPTANCE_BY_TARGET_ACCOUNT, got %s",
					createOut.AccountLink["AccountLinkStatus"],
				)
			}

			// GetAccountLink
			rec2 := doTargetRequest(t, h, "GetAccountLink", map[string]any{
				"LinkId": linkID,
			})
			if rec2.Code != http.StatusOK {
				t.Fatalf("get: expected 200, got %d", rec2.Code)
			}

			// ListAccountLinks
			rec3 := doTargetRequest(t, h, "ListAccountLinks", map[string]any{})
			if rec3.Code != http.StatusOK {
				t.Fatalf("list: expected 200, got %d", rec3.Code)
			}

			var listOut struct {
				AccountLinks []map[string]string `json:"AccountLinks"`
			}
			decodeJSON(t, rec3.Body.Bytes(), &listOut)

			if len(listOut.AccountLinks) != 1 {
				t.Fatalf("expected 1 link, got %d", len(listOut.AccountLinks))
			}

			// Accept or Reject
			var actionOp string
			var expectedStatus string

			if tc.action == "accept" {
				actionOp = "AcceptAccountLinkInvitation"
				expectedStatus = "LINKED"
			} else {
				actionOp = "RejectAccountLinkInvitation"
				expectedStatus = "REJECTED"
			}

			rec4 := doTargetRequest(t, h, actionOp, map[string]any{
				"LinkId": linkID,
			})
			if rec4.Code != http.StatusOK {
				t.Fatalf("%s: expected 200, got %d", actionOp, rec4.Code)
			}

			var actionOut struct {
				AccountLink map[string]string `json:"AccountLink"`
			}
			decodeJSON(t, rec4.Body.Bytes(), &actionOut)

			if actionOut.AccountLink["AccountLinkStatus"] != expectedStatus {
				t.Fatalf(
					"expected %s, got %s", expectedStatus, actionOut.AccountLink["AccountLinkStatus"],
				)
			}

			// Delete
			rec5 := doTargetRequest(t, h, "DeleteAccountLinkInvitation", map[string]any{
				"LinkId": linkID,
			})
			if rec5.Code != http.StatusOK {
				t.Fatalf("delete: expected 200, got %d", rec5.Code)
			}

			var delOut struct {
				AccountLink map[string]string `json:"AccountLink"`
			}
			decodeJSON(t, rec5.Body.Bytes(), &delOut)

			// "DELETED" is not a member of the real AccountLinkStatusEnum; the
			// deleted link keeps whatever status it already had (set by the
			// accept/reject step above) rather than a fabricated one.
			if delOut.AccountLink["AccountLinkStatus"] != expectedStatus {
				t.Fatalf(
					"expected %s, got %s", expectedStatus, delOut.AccountLink["AccountLinkStatus"],
				)
			}
		})
	}
}

// TestListAccountLinks_Pagination proves the op pages through every account
// link exactly once instead of returning them all on a single page with no
// cursor.
func TestListAccountLinks_Pagination(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	targets := []string{"111111111111", "222222222222", "333333333333"}
	for _, tgt := range targets {
		_, err := client.CreateAccountLinkInvitation(ctx, &wssdk.CreateAccountLinkInvitationInput{
			TargetAccountId: aws.String(tgt),
		})
		require.NoError(t, err)
	}

	page1, err := client.ListAccountLinks(ctx, &wssdk.ListAccountLinksInput{
		MaxResults: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, page1.AccountLinks, 2)
	require.NotNil(t, page1.NextToken, "first page must return a cursor when more links remain")

	page2, err := client.ListAccountLinks(ctx, &wssdk.ListAccountLinksInput{
		MaxResults: aws.Int32(2),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.AccountLinks, 1)
	require.Empty(t, aws.ToString(page2.NextToken))

	seen := map[string]bool{}
	for _, l := range page1.AccountLinks {
		seen[aws.ToString(l.AccountLinkId)] = true
	}

	for _, l := range page2.AccountLinks {
		id := aws.ToString(l.AccountLinkId)
		require.False(t, seen[id], "link %s returned on both pages", id)
		seen[id] = true
	}

	require.Len(t, seen, len(targets))
}
