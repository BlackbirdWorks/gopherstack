package workspaces_test

import (
	"net/http"
	"testing"
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

			linkID := createOut.AccountLink["LinkId"]
			if linkID == "" {
				t.Fatal("expected non-empty LinkId")
			}

			if createOut.AccountLink["Status"] != "PENDING_ACCEPTANCE" {
				t.Fatalf("expected PENDING_ACCEPTANCE, got %s", createOut.AccountLink["Status"])
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

			if actionOut.AccountLink["Status"] != expectedStatus {
				t.Fatalf("expected %s, got %s", expectedStatus, actionOut.AccountLink["Status"])
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

			if delOut.AccountLink["Status"] != "DELETED" {
				t.Fatalf("expected DELETED, got %s", delOut.AccountLink["Status"])
			}
		})
	}
}
