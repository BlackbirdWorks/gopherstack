package grafana

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// createWorkspaceServiceAccountRequest mirrors
// CreateWorkspaceServiceAccountInput's JSON body.
type createWorkspaceServiceAccountRequest struct {
	Name        string `json:"name"`
	GrafanaRole string `json:"grafanaRole"`
}

func (h *Handler) handleCreateWorkspaceServiceAccount(_ context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	var req createWorkspaceServiceAccountRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, validationError("invalid request body: " + err.Error())
	}

	sa, err := h.Backend.CreateWorkspaceServiceAccount(segs[1], req.Name, req.GrafanaRole)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"id":           sa.ID,
		"name":         sa.Name,
		"grafanaRole":  sa.GrafanaRole,
		keyWorkspaceID: sa.WorkspaceID,
	})
}

func (h *Handler) handleDeleteWorkspaceServiceAccount(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	workspaceID, serviceAccountID := segs[1], segs[3]

	if err := h.Backend.DeleteWorkspaceServiceAccount(workspaceID, serviceAccountID); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyServiceAccountID: serviceAccountID,
		keyWorkspaceID:      workspaceID,
	})
}

func (h *Handler) handleListWorkspaceServiceAccounts(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	workspaceID := segs[1]
	q := r.URL.Query()
	maxResults := queryMaxResults(q)
	token := q.Get("nextToken")

	accounts, err := h.Backend.ListWorkspaceServiceAccounts(workspaceID)
	if err != nil {
		return nil, err
	}

	items := make([]serviceAccountSummaryWire, len(accounts))
	for i, sa := range accounts {
		items[i] = toServiceAccountSummaryWire(sa)
	}

	p := page.New(items, token, maxResults, grafanaDefaultPageSize)

	out := map[string]any{"serviceAccounts": p.Data, keyWorkspaceID: workspaceID}
	if p.Next != "" {
		out["nextToken"] = p.Next
	}

	return json.Marshal(out)
}
