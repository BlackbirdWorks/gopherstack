package grafana

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// createWorkspaceServiceAccountTokenRequest mirrors
// CreateWorkspaceServiceAccountTokenInput's JSON body.
type createWorkspaceServiceAccountTokenRequest struct {
	Name          string `json:"name"`
	SecondsToLive int32  `json:"secondsToLive"`
}

func (h *Handler) handleCreateWorkspaceServiceAccountToken(
	_ context.Context, r *http.Request, body []byte,
) ([]byte, error) {
	segs := rawPathSegments(r)
	workspaceID, serviceAccountID := segs[1], segs[3]

	var req createWorkspaceServiceAccountTokenRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, validationError("invalid request body: " + err.Error())
	}

	tok, err := h.Backend.CreateWorkspaceServiceAccountToken(workspaceID, serviceAccountID, req.Name, req.SecondsToLive)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyServiceAccountID: tok.ServiceAccountID,
		keyWorkspaceID:      tok.WorkspaceID,
		"serviceAccountToken": serviceAccountTokenSummaryWithKeyWire{
			ID:   tok.ID,
			Name: tok.Name,
			Key:  tok.Key,
		},
	})
}

func (h *Handler) handleDeleteWorkspaceServiceAccountToken(
	_ context.Context, r *http.Request, _ []byte,
) ([]byte, error) {
	segs := rawPathSegments(r)
	workspaceID, serviceAccountID, tokenID := segs[1], segs[3], segs[5]

	if err := h.Backend.DeleteWorkspaceServiceAccountToken(workspaceID, serviceAccountID, tokenID); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyServiceAccountID: serviceAccountID,
		"tokenId":           tokenID,
		keyWorkspaceID:      workspaceID,
	})
}

func (h *Handler) handleListWorkspaceServiceAccountTokens(
	_ context.Context, r *http.Request, _ []byte,
) ([]byte, error) {
	segs := rawPathSegments(r)
	workspaceID, serviceAccountID := segs[1], segs[3]
	q := r.URL.Query()
	maxResults := queryMaxResults(q)
	token := q.Get("nextToken")

	toks, err := h.Backend.ListWorkspaceServiceAccountTokens(workspaceID, serviceAccountID)
	if err != nil {
		return nil, err
	}

	items := make([]serviceAccountTokenSummaryWire, len(toks))
	for i, t := range toks {
		items[i] = toServiceAccountTokenSummaryWire(t)
	}

	p := page.New(items, token, maxResults, grafanaDefaultPageSize)

	out := map[string]any{
		"serviceAccountTokens": p.Data,
		keyServiceAccountID:    serviceAccountID,
		keyWorkspaceID:         workspaceID,
	}
	if p.Next != "" {
		out["nextToken"] = p.Next
	}

	return json.Marshal(out)
}
