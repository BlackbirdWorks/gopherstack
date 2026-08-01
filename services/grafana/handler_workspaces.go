package grafana

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const grafanaDefaultPageSize = 100

func (h *Handler) handleCreateWorkspace(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req createWorkspaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, validationError("invalid request body: " + err.Error())
	}

	w, err := h.Backend.CreateWorkspace(&req)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyWorkspace: toWorkspaceDescriptionWire(w)})
}

func (h *Handler) handleDescribeWorkspace(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	w, err := h.Backend.DescribeWorkspace(segs[1])
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyWorkspace: toWorkspaceDescriptionWire(w)})
}

func (h *Handler) handleUpdateWorkspace(_ context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	var req updateWorkspaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, validationError("invalid request body: " + err.Error())
	}

	w, err := h.Backend.UpdateWorkspace(segs[1], &req)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyWorkspace: toWorkspaceDescriptionWire(w)})
}

func (h *Handler) handleDeleteWorkspace(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	w, err := h.Backend.DeleteWorkspace(segs[1])
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyWorkspace: toWorkspaceDescriptionWire(w)})
}

func (h *Handler) handleListWorkspaces(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	q := r.URL.Query()
	maxResults := queryMaxResults(q)
	token := q.Get("nextToken")

	all := h.Backend.ListWorkspaces()
	summaries := make([]workspaceSummaryWire, len(all))

	for i, w := range all {
		summaries[i] = toWorkspaceSummaryWire(w)
	}

	p := page.New(summaries, token, maxResults, grafanaDefaultPageSize)

	out := map[string]any{segWorkspaces: p.Data}
	if p.Next != "" {
		out["nextToken"] = p.Next
	}

	return json.Marshal(out)
}
