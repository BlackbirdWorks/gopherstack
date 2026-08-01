package grafana

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// workspaceIDQueryParam is the query parameter ListVersionsInput.WorkspaceId
// is bound to on the wire (confirmed via serializers.go:
// encoder.SetQuery("workspace-id") in
// awsRestjson1_serializeOpHttpBindingsListVersionsInput) -- a hyphenated
// "workspace-id", NOT "workspaceId" as every other operation in this
// service uses for the same logical parameter.
const workspaceIDQueryParam = "workspace-id"

func (h *Handler) handleListVersions(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	q := r.URL.Query()
	maxResults := queryMaxResults(q)
	token := q.Get("nextToken")
	workspaceID := q.Get(workspaceIDQueryParam)

	versions, err := h.Backend.ListVersions(workspaceID)
	if err != nil {
		return nil, err
	}

	p := page.New(versions, token, maxResults, grafanaDefaultPageSize)

	out := map[string]any{"grafanaVersions": p.Data}
	if p.Next != "" {
		out["nextToken"] = p.Next
	}

	return json.Marshal(out)
}
