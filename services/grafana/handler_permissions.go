package grafana

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func (h *Handler) handleListPermissions(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	q := r.URL.Query()
	maxResults := queryMaxResults(q)
	token := q.Get("nextToken")

	perms, err := h.Backend.ListPermissions(segs[1], q.Get("groupId"), q.Get("userId"), q.Get("userType"))
	if err != nil {
		return nil, err
	}

	entries := make([]permissionEntryWire, len(perms))
	for i, p := range perms {
		entries[i] = toPermissionEntryWire(p)
	}

	p := page.New(entries, token, maxResults, grafanaDefaultPageSize)

	out := map[string]any{"permissions": p.Data}
	if p.Next != "" {
		out["nextToken"] = p.Next
	}

	return json.Marshal(out)
}

func (h *Handler) handleUpdatePermissions(_ context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	var req updatePermissionsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, validationError("invalid request body: " + err.Error())
	}

	errs, err := h.Backend.UpdatePermissions(segs[1], req.UpdateInstructionBatch)
	if err != nil {
		return nil, err
	}

	if errs == nil {
		errs = []updateErrorWire{}
	}

	return json.Marshal(map[string]any{"errors": errs})
}
