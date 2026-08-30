package serverlessrepo

import (
	"encoding/json"
	"net/http"
)

func (h *Handler) handleListApplicationDependencies(req *http.Request) ([]byte, error) {
	appName, err := extractApplicationName(req)
	if err != nil {
		return nil, err
	}

	semanticVersion := req.URL.Query().Get(keySemanticVersion)

	deps, backendErr := h.Backend.ListApplicationDependencies(appName, semanticVersion)
	if backendErr != nil {
		return nil, backendErr
	}

	nextToken := req.URL.Query().Get("nextToken")
	maxItems := parseMaxItems(req.URL.Query().Get("maxItems"), maxItemsDefault)

	// deps is sorted by (ApplicationID, SemanticVersion) and the same
	// ApplicationID can repeat across versions, so an unresolved token
	// defaults to the end of the collection (an empty final page) rather
	// than index 0 -- restarting at page one would otherwise be
	// indistinguishable from a genuinely unresolvable cursor.
	start := 0

	if nextToken != "" {
		start = len(deps)

		for i, d := range deps {
			if d.ApplicationID == nextToken {
				start = i + 1

				break
			}
		}
	}

	end := min(start+maxItems, len(deps))
	page := deps[start:end]

	depList := make([]map[string]any, 0, len(page))

	for _, d := range page {
		depList = append(depList, map[string]any{
			keyApplicationID:   d.ApplicationID,
			keySemanticVersion: d.SemanticVersion,
		})
	}

	resp := map[string]any{"dependencies": depList}

	if end < len(deps) {
		resp["nextToken"] = deps[end-1].ApplicationID
	}

	return json.Marshal(resp)
}
