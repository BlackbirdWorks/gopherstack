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

	start := 0

	if nextToken != "" {
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
