package opensearch

import (
	"net/http"
	"strings"
)

// dispatchDomainGetHealthRoutes handles health/nodes/progress/dryRun GET sub-routes on a domain.
// Returns true if handled.
func (h *Handler) dispatchDomainGetHealthRoutes(
	w http.ResponseWriter,
	r *http.Request,
	trimmed string,
) bool {
	switch {
	case strings.HasSuffix(trimmed, "/progress"):
		// DescribeDomainChangeProgress
		domainName, _ := strings.CutSuffix(trimmed, "/progress")
		progress, err := h.Backend.GetChangeProgress(domainName)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return true
		}

		h.writeJSON(r, w, map[string]any{"ChangeProgressStatus": progress})
	case strings.HasSuffix(trimmed, "/health"):
		// DescribeDomainHealth
		domainName, _ := strings.CutSuffix(trimmed, "/health")
		health, err := h.Backend.GetDomainHealth(domainName)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return true
		}

		h.writeJSON(r, w, health)
	case strings.HasSuffix(trimmed, "/nodes"):
		// DescribeDomainNodes
		domainName, _ := strings.CutSuffix(trimmed, "/nodes")
		nodes, err := h.Backend.GetDomainNodes(domainName)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return true
		}

		h.writeJSON(r, w, map[string]any{"DomainNodesStatusList": nodes})
	case strings.HasSuffix(trimmed, "/dryRun"):
		// DescribeDryRunProgress
		domainName, _ := strings.CutSuffix(trimmed, "/dryRun")
		dr, err := h.Backend.GetDryRunProgress(domainName)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return true
		}

		h.writeJSON(r, w, map[string]any{"DryRunProgressStatus": dr})
	default:
		return false
	}

	return true
}
