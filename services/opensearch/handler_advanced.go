package opensearch

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// handleVersionsRoutes handles GET /2021-01-01/opensearch/versions → ListVersions.
func (h *Handler) handleVersionsRoutes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	versions := []string{
		"OpenSearch_2.17", "OpenSearch_2.15", "OpenSearch_2.13",
		engineVersionOpenSearch211, "OpenSearch_2.10",
		engineVersionOpenSearch29, "OpenSearch_2.8",
		engineVersionOpenSearch27, "Elasticsearch_8.11",
		"Elasticsearch_7.10", "Elasticsearch_6.8",
	}

	// Support nextToken-based pagination offset.
	if tok := r.URL.Query().Get("nextToken"); tok != "" {
		for i, v := range versions {
			if v == tok {
				versions = versions[i:]

				break
			}
		}
	}

	// Support maxResults limit.
	maxResults := len(versions)
	if mr := r.URL.Query().Get("maxResults"); mr != "" {
		if n, err := strconv.Atoi(mr); err == nil && n > 0 && n < maxResults {
			maxResults = n
		}
	}

	result := map[string]any{
		"Versions": versions[:maxResults],
	}

	if maxResults < len(versions) {
		result["NextToken"] = versions[maxResults]
	}

	h.writeJSON(r, w, result)
}

// handleInstanceTypeLimitsRoutes handles DescribeInstanceTypeLimits requests.
// Path: GET /2021-01-01/opensearch/instanceTypeLimits/{EngineVersion}/{InstanceType}.
func (h *Handler) handleInstanceTypeLimitsRoutes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	// Path: /2021-01-01/opensearch/instanceTypeLimits/{EngineVersion}/{InstanceType}
	rest := strings.TrimPrefix(r.URL.Path, openSearchInstanceTypeLimitsPath)
	rest = strings.TrimPrefix(rest, "/")
	engineVersion, instanceType, _ := strings.Cut(rest, "/")

	limits, err := h.Backend.DescribeInstanceTypeLimits(instanceType, engineVersion)
	if err != nil {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

		return
	}

	dataMap := map[string]any{
		"InstanceLimits": limits.InstanceLimits,
		"StorageTypes":   limits.StorageTypes,
	}

	if len(limits.AdditionalLimits) > 0 {
		dataMap["AdditionalLimits"] = limits.AdditionalLimits
	}

	h.writeJSON(r, w, map[string]any{
		"LimitsByRole": map[string]any{
			"data": dataMap,
		},
	})
}

// handleInstanceTypeDetailsRoutes handles GET /2021-01-01/opensearch/instanceTypeDetails → ListInstanceTypeDetails.
func (h *Handler) handleInstanceTypeDetailsRoutes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	engineVersion := r.URL.Query().Get("engineVersion")
	instanceType := r.URL.Query().Get("instanceType")
	details := h.Backend.ListInstanceTypeDetails(engineVersion, instanceType)
	h.writeJSON(r, w, map[string]any{"InstanceTypeDetails": details})
}

// handleCompatibleVersionsRoutes handles GET /2021-01-01/opensearch/compatibleVersions → GetCompatibleVersions.
func (h *Handler) handleCompatibleVersionsRoutes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	domainName := r.URL.Query().Get("domainName")
	if domainName != "" {
		if _, err := h.Backend.DescribeDomain(domainName); err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("domain %s not found", domainName))

			return
		}
	}

	versions := h.Backend.GetCompatibleVersions(domainName)
	h.writeJSON(r, w, map[string]any{"CompatibleVersions": versions})
}

// handleUpgradeDomainRoutes handles POST /2021-01-01/opensearch/upgradeDomain → UpgradeDomain.
func (h *Handler) handleUpgradeDomainRoutes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req struct {
		DomainName    string `json:"DomainName"`
		TargetVersion string `json:"TargetVersion"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}

	if upgradeErr := h.Backend.UpgradeDomain(req.DomainName, req.TargetVersion); upgradeErr != nil {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", upgradeErr.Error())

		return
	}

	h.writeJSON(r, w, map[string]any{
		"UpgradeId":     fmt.Sprintf("upgrade-%s", req.DomainName),
		"DomainName":    req.DomainName,
		"TargetVersion": req.TargetVersion,
		"StepStatus":    "REQUESTED",
	})
}

// dispatchDomainGetUpgradeRoutes handles upgrade-related GET sub-routes on a domain.
// Returns true if handled.
func (h *Handler) dispatchDomainGetUpgradeRoutes(
	w http.ResponseWriter,
	r *http.Request,
	trimmed string,
) bool {
	switch {
	case strings.HasSuffix(trimmed, "/upgradeHistory"):
		// GetUpgradeHistory
		domainName, _ := strings.CutSuffix(trimmed, "/upgradeHistory")
		history, err := h.Backend.GetUpgradeHistory(domainName)
		if err != nil {
			history = []*UpgradeHistory{}
		}

		h.writeJSON(r, w, map[string]any{"UpgradeHistories": history})
	case strings.HasSuffix(trimmed, "/upgrades"):
		// GetUpgradeStatus
		domainName, _ := strings.CutSuffix(trimmed, "/upgrades")
		upgradeName, upgradeStatus, upgradeStep, err := h.Backend.GetUpgradeStatus(domainName)
		if err != nil {
			upgradeName, upgradeStatus, upgradeStep = "INITIAL", upgradeStatusSucceeded, upgradeStepUpgrade
		}

		h.writeJSON(r, w, map[string]any{
			"UpgradeName": upgradeName,
			"StepStatus":  upgradeStatus,
			"UpgradeStep": upgradeStep,
		})
	default:
		return false
	}

	return true
}
