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
// handleInstanceTypeDetailsRoutes serves ListInstanceTypeDetails. EngineVersion
// is a URI label, not a query param -- unlike domainName/instanceType
// (api_op_ListInstanceTypeDetails.go, opensearch@v1.75.4 serializers.go:
// GET /2021-01-01/opensearch/instanceTypeDetails/{EngineVersion}) -- gopherstack-l5ir.
func (h *Handler) handleInstanceTypeDetailsRoutes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	engineVersion := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, openSearchInstanceTypesPath), "/")
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

// handleUpgradeDomainRoutes handles the /2021-01-01/opensearch/upgradeDomain
// prefix: POST on the bare path is UpgradeDomain; GET .../{DomainName}/history
// and .../{DomainName}/status are GetUpgradeHistory/GetUpgradeStatus (real
// paths per api_op_GetUpgradeHistory.go / api_op_GetUpgradeStatus.go,
// opensearch@v1.75.4 serializers.go -- NOT nested under the domain prefix,
// unlike most other domain sub-ops) -- gopherstack-l5ir.
func (h *Handler) handleUpgradeDomainRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchUpgradePath)

	if rest != "" && rest != "/" {
		h.dispatchUpgradeStatusRoutes(w, r, strings.TrimPrefix(rest, "/"))

		return
	}

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
		DomainName       string `json:"DomainName"`
		TargetVersion    string `json:"TargetVersion"`
		PerformCheckOnly bool   `json:"PerformCheckOnly"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}

	if upgradeErr := h.Backend.UpgradeDomain(req.DomainName, req.TargetVersion); upgradeErr != nil {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", upgradeErr.Error())

		return
	}

	// UpgradeDomainOutput (opensearch@v1.75.4 api_op_UpgradeDomain.go) has
	// AdvancedOptions/ChangeProgressDetails/DomainName/PerformCheckOnly/
	// TargetVersion/UpgradeId -- no StepStatus member (that belongs to
	// UpgradeStepItem, a GetUpgradeHistory/GetUpgradeStatus type).
	// AdvancedOptions/ChangeProgressDetails have no backing state here, so
	// they're left off rather than fabricated.
	h.writeJSON(r, w, map[string]any{
		"UpgradeId":        fmt.Sprintf("upgrade-%s", req.DomainName),
		"DomainName":       req.DomainName,
		"TargetVersion":    req.TargetVersion,
		"PerformCheckOnly": req.PerformCheckOnly,
	})
}

// dispatchUpgradeStatusRoutes handles GET {DomainName}/history and
// {DomainName}/status under openSearchUpgradePath (GetUpgradeHistory /
// GetUpgradeStatus).
func (h *Handler) dispatchUpgradeStatusRoutes(w http.ResponseWriter, r *http.Request, trimmed string) {
	switch {
	case r.Method == http.MethodGet && strings.HasSuffix(trimmed, "/history"):
		domainName, _ := strings.CutSuffix(trimmed, "/history")
		history, err := h.Backend.GetUpgradeHistory(domainName)
		if err != nil {
			// GetUpgradeHistory's own deserializer (opensearch@v1.75.4
			// deserializers.go) models ResourceNotFoundException for a
			// nonexistent domain -- this must not silently succeed with a
			// fabricated empty list.
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}

		h.writeJSON(r, w, map[string]any{"UpgradeHistories": history})
	case r.Method == http.MethodGet && strings.HasSuffix(trimmed, "/status"):
		domainName, _ := strings.CutSuffix(trimmed, "/status")
		upgradeName, upgradeStatus, upgradeStep, err := h.Backend.GetUpgradeStatus(domainName)
		if err != nil {
			// GetUpgradeStatus's own deserializer models ResourceNotFoundException
			// for a nonexistent domain -- see GetUpgradeHistory above.
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}

		h.writeJSON(r, w, map[string]any{
			"UpgradeName": upgradeName,
			"StepStatus":  upgradeStatus,
			"UpgradeStep": upgradeStep,
		})
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}
