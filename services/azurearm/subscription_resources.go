package azurearm

import (
	"net/http"
	"regexp"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"
)

const resourcesSegment = "resources"

var resourceTypeFilter = regexp.MustCompile(`(?i)resourceType\s+eq\s+'([^']+)'`)

// handleListSubscriptionResources serves GET /subscriptions/{sub}/resources
// (Resources - List), honouring a `resourceType eq '<ns>/<type>'` $filter.
// Without a filter it lists every top-level type of every registered provider.
func (h *Handler) handleListSubscriptionResources(c *echo.Context, sub string) error {
	var types []string

	if m := resourceTypeFilter.FindStringSubmatch(c.QueryParam("$filter")); m != nil {
		types = []string{m[1]}
	} else {
		types = h.registeredTopLevelTypes()
	}

	values := make([]map[string]any, 0)

	for _, rt := range types {
		ns, leaf, ok := strings.Cut(rt, "/")
		if !ok || strings.Contains(leaf, "/") {
			continue
		}

		listed, err := h.Registry.List(c.Request().Context(),
			ResourceID{SubscriptionID: sub, Namespace: ns, Types: []string{leaf}})
		if err != nil {
			return h.writeAPIError(c, err)
		}

		values = append(values, listed...)
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: values})
}

// registeredTopLevelTypes returns "<ns>/<type>" for every non-nested type of
// every registered provider, sorted.
func (h *Handler) registeredTopLevelTypes() []string {
	var out []string

	for _, ns := range h.Registry.Providers() {
		p, ok := h.Registry.ProviderNamed(ns)
		if !ok {
			continue
		}

		for _, rt := range p.ResourceTypes() {
			if !strings.Contains(rt.Type, "/") {
				out = append(out, ns+"/"+rt.Type)
			}
		}
	}

	sort.Strings(out)

	return out
}
