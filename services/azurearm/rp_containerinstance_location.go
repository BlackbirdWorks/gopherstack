package azurearm

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	aciLocSegs         = 7
	aciLocIdx          = 4
	aciLocNameIdx      = 5
	aciLocKindIdx      = 6
	aciLocUsages       = "usages"
	aciLocCachedImages = "cachedImages"
	aciLocCapabilities = "capabilities"
	aciGroupQuota      = 100
	aciMaxCPU          = 4
	aciMaxMemoryGB     = 16
)

//nolint:gochecknoglobals // static read-only list
var aciIPTypes = []string{"Public", "Private"}

func isACILocationRead(segs []string, r *http.Request) bool {
	return r.Method == http.MethodGet && len(segs) == aciLocSegs && isSubscriptionScoped(segs) &&
		segAt(segs, providersOrResourceGroupsIdx, providersSegment) &&
		segAt(segs, nsSubNamespaceIdx, namespaceMicrosoftContainerInstance) && segAt(segs, aciLocIdx, "locations") &&
		containsFold([]string{aciLocUsages, aciLocCachedImages, aciLocCapabilities}, segs[aciLocKindIdx])
}

func (p *ContainerInstanceProvider) groupsIn(sub, location string) int {
	p.mu.RLock("groupsIn")
	defer p.mu.RUnlock()

	n := 0

	for _, g := range p.groups {
		if (g.Sub == "" || g.Sub == sub) && normLocation(g.Location) == location {
			n++
		}
	}

	return n
}

func (h *Handler) handleACILocationRead(c *echo.Context, segs []string) error {
	p, ok := h.containerInstanceProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	loc := normLocation(segs[aciLocNameIdx])

	switch {
	case strings.EqualFold(segs[aciLocKindIdx], aciLocUsages):
		return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: []any{map[string]any{
			"unit": "Count", "currentValue": p.groupsIn(segs[1], loc), "limit": aciGroupQuota,
			fieldName: map[string]any{fieldValue: "ContainerGroups", "localizedValue": "Container Groups"},
		}}})
	case strings.EqualFold(segs[aciLocKindIdx], aciLocCachedImages):
		return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: []any{
			map[string]any{acaFieldImage: "ubuntu:16.04", cmpKeyOSType: aciOSLinux},
			map[string]any{acaFieldImage: "alpine:3.6", cmpKeyOSType: aciOSLinux},
		}})
	}

	caps := make([]any, 0, len(aciIPTypes))

	for _, ip := range aciIPTypes {
		caps = append(caps, map[string]any{
			cmpKeyResourceType: "containerGroups", cmpKeyOSType: aciOSLinux, "ipAddressType": ip, "gpu": apimVNetNone,
			fieldLocation: segs[aciLocNameIdx],
			cmpKeyCaps:    map[string]any{"maxCpu": aciMaxCPU, "maxMemoryInGB": aciMaxMemoryGB, "maxGpuCount": 0},
		})
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: caps})
}
