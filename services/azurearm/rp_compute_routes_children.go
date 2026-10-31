package azurearm

import (
	"net/http"
	"regexp"
	"strings"

	"github.com/labstack/echo/v5"
)

// computeCatalogScope serves the subscription-scope catalogs: run command documents and extension images.
func (h *Handler) computeCatalogScope(c *echo.Context, segs []string) (bool, error) {
	if c.Request().Method != http.MethodGet || !segAt(segs, providersOrResourceGroupsIdx, providersSegment) {
		return false, nil
	}

	inLocation := len(segs) > cmpSubLocIdx && segAt(segs, cmpSubLocIdx, cmpKeyLocations)

	switch {
	case inLocation && segAt(segs, cmpSubLocSegs-1, kidRunCommands) && len(segs) >= cmpSubLocSegs:
		return h.runCommandCatalog(c, segs)
	case inLocation && isExtensionImagePath(segs):
		return true, h.computeExtensionImages(c, segs)
	case len(segs) == cmpSubTypeSegs && segAt(segs, cmpSubLocIdx, cmpVMs) && c.QueryParam("$filter") != "":
		cp, ok := h.computeProvider()
		if !ok {
			return true, h.writeNotFound(c)
		}

		return true, h.computeFilteredVMs(c, cp, segs[1], "")
	}

	return false, nil
}

func isExtensionImagePath(segs []string) bool {
	if len(segs) < extImgTypesSegs || len(segs) > extImgVersionSegs || len(segs) == extImgTypeIdx+1 {
		return false
	}

	for off, want := range []string{"publishers", "", "artifacttypes", "vmextension", "types"} {
		if want != "" && !segAt(segs, extImgPubIdx-1+off, want) {
			return false
		}
	}

	return true
}

// computeChildScope serves resource-group scope reads of children and filtered virtual machine lists.
func (h *Handler) computeChildScope(c *echo.Context, cp *ComputeProvider, segs []string) (bool, error) {
	if c.Request().Method != http.MethodGet {
		return false, nil
	}

	switch {
	case len(segs) == cmpRGNameIdx && c.QueryParam("$filter") != "" && strings.EqualFold(segs[cmpRGNameIdx-1], cmpVMs):
		return true, h.computeFilteredVMs(c, cp, segs[1], segs[3])
	case len(segs) > cmpRGActionIdx && len(segs)%2 == 0 && c.QueryParam("$expand") != "":
		return true, h.computeGetKidExpanded(c, cp, segs)
	}

	return false, nil
}

var vmssFilterRe = regexp.MustCompile(`(?i)^\s*virtualMachineScaleSet/id\s+eq\s+'?([^']+?)'?\s*$`)

// computeFilteredVMs lists virtual machines filtered by virtualMachineScaleSet/id, the only filter Compute defines.
func (h *Handler) computeFilteredVMs(c *echo.Context, cp *ComputeProvider, sub, rg string) error {
	m := vmssFilterRe.FindStringSubmatch(c.QueryParam("$filter"))
	if m == nil {
		return h.writeAPIError(c, cmpInvalid("The $filter '%s' is not supported; the allowed value is "+
			"'virtualMachineScaleSet/id' eq '/subscriptions/{subId}/resourceGroups/{rgName}/providers/"+
			"Microsoft.Compute/virtualMachineScaleSets/{vmssName}'.", c.QueryParam("$filter")))
	}

	id := ResourceID{
		SubscriptionID: sub,
		ResourceGroup:  rg,
		Namespace:      namespaceMicrosoftCompute,
		Types:          []string{cmpVMs},
	}

	values, err := cp.list(id, c.QueryParam(cmpQueryStatusOnly) == cmpTrue)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	kept := values[:0:0]

	for _, v := range values {
		if strings.EqualFold(strOf(mapOf(mapOf(v[fieldProperties])[vmFieldVMSS]), cmpFieldID), m[1]) {
			kept = append(kept, v)
		}
	}

	return h.writeJSON(c, http.StatusOK, h.pageNS(c, kept, nsDefaultTop(id.Namespace)))
}

func (h *Handler) computeExtensionImages(c *echo.Context, segs []string) error {
	body, err := extensionImagesBody(segs)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, body)
}

func (h *Handler) computeGetKidExpanded(c *echo.Context, cp *ComputeProvider, segs []string) error {
	id, err := ParseGenericResourcePath("/" + joinSegs(segs))
	if err != nil || len(id.Types) < 2 {
		return h.getGenericResource(c, id)
	}

	out, err := cp.GetKidExpanded(id, c.QueryParam("$expand"))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

func (h *Handler) vmAction(c *echo.Context, cp *ComputeProvider, id ResourceID, action string) error {
	if !strings.EqualFold(action, rcActionRun) {
		return cp.VMAction(id, action)
	}

	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return err
	}

	return cp.RunCommand(id, body)
}

func (h *Handler) runCommandCatalog(c *echo.Context, segs []string) (bool, error) {
	switch len(segs) {
	case cmpSubLocSegs:
		return true, h.writeJSON(c, http.StatusOK, RunCommandDocs())
	case cmpSubLocSegs + 1:
		return true, h.writeAPIError(c, cmpNotImplemented("locations/runCommands/{commandId}"))
	}

	return false, nil
}
