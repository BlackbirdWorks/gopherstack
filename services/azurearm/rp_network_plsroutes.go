package azurearm

import (
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

const (
	plsActionVisibility = "checkprivatelinkservicevisibility"
	plsActionAutoApprov = "autoapprovedprivatelinkservices"
	plsSubActionSegs    = 7
	plsRGActionSegs     = 9
	plsRGLocationIdx    = 6
)

type plsActionPath struct {
	rg, loc, action string
}

// plsAction splits a locations/{loc}/<action> path into its parts; ok is false when segs is not one.
func plsAction(segs []string) (plsActionPath, bool) {
	last := strings.ToLower(segs[len(segs)-1])

	switch len(segs) {
	case plsSubActionSegs:
		if segAt(segs, providersOrResourceGroupsIdx, providersSegment) && segAt(segs, nsLocationsIdx, "locations") {
			return plsActionPath{loc: segs[nsLocationsIdx+1], action: last}, true
		}
	case plsRGActionSegs:
		if segAt(segs, providersOrResourceGroupsIdx, resourceGroupsSegment) &&
			segAt(segs, providerNamespaceActionIdx, providersSegment) && segAt(segs, plsRGLocationIdx, "locations") {
			return plsActionPath{
				rg: segs[providersOrResourceGroupsIdx+1], loc: segs[plsRGLocationIdx+1], action: last,
			}, true
		}
	}

	return plsActionPath{}, false
}

func isPLSAction(segs []string, r *http.Request) bool {
	if !isNamespaceScope(segs, namespaceMicrosoftNetwork) {
		return false
	}

	a, ok := plsAction(segs)

	return ok && ((r.Method == http.MethodPost && a.action == plsActionVisibility) ||
		(r.Method == http.MethodGet && a.action == plsActionAutoApprov))
}
func (h *Handler) handlePLSAction(c *echo.Context, segs []string) error {
	if !h.checkNSAPIVersion(c, namespaceMicrosoftNetwork, networkAPIVersions(), "Microsoft.Network/"+netPLS) {
		return nil
	}

	a, _ := plsAction(segs)
	rg, loc, action := a.rg, a.loc, a.action
	sub := segs[1]

	np, ok := h.networkProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	if action == plsActionAutoApprov {
		return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: np.AutoApprovedPLS(sub, rg, loc)})
	}

	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	alias := strOf(body, "privateLinkServiceAlias")
	if alias == "" {
		return h.writeError(c, http.StatusBadRequest, netCodeInvalid, "privateLinkServiceAlias is required.")
	}

	opID := uuid.NewString()
	np.storeOpResult(opID, np.CheckPLSVisibility(sub, rg, loc, alias))
	h.setNSOperationID(c, ResourceID{SubscriptionID: sub, Namespace: namespaceMicrosoftNetwork}, true, opID)

	return c.NoContent(http.StatusAccepted)
}
