package azurearm

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	aksNsIdx             = 5
	aksProfOffset        = 2
	aksTypeIdx           = 6
	aksClusterActSegs    = 9
	aksClusterProfSegs   = 10
	aksPoolActSegs       = 11
	aksPoolProfSegs      = 12
	aksPoolSegIdx        = 8
	aksClusterInstSegs   = 8
	aksSubLocSegs        = 7
	aksActionStart       = "start"
	aksActionStop        = "stop"
	aksActionRunCommand  = "runcommand"
	aksActionAbort       = "abort"
	aksActionRotate      = "rotateclustercertificates"
	aksActionResetAAD    = "resetaadprofile"
	aksActionResetSP     = "resetserviceprincipalprofile"
	aksCmdResultsType    = "commandResults"
	aksActionUserCred    = "listclusterusercredential"
	aksActionAdminCred   = "listclusteradmincredential"
	aksActionMonitorCred = "listclustermonitoringusercredential"
	aksActionUpgradeNode = "upgradenodeimageversion"
)

func isAKSScope(segs []string, n int) bool {
	return len(segs) == n && isSubscriptionScoped(segs) &&
		segAt(segs, providersOrResourceGroupsIdx, resourceGroupsSegment) &&
		segAt(segs, providerNamespaceActionIdx, providersSegment) &&
		segAt(segs, aksNsIdx, namespaceMicrosoftContainerService) && segAt(segs, aksTypeIdx, aksClusterType)
}

func aksLast(segs []string) string { return strings.ToLower(segs[len(segs)-1]) }

func isAKSSubLocation(segs []string, what string) bool {
	return len(segs) == aksSubLocSegs && isSubscriptionScoped(segs) &&
		segAt(segs, providersOrResourceGroupsIdx, providersSegment) &&
		segAt(segs, nsSubNamespaceIdx, namespaceMicrosoftContainerService) &&
		segAt(segs, nsLocationsIdx, "locations") && segAt(segs, aksSubLocSegs-1, what)
}

//nolint:gochecknoglobals // static route table, read-only after init
var containerServiceRoutes = []armRoute{
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodGet && isNamespaceOperation(segs, namespaceMicrosoftContainerService)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleNSOperation(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodGet && isAKSSubLocation(segs, "kubernetesVersions")
		},
		handle: func(h *Handler, c *echo.Context, _ []string) error {
			if !h.checkAKSAPIVersion(c) {
				return nil
			}

			return h.writeJSON(c, http.StatusOK, aksKubernetesVersionsBody())
		},
	},
	{
		match: func(segs []string, r *http.Request) bool {
			if r.Method != http.MethodPost || !isAKSScope(segs, aksClusterActSegs) {
				return false
			}

			switch aksLast(segs) {
			case aksActionStart, aksActionStop, aksActionRunCommand, aksActionUserCred, aksActionAdminCred,
				aksActionMonitorCred, aksActionAbort, aksActionRotate, aksActionResetAAD, aksActionResetSP:
				return true
			}

			return false
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleAKSClusterAction(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodGet && isAKSSubLocation(segs, "trustedAccessRoles")
		},
		handle: func(h *Handler, c *echo.Context, _ []string) error {
			if !h.checkAKSAPIVersion(c) {
				return nil
			}

			return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: aksTrustedRoles()})
		},
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodGet && isAKSScope(segs, aksClusterProfSegs) &&
				segAt(segs, aksClusterProfSegs-aksProfOffset, aksCmdResultsType)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleAKSCommandResult(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodPost && isAKSScope(segs, aksPoolActSegs) &&
				segAt(segs, aksPoolSegIdx, aksPoolType) && aksLast(segs) == aksActionUpgradeNode
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleAKSUpgradeNode(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			if r.Method != http.MethodGet {
				return false
			}

			return (isAKSScope(segs, aksClusterProfSegs) && aksLast(segs) == aksDefaultName &&
				segAt(segs, aksClusterProfSegs-aksProfOffset, "upgradeProfiles")) ||
				(isAKSScope(segs, aksClusterActSegs) && aksLast(segs) == "availableagentpoolversions") ||
				(isAKSScope(segs, aksPoolProfSegs) && aksLast(segs) == aksDefaultName &&
					segAt(segs, aksPoolSegIdx, aksPoolType) && segAt(segs, aksPoolProfSegs-aksProfOffset, "upgradeProfiles"))
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleAKSProfile(c, segs) },
	},
	{
		match: func(segs []string, _ *http.Request) bool {
			return isNamespaceScope(segs, namespaceMicrosoftContainerService)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error {
			return h.handleNamespace(c, segs, namespaceMicrosoftContainerService)
		},
	},
}

func (h *Handler) checkAKSAPIVersion(c *echo.Context) bool {
	return h.checkNSAPIVersion(c, namespaceMicrosoftContainerService, aksAPIVersions(),
		namespaceMicrosoftContainerService+"/"+aksClusterType)
}

func (h *Handler) containerServiceProvider() (*ContainerServiceProvider, bool) {
	p, ok := h.Registry.ProviderNamed(namespaceMicrosoftContainerService)
	if !ok {
		return nil, false
	}

	cp, ok := p.(*ContainerServiceProvider)

	return cp, ok
}

func (h *Handler) aksFor(c *echo.Context, segs []string, n int) (*ContainerServiceProvider, ResourceID, bool) {
	p, ok := h.containerServiceProvider()
	if !ok {
		_ = h.writeNotFound(c)

		return nil, ResourceID{}, false
	}

	if !h.checkAKSAPIVersion(c) {
		return nil, ResourceID{}, false
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:n]))
	if err != nil {
		_ = h.writeAPIError(c, err)

		return nil, ResourceID{}, false
	}

	return p, id, true
}

func (h *Handler) handleAKSClusterAction(c *echo.Context, segs []string) error {
	p, id, ok := h.aksFor(c, segs, aksClusterInstSegs)
	if !ok {
		return nil
	}

	ctx := c.Request().Context()

	switch act := aksLast(segs); act {
	case aksActionStart, aksActionStop:
		if err := p.setPower(ctx, id, act == aksActionStop); err != nil {
			return h.writeAPIError(c, err)
		}

		h.setNSOperation(c, id, false)

		return c.NoContent(http.StatusAccepted)
	case aksActionRunCommand:
		return h.handleAKSRunCommand(c, id, p)
	case aksActionAbort, aksActionRotate, aksActionResetAAD, aksActionResetSP:
		return h.handleAKSOperation(c, id, p, act)
	default:
		role := aksCredUser

		switch act {
		case aksActionAdminCred:
			role = aksCredAdmin
		case aksActionMonitorCred:
			role = aksCredMonitor
		}

		out, err := p.credentials(id, role)
		if err != nil {
			return h.writeAPIError(c, err)
		}

		return h.writeJSON(c, http.StatusOK, out)
	}
}

func (h *Handler) handleAKSUpgradeNode(c *echo.Context, segs []string) error {
	p, id, ok := h.aksFor(c, segs, aksPoolActSegs-1)
	if !ok {
		return nil
	}

	if err := p.upgradeNodeImage(id); err != nil {
		return h.writeAPIError(c, err)
	}

	h.setNSOperation(c, id, false)

	return c.NoContent(http.StatusAccepted)
}

func (h *Handler) handleAKSProfile(c *echo.Context, segs []string) error {
	var (
		out map[string]any
		err error
	)

	switch len(segs) {
	case aksClusterProfSegs:
		p, id, ok := h.aksFor(c, segs, aksClusterInstSegs)
		if !ok {
			return nil
		}

		out, err = p.upgradeProfile(id)
	case aksClusterActSegs:
		p, id, ok := h.aksFor(c, segs, aksClusterInstSegs)
		if !ok {
			return nil
		}

		out, err = p.availablePoolVersions(id)
	default:
		p, id, ok := h.aksFor(c, segs, aksPoolActSegs-1)
		if !ok {
			return nil
		}

		out, err = p.poolUpgradeProfile(id)
	}

	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

func (h *Handler) handleAKSOperation(c *echo.Context, id ResourceID, p *ContainerServiceProvider, act string) error {
	var err error

	switch act {
	case aksActionAbort:
		err = p.abortLatest(c.Request().Context(), id)
	case aksActionRotate:
		err = p.rotateCertificates(c.Request().Context(), id)
	default:
		body, derr := decodeJSONBody(c.Request())
		if derr != nil {
			return h.writeAPIError(c, derr)
		}

		if act == aksActionResetAAD {
			err = p.resetAADProfile(id, body)
		} else {
			err = p.resetServicePrincipalProfile(id, body)
		}
	}

	if err != nil {
		return h.writeAPIError(c, err)
	}

	h.setNSOperation(c, id, true)

	return c.NoContent(http.StatusAccepted)
}

func (h *Handler) commandLocation(c *echo.Context, id ResourceID, cmdID string) string {
	return h.baseURLFor(c.Request()) + id.ARMID() + "/" + aksCmdResultsType + "/" + cmdID + "?api-version=" +
		url.QueryEscape(c.QueryParam(nsAPIVersionKey))
}

func (h *Handler) handleAKSRunCommand(c *echo.Context, id ResourceID, p *ContainerServiceProvider) error {
	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	req, err := aksParseRunCommand(body)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	cmdID, err := p.runCommand(id, req)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	c.Response().Header().Set("Location", h.commandLocation(c, id, cmdID))

	return c.NoContent(http.StatusAccepted)
}

func (h *Handler) handleAKSCommandResult(c *echo.Context, segs []string) error {
	p, id, ok := h.aksFor(c, segs, aksClusterInstSegs)
	if !ok {
		return nil
	}

	out, done, err := p.commandResult(id, segs[len(segs)-1])
	if err != nil {
		return h.writeAPIError(c, err)
	}

	if !done {
		c.Response().Header().Set("Location", h.commandLocation(c, id, segs[len(segs)-1]))

		return c.NoContent(http.StatusAccepted)
	}

	return h.writeJSON(c, http.StatusOK, out)
}
