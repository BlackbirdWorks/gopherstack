package azurearm

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	aksNsIdx             = 5
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
	aksActionUserCred    = "listclusterusercredential"
	aksActionAdminCred   = "listclusteradmincredential"
	aksActionMonitorCred = "listclustermonitoringusercredential"
	aksActionUpgradeNode = "upgradenodeimageversion"
	aksCodeNotSupported  = "NotImplemented"
)

func isAKSScope(segs []string, typ string, n int) bool {
	return len(segs) == n && isSubscriptionScoped(segs) &&
		segAt(segs, providersOrResourceGroupsIdx, resourceGroupsSegment) &&
		segAt(segs, providerNamespaceActionIdx, providersSegment) &&
		segAt(segs, aksNsIdx, namespaceMicrosoftContainerService) && segAt(segs, aksTypeIdx, typ)
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
			if r.Method != http.MethodPost || !isAKSScope(segs, aksClusterType, aksClusterActSegs) {
				return false
			}

			switch aksLast(segs) {
			case aksActionStart, aksActionStop, aksActionRunCommand, aksActionUserCred, aksActionAdminCred,
				aksActionMonitorCred:
				return true
			}

			return false
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleAKSClusterAction(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodPost && isAKSScope(segs, aksClusterType, aksPoolActSegs) &&
				segAt(segs, aksPoolSegIdx, aksPoolType) && aksLast(segs) == aksActionUpgradeNode
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleAKSUpgradeNode(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			if r.Method != http.MethodGet {
				return false
			}

			return (isAKSScope(segs, aksClusterType, aksClusterProfSegs) && aksLast(segs) == "default" &&
				segAt(segs, aksClusterProfSegs-2, "upgradeProfiles")) ||
				(isAKSScope(segs, aksClusterType, aksClusterActSegs) && aksLast(segs) == "availableagentpoolversions") ||
				(isAKSScope(segs, aksClusterType, aksPoolProfSegs) && aksLast(segs) == "default" &&
					segAt(segs, aksPoolSegIdx, aksPoolType) && segAt(segs, aksPoolProfSegs-2, "upgradeProfiles"))
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
		return h.writeError(c, http.StatusNotImplemented, aksCodeNotSupported,
			"runCommand is not supported: the emulator does not schedule pods to run commands in.")
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
