package dashboard

import (
	"encoding/json"
	"net/http"
	"net/netip"
	"regexp"
	"slices"
	"strings"
)

const (
	azureFlexDBPrefix = "/dashboard/api/v1/azure/flexdb"
	flexPxServers     = "flexibleservers"
	flexPxDatabases   = "databases"
	flexPxFirewall    = "firewallrules"
	flexPxConfigs     = "configurations"
	flexPxRGList      = 7
	flexPxRGIdx       = 2
	flexPxRGProvIdx   = 4
	flexPxRGNSIdx     = 5
	flexPxRGTypeIdx   = 6
	flexPxChildItem   = 2
)

//nolint:gochecknoglobals // immutable allow-lists
var (
	flexPxNamespaces = []string{
		"microsoft.dbforpostgresql",
		"microsoft.dbformysql",
	}
	flexPxActions = []string{
		aciPxStart,
		aciPxStop,
		aciPxRestart,
	}
	flexPxLists = []string{
		flexPxDatabases,
		flexPxFirewall,
		flexPxConfigs,
	}
	flexPxNameRE = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
)

// registerAzureFlexDBRoutes exposes PostgreSQL and MySQL flexible servers by dispatching in-process to the ARM
// handler. Reads, start/stop/restart, database and firewall rule PUT/DELETE are reachable; every write forwards a
// body rebuilt from fixed fields, and administrator passwords are scrubbed from every response.
func (h *DashboardHandler) registerAzureFlexDBRoutes() {
	h.registerARMProxy(armProxy{
		name: "Database Flexible Server", prefix: azureFlexDBPrefix,
		queryKeys: []string{armQueryVersion},
		methods:   []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete},
		allowed:   flexProxyAllowed,
		body:      flexProxyBody,
	})
}

func flexProxyAllowed(method string, p []string) bool {
	if len(p) == flexPxRGList {
		return method == http.MethodGet && p[0] == armPxSubsSeg && p[flexPxRGIdx] == armPxRGsSeg &&
			p[flexPxRGProvIdx] == armPxProvSeg && slices.Contains(flexPxNamespaces, p[flexPxRGNSIdx]) &&
			p[flexPxRGTypeIdx] == flexPxServers
	}

	for _, ns := range flexPxNamespaces {
		typ, name, rest, ok := armScope(p, ns)
		if !ok || typ != flexPxServers {
			continue
		}

		return flexProxyScoped(method, name, rest)
	}

	return false
}

func flexProxyScoped(method, name string, rest []string) bool {
	if name == "" {
		return method == http.MethodGet
	}

	switch len(rest) {
	case 0:
		return method == http.MethodGet
	case 1:
		return (method == http.MethodGet && slices.Contains(flexPxLists, rest[0])) ||
			(method == http.MethodPost && slices.Contains(flexPxActions, rest[0]))
	case flexPxChildItem:
		switch method {
		case http.MethodGet:
			return slices.Contains(flexPxLists, rest[0])
		case http.MethodPut, http.MethodDelete:
			return rest[0] == flexPxDatabases || rest[0] == flexPxFirewall
		}
	}

	return false
}

// flexProxyBody rebuilds the body of every write; reads keep the (empty) original.
func flexProxyBody(method string, p []string, in []byte) ([]byte, bool) {
	switch method {
	case http.MethodPost:
		return []byte("{}"), true
	case http.MethodPut:
		var props map[string]any

		if p[len(p)-flexPxChildItem] == flexPxDatabases {
			props = flexDatabaseProps(in)
		} else {
			props = flexFirewallProps(in)
		}

		if props == nil {
			return nil, false
		}

		b, err := json.Marshal(map[string]any{"properties": props})

		return b, err == nil
	}

	return in, true
}

type flexPropsIn struct {
	Properties map[string]string `json:"properties"`
}

func flexDatabaseProps(in []byte) map[string]any {
	var req flexPropsIn
	if json.Unmarshal(in, &req) != nil {
		return nil
	}

	out := map[string]any{}

	for _, k := range []string{"charset", "collation"} {
		v := req.Properties[k]
		if v == "" {
			continue
		}

		if !flexPxNameRE.MatchString(v) {
			return nil
		}

		out[k] = v
	}

	return out
}

func flexFirewallProps(in []byte) map[string]any {
	var req flexPropsIn
	if json.Unmarshal(in, &req) != nil {
		return nil
	}

	start, err1 := netip.ParseAddr(strings.TrimSpace(req.Properties["startIpAddress"]))
	end, err2 := netip.ParseAddr(strings.TrimSpace(req.Properties["endIpAddress"]))

	if err1 != nil || err2 != nil || !start.Is4() || !end.Is4() {
		return nil
	}

	return map[string]any{"startIpAddress": start.String(), "endIpAddress": end.String()}
}
