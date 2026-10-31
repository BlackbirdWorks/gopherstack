package azurearm

import (
	"context"
	"maps"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	pecTypeName   = "privateEndpointConnections"
	plrTypeName   = "privateLinkResources"
	peStatusOK    = "Approved"
	peStatusWait  = "Pending"
	peStatusNo    = "Rejected"
	peStatusGone  = "Disconnected"
	peAutoMessage = "Auto-Approved"
	peDNSTTL      = 10
)

// peGroup is one private-link sub-resource of a target type.
type peGroup struct {
	fqdns func(name, loc string) []string
	zone  string
	pub   string
}

// peTarget is a resource type that private endpoints can connect to.
type peTarget struct {
	groups map[string]peGroup
	// actionKey is the JSON name of the connection state's action field in that provider's API.
	actionKey string
}

func plainFQDN(pub string) func(name, loc string) []string {
	return func(name, _ string) []string { return []string{name + "." + pub} }
}

func regionalFQDN(pub string) func(name, loc string) []string {
	return func(name, loc string) []string { return []string{name + "." + pub, name + "-" + loc + "." + pub} }
}

func grp(zonePub, pub string, f func(name, loc string) []string) peGroup {
	return peGroup{zone: "privatelink." + zonePub, pub: pub, fqdns: f}
}

func storageGroups() map[string]peGroup {
	out := map[string]peGroup{}

	for _, g := range []string{"blob", "table", "queue", "file", "dfs"} {
		pub := g + ".core.windows.net"
		out[g] = grp(pub, pub, plainFQDN(pub))
		out[g+"_secondary"] = grp(
			pub,
			pub,
			func(name, _ string) []string { return []string{name + "-secondary." + pub} },
		)
	}

	web := "web.core.windows.net"
	out["web"] = grp(web, web, func(name, _ string) []string { return []string{name + ".z13." + web} })
	out["web_secondary"] = grp(
		web,
		web,
		func(name, _ string) []string { return []string{name + "-secondary.z13." + web} },
	)

	return out
}

func cosmosGroups() map[string]peGroup {
	out := map[string]peGroup{
		"Sql": grp("documents.azure.com", "documents.azure.com", regionalFQDN("documents.azure.com")),
	}

	for g, pub := range map[string]string{
		"SqlDedicated": "sqlx.cosmos.azure.com", "MongoDB": "mongo.cosmos.azure.com",
		"Cassandra": "cassandra.cosmos.azure.com", "Gremlin": "gremlin.cosmos.azure.com",
		"Table": "table.cosmos.azure.com", "Analytical": "analytics.cosmos.azure.com",
	} {
		out[g] = grp(pub, pub, regionalFQDN(pub))
	}

	return out
}

func single(group string, g peGroup) map[string]peGroup { return map[string]peGroup{group: g} }

// peTargets maps a lowercase "namespace/type" to its private-link sub-resources. Group ids are from Learn's
// private-endpoint-overview table; the zone and FQDN patterns follow Learn's private-endpoint-dns table.
//
//nolint:gochecknoglobals // read-only table
var peTargets = map[string]peTarget{
	"microsoft.storage/storageaccounts": {groups: storageGroups(), actionKey: "actionRequired"},
	"microsoft.keyvault/vaults": {
		groups:    single("vault", grp("vaultcore.azure.net", "vault.azure.net", plainFQDN("vault.azure.net"))),
		actionKey: propActionsRequired,
	},
	"microsoft.documentdb/databaseaccounts": {groups: cosmosGroups(), actionKey: propActionsRequired},
	"microsoft.sql/servers": {
		groups: single("sqlServer", grp("database.windows.net", "database.windows.net",
			plainFQDN("database.windows.net"))),
		actionKey: "actionRequired",
	},
	"microsoft.containerregistry/registries": {
		groups: single("registry", grp("azurecr.io", "azurecr.io", func(name, loc string) []string {
			return []string{name + ".azurecr.io", name + "." + loc + ".data.azurecr.io"}
		})),
		actionKey: propActionsRequired,
	},
	"microsoft.servicebus/namespaces": {
		groups: single(
			"namespace",
			grp("servicebus.windows.net", "servicebus.windows.net", plainFQDN("servicebus.windows.net")),
		),
		actionKey: propActionsRequired,
	},
	"microsoft.eventhub/namespaces": {
		groups: single(
			"namespace",
			grp("servicebus.windows.net", "servicebus.windows.net", plainFQDN("servicebus.windows.net")),
		),
		actionKey: propActionsRequired,
	},
	"microsoft.appconfiguration/configurationstores": {
		groups:    single("configurationStores", grp("azconfig.io", "azconfig.io", plainFQDN("azconfig.io"))),
		actionKey: propActionsRequired,
	},
	"microsoft.cache/redis": {
		groups: single(
			"redisCache",
			grp("redis.cache.windows.net", "redis.cache.windows.net", plainFQDN("redis.cache.windows.net")),
		),
		actionKey: propActionsRequired,
	},
	"microsoft.web/sites": {
		groups:    single("sites", grp("azurewebsites.net", "azurewebsites.net", plainFQDN("azurewebsites.net"))),
		actionKey: propActionsRequired,
	},
}

func peTargetKey(id ResourceID) string {
	if len(id.Types) == 0 {
		return ""
	}

	return strings.ToLower(id.Namespace + "/" + id.Types[0])
}

func (t peTarget) group(name string) (string, peGroup, bool) {
	for k, g := range t.groups {
		if strings.EqualFold(k, name) {
			return k, g, true
		}
	}

	return "", peGroup{}, false
}

// peConn is one private endpoint connection as the target resource sees it.
type peConn struct {
	pe     *netParent
	conn   map[string]any
	target string
	name   string
	guid   string
}

func (c peConn) targetName() string { return c.pe.Obj.Name + "." + c.guid }

// connsOf lists the live connections whose target is targetID.
func (p *NetworkProvider) connsOf(targetID string) []peConn {
	var out []peConn

	for _, pe := range p.ofType(netPEs) {
		for _, key := range []string{"privateLinkServiceConnections", "manualPrivateLinkServiceConnections"} {
			for _, c := range anyList(pe.Obj.Props[key]) {
				m, _ := c.(map[string]any)
				cn := strOf(m, fieldName)

				if !strings.EqualFold(strOf(propsOf(m), "privateLinkServiceId"), targetID) ||
					pe.Obj.Aux["gone:"+cn] != "" {
					continue
				}

				out = append(out, peConn{pe: pe, conn: m, target: targetID, name: cn, guid: pe.Obj.Aux["guid:"+cn]})
			}
		}
	}

	return out
}

func connState(pe *netParent, name string) (string, string) {
	if pe.Obj.Aux["gone:"+name] != "" {
		return peStatusGone, "Removed by the private link resource owner."
	}

	return pe.Obj.Aux["st:"+name], pe.Obj.Aux["desc:"+name]
}

func (p *NetworkProvider) targetConnBody(c peConn, targetID string) map[string]any {
	t := peTargets[strings.ToLower(peTargetKeyOf(targetID))]
	status, desc := connState(c.pe, c.name)

	state := map[string]any{netPropStatus: status, netPropDescription: desc, t.actionKey: netNone}
	props := map[string]any{
		"privateEndpoint":                   idRef(c.pe.armID()),
		"privateLinkServiceConnectionState": state,
		fieldProvisioningState:              netStatusSucceeded,
		propGroupIDs:                        propsOf(c.conn)[propGroupIDs],
	}

	typ := targetTypeOf(targetID)

	return map[string]any{
		netFieldID: targetID + "/" + pecTypeName + "/" + c.targetName(), fieldName: c.targetName(),
		fieldType: typ + "/" + pecTypeName, fieldProperties: props,
	}
}

func peTargetKeyOf(targetID string) string {
	rid, err := ParseGenericResourcePath(targetID)
	if err != nil {
		return ""
	}

	return peTargetKey(rid)
}

func targetTypeOf(targetID string) string {
	rid, err := ParseGenericResourcePath(targetID)
	if err != nil || len(rid.Types) == 0 {
		return ""
	}

	return rid.Namespace + "/" + rid.Types[0]
}

// --- registry integration ---

func (r *Registry) networkProviderFor() *NetworkProvider {
	p, ok := r.providers[strings.ToLower(namespaceMicrosoftNetwork)]
	if !ok {
		return nil
	}

	np, _ := p.(*NetworkProvider)

	return np
}

// peRoute reports whether id addresses a private endpoint connection or private link resource of a supported target.
func (r *Registry) peRoute(id ResourceID) *NetworkProvider {
	if len(id.Types) != 2 || len(id.Names) == 0 {
		return nil
	}

	if _, ok := peTargets[peTargetKey(id)]; !ok {
		return nil
	}

	if !strings.EqualFold(id.Types[1], pecTypeName) && !strings.EqualFold(id.Types[1], plrTypeName) {
		return nil
	}

	return r.networkProviderFor()
}

// enrichTarget adds the privateEndpointConnections list to a target resource body.
func (r *Registry) enrichTarget(body map[string]any) map[string]any {
	np := r.networkProviderFor()
	if np == nil {
		return body
	}

	return np.enrichTarget(body)
}

func (p *NetworkProvider) enrichTarget(body map[string]any) map[string]any {
	tid := strOf(body, netFieldID)
	if _, ok := peTargets[peTargetKeyOf(tid)]; !ok {
		return body
	}

	p.mu.RLock("enrichTarget")
	defer p.mu.RUnlock()

	conns := p.connsOf(tid)
	if len(conns) == 0 {
		return body
	}

	list := make([]any, 0, len(conns))
	for _, c := range conns {
		list = append(list, p.targetConnBody(c, tid))
	}

	out := maps.Clone(body)
	props := maps.Clone(propsOf(body))

	if props == nil {
		props = map[string]any{}
	}

	props["privateEndpointConnections"] = list
	out[fieldProperties] = props

	return out
}

func targetID(id ResourceID) string {
	return ResourceID{
		SubscriptionID: id.SubscriptionID, ResourceGroup: id.ResourceGroup, Namespace: id.Namespace,
		Types: id.Types[:1], Names: id.Names[:1],
	}.ARMID()
}

// TargetGet serves GET of a target's privateEndpointConnections / privateLinkResources child.
func (p *NetworkProvider) targetGet(
	ctx context.Context,
	id ResourceID,
	parentExists func(context.Context) error,
) (map[string]any, error) {
	if err := parentExists(ctx); err != nil {
		return nil, err
	}

	tid := targetID(id)

	p.mu.RLock("targetGet")
	defer p.mu.RUnlock()

	if strings.EqualFold(id.Types[1], plrTypeName) {
		return p.linkResourceBody(id, tid)
	}

	for _, c := range p.connsOf(tid) {
		if strings.EqualFold(c.targetName(), id.Names[1]) {
			return p.targetConnBody(c, tid), nil
		}
	}

	return nil, ErrResourceNotFound
}

func (p *NetworkProvider) targetList(
	ctx context.Context,
	id ResourceID,
	parentExists func(context.Context) error,
) ([]map[string]any, error) {
	if err := parentExists(ctx); err != nil {
		return nil, err
	}

	tid := targetID(id)

	p.mu.RLock("targetList")
	defer p.mu.RUnlock()

	out := []map[string]any{}

	if strings.EqualFold(id.Types[1], plrTypeName) {
		t := peTargets[peTargetKey(id)]
		for name := range t.groups {
			b, _ := p.linkResourceBody(ResourceID{
				SubscriptionID: id.SubscriptionID, ResourceGroup: id.ResourceGroup, Namespace: id.Namespace,
				Types: id.Types, Names: []string{id.Names[0], name},
			}, tid)
			out = append(out, b)
		}

		sortByName(out)

		return out, nil
	}

	for _, c := range p.connsOf(tid) {
		out = append(out, p.targetConnBody(c, tid))
	}

	return out, nil
}

func (p *NetworkProvider) linkResourceBody(id ResourceID, tid string) (map[string]any, error) {
	t := peTargets[peTargetKey(id)]

	key, g, ok := t.group(id.Names[1])
	if !ok {
		return nil, ErrResourceNotFound
	}

	return map[string]any{
		netFieldID: tid + "/" + plrTypeName + "/" + key, fieldName: key,
		fieldType: targetTypeOf(tid) + "/" + plrTypeName,
		fieldProperties: map[string]any{
			"groupId": key, "requiredMembers": []any{key}, "requiredZoneNames": []any{g.zone},
		},
	}, nil
}

// targetPut serves PUT of a target's privateEndpointConnections child: the owner approves or rejects a connection.
func (p *NetworkProvider) targetPut(
	ctx context.Context,
	id ResourceID,
	body map[string]any,
	parentExists func(context.Context) error,
) (map[string]any, error) {
	if err := parentExists(ctx); err != nil {
		return nil, err
	}

	if !strings.EqualFold(id.Types[1], pecTypeName) {
		return nil, netErr(http.StatusMethodNotAllowed, "MethodNotAllowed", "Method not allowed.")
	}

	state, _ := propsOf(body)["privateLinkServiceConnectionState"].(map[string]any)

	status, ok := canonEnum(strOf(state, netPropStatus), []string{peStatusOK, peStatusNo, peStatusWait})
	if !ok {
		return nil, netBad("privateLinkServiceConnectionState.status must be Approved, Rejected or Pending.")
	}

	tid := targetID(id)

	p.mu.Lock("targetPut")
	defer p.mu.Unlock()

	for _, c := range p.connsOf(tid) {
		if strings.EqualFold(c.targetName(), id.Names[1]) {
			c.pe.Obj.Aux["st:"+c.name] = status
			c.pe.Obj.Aux["desc:"+c.name] = strOf(state, netPropDescription)
			p.touch(c.pe)

			return p.targetConnBody(c, tid), nil
		}
	}

	return nil, ErrResourceNotFound
}

// targetDelete serves DELETE of a target's privateEndpointConnections child: the endpoint becomes Disconnected.
func (p *NetworkProvider) targetDelete(
	ctx context.Context,
	id ResourceID,
	parentExists func(context.Context) error,
) error {
	if err := parentExists(ctx); err != nil {
		return err
	}

	if !strings.EqualFold(id.Types[1], pecTypeName) {
		return netErr(http.StatusMethodNotAllowed, "MethodNotAllowed", "Method not allowed.")
	}

	tid := targetID(id)

	p.mu.Lock("targetDelete")
	defer p.mu.Unlock()

	for _, c := range p.connsOf(tid) {
		if strings.EqualFold(c.targetName(), id.Names[1]) {
			c.pe.Obj.Aux["gone:"+c.name] = "1"
			p.touch(c.pe)

			return nil
		}
	}

	return ErrResourceNotFound
}

const (
	plListSegs     = 9
	plInstanceSegs = 10
	plChildIdx     = 8
)

// privateLinkRoutes serve the privateEndpointConnections and privateLinkResources children of supported targets.
//
//nolint:gochecknoglobals // static route table, read-only after init
var privateLinkRoutes = []armRoute{
	{
		match: func(segs []string, r *http.Request) bool {
			return len(segs) == plListSegs && r.Method == http.MethodGet && isPrivateLinkChild(segs)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error {
			parent, err := ParseGenericResourcePath("/" + joinSegs(segs[:plChildIdx]))
			if err != nil {
				return h.writeAPIError(c, err)
			}

			parent.Types = append(parent.Types, segs[plChildIdx])

			values, err := h.Registry.List(c.Request().Context(), parent)
			if err != nil {
				return h.writeAPIError(c, err)
			}

			return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: values})
		},
	},
	{
		match: func(segs []string, _ *http.Request) bool {
			return len(segs) == plInstanceSegs && isPrivateLinkChild(segs)
		},
		handle: func(h *Handler, c *echo.Context, _ []string) error { return h.handleGenericResource(c) },
	},
}

func isPrivateLinkChild(segs []string) bool {
	if !isSubscriptionScoped(segs) || !segAt(segs, providersOrResourceGroupsIdx, resourceGroupsSegment) ||
		!segAt(segs, providerNamespaceActionIdx, providersSegment) {
		return false
	}

	if _, ok := peTargets[strings.ToLower(segs[nsRGNamespaceIdx]+"/"+segs[nsRGNamespaceIdx+1])]; !ok {
		return false
	}

	return strings.EqualFold(segs[plChildIdx], pecTypeName) || strings.EqualFold(segs[plChildIdx], plrTypeName)
}
