package opensearch

// Fine-grained access control (the OpenSearch security plugin) state. Wire shapes follow the
// security plugin REST API docs:
// https://docs.opensearch.org/latest/security/access-control/api/

// SecurityConfig is the per-domain security plugin configuration stored by the backend.
// Built-in (static) roles and action groups are not stored; they are synthesised on read.
type SecurityConfig struct {
	Users        map[string]*InternalUser        `json:"users"`
	Roles        map[string]*SecurityRole        `json:"roles"`
	RolesMapping map[string]*RoleMapping         `json:"rolesMapping"`
	ActionGroups map[string]*SecurityActionGroup `json:"actionGroups"`
	Tenants      map[string]*SecurityTenant      `json:"tenants"`
	Domain       string                          `json:"domain"`
}

// InternalUser is an entry of the security plugin internal user database. Hash is a bcrypt hash
// and is never returned by the API.
type InternalUser struct {
	Attributes   map[string]string `json:"attributes"`
	Hash         string            `json:"hash"`
	Description  string            `json:"description"`
	BackendRoles []string          `json:"backend_roles"`
	Reserved     bool              `json:"reserved"`
	Hidden       bool              `json:"hidden"`
}

// RoleIndexPermission is one index_permissions entry of a role.
type RoleIndexPermission struct {
	DLS            string   `json:"dls"`
	IndexPatterns  []string `json:"index_patterns"`
	FLS            []string `json:"fls"`
	MaskedFields   []string `json:"masked_fields"`
	AllowedActions []string `json:"allowed_actions"`
}

// RoleTenantPermission is one tenant_permissions entry of a role.
type RoleTenantPermission struct {
	TenantPatterns []string `json:"tenant_patterns"`
	AllowedActions []string `json:"allowed_actions"`
}

// SecurityRole is a security plugin role.
type SecurityRole struct {
	Description        string                 `json:"description"`
	ClusterPermissions []string               `json:"cluster_permissions"`
	IndexPermissions   []RoleIndexPermission  `json:"index_permissions"`
	TenantPermissions  []RoleTenantPermission `json:"tenant_permissions"`
	Reserved           bool                   `json:"reserved"`
	Hidden             bool                   `json:"hidden"`
	Static             bool                   `json:"static"`
}

// RoleMapping maps users and backend roles onto a role.
type RoleMapping struct {
	Description     string   `json:"description"`
	BackendRoles    []string `json:"backend_roles"`
	Hosts           []string `json:"hosts"`
	Users           []string `json:"users"`
	AndBackendRoles []string `json:"and_backend_roles"`
	Reserved        bool     `json:"reserved"`
	Hidden          bool     `json:"hidden"`
}

// SecurityActionGroup is a named set of permissions.
type SecurityActionGroup struct {
	Description    string   `json:"description"`
	Type           string   `json:"type"`
	AllowedActions []string `json:"allowed_actions"`
	Reserved       bool     `json:"reserved"`
	Hidden         bool     `json:"hidden"`
	Static         bool     `json:"static"`
}

// SecurityTenant is a Dashboards tenant.
type SecurityTenant struct {
	Description string `json:"description"`
	Reserved    bool   `json:"reserved"`
	Hidden      bool   `json:"hidden"`
}

const (
	segIndex                = "index"
	segBulk                 = "_bulk"
	jsonKeyError            = "error"
	jsonKeyReason           = "reason"
	jsonKeyBackendRoles     = "backend_roles"
	secCodeNotFound         = slErrorCodeNotFound
	queryKeyBool            = "bool"
	queryKeyFilter          = "filter"
	patchAdd                = "add"
	patchReplace            = "replace"
	patchRemove             = "remove"
	groupClusterCompositeRO = "cluster_composite_ops_ro"

	roleAllAccess       = "all_access"
	roleSecurityManager = "security_manager"
	roleSecurityRESTAPI = "security_rest_api_access"

	actionGroupTypeCluster = "cluster"
	actionGroupTypeIndex   = segIndex
	actionGroupTypeAll     = "all"
)

func staticRole(
	desc string,
	cluster []string,
	idx []RoleIndexPermission,
	tenants []RoleTenantPermission,
) *SecurityRole {
	return &SecurityRole{
		Description: desc, ClusterPermissions: cluster, IndexPermissions: idx, TenantPermissions: tenants,
		Reserved: true,
	}
}

func idxPerm(patterns []string, actions ...string) RoleIndexPermission {
	return RoleIndexPermission{
		IndexPatterns: patterns, AllowedActions: actions, FLS: []string{}, MaskedFields: []string{},
	}
}

func tenantPerm(actions ...string) []RoleTenantPermission {
	return []RoleTenantPermission{{TenantPatterns: []string{"*"}, AllowedActions: actions}}
}

// builtinRoles returns the roles the service ships with (reserved, not editable).
func builtinRoles() map[string]*SecurityRole {
	kibanaIdx := []string{
		".kibana", ".kibana-6", ".kibana_*", ".opensearch_dashboards", ".opensearch_dashboards-6",
		".opensearch_dashboards_*",
	}
	taskIdx := []string{".tasks", ".management-beats", "*:.tasks", "*:.management-beats"}

	allAccess := staticRole("Allow full access to all indices and all cluster APIs",
		[]string{"*"}, []RoleIndexPermission{idxPerm([]string{"*"}, "*")}, tenantPerm("kibana_all_write"))
	allAccess.Static = true

	ownIndex := staticRole("Allow all for indices named like the current user",
		[]string{}, []RoleIndexPermission{idxPerm([]string{"${user_name}"}, "indices_all")}, nil)
	ownIndex.Static = true

	restAPI := staticRole("Allow access to the security REST API",
		[]string{"restapi_all"}, nil, nil)
	restAPI.Static = true

	return map[string]*SecurityRole{
		roleAllAccess: allAccess,
		roleSecurityManager: staticRole("Provide access to the security plugin APIs and nothing else",
			[]string{"cluster:admin/opendistro_security/*", "cluster:admin/opensearch/security/*"}, nil, nil),
		roleSecurityRESTAPI: restAPI,
		"own_index":         ownIndex,
		"readall": staticRole("Provide the minimum permissions for a readall user",
			[]string{groupClusterCompositeRO},
			[]RoleIndexPermission{idxPerm([]string{"*"}, "read")}, nil),
		"readall_and_monitor": staticRole("Provide the minimum permissions for a readall and monitor user",
			[]string{"cluster_monitor", groupClusterCompositeRO},
			[]RoleIndexPermission{idxPerm([]string{"*"}, "read")}, nil),
		"kibana_user": staticRole("Provide the minimum permissions for a kibana user",
			[]string{"cluster_composite_ops"},
			[]RoleIndexPermission{
				idxPerm(kibanaIdx, "read", "delete", "manage", segIndex),
				idxPerm(taskIdx, "indices_all"),
			}, tenantPerm("kibana_all_write")),
		"kibana_read_only": staticRole("A user who can read dashboards", []string{}, nil, nil),
	}
}

func staticActionGroup(typ, desc string, actions ...string) *SecurityActionGroup {
	return &SecurityActionGroup{Type: typ, Description: desc, AllowedActions: actions, Reserved: true, Static: true}
}

// builtinActionGroups returns the reserved action groups.
func builtinActionGroups() map[string]*SecurityActionGroup {
	return map[string]*SecurityActionGroup{
		"unlimited":   staticActionGroup(actionGroupTypeAll, "Unlimited access", "*"),
		"cluster_all": staticActionGroup(actionGroupTypeCluster, "Everything on cluster level", "cluster:*"),
		"cluster_monitor": staticActionGroup(actionGroupTypeCluster, "Monitor everything on cluster level",
			"cluster:monitor/*"),
		groupClusterCompositeRO: staticActionGroup(actionGroupTypeCluster,
			"Read only composite operations on cluster level",
			"indices:data/read/scroll*", "indices:data/read/msearch*", "indices:data/read/mget*",
			"indices:data/read/mtv*", "indices:data/read/coordinate-msearch*",
			"indices:admin/aliases/exists*", "indices:admin/aliases/get*"),
		"cluster_composite_ops": staticActionGroup(actionGroupTypeCluster, "Composite operations on cluster level",
			"indices:data/write/bulk", "indices:admin/aliases*", "indices:data/write/reindex",
			groupClusterCompositeRO),
		"indices_all":     staticActionGroup(actionGroupTypeIndex, "Everything on index level", "indices:*"),
		"data_access":     staticActionGroup(actionGroupTypeIndex, "All document operations", "indices:data/*"),
		"indices_monitor": staticActionGroup(actionGroupTypeIndex, "Monitor indices", "indices:monitor/*"),
		"manage": staticActionGroup(actionGroupTypeIndex, "Manage indices", "indices:monitor/*",
			"indices:admin/*"),
		"create_index": staticActionGroup(actionGroupTypeIndex, "Create indices", "indices:admin/create",
			"indices:admin/mapping/put"),
		"manage_aliases": staticActionGroup(actionGroupTypeIndex, "Manage aliases", "indices:admin/aliases*"),
		"get": staticActionGroup(actionGroupTypeIndex, "Get documents", "indices:data/read/get*",
			"indices:data/read/mget*"),
		"search": staticActionGroup(actionGroupTypeIndex, "Search documents", "indices:data/read/search*",
			"indices:data/read/msearch*", "indices:data/read/suggest*"),
		"read": staticActionGroup(actionGroupTypeIndex, "Read documents", "indices:data/read*",
			"indices:admin/mappings/fields/get*"),
		"write": staticActionGroup(actionGroupTypeIndex, "Write documents", "indices:data/write*",
			"indices:admin/mapping/put"),
		"delete": staticActionGroup(actionGroupTypeIndex, "Delete documents", "indices:data/write/delete*"),
		segIndex: staticActionGroup(actionGroupTypeIndex, "Index and update documents",
			"indices:data/write/index*", "indices:data/write/update*", "indices:data/write/bulk*",
			"indices:admin/mapping/put"),
		"crud": staticActionGroup(actionGroupTypeIndex, "Read, write and delete documents", "read", "write"),
	}
}
