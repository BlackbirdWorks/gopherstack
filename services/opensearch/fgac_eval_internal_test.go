package opensearch

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWildcardMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		input   string
		want    bool
	}{
		{name: "star all", pattern: "*", input: "anything", want: true},
		{name: "prefix", pattern: "logs-*", input: "logs-2024", want: true},
		{name: "prefix miss", pattern: "logs-*", input: "metrics-1", want: false},
		{name: "suffix", pattern: "*-prod", input: "app-prod", want: true},
		{name: "middle", pattern: "a*c", input: "abbbc", want: true},
		{name: "question", pattern: "log?", input: "logs", want: true},
		{name: "question too long", pattern: "log?", input: "logss", want: false},
		{name: "exact", pattern: "idx", input: "idx", want: true},
		{name: "exact miss", pattern: "idx", input: "idx2", want: false},
		{name: "regex", pattern: "/^logs-[0-9]+$/", input: "logs-12", want: true},
		{name: "regex miss", pattern: "/^logs-[0-9]+$/", input: "logs-ab", want: false},
		{name: "bad regex", pattern: "/(/", input: "x", want: false},
		{name: "empty pattern", pattern: "", input: "x", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, wildcardMatch(tt.pattern, tt.input))
		})
	}
}

func evalConfig() *SecurityConfig {
	cfg := newSecurityConfig("d")
	cfg.Roles["logs_reader"] = &SecurityRole{
		ClusterPermissions: []string{"cluster_composite_ops_ro"},
		IndexPermissions: []RoleIndexPermission{{
			IndexPatterns: []string{"logs-*"}, AllowedActions: []string{"read"},
		}},
	}
	cfg.Roles["own"] = &SecurityRole{
		IndexPermissions: []RoleIndexPermission{{
			IndexPatterns: []string{"user-${user_name}-*"}, AllowedActions: []string{"crud"},
		}},
	}
	cfg.Roles["custom_group"] = &SecurityRole{
		IndexPermissions: []RoleIndexPermission{{
			IndexPatterns: []string{"data"}, AllowedActions: []string{"my_writes"},
		}},
	}
	cfg.ActionGroups["my_writes"] = &SecurityActionGroup{
		Type: actionGroupTypeIndex, AllowedActions: []string{"indices:data/write/index", "get"},
	}
	cfg.ActionGroups["loop_a"] = &SecurityActionGroup{AllowedActions: []string{"loop_b"}}
	cfg.ActionGroups["loop_b"] = &SecurityActionGroup{AllowedActions: []string{"loop_a"}}
	cfg.Roles["looping"] = &SecurityRole{
		IndexPermissions: []RoleIndexPermission{{IndexPatterns: []string{"x"}, AllowedActions: []string{"loop_a"}}},
	}
	cfg.RolesMapping["logs_reader"] = &RoleMapping{
		BackendRoles: []string{"arn:aws:iam::1:role/reader"},
		Users:        []string{"bob"},
	}
	cfg.RolesMapping["own"] = &RoleMapping{Users: []string{"*"}}
	cfg.RolesMapping["custom_group"] = &RoleMapping{Users: []string{"carol"}}
	cfg.RolesMapping["looping"] = &RoleMapping{Users: []string{"dave"}}
	cfg.RolesMapping[roleAllAccess] = &RoleMapping{Users: []string{"admin"}}
	cfg.RolesMapping["readall"] = &RoleMapping{AndBackendRoles: []string{"team-*", "oncall"}}

	return cfg
}

func TestEffectivePerms_IndexAndCluster(t *testing.T) {
	t.Parallel()

	cfg := evalConfig()

	tests := []struct {
		name        string
		index       string
		action      string
		cluster     string
		principal   securityPrincipal
		wantIndex   bool
		wantCluster bool
	}{
		{
			name: "user read matching pattern", principal: securityPrincipal{Name: "bob"},
			index: "logs-1", action: "indices:data/read/search", wantIndex: true,
		},
		{
			name: "user read other index", principal: securityPrincipal{Name: "bob"},
			index: "metrics-1", action: "indices:data/read/search",
		},
		{
			name: "read group excludes write", principal: securityPrincipal{Name: "bob"},
			index: "logs-1", action: "indices:data/write/index",
		},
		{
			name:      "backend role mapping",
			principal: securityPrincipal{Name: "x", BackendRoles: []string{"arn:aws:iam::1:role/reader"}},
			index:     "logs-9", action: "indices:data/read/get", wantIndex: true,
		},
		{
			name: "cluster composite ro group", principal: securityPrincipal{Name: "bob"},
			cluster: "indices:data/read/mget", wantCluster: true,
		},
		{
			name: "cluster composite ro excludes bulk", principal: securityPrincipal{Name: "bob"},
			cluster: "indices:data/write/bulk",
		},
		{
			name: "all access index", principal: securityPrincipal{Name: "admin"},
			index: "anything", action: "indices:admin/delete", wantIndex: true,
		},
		{
			name: "all access cluster", principal: securityPrincipal{Name: "admin"},
			cluster: "cluster:admin/whatever", wantCluster: true,
		},
		{
			name: "user_name substitution", principal: securityPrincipal{Name: "erin"},
			index: "user-erin-1", action: "indices:data/write/index", wantIndex: true,
		},
		{
			name: "user_name substitution other user", principal: securityPrincipal{Name: "erin"},
			index: "user-frank-1", action: "indices:data/write/index",
		},
		{
			name: "custom action group", principal: securityPrincipal{Name: "carol"},
			index: "data", action: "indices:data/read/get", wantIndex: true,
		},
		{
			name: "custom action group no search", principal: securityPrincipal{Name: "carol"},
			index: "data", action: "indices:data/read/search",
		},
		{
			name: "cyclic groups do not hang", principal: securityPrincipal{Name: "dave"},
			index: "x", action: "indices:data/read/search",
		},
		{
			name:      "and_backend_roles needs all",
			principal: securityPrincipal{Name: "g", BackendRoles: []string{"team-a"}},
			index:     "any", action: "indices:data/read/search",
		},
		{
			name:      "and_backend_roles satisfied",
			principal: securityPrincipal{Name: "g", BackendRoles: []string{"team-a", "oncall"}},
			index:     "any", action: "indices:data/read/search", wantIndex: true,
		},
		{
			name: "unmapped user", principal: securityPrincipal{Name: "nobody"},
			index: "logs-1", action: "indices:data/read/search",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			perms := buildEffectivePerms(cfg, tt.principal)

			if tt.index != "" {
				assert.Equal(t, tt.wantIndex, perms.AllowIndex(tt.index, tt.action))
			}

			if tt.cluster != "" {
				assert.Equal(t, tt.wantCluster, perms.AllowCluster(tt.cluster))
			}
		})
	}
}

func TestEffectivePerms_NilConfigGrantsNothing(t *testing.T) {
	t.Parallel()

	perms := buildEffectivePerms(nil, securityPrincipal{Name: "admin"})
	assert.False(t, perms.AllowCluster("cluster:monitor/main"))
	assert.False(t, perms.AllowIndex("i", "indices:data/read/search"))
}

func restrictionDoc() map[string]any {
	return map[string]any{
		"owner": "alice",
		"ssn":   "123-45-6789",
		"email": "alice@example.com",
		"address": map[string]any{
			"city": "Oslo",
			"zip":  "0150",
		},
		"tags": []any{"a", "b"},
	}
}

func TestAccessRestriction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantDocs  map[string]bool
		wantMask  map[string]string
		name      string
		grants    []RoleIndexPermission
		wantKeys  []string
		wantAbsnt []string
	}{
		{
			name:     "no restrictions",
			grants:   []RoleIndexPermission{{IndexPatterns: []string{"i"}, AllowedActions: []string{"read"}}},
			wantKeys: []string{"owner", "ssn", "email", "address", "tags"},
		},
		{
			name: "fls include",
			grants: []RoleIndexPermission{{
				IndexPatterns: []string{"i"}, AllowedActions: []string{"read"}, FLS: []string{"owner", "address.city"},
			}},
			wantKeys:  []string{"owner", "address"},
			wantAbsnt: []string{"ssn", "email", "tags"},
		},
		{
			name: "fls exclude",
			grants: []RoleIndexPermission{{
				IndexPatterns: []string{"i"}, AllowedActions: []string{"read"}, FLS: []string{"~ssn", "~address.zip"},
			}},
			wantKeys:  []string{"owner", "email", "address", "tags"},
			wantAbsnt: []string{"ssn"},
		},
		{
			name: "fls union across grants",
			grants: []RoleIndexPermission{
				{IndexPatterns: []string{"i"}, AllowedActions: []string{"read"}, FLS: []string{"owner"}},
				{IndexPatterns: []string{"i"}, AllowedActions: []string{"read"}, FLS: []string{"email"}},
			},
			wantKeys:  []string{"owner", "email"},
			wantAbsnt: []string{"ssn", "address", "tags"},
		},
		{
			name: "grant without fls shows all",
			grants: []RoleIndexPermission{
				{IndexPatterns: []string{"i"}, AllowedActions: []string{"read"}, FLS: []string{"owner"}},
				{IndexPatterns: []string{"i"}, AllowedActions: []string{"read"}},
			},
			wantKeys: []string{"owner", "ssn", "email", "address", "tags"},
		},
		{
			name: "mask default hashes",
			grants: []RoleIndexPermission{{
				IndexPatterns: []string{"i"}, AllowedActions: []string{"read"}, MaskedFields: []string{"ssn"},
			}},
			wantKeys: []string{"owner", "ssn", "email"},
			wantMask: map[string]string{"ssn": applyMask("", "123-45-6789")},
		},
		{
			name: "mask regex",
			grants: []RoleIndexPermission{{
				IndexPatterns: []string{"i"}, AllowedActions: []string{"read"},
				MaskedFields: []string{"email::/[a-z]/::*"},
			}},
			wantKeys: []string{"owner", "email"},
			wantMask: map[string]string{"email": "*****@*******.***"},
		},
		{
			name: "mask sha256",
			grants: []RoleIndexPermission{{
				IndexPatterns: []string{
					"i",
				}, AllowedActions: []string{"read"}, MaskedFields: []string{"owner::SHA-256"},
			}},
			wantKeys: []string{"owner"},
			wantMask: map[string]string{"owner": "2bd806c97f0e00af1a1fc3328fa763a9269723c8db8fac4f93af71db186d6e90"},
		},
		{
			name: "dls term",
			grants: []RoleIndexPermission{{
				IndexPatterns: []string{"i"}, AllowedActions: []string{"read"}, DLS: `{"term":{"owner":"alice"}}`,
			}},
			wantKeys: []string{"owner", "ssn", "email", "address", "tags"},
			wantDocs: map[string]bool{"alice": true, "bob": false},
		},
		{
			name: "dls or across grants",
			grants: []RoleIndexPermission{
				{IndexPatterns: []string{"i"}, AllowedActions: []string{"read"}, DLS: `{"term":{"owner":"alice"}}`},
				{IndexPatterns: []string{"i"}, AllowedActions: []string{"read"}, DLS: `{"term":{"owner":"bob"}}`},
			},
			wantKeys: []string{"owner"},
			wantDocs: map[string]bool{"alice": true, "bob": true, "carol": false},
		},
		{
			name: "grant without dls lifts dls",
			grants: []RoleIndexPermission{
				{IndexPatterns: []string{"i"}, AllowedActions: []string{"read"}, DLS: `{"term":{"owner":"alice"}}`},
				{IndexPatterns: []string{"i"}, AllowedActions: []string{"read"}},
			},
			wantKeys: []string{"owner"},
			wantDocs: map[string]bool{"alice": true, "bob": true},
		},
		{
			name: "dls bool",
			grants: []RoleIndexPermission{{
				IndexPatterns: []string{"i"}, AllowedActions: []string{"read"},
				DLS: `{"bool":{"must":[{"term":{"owner":"alice"}}],"must_not":[{"term":{"email":"x"}}]}}`,
			}},
			wantKeys: []string{"owner"},
			wantDocs: map[string]bool{"alice": true, "bob": false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := newSecurityConfig("d")
			cfg.Roles["r"] = &SecurityRole{IndexPermissions: tt.grants}
			cfg.RolesMapping["r"] = &RoleMapping{Users: []string{"u"}}

			perms := buildEffectivePerms(cfg, securityPrincipal{Name: "u"})
			res, err := perms.restrictionFor("i", "indices:data/read/search")
			require.NoError(t, err)

			out := res.Apply(restrictionDoc())
			for _, k := range tt.wantKeys {
				assert.Contains(t, out, k)
			}

			for _, k := range tt.wantAbsnt {
				assert.NotContains(t, out, k)
			}

			for k, want := range tt.wantMask {
				assert.Equal(t, want, out[k], k)
			}

			for owner, want := range tt.wantDocs {
				assert.Equal(t, want, res.docVisible(map[string]any{"owner": owner}), owner)
			}
		})
	}
}

func TestAccessRestriction_MaskValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		spec string
		in   string
		want string
	}{
		{
			name: "sha256",
			spec: "SHA-256",
			in:   "abc",
			want: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
		},
		{name: "regex replace", spec: "/[0-9]/::#", in: "a1b22", want: "a#b##"},
		{name: "unknown algorithm stars", spec: "weird", in: "abc", want: "***"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, applyMask(tt.spec, tt.in))
		})
	}

	t.Run("default is blake2b-512 hex", func(t *testing.T) {
		t.Parallel()

		got := applyMask("", "secret")
		assert.Len(t, got, 128)
		assert.NotEqual(t, "secret", got)
		assert.Equal(t, got, applyMask("", "secret"))
	})
}

func TestRestrictionFor_InvalidDLS(t *testing.T) {
	t.Parallel()

	cfg := newSecurityConfig("d")
	cfg.Roles["r"] = &SecurityRole{IndexPermissions: []RoleIndexPermission{{
		IndexPatterns: []string{"i"}, AllowedActions: []string{"read"}, DLS: `not json`,
	}}}
	cfg.RolesMapping["r"] = &RoleMapping{Users: []string{"u"}}

	_, err := buildEffectivePerms(cfg, securityPrincipal{Name: "u"}).restrictionFor("i", "indices:data/read/search")
	require.ErrorIs(t, err, errBadDLSQuery)
}

func TestAnalyzeDataRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		method      string
		path        string
		wantKind    string
		wantIndex   string
		wantAction  string
		wantCluster string
	}{
		{
			name:       "search",
			method:     http.MethodPost,
			path:       "/logs-1/_search",
			wantKind:   opKindSearch,
			wantIndex:  "logs-1",
			wantAction: actionSearch,
		},
		{
			name:       "canonical search",
			method:     http.MethodPost,
			path:       "/index/logs-1/_search",
			wantKind:   opKindSearch,
			wantIndex:  "logs-1",
			wantAction: actionSearch,
		},
		{
			name:       "count is search",
			method:     http.MethodGet,
			path:       "/logs-1/_count",
			wantKind:   opKindCount,
			wantIndex:  "logs-1",
			wantAction: actionSearch,
		},
		{
			name:       "get doc",
			method:     http.MethodGet,
			path:       "/i/_doc/1",
			wantKind:   opKindGet,
			wantIndex:  "i",
			wantAction: actionGetDoc,
		},
		{
			name:       "index doc",
			method:     http.MethodPost,
			path:       "/i/_doc/1",
			wantKind:   opKindOther,
			wantIndex:  "i",
			wantAction: actionIndexDoc,
		},
		{
			name:       "put doc",
			method:     http.MethodPut,
			path:       "/i/_doc/1",
			wantKind:   opKindOther,
			wantIndex:  "i",
			wantAction: actionIndexDoc,
		},
		{
			name:       "delete doc",
			method:     http.MethodDelete,
			path:       "/i/_doc/1",
			wantKind:   opKindOther,
			wantIndex:  "i",
			wantAction: actionDeleteDoc,
		},
		{
			name:       "create index",
			method:     http.MethodPut,
			path:       "/i",
			wantKind:   opKindOther,
			wantIndex:  "i",
			wantAction: actionCreateIndex,
		},
		{
			name:       "delete index",
			method:     http.MethodDelete,
			path:       "/i",
			wantKind:   opKindOther,
			wantIndex:  "i",
			wantAction: actionDeleteIndex,
		},
		{
			name:       "get index",
			method:     http.MethodGet,
			path:       "/i",
			wantKind:   opKindOther,
			wantIndex:  "i",
			wantAction: actionGetIndex,
		},
		{name: "bulk", method: http.MethodPost, path: "/_bulk", wantKind: opKindBulk, wantCluster: actionBulk},
		{name: "index bulk", method: http.MethodPost, path: "/i/_bulk", wantKind: opKindBulk, wantCluster: actionBulk},
		{name: "root", method: http.MethodGet, path: "/", wantKind: opKindOther, wantCluster: actionClusterMain},
		{
			name:        "health",
			method:      http.MethodGet,
			path:        "/_cluster/health",
			wantKind:    opKindOther,
			wantCluster: actionClusterHlth,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			op := analyzeDataRequest(tt.method, tt.path)
			assert.Equal(t, tt.wantKind, op.Kind)

			if tt.wantAction != "" {
				require.Len(t, op.Indices, 1)
				assert.Equal(t, indexCheck{Index: tt.wantIndex, Action: tt.wantAction}, op.Indices[0])
			}

			if tt.wantCluster != "" {
				assert.Equal(t, []string{tt.wantCluster}, op.Cluster)
			}
		})
	}
}

func TestApplyPatchOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		doc     string
		ops     string
		want    string
		wantErr bool
	}{
		{name: "replace", doc: `{"a":1}`, ops: `[{"op":"replace","path":"/a","value":2}]`, want: `{"a":2}`},
		{name: "add key", doc: `{"a":1}`, ops: `[{"op":"add","path":"/b","value":"x"}]`, want: `{"a":1,"b":"x"}`},
		{name: "remove", doc: `{"a":1,"b":2}`, ops: `[{"op":"remove","path":"/b"}]`, want: `{"a":1}`},
		{
			name: "nested",
			doc:  `{"a":{"b":1}}`,
			ops:  `[{"op":"replace","path":"/a/b","value":9}]`,
			want: `{"a":{"b":9}}`,
		},
		{name: "array append", doc: `{"l":[1]}`, ops: `[{"op":"add","path":"/l/-","value":2}]`, want: `{"l":[1,2]}`},
		{
			name: "array insert",
			doc:  `{"l":[1,3]}`,
			ops:  `[{"op":"add","path":"/l/1","value":2}]`,
			want: `{"l":[1,2,3]}`,
		},
		{name: "array remove", doc: `{"l":[1,2]}`, ops: `[{"op":"remove","path":"/l/0"}]`, want: `{"l":[2]}`},
		{name: "escaped path", doc: `{"a/b":1}`, ops: `[{"op":"replace","path":"/a~1b","value":2}]`, want: `{"a/b":2}`},
		{name: "replace missing", doc: `{"a":1}`, ops: `[{"op":"replace","path":"/z","value":2}]`, wantErr: true},
		{name: "bad op", doc: `{"a":1}`, ops: `[{"op":"move","path":"/a"}]`, wantErr: true},
		{name: "bad index", doc: `{"l":[1]}`, ops: `[{"op":"replace","path":"/l/5","value":2}]`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var doc any
			require.NoError(t, json.Unmarshal([]byte(tt.doc), &doc))

			var ops []patchOp
			require.NoError(t, json.Unmarshal([]byte(tt.ops), &ops))

			got, err := applyPatchOps(doc, ops)
			if tt.wantErr {
				require.ErrorIs(t, err, errPatch)

				return
			}

			require.NoError(t, err)
			b, _ := json.Marshal(got)
			assert.JSONEq(t, tt.want, string(b))
		})
	}
}

func TestValidateAdvancedSecurity(t *testing.T) {
	t.Parallel()

	base := func() *Domain {
		return &Domain{
			EngineVersion:               "OpenSearch_2.11",
			EncryptionAtRestOptions:     &EncryptionAtRestOptions{Enabled: true},
			NodeToNodeEncryptionOptions: &NodeToNodeEncryptionOptions{Enabled: true},
			DomainEndpointOptions:       &DomainEndpointOptions{EnforceHTTPS: true},
			AdvancedSecurityOptions: &AdvancedSecurityOptions{
				Enabled: true, InternalUserDatabaseEnabled: true,
				MasterUserName: "admin", MasterUserPasswordHash: "$2a$10$x",
			},
		}
	}

	tests := []struct {
		name    string
		mutate  func(d *Domain)
		prev    *AdvancedSecurityOptions
		wantErr string
	}{
		{name: "valid", mutate: func(*Domain) {}},
		{
			name:    "no encryption at rest",
			mutate:  func(d *Domain) { d.EncryptionAtRestOptions = nil },
			wantErr: "encryption at rest",
		},
		{
			name:    "no node to node",
			mutate:  func(d *Domain) { d.NodeToNodeEncryptionOptions.Enabled = false },
			wantErr: "node-to-node",
		},
		{name: "no https", mutate: func(d *Domain) { d.DomainEndpointOptions.EnforceHTTPS = false }, wantErr: "HTTPS"},
		{
			name:    "old elasticsearch",
			mutate:  func(d *Domain) { d.EngineVersion = "Elasticsearch_6.5" },
			wantErr: "Elasticsearch 6.7",
		},
		{name: "elasticsearch 7", mutate: func(d *Domain) { d.EngineVersion = "Elasticsearch_7.10" }},
		{
			name:    "internal db without master",
			mutate:  func(d *Domain) { d.AdvancedSecurityOptions.MasterUserName = "" },
			wantErr: "MasterUserName",
		},
		{
			name: "internal db with arn",
			mutate: func(d *Domain) {
				d.AdvancedSecurityOptions.MasterUserARN = "arn:aws:iam::123456789012:role/m"
			},
			wantErr: "MasterUserARN",
		},
		{
			name: "iam master valid",
			mutate: func(d *Domain) {
				d.AdvancedSecurityOptions = &AdvancedSecurityOptions{
					Enabled: true, MasterUserARN: "arn:aws:iam::123456789012:role/m",
				}
			},
		},
		{
			name: "iam master missing arn",
			mutate: func(d *Domain) {
				d.AdvancedSecurityOptions = &AdvancedSecurityOptions{Enabled: true}
			},
			wantErr: "MasterUserARN is required",
		},
		{
			name:    "cannot disable",
			mutate:  func(d *Domain) { d.AdvancedSecurityOptions.Enabled = false },
			prev:    &AdvancedSecurityOptions{Enabled: true},
			wantErr: "cannot be disabled",
		},
		{
			name: "saml without fgac",
			mutate: func(d *Domain) {
				d.AdvancedSecurityOptions = &AdvancedSecurityOptions{SAMLOptions: &SAMLOptionsInput{Enabled: true}}
			},
			wantErr: "require fine-grained access control",
		},
		{
			name: "saml needs idp",
			mutate: func(d *Domain) {
				d.AdvancedSecurityOptions.SAMLOptions = &SAMLOptionsInput{Enabled: true}
			},
			wantErr: "Idp",
		},
		{
			name: "jwt needs key",
			mutate: func(d *Domain) {
				d.AdvancedSecurityOptions.JWTOptions = &JWTOptions{Enabled: true}
			},
			wantErr: "JWTOptions",
		},
		{
			name: "jwt single key source",
			mutate: func(d *Domain) {
				d.AdvancedSecurityOptions.JWTOptions = &JWTOptions{Enabled: true, PublicKey: "k", JwksURL: "https://x"}
			},
			wantErr: "only one",
		},
		{
			name: "jwt valid",
			mutate: func(d *Domain) {
				d.AdvancedSecurityOptions.JWTOptions = &JWTOptions{Enabled: true, PublicKey: "k"}
			},
		},
		{
			name:   "update keeps master",
			mutate: func(d *Domain) { d.AdvancedSecurityOptions.MasterUserPasswordHash = "" },
			prev:   &AdvancedSecurityOptions{Enabled: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := base()
			tt.mutate(d)

			err := validateAdvancedSecurity(d, tt.prev)
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, ErrValidation)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestMasterPasswordComplexity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pw      string
		wantErr bool
	}{
		{name: "valid", pw: "Sup3r-Secret!"},
		{name: "too short", pw: "Ab1!", wantErr: true},
		{name: "no upper", pw: "supersecret1!", wantErr: true},
		{name: "no lower", pw: "SUPERSECRET1!", wantErr: true},
		{name: "no digit", pw: "SuperSecret!!", wantErr: true},
		{name: "no special", pw: "SuperSecret12", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := masterPasswordComplexity(tt.pw)
			if tt.wantErr {
				require.ErrorIs(t, err, ErrValidation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPasswordHashing(t *testing.T) {
	t.Parallel()

	hash := hashMasterPassword("Sup3r-Secret!")
	require.NotEmpty(t, hash)
	assert.NotContains(t, hash, "Sup3r")
	assert.True(t, masterPasswordMatches(hash, "Sup3r-Secret!"))
	assert.False(t, masterPasswordMatches(hash, "wrong"))
	assert.False(t, masterPasswordMatches("", "x"))
}
