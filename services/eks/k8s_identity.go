package eks

import (
	"path"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	authModeAPI          = "API"
	authModeConfigMap    = "CONFIG_MAP"
	authModeAPIAndConfig = "API_AND_CONFIG_MAP"

	mastersGroup      = "system:masters"
	accessGroupPrefix = "gopherstack:eks:access:"

	principalRole = "role"
	principalUser = "user"
	principalRoot = "root"

	arnParts       = 6
	assumedParts   = 3
	sessionVarName = "{{SessionName}}"
	sessionVarRaw  = "{{SessionNameRaw}}"
	accessKeyVar   = "{{AccessKeyID}}"
)

// principal is a parsed IAM principal; role sessions and role ARNs share a canonical key.
type principal struct {
	arn     string
	kind    string
	account string
	name    string
	session string
}

func (p principal) canonical() string { return p.kind + "/" + p.account + "/" + p.name }

func parsePrincipal(a string) (principal, bool) {
	parts := strings.SplitN(a, ":", arnParts)
	if len(parts) != arnParts {
		return principal{}, false
	}

	service, account, resource := parts[2], parts[4], parts[5]
	p := principal{arn: a, account: account}

	switch {
	case service == "sts" && strings.HasPrefix(resource, "assumed-role/"):
		segs := strings.SplitN(strings.TrimPrefix(resource, "assumed-role/"), "/", assumedParts)
		if len(segs) < 2 || segs[0] == "" {
			return principal{}, false
		}

		p.kind, p.name, p.session = principalRole, segs[0], segs[len(segs)-1]
	case service == arnSvcIAM && strings.HasPrefix(resource, "role/"):
		p.kind, p.name = principalRole, lastSegment(resource)
	case service == arnSvcIAM && strings.HasPrefix(resource, "user/"):
		p.kind, p.name = principalUser, lastSegment(resource)
	case service == arnSvcIAM && resource == "root":
		p.kind, p.name = principalRoot, "root"
	default:
		return principal{}, false
	}

	return p, p.name != ""
}

func lastSegment(resource string) string {
	return path.Base(resource)
}

// k8sIdentity is the Kubernetes user a verified AWS principal is mapped to.
type k8sIdentity struct {
	Extra    map[string][]string
	Username string
	Groups   []string
}

// awsAuthMaps is the parsed content of a cluster's kube-system/aws-auth ConfigMap.
type awsAuthMaps struct {
	Roles []awsAuthEntry
	Users []awsAuthEntry
}

type awsAuthEntry struct {
	RoleARN  string   `yaml:"rolearn"`
	UserARN  string   `yaml:"userarn"`
	Username string   `yaml:"username"`
	Groups   []string `yaml:"groups"`
}

func parseAwsAuth(data map[string]string) awsAuthMaps {
	var m awsAuthMaps

	_ = yaml.Unmarshal([]byte(data["mapRoles"]), &m.Roles)
	_ = yaml.Unmarshal([]byte(data["mapUsers"]), &m.Users)

	return m
}

func (m awsAuthMaps) lookup(p principal) (awsAuthEntry, bool) {
	list, field := m.Users, func(e awsAuthEntry) string { return e.UserARN }
	if p.kind == principalRole {
		list, field = m.Roles, func(e awsAuthEntry) string { return e.RoleARN }
	}

	for _, e := range list {
		if ep, ok := parsePrincipal(field(e)); ok && ep.canonical() == p.canonical() {
			return e, true
		}
	}

	return awsAuthEntry{}, false
}

func accessGroupName(clusterName string, p principal) string {
	return accessGroupPrefix + stableID(clusterName+"/"+p.canonical())
}

func defaultUsername(p principal) string {
	if p.kind == principalRole {
		return arn.Build(arnSvcSTS, "", p.account, "assumed-role/"+p.name+"/"+sessionVarName)
	}

	return p.arn
}

func expandUsername(tmpl string, p principal, accessKeyID string) string {
	if tmpl == "" {
		tmpl = defaultUsername(p)
	}

	r := strings.NewReplacer(
		sessionVarName, strings.ReplaceAll(p.session, "@", "-"),
		sessionVarRaw, p.session,
		accessKeyVar, accessKeyID,
	)

	return r.Replace(tmpl)
}

func authModeAllows(mode, want string) bool {
	if mode == "" {
		mode = authModeAPIAndConfig
	}

	return mode == authModeAPIAndConfig || mode == want
}

// k8sIdentityForLocked maps a verified principal to a Kubernetes identity; granted is false when access is denied.
// Must be called with b.mu held.
func (b *InMemoryBackend) k8sIdentityForLocked(
	cl *Cluster, p principal, accessKeyID string, awsAuth awsAuthMaps,
) (k8sIdentity, bool) {
	id := k8sIdentity{
		Extra: map[string][]string{
			"arn":          {p.arn},
			"canonicalarn": {canonicalARN(p)},
			"sessionname":  {p.session},
			"accesskeyid":  {accessKeyID},
		},
	}

	granted := false

	if authModeAllows(accessMode(cl), authModeAPI) {
		for _, e := range b.accessEntriesByCluster.Get(cl.Name) {
			if ep, ok := parsePrincipal(e.PrincipalARN); ok && ep.canonical() == p.canonical() {
				id.Username = expandUsername(e.Username, p, accessKeyID)
				id.Groups = append(append(id.Groups, e.KubernetesGroups...), accessGroupName(cl.Name, p))
				granted = true

				break
			}
		}
	}

	if !granted && authModeAllows(accessMode(cl), authModeConfigMap) {
		if e, ok := awsAuth.lookup(p); ok {
			id.Username = expandUsername(e.Username, p, accessKeyID)
			id.Groups = append(id.Groups, e.Groups...)
			granted = true
		}
	}

	if b.isClusterCreatorLocked(cl, p) {
		if id.Username == "" {
			id.Username = expandUsername("", p, accessKeyID)
		}

		id.Groups = append(id.Groups, mastersGroup)
		granted = true
	}

	return id, granted
}

func accessMode(cl *Cluster) string {
	if cl.AccessConfig == nil {
		return ""
	}

	return cl.AccessConfig.AuthenticationMode
}

func canonicalARN(p principal) string {
	if p.kind == principalRole {
		return arn.Build(arnSvcIAM, "", p.account, "role/"+p.name)
	}

	return p.arn
}

func (b *InMemoryBackend) isClusterCreatorLocked(cl *Cluster, p principal) bool {
	if cl.AccessConfig != nil && !cl.AccessConfig.BootstrapClusterCreatorAdminPermissions {
		return false
	}

	creator, ok := parsePrincipal(cl.CreatorARN)

	return ok && creator.canonical() == p.canonical()
}
