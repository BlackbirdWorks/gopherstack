package eks

import (
	"context"
	"fmt"
	"slices"
	"strings"
)

const (
	rbacGroup         = "rbac.authorization.k8s.io"
	kindCRB           = "ClusterRoleBinding"
	kindRB            = "RoleBinding"
	adminViewRoleName = "gopherstack-eks-admin-view"
	accessScopeNS     = "namespace"
)

// accessPolicyRoles maps EKS access policy names to the Kubernetes ClusterRole that implements them.
func accessPolicyRoles() map[string]string {
	return map[string]string{
		"AmazonEKSClusterAdminPolicy": "cluster-admin",
		"AmazonEKSAdminPolicy":        "admin",
		"AmazonEKSEditPolicy":         "edit",
		"AmazonEKSViewPolicy":         "view",
		"AmazonEKSAdminViewPolicy":    adminViewRoleName,
	}
}

type bindingDesired struct {
	kind      string
	name      string
	namespace string
	role      string
	group     string
}

func (d bindingDesired) key() string { return d.kind + "/" + d.namespace + "/" + d.name }

func (d bindingDesired) path() string {
	if d.kind == kindRB {
		return rbacAPIPath + "/namespaces/" + d.namespace + "/rolebindings/" + d.name
	}

	return rbacAPIPath + "/clusterrolebindings/" + d.name
}

func (d bindingDesired) object() map[string]any {
	meta := map[string]any{keyName: d.name, keyLabels: map[string]string{managedByLabel: managedByValue}}
	if d.kind == kindRB {
		meta["namespace"] = d.namespace
	}

	return map[string]any{
		keyAPIVersion: rbacGroup + "/v1",
		keyKind:       d.kind,
		keyMetadata:   meta,
		"roleRef":     map[string]string{"apiGroup": rbacGroup, keyKind: "ClusterRole", keyName: d.role},
		"subjects":    []map[string]string{{"apiGroup": rbacGroup, keyKind: "Group", keyName: d.group}},
	}
}

func scopeNamespaces(scope map[string]any) []string {
	if t, _ := scope["type"].(string); t != accessScopeNS {
		return nil
	}

	var out []string

	switch ns := scope["namespaces"].(type) {
	case []string:
		out = slices.Clone(ns)
	case []any:
		for _, v := range ns {
			if s, ok := v.(string); ok {
				out = append(out, s)
			}
		}
	}

	return out
}

// desiredBindingsLocked renders every associated access policy as RBAC bindings to the entry's access group.
// Must be called with b.mu held.
func (b *InMemoryBackend) desiredBindingsLocked(cluster string) []bindingDesired {
	roles := accessPolicyRoles()

	var out []bindingDesired

	for _, e := range b.accessEntriesByCluster.Get(cluster) {
		p, ok := parsePrincipal(e.PrincipalARN)
		if !ok {
			continue
		}

		group := accessGroupName(cluster, p)
		base := "gopherstack-eks-" + stableID(cluster+"/"+p.canonical())

		for _, assoc := range b.accessPolicies[cluster][e.PrincipalARN] {
			policy := assoc.PolicyARN[strings.LastIndex(assoc.PolicyARN, "/")+1:]

			role, mapped := roles[policy]
			if !mapped {
				continue
			}

			name := base + "-" + strings.ToLower(policy)

			if nss := scopeNamespaces(assoc.AccessScope); len(nss) > 0 {
				for _, ns := range nss {
					out = append(out, bindingDesired{kind: kindRB, name: name, namespace: ns, role: role, group: group})
				}

				continue
			}

			out = append(out, bindingDesired{kind: kindCRB, name: name, role: role, group: group})
		}
	}

	return out
}

func adminViewClusterRole() map[string]any {
	return map[string]any{
		keyAPIVersion: rbacGroup + "/v1",
		keyKind:       "ClusterRole",
		keyMetadata: map[string]any{
			keyName: adminViewRoleName, keyLabels: map[string]string{managedByLabel: managedByValue},
		},
		"rules": []map[string]any{{
			"apiGroups": []string{"*"}, "resources": []string{"*"}, "verbs": []string{"get", "list", "watch"},
		}},
	}
}

type managedBindings struct {
	have map[string]struct{}
	crbs []k8sMeta
	rbs  []k8sMeta
}

func listBindings(ctx context.Context, api *k8sAPI) (managedBindings, error) {
	crbs, err := api.listManaged(ctx, rbacAPIPath+"/clusterrolebindings")
	if err != nil {
		return managedBindings{}, fmt.Errorf("list cluster role bindings: %w", err)
	}

	rbs, err := api.listManaged(ctx, rbacAPIPath+"/rolebindings")
	if err != nil {
		return managedBindings{}, fmt.Errorf("list role bindings: %w", err)
	}

	m := managedBindings{crbs: crbs, rbs: rbs, have: map[string]struct{}{}}
	for _, c := range crbs {
		m.have[kindCRB+"//"+c.Name] = struct{}{}
	}

	for _, r := range rbs {
		m.have[kindRB+"/"+r.Namespace+"/"+r.Name] = struct{}{}
	}

	return m, nil
}

func (m managedBindings) prune(ctx context.Context, api *k8sAPI, want map[string]bindingDesired) error {
	for _, c := range m.crbs {
		if _, ok := want[kindCRB+"//"+c.Name]; !ok {
			if err := api.remove(ctx, rbacAPIPath+"/clusterrolebindings/"+c.Name); err != nil {
				return fmt.Errorf("delete %s: %w", c.Name, err)
			}
		}
	}

	for _, r := range m.rbs {
		if _, ok := want[kindRB+"/"+r.Namespace+"/"+r.Name]; !ok {
			if err := api.remove(ctx, rbacAPIPath+"/namespaces/"+r.Namespace+"/rolebindings/"+r.Name); err != nil {
				return fmt.Errorf("delete %s/%s: %w", r.Namespace, r.Name, err)
			}
		}
	}

	return nil
}

// syncBindings makes the cluster's gopherstack-managed bindings equal to desired.
func syncBindings(ctx context.Context, api *k8sAPI, desired []bindingDesired) error {
	cur, err := listBindings(ctx, api)
	if err != nil {
		return err
	}

	want := make(map[string]bindingDesired, len(desired))
	needAdminView := false

	for _, d := range desired {
		want[d.key()] = d
		needAdminView = needAdminView || d.role == adminViewRoleName
	}

	if needAdminView {
		if err = api.apply(ctx, rbacAPIPath+"/clusterroles/"+adminViewRoleName, adminViewClusterRole()); err != nil {
			return fmt.Errorf("apply admin-view role: %w", err)
		}
	}

	for k, d := range want {
		if _, ok := cur.have[k]; ok {
			continue
		}

		if err = api.apply(ctx, d.path(), d.object()); err != nil {
			return fmt.Errorf("apply %s %s: %w", d.kind, d.name, err)
		}
	}

	return cur.prune(ctx, api, want)
}
