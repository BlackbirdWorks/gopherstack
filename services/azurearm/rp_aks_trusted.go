package azurearm

import (
	"net/http"
	"regexp"
	"strings"
	"time"
)

const (
	aksBindingType      = "trustedAccessRoleBindings"
	aksBindingMax       = 8
	aksBindingNameMax   = 24
	aksBindingSucceeded = "Succeeded"
	aksArmProviders     = "providers"
)

var aksBindingNameRe = regexp.MustCompile(`^([A-Za-z0-9-])+$`)

type storedAKSBinding struct {
	Created  time.Time `json:"created"`
	Modified time.Time `json:"modified"`
	Name     string    `json:"name"`
	Source   string    `json:"source"`
	Roles    []string  `json:"roles"`
}

func (c *storedAKSCluster) bindingBody(b *storedAKSBinding) map[string]any {
	roles := make([]any, 0, len(b.Roles))
	for _, r := range b.Roles {
		roles = append(roles, r)
	}

	return map[string]any{
		"id": c.childID(aksBindingType, b.Name), fieldName: b.Name,
		fieldType: namespaceMicrosoftContainerService + "/" + aksClusterType + "/" + aksBindingType,
		fieldProperties: map[string]any{
			fieldProvisioningState: aksBindingSucceeded, "sourceResourceId": b.Source, "roles": roles,
		},
		acaFieldSystemData: acaSystemData(b.Created, b.Modified),
	}
}

// aksTrustedRoles is the trusted access role catalog. Only roles whose rules the API reference publishes are
// listed (the 2026-08-01 TrustedAccessRoles_List example); bindings accept any role of the source's type.
func aksTrustedRoles() []any {
	return []any{map[string]any{
		fieldName:            "reader",
		"sourceResourceType": "Microsoft.MachineLearningServices/workspaces",
		"rules": []any{map[string]any{
			"apiGroups": []any{""}, "nonResourceURLs": []any{}, "resourceNames": []any{},
			"resources": []any{"pods"}, "verbs": []any{"get"},
		}},
	}}
}

// aksSourceType extracts "Microsoft.X/type[/subtype]" from an ARM resource ID.
func aksSourceType(id string) (string, bool) {
	segs := strings.Split(strings.Trim(id, "/"), "/")

	at := -1

	for i, s := range segs {
		if strings.EqualFold(s, aksArmProviders) {
			at = i
		}
	}

	rest := segs[at+1:]
	if at < 0 || len(rest) < 3 || len(rest)%2 == 0 || !strings.EqualFold(segs[0], "subscriptions") {
		return "", false
	}

	parts := []string{rest[0]}
	for i := 1; i < len(rest); i += 2 {
		if rest[i] == "" || rest[i+1] == "" {
			return "", false
		}

		parts = append(parts, rest[i])
	}

	return strings.Join(parts, "/"), true
}

func aksCheckBinding(name string, body map[string]any) (string, []string, error) {
	if len(name) > aksBindingNameMax || !aksBindingNameRe.MatchString(name) {
		return "", nil, aksBad("Trusted access role binding name '%s' is invalid: use 1-%d letters, digits or hyphens.",
			name, aksBindingNameMax)
	}

	props := mapOf(body[fieldProperties])
	if props == nil {
		return "", nil, aksBad("properties is required.")
	}

	source := strOf(props, "sourceResourceId")

	srcType, ok := aksSourceType(source)
	if !ok {
		return "", nil, aksBad("sourceResourceId '%s' is not a valid Azure resource ID.", source)
	}

	raw := acaList(props["roles"])
	if len(raw) == 0 {
		return "", nil, aksBad("roles must list at least one role.")
	}

	roles := make([]string, 0, len(raw))

	for _, r := range raw {
		role, _ := r.(string)

		prefix := srcType + "/"
		if len(role) <= len(prefix) || !strings.EqualFold(role[:len(prefix)], prefix) ||
			strings.Contains(role[len(prefix):], "/") {
			return "", nil, aksBad("Role '%s' is not a role of source resource type '%s'; roles are named "+
				"<resource type>/<role>.", role, srcType)
		}

		roles = append(roles, role)
	}

	return source, roles, nil
}

func (p *ContainerServiceProvider) putBinding(id ResourceID, body map[string]any) (map[string]any, error) {
	p.opMu.Lock("putBinding")
	defer p.opMu.Unlock()

	c, ok := p.lookup(id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, aksNotFound(id.ResourceGroup, id.Names[0])
	}

	name := id.Names[1]

	source, roles, err := aksCheckBinding(name, body)
	if err != nil {
		return nil, err
	}

	p.mu.Lock("putBinding")
	defer p.mu.Unlock()

	key := strings.ToLower(name)
	if _, exists := c.Bindings[key]; !exists && len(c.Bindings) >= aksBindingMax {
		return nil, netErr(http.StatusBadRequest, aksCodeNoOp,
			"A managed cluster can have at most %d trusted access role bindings.", aksBindingMax)
	}

	if c.Bindings == nil {
		c.Bindings = map[string]*storedAKSBinding{}
	}

	now := p.now().UTC()
	rec := &storedAKSBinding{Name: name, Source: source, Roles: roles, Created: now, Modified: now}

	if old := c.Bindings[key]; old != nil {
		rec.Created, rec.Name = old.Created, old.Name
	}

	c.Bindings[key] = rec

	return c.bindingBody(rec), nil
}

func (p *ContainerServiceProvider) getBinding(id ResourceID) (map[string]any, error) {
	p.mu.RLock("getBinding")
	defer p.mu.RUnlock()

	c, ok := p.clusters[aksKey(id.ResourceGroup, id.Names[0])]
	if !ok {
		return nil, aksNotFound(id.ResourceGroup, id.Names[0])
	}

	b, found := c.Bindings[strings.ToLower(id.Names[1])]
	if !found {
		return nil, ErrResourceNotFound
	}

	return c.bindingBody(b), nil
}

func (p *ContainerServiceProvider) listBindings(id ResourceID) ([]map[string]any, error) {
	p.mu.RLock("listBindings")
	defer p.mu.RUnlock()

	c, ok := p.clusters[aksKey(id.ResourceGroup, id.Names[0])]
	if !ok {
		return nil, aksNotFound(id.ResourceGroup, id.Names[0])
	}

	out := []map[string]any{}
	for _, k := range sortedKeys(c.Bindings) {
		out = append(out, c.bindingBody(c.Bindings[k]))
	}

	return out, nil
}

func (p *ContainerServiceProvider) deleteBinding(id ResourceID) error {
	p.opMu.Lock("deleteBinding")
	defer p.opMu.Unlock()

	p.mu.Lock("deleteBinding")
	defer p.mu.Unlock()

	c, ok := p.clusters[aksKey(id.ResourceGroup, id.Names[0])]
	if !ok {
		return aksNotFound(id.ResourceGroup, id.Names[0])
	}

	key := strings.ToLower(id.Names[1])
	if _, found := c.Bindings[key]; !found {
		return ErrResourceNotFound
	}

	delete(c.Bindings, key)

	return nil
}
