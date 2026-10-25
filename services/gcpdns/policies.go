package gcpdns

import (
	"fmt"
	"slices"
	"strings"

	dnsapi "google.golang.org/api/dns/v1"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const policyPath = "entity.policy"

func validatePolicy(p *dnsapi.Policy) error {
	if p.Name == "" {
		return errRequired(policyPath + ".name")
	}

	if !policyNameRE.MatchString(p.Name) {
		return errInvalidValue(policyPath+".name", p.Name)
	}

	p.Kind = "dns#policy"

	for i, n := range p.Networks {
		n.Kind = "dns#policyNetwork"

		if strings.TrimSpace(n.NetworkUrl) == "" {
			return errRequired(fmt.Sprintf("%s.networks[%d].networkUrl", policyPath, i))
		}
	}

	if len(p.Networks) > 100 {
		return errQuota("Quota 'NETWORKS_PER_POLICY' exceeded.")
	}

	if cfg := p.AlternativeNameServerConfig; cfg != nil {
		cfg.Kind = "dns#policyAlternativeNameServerConfig"

		if len(cfg.TargetNameServers) > 50 {
			return errQuota("Quota 'TARGET_NAME_SERVERS_PER_POLICY' exceeded.")
		}

		for i, t := range cfg.TargetNameServers {
			t.Kind = "dns#policyAlternativeNameServerConfigTargetNameServer"

			if t.Ipv4Address == "" && t.Ipv6Address == "" {
				return errInvalid("Invalid value for '%s.alternativeNameServerConfig.targetNameServers[%d]': address required",
					policyPath, i)
			}
		}
	}

	if p.Dns64Config != nil {
		p.Dns64Config.Kind = "dns#policyDns64Config"

		if p.Dns64Config.Scope != nil {
			p.Dns64Config.Scope.Kind = "dns#policyDns64ConfigScope"
		}
	}

	return nil
}

func (b *InMemoryBackend) networkTaken(project, except string, nets []*dnsapi.PolicyNetwork) error {
	for _, n := range nets {
		for k, other := range b.policies {
			if !strings.HasPrefix(k, project+"/") || other.Name == except {
				continue
			}

			for _, on := range other.Networks {
				if on.NetworkUrl == n.NetworkUrl {
					return errInvalid("Invalid value for '%s.networks': network '%s' is already attached to policy '%s'",
						policyPath, n.NetworkUrl, other.Name)
				}
			}
		}
	}

	return nil
}

// CreatePolicy stores a DNS server policy.
func (b *InMemoryBackend) CreatePolicy(project string, in *dnsapi.Policy) (*dnsapi.Policy, error) {
	p := clone(in)
	if err := validatePolicy(p); err != nil {
		return nil, err
	}

	b.mu.Lock("CreatePolicy")
	defer b.mu.Unlock()

	if _, ok := b.policies[key(project, p.Name)]; ok {
		return nil, errExists(policyPath, p.Name)
	}

	if err := b.networkTaken(project, "", p.Networks); err != nil {
		return nil, err
	}

	p.Id = newID()
	b.policies[key(project, p.Name)] = p

	return clone(p), nil
}

// GetPolicy returns one policy.
func (b *InMemoryBackend) GetPolicy(project, name string) (*dnsapi.Policy, error) {
	b.mu.RLock("GetPolicy")
	defer b.mu.RUnlock()

	p, ok := b.policies[key(project, name)]
	if !ok {
		return nil, errNotFound("parameters.policy", name)
	}

	return clone(p), nil
}

// ListPolicies returns a page of the project's policies ordered by name.
func (b *InMemoryBackend) ListPolicies(project, token string, limit int) (page.Page[*dnsapi.Policy], error) {
	if err := page.ValidateToken(token); err != nil {
		return page.Page[*dnsapi.Policy]{}, errInvalidValue("pageToken", token)
	}

	b.mu.RLock("ListPolicies")
	defer b.mu.RUnlock()

	var all []*dnsapi.Policy

	for k, p := range b.policies {
		if strings.HasPrefix(k, project+"/") {
			all = append(all, clone(p))
		}
	}

	slices.SortFunc(all, func(a, c *dnsapi.Policy) int { return strings.Compare(a.Name, c.Name) })

	return page.New(all, token, limit, 100), nil
}

// DeletePolicy removes a policy that is not attached to any network.
func (b *InMemoryBackend) DeletePolicy(project, name string) error {
	b.mu.Lock("DeletePolicy")
	defer b.mu.Unlock()

	p, ok := b.policies[key(project, name)]
	if !ok {
		return errNotFound("parameters.policy", name)
	}

	if len(p.Networks) > 0 {
		return errNotEmpty("The policy '%s' cannot be deleted because it is attached to networks.", name)
	}

	delete(b.policies, key(project, name))

	return nil
}

// UpdatePolicy replaces (in) or merges (patch) a policy.
func (b *InMemoryBackend) UpdatePolicy(
	project, name string, in *dnsapi.Policy, patch map[string]any,
) (*dnsapi.Policy, error) {
	b.mu.Lock("UpdatePolicy")
	defer b.mu.Unlock()

	cur, ok := b.policies[key(project, name)]
	if !ok {
		return nil, errNotFound("parameters.policy", name)
	}

	np := clone(in)

	if patch != nil {
		var err error
		if np, err = patched(cur, patch); err != nil {
			return nil, err
		}
	}

	if np.Name != "" && np.Name != cur.Name {
		return nil, errInvalidValue(policyPath+".name", np.Name)
	}

	np.Name, np.Id = cur.Name, cur.Id

	if err := validatePolicy(np); err != nil {
		return nil, err
	}

	if err := b.networkTaken(project, name, np.Networks); err != nil {
		return nil, err
	}

	b.policies[key(project, name)] = np

	return clone(np), nil
}
