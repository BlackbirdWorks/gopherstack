package azurearm

import (
	"context"
	"io"
	"net/netip"
	"strings"
)

// FlexSpec is what the database engine needs to bring up one flexible server. AdminPassword is empty when the
// request did not carry one.
type FlexSpec struct {
	Version       string
	AdminLogin    string
	AdminPassword string
}

// FlexEndpoint is where a server's engine listens.
type FlexEndpoint struct {
	Host string
	Port int
}

// FlexParam is one server parameter change for the running engine. Reset restores the engine default; Static
// parameters only take effect after the next restart.
type FlexParam struct {
	Name   string
	Value  string
	Reset  bool
	Static bool
}

// FlexEngine is the seam the flexible server providers use to run real database servers. id is the lower-cased
// ARM resource ID of the server. Primitive types only.
type FlexEngine interface {
	Ensure(ctx context.Context, id string, spec FlexSpec) (FlexEndpoint, error)
	Endpoint(id string) (FlexEndpoint, bool)
	SetPassword(ctx context.Context, id, login, password string) error
	Stop(ctx context.Context, id string) error
	Start(ctx context.Context, id string) error
	Restart(ctx context.Context, id string) error
	CreateDatabase(ctx context.Context, id, name, charset, collation string) error
	DropDatabase(ctx context.Context, id, name string) error
	Delete(ctx context.Context, id string) error
	// ApplyParameters persists the parameters in the running engine and reloads it; static ones wait for a restart.
	ApplyParameters(ctx context.Context, id string, params []FlexParam) error
	// Dump streams a logical dump of the named user databases (and the engine's default database where it has
	// one) to w.
	Dump(ctx context.Context, id string, dbs []string, w io.Writer) error
	// Restore loads a Dump stream into the server and stops at the first error.
	Restore(ctx context.Context, id string, r io.Reader) error
	// CopyAdmin gives the server dst the administrator login and password of the server src, which stay inside
	// the engine.
	CopyAdmin(ctx context.Context, src, dst string) error
	// SetReadOnly makes the server refuse writes (replicas) or accept them again (promotion).
	SetReadOnly(ctx context.Context, id string, on bool) error
}

// subnetDelegatedTo reports whether the subnet exists and carries a delegation to service.
func (p *NetworkProvider) subnetDelegatedTo(subnetID, service string) (bool, bool) {
	p.mu.RLock("subnetDelegatedTo")
	defer p.mu.RUnlock()

	_, s, ok := p.subnetByID(subnetID)
	if !ok {
		return false, false
	}

	for _, d := range anyList(s.Props["delegations"]) {
		m, _ := d.(map[string]any)
		if strings.EqualFold(strOf(propsOf(m), "serviceName"), service) {
			return true, true
		}
	}

	return true, false
}

// subnetPrefixBits returns the prefix length of the subnet's address range.
func (p *NetworkProvider) subnetPrefixBits(subnetID string) (int, bool) {
	p.mu.RLock("subnetPrefixBits")
	defer p.mu.RUnlock()

	_, s, ok := p.subnetByID(subnetID)
	if !ok {
		return 0, false
	}

	prefixes := strList(s.Props[netFieldAddrPrefixes])
	if len(prefixes) == 0 {
		return 0, false
	}

	pfx, err := netip.ParsePrefix(prefixes[0])

	return pfx.Bits(), err == nil
}

// delegatedSubnetIDs lists the subnets of the virtual network delegated to service, as ids; ok is false when the
// virtual network does not exist.
func (p *NetworkProvider) delegatedSubnetIDs(vnetID, service string) ([]string, bool) {
	p.mu.RLock("delegatedSubnetIDs")
	defer p.mu.RUnlock()

	rid, err := ParseGenericResourcePath(vnetID)
	if err != nil || !strings.EqualFold(rid.Namespace, namespaceMicrosoftNetwork) || len(rid.Types) != 1 ||
		!strings.EqualFold(rid.Types[0], netVNets) {
		return nil, false
	}

	v, ok := p.top(netVNets, rid.ResourceGroup, rid.Names[0])
	if !ok {
		return nil, false
	}

	var out []string

	for _, s := range v.Kids[netSubnets] {
		for _, d := range anyList(s.Props["delegations"]) {
			m, _ := d.(map[string]any)
			if strings.EqualFold(strOf(propsOf(m), "serviceName"), service) {
				out = append(out, subnetIDOf(v, s.Name))

				break
			}
		}
	}

	return out, true
}
