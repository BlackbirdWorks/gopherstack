package route53recoveryreadiness

import (
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// resourceSetTypes are the documented ResourceSetType values (route53recoveryreadiness api_op_CreateResourceSet.go).
func resourceSetTypes() []string {
	return []string{
		typeAPIGWStage, typeAPIGWV2Stage, typeASG,
		typeAlarm, typeCGW, typeTable, typeVolume,
		typeELBClassic, typeELBv2, typeLambda,
		typeMSK, typeRDSCluster, typeHealthCheck, typeSQS, typeSNS,
		typeSNSSub, typeVPC, typeVPNConn, typeVPNGateway, typeDNSTarget,
	}
}

// SetView is the wire form of a resource set.
type SetView struct {
	Tags            map[string]string `json:"tags,omitempty"`
	ResourceSetArn  string            `json:"resourceSetArn"`
	ResourceSetName string            `json:"resourceSetName"`
	ResourceSetType string            `json:"resourceSetType"`
	Resources       []Resource        `json:"resources"`
}

func setView(s *ResourceSet) SetView {
	return SetView{
		ResourceSetArn: s.ARN, ResourceSetName: s.Name, ResourceSetType: s.Type,
		Resources: slices.Clone(s.Resources), Tags: cloneTags(s.Tags),
	}
}

func normalizeResources(typ string, in []Resource) ([]Resource, error) {
	out := make([]Resource, 0, len(in))
	ids := map[string]bool{}

	for i, raw := range in {
		r, err := checkResource(typ, i, raw)
		if err != nil {
			return nil, err
		}

		id := r.identifier()
		if ids[id] {
			return nil, restroute.Validation("Resource %s appears more than once", id)
		}

		ids[id] = true
		out = append(out, r)
	}

	return out, nil
}

// checkResource validates one resource against the set type and fills in a generated component ID for DNS targets.
func checkResource(typ string, i int, r Resource) (Resource, error) {
	if r.DNS != nil && r.DNS.empty() {
		r.DNS = nil
	}

	hasArn, hasDNS := r.ResourceArn != "", r.DNS != nil

	switch {
	case hasArn == hasDNS:
		return r, restroute.Validation("Resource %d must set exactly one of resourceArn or dnsTargetResource", i)
	case typ == typeDNSTarget && !hasDNS:
		return r, restroute.Validation("Resources of type %s must set dnsTargetResource", typeDNSTarget)
	case typ != typeDNSTarget && !hasArn:
		return r, restroute.Validation("Resources of type %s must set resourceArn", typ)
	case hasArn && !strings.HasPrefix(r.ResourceArn, "arn:"):
		return r, restroute.Validation("resourceArn %q is not a valid ARN", r.ResourceArn)
	case hasDNS && r.DNS.DomainName == "":
		return r, restroute.Validation("dnsTargetResource.domainName is required")
	}

	if r.ComponentID == "" && hasDNS {
		r.ComponentID = r.DNS.DomainName + "-" + r.DNS.RecordType + "-" + r.DNS.RecordSetID
	}

	return r, nil
}

// empty reports a dnsTargetResource with no members set, which clients send for "no DNS target".
func (d *DNSTarget) empty() bool {
	return d.DomainName == "" && d.HostedZoneArn == "" && d.RecordSetID == "" && d.RecordType == "" &&
		(d.Target == nil || (d.Target.NLB == nil && d.Target.R53 == nil))
}

func validateSetType(typ string) error {
	if !slices.Contains(resourceSetTypes(), typ) {
		return restroute.Validation("ResourceSetType %q is not supported", typ)
	}

	return nil
}

// CreateResourceSet creates a resource set.
func (b *InMemoryBackend) CreateResourceSet(
	name, typ string,
	resources []Resource,
	tags map[string]string,
) (SetView, error) {
	if err := validName("resource set", name); err != nil {
		return SetView{}, err
	}

	if err := validateSetType(typ); err != nil {
		return SetView{}, err
	}

	res, err := normalizeResources(typ, resources)
	if err != nil {
		return SetView{}, err
	}

	b.mu.Lock("CreateResourceSet")
	defer b.mu.Unlock()

	if b.sets.Has(name) {
		return SetView{}, restroute.Conflict("Resource set %s already exists", name)
	}

	s := &ResourceSet{ARN: b.arnFor("resource-set", name), Name: name, Type: typ, Resources: res, Tags: cloneTags(tags)}
	b.sets.Put(s)

	return setView(s), nil
}

// GetResourceSet returns a resource set.
func (b *InMemoryBackend) GetResourceSet(name string) (SetView, error) {
	b.mu.RLock("GetResourceSet")
	defer b.mu.RUnlock()

	s, err := b.getSet(name)
	if err != nil {
		return SetView{}, err
	}

	return setView(s), nil
}

// ListResourceSets lists resource sets.
func (b *InMemoryBackend) ListResourceSets(token string, maxResults int) ([]SetView, string, error) {
	b.mu.RLock("ListResourceSets")
	defer b.mu.RUnlock()

	pg, err := restroute.Paginate(sorted(b.sets.All(), func(s *ResourceSet) string { return s.Name }),
		token, maxResults, listDefault, listMax)
	if err != nil {
		return nil, "", err
	}

	out := make([]SetView, 0, len(pg.Data))
	for _, s := range pg.Data {
		out = append(out, setView(s))
	}

	return out, pg.Next, nil
}

// UpdateResourceSet replaces a resource set's type and resources.
func (b *InMemoryBackend) UpdateResourceSet(name, typ string, resources []Resource) (SetView, error) {
	if err := validateSetType(typ); err != nil {
		return SetView{}, err
	}

	res, err := normalizeResources(typ, resources)
	if err != nil {
		return SetView{}, err
	}

	b.mu.Lock("UpdateResourceSet")
	defer b.mu.Unlock()

	s, err := b.getSet(name)
	if err != nil {
		return SetView{}, err
	}

	s.Type, s.Resources = typ, res

	return setView(s), nil
}

// DeleteResourceSet deletes a resource set no readiness check uses.
func (b *InMemoryBackend) DeleteResourceSet(name string) error {
	b.mu.Lock("DeleteResourceSet")
	defer b.mu.Unlock()

	if _, err := b.getSet(name); err != nil {
		return err
	}

	for _, c := range b.checks.All() {
		if c.ResourceSet == name {
			return restroute.Conflict("Resource set %s is used by readiness check %s", name, c.Name)
		}
	}

	b.sets.Delete(name)

	return nil
}
