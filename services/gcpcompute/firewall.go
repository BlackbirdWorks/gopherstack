package gcpcompute

import (
	"regexp"
	"slices"
	"strconv"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	defaultFirewallPrio = 1000
	maxPriority         = 65535
	maxTags             = 256
	maxFirewallRanges   = 256
	dirIngress          = "INGRESS"
	dirEgress           = "EGRESS"
)

var (
	portRE      = regexp.MustCompile(`^(\d{1,5})(-(\d{1,5}))?$`)
	protocolSet = map[string]bool{ //nolint:gochecknoglobals // read-only lookup table
		"tcp":  true,
		"udp":  true,
		"icmp": true,
		"esp":  true,
		"ah":   true,
		"sctp": true,
		"ipip": true,
		"all":  true,
	}
	portProtos = map[string]bool{ //nolint:gochecknoglobals // read-only lookup table
		"tcp":  true,
		"udp":  true,
		"sctp": true,
	}
	tagRE = nameRE //nolint:gochecknoglobals // read-only lookup table
)

func fwTarget(project string, f *computeapi.Firewall) target {
	return target{project: project, scope: scopeGlobal, collection: "firewalls", name: f.Name, id: f.Id}
}

func validProtocol(field, proto string, ports []string) *APIError {
	p := strings.ToLower(proto)

	if !protocolSet[p] {
		if n, err := strconv.Atoi(proto); err != nil || n < 0 || n > 255 {
			return errField(
				field+".IPProtocol",
				proto,
				"Must be one of tcp, udp, icmp, esp, ah, sctp, ipip, all or a protocol number.",
			)
		}

		p = ""
	}

	if len(ports) > 0 && !portProtos[p] {
		return errField(field+".ports", strings.Join(ports, ","), "Ports may only be specified for tcp, udp and sctp.")
	}

	for _, port := range ports {
		m := portRE.FindStringSubmatch(port)
		if m == nil {
			return errField(field+".ports", port, "Must be a port number or range.")
		}

		lo, _ := strconv.Atoi(m[1])
		hi := lo

		if m[3] != "" {
			hi, _ = strconv.Atoi(m[3])
		}

		if hi > maxPriority || lo > hi {
			return errField(field+".ports", port, "Port out of range.")
		}
	}

	return nil
}

func validRanges(field string, ranges []string) *APIError {
	if len(ranges) > maxFirewallRanges {
		return errField(field, "", "Too many ranges.")
	}

	for _, r := range ranges {
		if _, err := parseCIDR(field, r); err != nil {
			return err
		}
	}

	return nil
}

func validTags(field string, tags []string) *APIError {
	if len(tags) > maxTags {
		return errField(field, "", "Too many tags.")
	}

	for _, t := range tags {
		if !tagRE.MatchString(t) {
			return errField(field, t, "Tags must match regex '[a-z]([-a-z0-9]*[a-z0-9])?'.")
		}
	}

	return nil
}

func validateFirewall(f *computeapi.Firewall) *APIError {
	if (len(f.Allowed) == 0) == (len(f.Denied) == 0) {
		return errInvalid("Invalid value for field 'resource': ''. Exactly one of allowed or denied must be specified.")
	}

	for i, a := range f.Allowed {
		if err := validProtocol("resource.allowed["+strconv.Itoa(i)+"]", a.IPProtocol, a.Ports); err != nil {
			return err
		}
	}

	for i, d := range f.Denied {
		if err := validProtocol("resource.denied["+strconv.Itoa(i)+"]", d.IPProtocol, d.Ports); err != nil {
			return err
		}
	}

	if f.Priority < 0 || f.Priority > maxPriority {
		return errField("resource.priority", strconv.FormatInt(f.Priority, 10), "Priority must be between 0 and 65535.")
	}

	if err := validateDirection(f); err != nil {
		return err
	}

	return validateFirewallTargets(f)
}

func validateDirection(f *computeapi.Firewall) *APIError {
	switch f.Direction {
	case dirIngress:
		if len(f.DestinationRanges) > 0 {
			return errField("resource.destinationRanges", "", "destinationRanges is not valid for INGRESS rules.")
		}

		if len(f.TargetServiceAccounts) > 0 && len(f.TargetTags) > 0 {
			return errInvalid("Invalid value for field 'resource.targetTags': " +
				"targetTags and targetServiceAccounts are mutually exclusive.")
		}
	case dirEgress:
		if len(f.SourceRanges)+len(f.SourceTags)+len(f.SourceServiceAccounts) > 0 {
			return errInvalid(
				"Invalid value for field 'resource.sourceRanges': source fields are not valid for EGRESS rules.",
			)
		}
	default:
		return errField("resource.direction", f.Direction, "Must be INGRESS or EGRESS.")
	}

	return nil
}

func validateFirewallTargets(f *computeapi.Firewall) *APIError {
	if err := validRanges("resource.sourceRanges", f.SourceRanges); err != nil {
		return err
	}

	if err := validRanges("resource.destinationRanges", f.DestinationRanges); err != nil {
		return err
	}

	if err := validTags("resource.sourceTags", f.SourceTags); err != nil {
		return err
	}

	return validTags("resource.targetTags", f.TargetTags)
}

func (b *InMemoryBackend) getFirewallLocked(project, name string) (*computeapi.Firewall, *APIError) {
	f, ok := b.firewalls[key(project, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scopeGlobal, "", "firewalls", name))
	}

	return f, nil
}

func (b *InMemoryBackend) insertFirewall(project string, in *computeapi.Firewall) (*computeapi.Operation, error) {
	b.mu.Lock("InsertFirewall")
	defer b.mu.Unlock()

	if err := validName("name", in.Name); err != nil {
		return nil, err
	}

	if _, dup := b.firewalls[key(project, in.Name)]; dup {
		return nil, errExists(relPath(project, scopeGlobal, "", "firewalls", in.Name))
	}

	f := clone(in)
	if f.Direction == "" {
		f.Direction = dirIngress
	}

	if f.Priority == 0 && !hasForced(in.ForceSendFields, fieldPriority) {
		f.Priority = defaultFirewallPrio
	}

	netRef := in.Network
	if netRef == "" {
		netRef = "global/networks/default"
	}

	n, err := b.resolveNetworkLocked(project, netRef)
	if err != nil {
		return nil, err
	}

	f.Network = n.SelfLink

	if apiErr := validateFirewall(f); apiErr != nil {
		return nil, apiErr
	}

	f.Kind, f.Id, f.CreationTimestamp = "compute#firewall", newID(), b.stamp()
	f.SelfLink = globalLink(project, "firewalls", f.Name)
	b.firewalls[key(project, f.Name)] = f

	return b.newOpLocked("insert", fwTarget(project, f), b.timing.Done), nil
}

func hasForced(fields []string, name string) bool {
	return slices.Contains(fields, name)
}

func (b *InMemoryBackend) getFirewall(project, name string) (*computeapi.Firewall, error) {
	b.mu.RLock("GetFirewall")
	defer b.mu.RUnlock()

	f, err := b.getFirewallLocked(project, name)
	if err != nil {
		return nil, err
	}

	return viewFirewall(f), nil
}

func viewFirewall(f *computeapi.Firewall) *computeapi.Firewall {
	out := clone(f)
	out.ForceSendFields = []string{"Disabled", fieldPriority}

	return out
}

func (b *InMemoryBackend) listFirewalls(project string) []entry {
	b.mu.RLock("ListFirewalls")
	defer b.mu.RUnlock()

	var out []entry

	for k, f := range b.firewalls {
		if strings.HasPrefix(k, project+"/") {
			out = append(out, entry{scope: scopeGlobal, item: viewFirewall(f)})
		}
	}

	return out
}

func (b *InMemoryBackend) deleteFirewall(project, name string) (*computeapi.Operation, error) {
	b.mu.Lock("DeleteFirewall")
	defer b.mu.Unlock()

	f, err := b.getFirewallLocked(project, name)
	if err != nil {
		return nil, err
	}

	delete(b.firewalls, key(project, name))

	return b.newOpLocked("delete", fwTarget(project, f), b.timing.Done), nil
}

// updateFirewall applies a PUT (full replace) when merged is the complete new body, or a PATCH merge result.
func (b *InMemoryBackend) updateFirewall(
	project, name, opType string,
	build func(cur *computeapi.Firewall) (*computeapi.Firewall, *APIError),
) (*computeapi.Operation, error) {
	b.mu.Lock("UpdateFirewall")
	defer b.mu.Unlock()

	cur, err := b.getFirewallLocked(project, name)
	if err != nil {
		return nil, err
	}

	next, apiErr := build(clone(cur))
	if apiErr != nil {
		return nil, apiErr
	}

	if next.Name != "" && next.Name != cur.Name {
		return nil, errField("resource.name", next.Name, "Field is not updatable.")
	}

	if next.Network != "" && next.Network != cur.Network {
		n, nerr := b.resolveNetworkLocked(project, next.Network)
		if nerr != nil || n.SelfLink != cur.Network {
			return nil, errField("resource.network", next.Network, "Field is not updatable.")
		}
	}

	next.Name, next.Network, next.Id, next.Kind = cur.Name, cur.Network, cur.Id, cur.Kind
	next.CreationTimestamp, next.SelfLink = cur.CreationTimestamp, cur.SelfLink

	if next.Direction == "" {
		next.Direction = cur.Direction
	}

	if next.Priority == 0 && !hasForced(next.ForceSendFields, fieldPriority) {
		next.Priority = defaultFirewallPrio
	}

	if apiErr = validateFirewall(next); apiErr != nil {
		return nil, apiErr
	}

	b.firewalls[key(project, name)] = next

	return b.newOpLocked(opType, fwTarget(project, next), b.timing.Done), nil
}
