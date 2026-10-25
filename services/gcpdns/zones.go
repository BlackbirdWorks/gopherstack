package gcpdns

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/miekg/dns"
	dnsapi "google.golang.org/api/dns/v1"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	visPublic  = "public"
	visPrivate = "private"
	stateOff   = "off"
	stateOn    = "on"
	stateXfer  = "transfer"

	soaTTL       = 21600
	defaultTTL   = 300
	soaPublicNS  = "ns-cloud-%s1.googledomains.com."
	soaMbox      = "cloud-dns-hostmaster.google.com."
	soaPrivateNS = "ns-gcp-private.googledomains.com."
	zonePath     = "entity.managedZone"
)

var (
	zoneNameRE   = regexp.MustCompile(`^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$`)
	policyNameRE = regexp.MustCompile(`^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$`)
)

func zoneState(z *dnsapi.ManagedZone) string {
	if z.DnssecConfig == nil || z.DnssecConfig.State == "" {
		return stateOff
	}

	return z.DnssecConfig.State
}

func nameServersFor(id uint64) []string {
	letter := string(rune('a' + id%5))
	out := make([]string, 0, 4)

	for i := 1; i <= 4; i++ {
		out = append(out, fmt.Sprintf("ns-cloud-%s%d.googledomains.com.", letter, i))
	}

	return out
}

func validateLabels(labels map[string]string, path string) error {
	if len(labels) > maxLabels {
		return errQuota("Quota 'LABELS_PER_MANAGED_ZONE' exceeded.")
	}

	for k, v := range labels {
		if k == "" || len(k) > maxLabelLen || len(v) > maxLabelLen {
			return errInvalidValue(path+".labels", k)
		}
	}

	return nil
}

func validateForwarding(z *dnsapi.ManagedZone) error {
	fc := z.ForwardingConfig
	if fc == nil {
		return nil
	}

	if z.Visibility != visPrivate {
		return errInvalid("Invalid value for '%s.forwardingConfig': forwarding requires a private zone", zonePath)
	}

	if len(fc.TargetNameServers) == 0 {
		return errRequired(zonePath + ".forwardingConfig.targetNameServers")
	}

	if len(fc.TargetNameServers) > 50 {
		return errQuota("Quota 'TARGET_NAME_SERVERS_PER_MANAGED_ZONE' exceeded.")
	}

	for i, t := range fc.TargetNameServers {
		t.Kind = "dns#managedZoneForwardingConfigNameServerTarget"

		if t.Ipv4Address == "" && t.Ipv6Address == "" {
			return errInvalid("Invalid value for '%s.forwardingConfig.targetNameServers[%d]': address required",
				zonePath, i)
		}

		if t.ForwardingPath != "" && t.ForwardingPath != "default" && t.ForwardingPath != "private" {
			return errInvalidValue(fmt.Sprintf("%s.forwardingConfig.targetNameServers[%d].forwardingPath", zonePath, i),
				t.ForwardingPath)
		}
	}

	fc.Kind = "dns#managedZoneForwardingConfig"

	return nil
}

func validateVisibilityConfigs(z *dnsapi.ManagedZone) error {
	pc := z.PrivateVisibilityConfig

	if z.Visibility == visPublic {
		if pc != nil {
			return errInvalid("Invalid value for '%s.privateVisibilityConfig': only valid for private zones", zonePath)
		}

		if z.PeeringConfig != nil {
			return errInvalid("Invalid value for '%s.peeringConfig': peering requires a private zone", zonePath)
		}
	}

	if z.ForwardingConfig != nil && z.PeeringConfig != nil {
		return errInvalid("Invalid value for '%s': forwardingConfig and peeringConfig are mutually exclusive", zonePath)
	}

	if pc != nil {
		pc.Kind = "dns#managedZonePrivateVisibilityConfig"

		for i, n := range pc.Networks {
			n.Kind = "dns#managedZonePrivateVisibilityConfigNetwork"

			if strings.TrimSpace(n.NetworkUrl) == "" {
				return errRequired(fmt.Sprintf("%s.privateVisibilityConfig.networks[%d].networkUrl", zonePath, i))
			}
		}

		for _, g := range pc.GkeClusters {
			g.Kind = "dns#managedZonePrivateVisibilityConfigGKECluster"
		}
	}

	if z.PeeringConfig != nil {
		pcfg := z.PeeringConfig
		pcfg.Kind = "dns#managedZonePeeringConfig"

		if pcfg.TargetNetwork == nil || strings.TrimSpace(pcfg.TargetNetwork.NetworkUrl) == "" {
			return errRequired(zonePath + ".peeringConfig.targetNetwork.networkUrl")
		}

		pcfg.TargetNetwork.Kind = "dns#managedZonePeeringConfigTargetNetwork"
	}

	return validateForwarding(z)
}

func defaultKeySpecs() []*dnsapi.DnsKeySpec {
	return []*dnsapi.DnsKeySpec{
		{Algorithm: "rsasha256", KeyLength: 2048, KeyType: keySigning, Kind: "dns#dnsKeySpec"},
		{Algorithm: "rsasha256", KeyLength: 1024, KeyType: zoneSigning, Kind: "dns#dnsKeySpec"},
	}
}

func validateDNSSEC(z *dnsapi.ManagedZone) error {
	cfg := z.DnssecConfig

	if z.Visibility == visPrivate {
		if cfg != nil && cfg.State != "" && cfg.State != stateOff {
			return errInvalid("Invalid value for '%s.dnssecConfig.state': DNSSEC requires a public zone", zonePath)
		}

		z.DnssecConfig = nil

		return nil
	}

	if cfg == nil {
		cfg = &dnsapi.ManagedZoneDnsSecConfig{}
		z.DnssecConfig = cfg
	}

	cfg.Kind = "dns#managedZoneDnsSecConfig"

	switch cfg.State {
	case "":
		cfg.State = stateOff
	case stateOff, stateOn, stateXfer:
	default:
		return errInvalidValue(zonePath+".dnssecConfig.state", cfg.State)
	}

	switch cfg.NonExistence {
	case "", "nsec", "nsec3":
	default:
		return errInvalidValue(zonePath+".dnssecConfig.nonExistence", cfg.NonExistence)
	}

	if cfg.State == stateOff && len(cfg.DefaultKeySpecs) == 0 && cfg.NonExistence == "" {
		return nil
	}

	if cfg.NonExistence == "" {
		cfg.NonExistence = "nsec3"
	}

	if len(cfg.DefaultKeySpecs) == 0 {
		cfg.DefaultKeySpecs = defaultKeySpecs()
	}

	return normalizeKeySpecs(cfg.DefaultKeySpecs)
}

func validateZone(z *dnsapi.ManagedZone) error {
	if z.Name == "" {
		return errRequired(zonePath + ".name")
	}

	if !zoneNameRE.MatchString(z.Name) {
		return errInvalidValue(zonePath+".name", z.Name)
	}

	if z.DnsName == "" {
		return errRequired(zonePath + ".dnsName")
	}

	if !dns.IsFqdn(z.DnsName) || !isDomain(z.DnsName) {
		return errInvalidValue(zonePath+".dnsName", z.DnsName)
	}

	if z.Description == "" {
		return errRequired(zonePath + ".description")
	}

	if len(z.Description) > 1024 {
		return errInvalidValue(zonePath+".description", z.Description[:32])
	}

	z.DnsName = strings.ToLower(z.DnsName)

	switch z.Visibility {
	case "":
		z.Visibility = visPublic
	case visPublic, visPrivate:
	default:
		return errInvalidValue(zonePath+".visibility", z.Visibility)
	}

	if err := validateLabels(z.Labels, zonePath); err != nil {
		return err
	}

	if z.CloudLoggingConfig != nil {
		z.CloudLoggingConfig.Kind = "dns#managedZoneCloudLoggingConfig"
	}

	if z.ServiceDirectoryConfig != nil {
		z.ServiceDirectoryConfig.Kind = "dns#managedZoneServiceDirectoryConfig"

		if z.Visibility != visPrivate {
			return errInvalid("Invalid value for '%s.serviceDirectoryConfig': requires a private zone", zonePath)
		}
	}

	if z.ReverseLookupConfig != nil {
		z.ReverseLookupConfig.Kind = "dns#managedZoneReverseLookupConfig"
	}

	z.Kind = "dns#managedZone"

	if err := validateVisibilityConfigs(z); err != nil {
		return err
	}

	return validateDNSSEC(z)
}

func (b *InMemoryBackend) defaultRRSets(z *dnsapi.ManagedZone) []*dnsapi.ResourceRecordSet {
	var soaNS string

	var ns []string

	if z.Visibility == visPrivate {
		soaNS, ns = soaPrivateNS, []string{soaPrivateNS}
	} else {
		ns = z.NameServers
		soaNS = ns[0]
	}

	return []*dnsapi.ResourceRecordSet{
		{
			Kind: "dns#resourceRecordSet", Name: z.DnsName, Type: "SOA", Ttl: soaTTL,
			Rrdatas: []string{soaNS + " " + soaMbox + " 1 21600 3600 259200 300"},
		},
		{Kind: "dns#resourceRecordSet", Name: z.DnsName, Type: "NS", Ttl: soaTTL, Rrdatas: slices.Clone(ns)},
	}
}

// CreateZone validates and stores a managed zone with its default SOA and NS records.
func (b *InMemoryBackend) CreateZone(project string, in *dnsapi.ManagedZone) (*dnsapi.ManagedZone, error) {
	z := clone(in)
	if err := validateZone(z); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateZone")
	defer b.mu.Unlock()

	if _, ok := b.zones[key(project, z.Name)]; ok {
		return nil, errExists(zonePath, z.Name)
	}

	count := 0

	for _, r := range b.zones {
		if r.Project == project {
			count++
		}
	}

	if count >= maxZones {
		return nil, errQuota("Quota 'MANAGED_ZONES' exceeded.")
	}

	z.Id = newID()
	z.CreationTime = b.stamp()

	if z.Visibility == visPublic {
		z.NameServers = nameServersFor(z.Id)
	} else {
		z.NameServers = nil
	}

	rec := &zoneRec{Zone: z, Project: project, RRSets: map[string]*dnsapi.ResourceRecordSet{}}

	var adds []*dnsapi.ResourceRecordSet

	for _, rr := range b.defaultRRSets(z) {
		rec.RRSets[rrKey(rr.Name, rr.Type)] = rr
		adds = append(adds, clone(rr))
	}

	rec.Changes = append(rec.Changes, &dnsapi.Change{
		Kind: "dns#change", Id: "0", StartTime: z.CreationTime, Status: "done", IsServing: true, Additions: adds,
	})

	if zoneState(z) != stateOff {
		keys, err := generateKeys(z, 0, z.CreationTime)
		if err != nil {
			return nil, err
		}

		rec.Keys = keys
	}

	b.zones[key(project, z.Name)] = rec

	return clone(z), nil
}

func (b *InMemoryBackend) zoneLocked(project, name string) (*zoneRec, error) {
	rec, ok := b.zones[key(project, name)]
	if !ok {
		return nil, errNotFound("parameters.managedZone", name)
	}

	return rec, nil
}

// GetZone returns one managed zone.
func (b *InMemoryBackend) GetZone(project, name string) (*dnsapi.ManagedZone, error) {
	b.mu.RLock("GetZone")
	defer b.mu.RUnlock()

	rec, err := b.zoneLocked(project, name)
	if err != nil {
		return nil, err
	}

	return clone(rec.Zone), nil
}

// ListZones returns a page of the project's zones ordered by name, optionally filtered by dnsName.
func (b *InMemoryBackend) ListZones(project, dnsName, token string, limit int) (page.Page[*dnsapi.ManagedZone], error) {
	if err := page.ValidateToken(token); err != nil {
		return page.Page[*dnsapi.ManagedZone]{}, errInvalidValue("pageToken", token)
	}

	b.mu.RLock("ListZones")
	defer b.mu.RUnlock()

	var all []*dnsapi.ManagedZone

	for _, rec := range b.zones {
		if rec.Project == project && (dnsName == "" || rec.Zone.DnsName == strings.ToLower(dnsName)) {
			all = append(all, clone(rec.Zone))
		}
	}

	slices.SortFunc(all, func(a, c *dnsapi.ManagedZone) int { return strings.Compare(a.Name, c.Name) })

	return page.New(all, token, limit, 100), nil
}

// DeleteZone removes a zone that holds only its default SOA and NS records.
func (b *InMemoryBackend) DeleteZone(project, name string) error {
	b.mu.Lock("DeleteZone")
	defer b.mu.Unlock()

	rec, err := b.zoneLocked(project, name)
	if err != nil {
		return err
	}

	for _, rr := range rec.RRSets {
		if canon(rr.Name) != rec.Zone.DnsName || rr.Type != "SOA" && rr.Type != "NS" {
			return errNotEmpty("The managed zone named '%s' cannot be deleted because it is not empty.", name)
		}
	}

	delete(b.zones, key(project, name))

	return nil
}

// UpdateZone replaces the mutable fields of a zone (PUT); patch is non-nil for PATCH merge semantics.
func (b *InMemoryBackend) UpdateZone(
	project, name string, in *dnsapi.ManagedZone, patch map[string]any,
) (*dnsapi.Operation, error) {
	b.mu.Lock("UpdateZone")
	defer b.mu.Unlock()

	rec, err := b.zoneLocked(project, name)
	if err != nil {
		return nil, err
	}

	old := clone(rec.Zone)

	nz := clone(in)
	if patch != nil {
		if nz, err = patched(old, patch); err != nil {
			return nil, err
		}
	}

	if err = checkImmutable(old, nz); err != nil {
		return nil, err
	}

	nz.Name, nz.Id, nz.CreationTime = old.Name, old.Id, old.CreationTime
	nz.DnsName, nz.Visibility, nz.NameServers, nz.NameServerSet = old.DnsName, old.Visibility, old.NameServers,
		old.NameServerSet

	if err = validateZone(nz); err != nil {
		return nil, err
	}

	if err = b.transitionDNSSEC(rec, old, nz); err != nil {
		return nil, err
	}

	rec.Zone = nz
	op := &dnsapi.Operation{
		Kind: "dns#operation", Id: fmt.Sprint(newID()), StartTime: b.stamp(), Status: "done", Type: "update",
		ZoneContext: &dnsapi.OperationManagedZoneContext{OldValue: old, NewValue: clone(nz)},
	}
	rec.Ops = append(rec.Ops, op)

	return clone(op), nil
}

func checkImmutable(old, nz *dnsapi.ManagedZone) error {
	if nz.Name != "" && nz.Name != old.Name {
		return errInvalidValue(zonePath+".name", nz.Name)
	}

	if nz.DnsName != "" && canon(nz.DnsName) != old.DnsName {
		return errInvalidValue(zonePath+".dnsName", nz.DnsName)
	}

	if nz.Visibility != "" && nz.Visibility != old.Visibility {
		return errInvalidValue(zonePath+".visibility", nz.Visibility)
	}

	return nil
}

func (b *InMemoryBackend) transitionDNSSEC(rec *zoneRec, old, nz *dnsapi.ManagedZone) error {
	was, now := zoneState(old), zoneState(nz)

	if was != stateOff && now != stateOff {
		oc, nc := old.DnssecConfig, nz.DnssecConfig
		if oc.NonExistence != nc.NonExistence || len(oc.DefaultKeySpecs) != len(nc.DefaultKeySpecs) {
			return errInvalid("Invalid value for '%s.dnssecConfig': can only be changed while state is off", zonePath)
		}

		return nil
	}

	switch {
	case was == stateOff && now != stateOff:
		keys, err := generateKeys(nz, 0, b.stamp())
		if err != nil {
			return err
		}

		rec.Keys = keys
	case was != stateOff && now == stateOff:
		rec.Keys = nil
	}

	return nil
}
