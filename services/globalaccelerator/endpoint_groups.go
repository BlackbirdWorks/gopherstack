package globalaccelerator

import (
	"cmp"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// EndpointConfig is one endpoint to add to a standard endpoint group.
type EndpointConfig struct {
	Weight               *int32
	ClientIPPreservation *bool
	EndpointID           string
	AttachmentARN        string
}

// EndpointGroupInput holds create/update endpoint group parameters; nil fields are unchanged on update.
type EndpointGroupInput struct {
	HealthCheckInterval *int32
	HealthCheckPath     *string
	HealthCheckPort     *int32
	ThresholdCount      *int32
	TrafficDial         *float32
	Region              string
	HealthCheckProtocol string
	Endpoints           []EndpointConfig
	PortOverrides       []PortOverride
}

func (b *InMemoryBackend) groupLocked(arn string, custom bool) (*EndpointGroup, error) {
	g, ok := b.endpointGroups.Get(arn)
	if !ok || g.Custom != custom {
		return nil, fail(ErrEndpointGroupNotFound, "endpoint group %s not found", arn)
	}

	return g, nil
}

func (b *InMemoryBackend) groupsOfLocked(listenerARN string) []*EndpointGroup {
	var out []*EndpointGroup

	b.endpointGroups.Range(func(g *EndpointGroup) bool {
		if g.ListenerARN == listenerARN {
			out = append(out, g)
		}

		return true
	})

	slices.SortFunc(out, func(x, y *EndpointGroup) int { return cmp.Compare(x.Seq, y.Seq) })

	return out
}

func (b *InMemoryBackend) viewGroupLocked(g *EndpointGroup) *EndpointGroup {
	cp := cloneEndpointGroup(g)
	now := b.now()

	for i := range cp.Endpoints {
		cp.Endpoints[i].HealthState = healthHealthy
		if now.Before(cp.Endpoints[i].AddedAt.Add(b.dwell())) {
			cp.Endpoints[i].HealthState = healthInitial
		}
	}

	return cp
}

func (b *InMemoryBackend) newGroupLocked(l *Listener, region string, custom bool) (*EndpointGroup, error) {
	if err := validateRegion(region); err != nil {
		return nil, err
	}

	for _, g := range b.groupsOfLocked(l.ARN) {
		if g.Region == region {
			return nil, fail(
				ErrEndpointGroupAlreadyExists,
				"listener %s already has an endpoint group in %s",
				l.ARN,
				region,
			)
		}
	}

	return &EndpointGroup{
		ARN:         l.ARN + "/endpoint-group/" + awsid.NewN("", endpointGroupIDLen),
		ListenerARN: l.ARN,
		Region:      region,
		Custom:      custom,
		Seq:         b.nextSeqLocked(),
	}, nil
}

func (b *InMemoryBackend) buildEndpointsLocked(cfgs []EndpointConfig, existing []Endpoint) ([]Endpoint, error) {
	out := slices.Clone(existing)
	now := b.now()

	for _, c := range cfgs {
		if err := validateEndpointID(c.EndpointID); err != nil {
			return nil, err
		}

		if slices.ContainsFunc(out, func(e Endpoint) bool { return e.ID == c.EndpointID }) {
			return nil, fail(ErrEndpointAlreadyExists, "endpoint %s already exists in the endpoint group", c.EndpointID)
		}

		weight := int32(defaultWeight)
		if c.Weight != nil {
			weight = *c.Weight
		}

		if weight < 0 || weight > maxWeight {
			return nil, fail(ErrInvalidArgument, "weight must be between 0 and %d", maxWeight)
		}

		if c.AttachmentARN != "" && !b.attachments.Has(c.AttachmentARN) {
			return nil, fail(ErrAttachmentNotFound, "attachment %s not found", c.AttachmentARN)
		}

		preserve := strings.Contains(c.EndpointID, ":loadbalancer/app/")
		if c.ClientIPPreservation != nil {
			preserve = *c.ClientIPPreservation
		}

		out = append(out, Endpoint{
			ID: c.EndpointID, Weight: weight, ClientIPPreservation: preserve,
			AttachmentARN: c.AttachmentARN, AddedAt: now,
		})
	}

	if len(out) > maxEndpoints {
		return nil, fail(ErrLimitExceeded, "an endpoint group can have at most %d endpoints", maxEndpoints)
	}

	return out, nil
}

func (b *InMemoryBackend) applyGroupSettingsLocked(g *EndpointGroup, l *Listener, in EndpointGroupInput) error {
	if err := applyHealthSettings(g, in); err != nil {
		return err
	}

	if in.ThresholdCount != nil {
		if *in.ThresholdCount < 1 || *in.ThresholdCount > maxThreshold {
			return fail(ErrInvalidArgument, "thresholdCount must be between 1 and %d", maxThreshold)
		}

		g.ThresholdCount = *in.ThresholdCount
	}

	if in.TrafficDial != nil {
		if *in.TrafficDial < 0 || *in.TrafficDial > maxTrafficDial {
			return fail(ErrInvalidArgument, "trafficDialPercentage must be between 0 and %d", maxTrafficDial)
		}

		g.TrafficDial = *in.TrafficDial
	}

	if in.PortOverrides != nil {
		if err := validatePortOverrides(in.PortOverrides, l); err != nil {
			return err
		}

		g.PortOverrides = slices.Clone(in.PortOverrides)
	}

	return nil
}

func applyHealthSettings(g *EndpointGroup, in EndpointGroupInput) error {
	switch in.HealthCheckProtocol {
	case "":
	case protocolTCP, healthProtocolHTTP, healthProtocolHTTPS:
		g.HealthCheckProtocol = in.HealthCheckProtocol
	default:
		return fail(ErrInvalidArgument, "healthCheckProtocol %q must be TCP, HTTP or HTTPS", in.HealthCheckProtocol)
	}

	if in.HealthCheckInterval != nil {
		if *in.HealthCheckInterval != fastHealthSecs && *in.HealthCheckInterval != defaultHealthSecs {
			return fail(ErrInvalidArgument, "healthCheckIntervalSeconds must be 10 or 30")
		}

		g.HealthCheckInterval = *in.HealthCheckInterval
	}

	if in.HealthCheckPort != nil {
		if *in.HealthCheckPort < 1 || *in.HealthCheckPort > maxPort {
			return fail(ErrInvalidArgument, "healthCheckPort must be between 1 and %d", maxPort)
		}

		g.HealthCheckPort = *in.HealthCheckPort
	}

	if in.HealthCheckPath != nil {
		g.HealthCheckPath = *in.HealthCheckPath
	}

	return nil
}

func validatePortOverrides(overrides []PortOverride, l *Listener) error {
	if len(overrides) > maxPortOverrides {
		return fail(ErrLimitExceeded, "at most %d port overrides are allowed", maxPortOverrides)
	}

	seen := make(map[int32]bool, len(overrides))

	for _, o := range overrides {
		switch {
		case o.EndpointPort < 1 || o.EndpointPort > maxPort:
			return fail(ErrInvalidArgument, "endpointPort %d is out of range", o.EndpointPort)
		case !portInRanges(o.ListenerPort, l.PortRanges):
			return fail(ErrInvalidArgument, "listenerPort %d is not in the listener's port ranges", o.ListenerPort)
		case seen[o.ListenerPort]:
			return fail(ErrInvalidArgument, "listenerPort %d is overridden twice", o.ListenerPort)
		}

		seen[o.ListenerPort] = true
	}

	return nil
}

// CreateEndpointGroup creates a standard endpoint group; at most one per Region per listener.
func (b *InMemoryBackend) CreateEndpointGroup(listenerARN string, in EndpointGroupInput) (*EndpointGroup, error) {
	b.mu.Lock("CreateEndpointGroup")
	defer b.mu.Unlock()

	l, err := b.listenerLocked(listenerARN, false)
	if err != nil {
		return nil, err
	}

	g, err := b.newGroupLocked(l, in.Region, false)
	if err != nil {
		return nil, err
	}

	g.HealthCheckProtocol, g.HealthCheckPath = protocolTCP, "/"
	g.HealthCheckInterval, g.ThresholdCount, g.TrafficDial = defaultHealthSecs, defaultThreshold, maxTrafficDial
	g.HealthCheckPort = l.PortRanges[0].FromPort

	if err = b.applyGroupSettingsLocked(g, l, in); err != nil {
		return nil, err
	}

	if g.Endpoints, err = b.buildEndpointsLocked(in.Endpoints, nil); err != nil {
		return nil, err
	}

	b.endpointGroups.Put(g)
	b.touchAcceleratorLocked(l.AcceleratorARN)

	return b.viewGroupLocked(g), nil
}

// GetEndpointGroup returns one endpoint group.
func (b *InMemoryBackend) GetEndpointGroup(arn string, custom bool) (*EndpointGroup, error) {
	b.mu.RLock("GetEndpointGroup")
	defer b.mu.RUnlock()

	g, err := b.groupLocked(arn, custom)
	if err != nil {
		return nil, err
	}

	return b.viewGroupLocked(g), nil
}

// ListEndpointGroups pages the endpoint groups of a listener.
func (b *InMemoryBackend) ListEndpointGroups(
	listenerARN string, custom bool, token string, maxResults *int32,
) (page.Page[*EndpointGroup], error) {
	b.mu.RLock("ListEndpointGroups")
	defer b.mu.RUnlock()

	if _, err := b.listenerLocked(listenerARN, custom); err != nil {
		return page.Page[*EndpointGroup]{}, err
	}

	pg, err := paginate(b.groupsOfLocked(listenerARN), token, maxResults)
	if err != nil {
		return pg, err
	}

	for i, g := range pg.Data {
		pg.Data[i] = b.viewGroupLocked(g)
	}

	return pg, nil
}

// UpdateEndpointGroup applies a partial update; a non-nil Endpoints list replaces the members.
func (b *InMemoryBackend) UpdateEndpointGroup(arn string, in EndpointGroupInput) (*EndpointGroup, error) {
	b.mu.Lock("UpdateEndpointGroup")
	defer b.mu.Unlock()

	g, err := b.groupLocked(arn, false)
	if err != nil {
		return nil, err
	}

	l, _ := b.listeners.Get(g.ListenerARN)
	next := *g

	if err = b.applyGroupSettingsLocked(&next, l, in); err != nil {
		return nil, err
	}

	if in.Endpoints != nil {
		next.Endpoints, err = b.replaceEndpointsLocked(g.Endpoints, in.Endpoints)
		if err != nil {
			return nil, err
		}
	}

	*g = next
	b.touchAcceleratorLocked(l.AcceleratorARN)

	return b.viewGroupLocked(g), nil
}

// replaceEndpointsLocked builds the new member list, keeping AddedAt of members that stay.
func (b *InMemoryBackend) replaceEndpointsLocked(old []Endpoint, cfgs []EndpointConfig) ([]Endpoint, error) {
	next, err := b.buildEndpointsLocked(cfgs, nil)
	if err != nil {
		return nil, err
	}

	for i := range next {
		if j := slices.IndexFunc(old, func(e Endpoint) bool { return e.ID == next[i].ID }); j >= 0 {
			next[i].AddedAt = old[j].AddedAt
		}
	}

	return next, nil
}

// DeleteEndpointGroup removes an endpoint group.
func (b *InMemoryBackend) DeleteEndpointGroup(arn string, custom bool) error {
	b.mu.Lock("DeleteEndpointGroup")
	defer b.mu.Unlock()

	g, err := b.groupLocked(arn, custom)
	if err != nil {
		return err
	}

	b.endpointGroups.Delete(arn)

	if l, ok := b.listeners.Get(g.ListenerARN); ok {
		b.touchAcceleratorLocked(l.AcceleratorARN)
	}

	return nil
}

// AddEndpoints adds endpoints to a standard endpoint group.
func (b *InMemoryBackend) AddEndpoints(arn string, cfgs []EndpointConfig) (*EndpointGroup, []Endpoint, error) {
	if len(cfgs) == 0 {
		return nil, nil, fail(ErrInvalidArgument, "endpointConfigurations must not be empty")
	}

	b.mu.Lock("AddEndpoints")
	defer b.mu.Unlock()

	g, err := b.groupLocked(arn, false)
	if err != nil {
		return nil, nil, err
	}

	next, err := b.buildEndpointsLocked(cfgs, g.Endpoints)
	if err != nil {
		return nil, nil, err
	}

	g.Endpoints = next
	b.touchGroupAcceleratorLocked(g)

	view := b.viewGroupLocked(g)

	return view, view.Endpoints[len(view.Endpoints)-len(cfgs):], nil
}

// RemoveEndpoints removes endpoints by ID; every ID must exist.
func (b *InMemoryBackend) RemoveEndpoints(arn string, ids []string) error {
	b.mu.Lock("RemoveEndpoints")
	defer b.mu.Unlock()

	g, err := b.groupLocked(arn, false)
	if err != nil {
		return err
	}

	return b.removeEndpointsLocked(g, ids)
}

func (b *InMemoryBackend) removeEndpointsLocked(g *EndpointGroup, ids []string) error {
	for _, id := range ids {
		if !slices.ContainsFunc(g.Endpoints, func(e Endpoint) bool { return e.ID == id }) {
			return fail(ErrEndpointNotFound, "endpoint %s not found in the endpoint group", id)
		}
	}

	g.Endpoints = slices.DeleteFunc(g.Endpoints, func(e Endpoint) bool { return slices.Contains(ids, e.ID) })
	b.touchGroupAcceleratorLocked(g)

	return nil
}

func (b *InMemoryBackend) touchGroupAcceleratorLocked(g *EndpointGroup) {
	if l, ok := b.listeners.Get(g.ListenerARN); ok {
		b.touchAcceleratorLocked(l.AcceleratorARN)
	}
}
