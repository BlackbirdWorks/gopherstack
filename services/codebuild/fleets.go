package codebuild

import (
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) buildFleetARN(name string) string {
	return arn.Build("codebuild", b.region, b.accountID, "fleet/"+name)
}

// CreateFleetOptions carries CreateFleet's fields beyond the always-required
// name/baseCapacity.
type CreateFleetOptions struct {
	Tags                 map[string]string
	ComputeConfiguration *ComputeConfiguration
	ProxyConfiguration   *ProxyConfiguration
	VpcConfig            *VpcConfig
	ScalingConfiguration *ScalingConfiguration
	ComputeType          string
	EnvironmentType      string
	OverflowBehavior     string
	ImageID              string
	FleetServiceRole     string
}

// CreateFleet creates a new compute fleet.
func (b *InMemoryBackend) CreateFleet(name string, baseCapacity int32, opts CreateFleetOptions) (*Fleet, error) {
	b.mu.Lock("CreateFleet")
	defer b.mu.Unlock()

	if b.fleets.Has(name) {
		return nil, ErrAlreadyExists
	}

	tagsCopy := make(map[string]string, len(opts.Tags))
	maps.Copy(tagsCopy, opts.Tags)

	now := float64(time.Now().Unix())
	f := &Fleet{
		Arn:                  b.buildFleetARN(name),
		ID:                   uuid.NewString(),
		Name:                 name,
		BaseCapacity:         baseCapacity,
		ComputeType:          opts.ComputeType,
		EnvironmentType:      opts.EnvironmentType,
		OverflowBehavior:     opts.OverflowBehavior,
		ImageID:              opts.ImageID,
		FleetServiceRole:     opts.FleetServiceRole,
		ComputeConfiguration: opts.ComputeConfiguration,
		ProxyConfiguration:   opts.ProxyConfiguration,
		VpcConfig:            opts.VpcConfig,
		ScalingConfiguration: outputScalingConfiguration(opts.ScalingConfiguration, baseCapacity),
		Status:               &FleetStatus{StatusCode: "ACTIVE"},
		Tags:                 tagsCopy,
		Created:              now,
		LastModified:         now,
	}
	b.fleets.Put(f)

	out := *f

	return &out, nil
}

// BatchGetFleets returns fleets by name or ARN. Missing names are returned separately.
func (b *InMemoryBackend) BatchGetFleets(names []string) ([]*Fleet, []string) {
	b.mu.RLock("BatchGetFleets")
	defer b.mu.RUnlock()

	found := make([]*Fleet, 0, len(names))
	notFound := make([]string, 0, len(names))

	for _, nameOrARN := range names {
		f, ok := b.fleets.Get(nameOrARN)
		if !ok {
			if matches := b.fleetsByARN.Get(nameOrARN); len(matches) > 0 {
				f, ok = matches[0], true
			}
		}

		if ok {
			out := *f
			found = append(found, &out)
		} else {
			notFound = append(notFound, nameOrARN)
		}
	}

	return found, notFound
}

// ListFleets returns all fleet ARNs ordered by fleet name, ascending.
func (b *InMemoryBackend) ListFleets() []string {
	return b.ListFleetsSortedBy("")
}

// ListFleetsSortedBy returns all fleet ARNs ordered per sortBy
// (CREATED_TIME|LAST_MODIFIED_TIME|NAME; any other value, including "",
// defaults to NAME), always ascending. Callers apply sortOrder/pagination on
// top via [paginateIDs].
func (b *InMemoryBackend) ListFleetsSortedBy(sortBy string) []string {
	b.mu.RLock("ListFleetsSortedBy")
	defer b.mu.RUnlock()

	items := b.fleets.Snapshot() // NAME-ascending by construction

	switch sortBy {
	case sortByCreatedTime:
		sort.SliceStable(items, func(i, j int) bool { return items[i].Created < items[j].Created })
	case sortByLastModifiedTime:
		sort.SliceStable(items, func(i, j int) bool { return items[i].LastModified < items[j].LastModified })
	}

	arns := make([]string, len(items))
	for i, f := range items {
		arns[i] = f.Arn
	}

	return arns
}

// DeleteFleet removes a fleet by ARN.
func (b *InMemoryBackend) DeleteFleet(arnStr string) error {
	b.mu.Lock("DeleteFleet")
	defer b.mu.Unlock()

	if matches := b.fleetsByARN.Get(arnStr); len(matches) > 0 {
		b.fleets.Delete(matches[0].Name)

		return nil
	}

	// also try by name for convenience
	if b.fleets.Delete(arnStr) {
		return nil
	}

	return ErrNotFound
}

// UpdateFleetOptions carries UpdateFleet's optional fields. An empty string
// leaves the corresponding Fleet field unchanged (real AWS's UpdateFleet
// only updates members actually present in the request; gopherstack
// approximates that with "non-empty overwrites", the same convention already
// used by Project's optional-field updates -- see applyProjectOptionalFields).
// ComputeConfiguration/ProxyConfiguration/VpcConfig/ScalingConfiguration
// follow the same convention: a non-nil pointer overwrites, nil leaves the
// existing value unchanged (real UpdateFleetInput only mutates members
// actually present in the request).
type UpdateFleetOptions struct {
	ComputeConfiguration *ComputeConfiguration
	ProxyConfiguration   *ProxyConfiguration
	VpcConfig            *VpcConfig
	ScalingConfiguration *ScalingConfiguration
	ComputeType          string
	EnvironmentType      string
	OverflowBehavior     string
	ImageID              string
	FleetServiceRole     string
}

// UpdateFleet updates a fleet's base capacity and optional fields.
func (b *InMemoryBackend) UpdateFleet(arnStr string, baseCapacity int32, opts UpdateFleetOptions) (*Fleet, error) {
	b.mu.Lock("UpdateFleet")
	defer b.mu.Unlock()

	matches := b.fleetsByARN.Get(arnStr)
	if len(matches) == 0 {
		return nil, ErrNotFound
	}

	f := matches[0]
	f.BaseCapacity = baseCapacity

	if opts.ComputeType != "" {
		f.ComputeType = opts.ComputeType
	}

	if opts.EnvironmentType != "" {
		f.EnvironmentType = opts.EnvironmentType
	}

	if opts.OverflowBehavior != "" {
		f.OverflowBehavior = opts.OverflowBehavior
	}

	if opts.ImageID != "" {
		f.ImageID = opts.ImageID
	}

	if opts.FleetServiceRole != "" {
		f.FleetServiceRole = opts.FleetServiceRole
	}

	if opts.ComputeConfiguration != nil {
		f.ComputeConfiguration = opts.ComputeConfiguration
	}

	if opts.ProxyConfiguration != nil {
		f.ProxyConfiguration = opts.ProxyConfiguration
	}

	if opts.VpcConfig != nil {
		f.VpcConfig = opts.VpcConfig
	}

	if opts.ScalingConfiguration != nil {
		f.ScalingConfiguration = outputScalingConfiguration(opts.ScalingConfiguration, baseCapacity)
	}

	f.LastModified = float64(time.Now().Unix())
	out := *f

	return &out, nil
}

// outputScalingConfiguration copies in (the caller-supplied request-side
// ScalingConfigurationInput equivalent) into a response-side
// ScalingConfigurationOutput equivalent, filling DesiredCapacity with
// baseCapacity. Real AWS computes DesiredCapacity server-side; since this
// emulator does not model live auto-scaling telemetry, baseCapacity (the
// fleet's initial/no-scaling-event size) is the only non-fabricated value
// available -- it matches AWS's own behavior immediately after
// Create/UpdateFleet, before any scaling event has occurred.
func outputScalingConfiguration(in *ScalingConfiguration, baseCapacity int32) *ScalingConfiguration {
	if in == nil {
		return nil
	}

	out := *in
	out.DesiredCapacity = baseCapacity

	return &out
}
