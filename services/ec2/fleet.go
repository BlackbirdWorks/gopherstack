package ec2

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
)

// FleetLaunchTemplateOverride mirrors FleetLaunchTemplateOverridesRequest
// (ec2@v1.319.1 types/types.go:7052): the fields this backend can act on --
// which AMI/instance type/subnet to launch from, and the weight each
// launched instance counts against TargetCapacity.
type FleetLaunchTemplateOverride struct {
	ImageID          string
	InstanceType     string
	SubnetID         string
	AvailabilityZone string
	WeightedCapacity float64
}

// FleetLaunchTemplateConfig mirrors FleetLaunchTemplateConfigRequest
// (ec2@v1.319.1 types/types.go:6910).
type FleetLaunchTemplateConfig struct {
	LaunchTemplateID   string
	LaunchTemplateName string
	Version            string
	Overrides          []FleetLaunchTemplateOverride
}

// FleetCreateInput bundles CreateFleet's request fields (ec2@v1.319.1
// api_op_CreateFleet.go CreateFleetInput).
type FleetCreateInput struct {
	Type                             string
	ExcessCapacityTerminationPolicy  string
	TargetCapacityUnitType           string
	DefaultTargetCapacityType        string
	LaunchTemplateConfigs            []FleetLaunchTemplateConfig
	TotalTargetCapacity              int
	OnDemandTargetCapacity           int
	SpotTargetCapacity               int
	TerminateInstancesWithExpiration bool
}

// CreateFleetInstanceResult groups instances CreateFleet launched by
// instance type, matching CreateFleetInstance (ec2@v1.319.1
// types/types.go:3824) -- the shape CreateFleetOutput.Instances uses, valid
// only for fleets of type instant.
type CreateFleetInstanceResult struct {
	InstanceType string
	InstanceIDs  []string
}

// FleetHistoryRecord is a single EC2 Fleet history event, mirroring
// HistoryRecordEntry (ec2@v1.319.1 types/types.go:7778).
type FleetHistoryRecord struct {
	Timestamp        time.Time `json:"timestamp"`
	EventType        string    `json:"eventType,omitempty"`
	EventInformation string    `json:"eventInformation,omitempty"`
}

// ActiveFleetInstance mirrors ActiveInstance (ec2@v1.319.1
// types/types.go:202), the shape DescribeFleetInstances returns.
type ActiveFleetInstance struct {
	InstanceID     string
	InstanceType   string
	InstanceHealth string
}

const fleetHistoryEventType = "fleet-change"

// CreateFleet launches instances against the fleet's LaunchTemplateConfigs up
// to TotalTargetCapacity -- the real CreateFleet doc (api_op_CreateFleet.go)
// states instances "are launched immediately if there is available
// capacity" regardless of Type. Only fleets of type instant report the
// launched instances back on CreateFleetOutput itself (returned here as the
// second value); request/maintain fleets launch the same way, but a real
// client only learns about them later via DescribeFleetInstances/
// DescribeFleets.
func (b *InMemoryBackend) CreateFleet(input FleetCreateInput) (*Fleet, []CreateFleetInstanceResult, error) {
	b.mu.Lock("CreateFleet")
	defer b.mu.Unlock()

	fleetType := input.Type
	if fleetType == "" {
		fleetType = fleetTypeDefault
	}

	excessPolicy := input.ExcessCapacityTerminationPolicy
	if excessPolicy == "" {
		excessPolicy = "termination"
	}

	id := "fleet-" + uuid.New().String()[:8]
	f := &Fleet{
		FleetID:                          id,
		FleetState:                       SpotFleetStateActive,
		FleetType:                        fleetType,
		TargetCapacityUnitType:           input.TargetCapacityUnitType,
		ExcessCapacityTerminationPolicy:  excessPolicy,
		DefaultTargetCapacityType:        input.DefaultTargetCapacityType,
		TotalTargetCapacity:              input.TotalTargetCapacity,
		OnDemandTargetCapacity:           input.OnDemandTargetCapacity,
		SpotTargetCapacity:               input.SpotTargetCapacity,
		TerminateInstancesWithExpiration: input.TerminateInstancesWithExpiration,
	}

	results := b.launchFleetInstancesLocked(f, input.LaunchTemplateConfigs, input.TotalTargetCapacity)

	b.fleets.Put(f)

	b.appendEC2FleetHistoryLocked(id, FleetHistoryRecord{
		Timestamp: time.Now().UTC(),
		EventType: fleetHistoryEventType,
		EventInformation: fmt.Sprintf(
			"fleet %s moved to active state with %d instances", id, len(f.InstanceIDs),
		),
	})

	cp := *f
	cp.InstanceIDs = append([]string(nil), f.InstanceIDs...)

	return &cp, results, nil
}

// launchFleetInstancesLocked resolves the fleet's launch template configs
// and spawns instances round-robin across the resolved overrides until
// fulfilled weighted capacity reaches targetCapacity, appending each
// instance's ID to fleet.InstanceIDs. Must be called with b.mu held for
// writing. Returns the launched instances grouped by instance type, the
// shape CreateFleetOutput.Instances needs for fleets of type instant.
func (b *InMemoryBackend) launchFleetInstancesLocked(
	fleet *Fleet, configs []FleetLaunchTemplateConfig, targetCapacity int,
) []CreateFleetInstanceResult {
	overrides := b.resolveFleetLaunchOverridesLocked(configs)

	var order []string

	byType := make(map[string][]string)
	fulfilled := 0.0
	spawned := 0

	for i := 0; fulfilled < float64(targetCapacity) && spawned < spotFleetMaxInstances; i++ {
		ov := overrides[i%len(overrides)]

		vpcID := ""
		if sub, ok := b.subnets.Get(ov.SubnetID); ok {
			vpcID = sub.VPCID
		}

		instID := b.spawnFleetMemberInstanceLocked(fleet, ov.ImageID, ov.InstanceType, ov.SubnetID, vpcID)

		if _, seen := byType[ov.InstanceType]; !seen {
			order = append(order, ov.InstanceType)
		}

		byType[ov.InstanceType] = append(byType[ov.InstanceType], instID)
		fulfilled += ov.WeightedCapacity
		spawned++
	}

	results := make([]CreateFleetInstanceResult, 0, len(order))
	for _, it := range order {
		results = append(results, CreateFleetInstanceResult{InstanceType: it, InstanceIDs: byType[it]})
	}

	return results
}

// resolveFleetLaunchOverridesLocked expands each launch template config's
// overrides into concrete (image, instance type, subnet) launch entries,
// falling back to this backend's spot-fleet defaults (spotFleetDefaultImageID
// / spotFleetDefaultInstanceType) when a referenced launch template or field
// is absent -- consistent with RequestSpotFleet's own fallback, and needed
// because a real client is not required to have pre-created the launch
// template a mock resolves against. Must be called with b.mu held.
func (b *InMemoryBackend) resolveFleetLaunchOverridesLocked(
	configs []FleetLaunchTemplateConfig,
) []FleetLaunchTemplateOverride {
	var out []FleetLaunchTemplateOverride

	for _, cfg := range configs {
		out = append(out, b.resolveFleetConfigOverridesLocked(cfg)...)
	}

	if len(out) == 0 {
		out = append(out, FleetLaunchTemplateOverride{
			ImageID:          spotFleetDefaultImageID,
			InstanceType:     spotFleetDefaultInstanceType,
			SubnetID:         b.resolveFleetSubnetLocked(""),
			WeightedCapacity: 1.0,
		})
	}

	return out
}

// resolveFleetConfigOverridesLocked expands one launch template config's
// overrides (or, absent any, a single entry built from the template's own
// AMI/instance type) into concrete launch entries. Must be called with b.mu
// held.
func (b *InMemoryBackend) resolveFleetConfigOverridesLocked(
	cfg FleetLaunchTemplateConfig,
) []FleetLaunchTemplateOverride {
	lt := b.resolveFleetLaunchTemplateLocked(cfg.LaunchTemplateID, cfg.LaunchTemplateName)

	baseImage := spotFleetDefaultImageID
	baseType := spotFleetDefaultInstanceType

	if lt != nil {
		if lt.ImageID != "" {
			baseImage = lt.ImageID
		}

		if lt.InstanceType != "" {
			baseType = lt.InstanceType
		}
	}

	if len(cfg.Overrides) == 0 {
		return []FleetLaunchTemplateOverride{{
			ImageID:          baseImage,
			InstanceType:     baseType,
			SubnetID:         b.resolveFleetSubnetLocked(""),
			WeightedCapacity: 1.0,
		}}
	}

	out := make([]FleetLaunchTemplateOverride, 0, len(cfg.Overrides))
	for _, ov := range cfg.Overrides {
		out = append(out, b.resolveFleetOverrideLocked(ov, baseImage, baseType))
	}

	return out
}

// resolveFleetOverrideLocked fills in an override's AMI/instance
// type/weighted capacity from the launch template's base values wherever the
// override itself leaves them unset, and resolves its subnet. Must be called
// with b.mu held.
func (b *InMemoryBackend) resolveFleetOverrideLocked(
	ov FleetLaunchTemplateOverride, baseImage, baseType string,
) FleetLaunchTemplateOverride {
	imageID := ov.ImageID
	if imageID == "" {
		imageID = baseImage
	}

	instanceType := ov.InstanceType
	if instanceType == "" {
		instanceType = baseType
	}

	weighted := ov.WeightedCapacity
	if weighted <= 0 {
		weighted = 1.0
	}

	return FleetLaunchTemplateOverride{
		ImageID:          imageID,
		InstanceType:     instanceType,
		SubnetID:         b.resolveFleetSubnetLocked(ov.SubnetID),
		WeightedCapacity: weighted,
	}
}

// resolveFleetLaunchTemplateLocked mirrors GetLaunchTemplate's id-then-name
// lookup without taking b.mu, which CreateFleet already holds for writing.
func (b *InMemoryBackend) resolveFleetLaunchTemplateLocked(id, name string) *LaunchTemplate {
	idOrName := id
	if idOrName == "" {
		idOrName = name
	}

	if idOrName == "" {
		return nil
	}

	if lt, ok := b.launchTemplates.Get(idOrName); ok {
		return lt
	}

	for _, lt := range b.launchTemplates.All() {
		if lt.Name == idOrName {
			return lt
		}
	}

	return nil
}

// resolveFleetSubnetLocked returns subnetID if it names a real subnet,
// otherwise falls back to the backend's default subnet -- mirrors spot
// fleet's identical fallback in spawnFleetInstancesLocked (spot_fleet.go).
func (b *InMemoryBackend) resolveFleetSubnetLocked(subnetID string) string {
	if subnetID == "" {
		return b.findDefaultSubnetID()
	}

	if _, ok := b.subnets.Get(subnetID); !ok {
		return b.findDefaultSubnetID()
	}

	return subnetID
}

// spawnFleetMemberInstanceLocked creates a single instance (with a matching
// primary ENI) for an EC2 Fleet at the given image/instance-type/subnet/vpc,
// indexes it, and appends its ID to fleet.InstanceIDs. Must be called with
// b.mu held for writing. Mirrors spawnFleetInstanceLocked (spot_fleet.go) for
// the (non-spot) Fleet type.
func (b *InMemoryBackend) spawnFleetMemberInstanceLocked(
	fleet *Fleet, imageID, instanceType, subnetID, vpcID string,
) string {
	id := newInstanceID()
	inst := &Instance{
		ID:           id,
		ImageID:      imageID,
		InstanceType: instanceType,
		State:        StateRunning,
		VPCID:        vpcID,
		SubnetID:     subnetID,
		LaunchTime:   time.Now().UTC(),
	}
	inst.PrivateIP = b.allocPrivateIP()

	eniID := newENIID()
	attachID := "eni-attach-" + uuid.New().String()[:8]
	b.networkInterfaces.Put(&NetworkInterface{
		ID:                  eniID,
		SubnetID:            subnetID,
		VPCID:               vpcID,
		PrivateIP:           inst.PrivateIP,
		InstanceID:          id,
		AttachmentID:        attachID,
		DeviceIndex:         0,
		Status:              stateInUse,
		OwnerID:             b.AccountID,
		SourceDestCheck:     true,
		DeleteOnTermination: true,
	})
	b.instances.Put(inst)
	b.indexInstanceLocked(inst)
	eni, _ := b.networkInterfaces.Get(eniID)
	b.indexENILocked(eniID, eni)
	b.indexENIByVPCLocked(eniID, eni)

	fleet.InstanceIDs = append(fleet.InstanceIDs, id)

	return id
}

// appendEC2FleetHistoryLocked appends a history record for fleetID while
// capping the slice at maxSpotFleetHistoryEntries, mirroring spot fleet's
// appendFleetHistoryLocked (spot_fleet.go) to bound memory growth. Must be
// called with b.mu held for writing.
func (b *InMemoryBackend) appendEC2FleetHistoryLocked(fleetID string, rec FleetHistoryRecord) {
	records := b.fleetHistory[fleetID]
	records = append(records, rec)

	if len(records) > maxSpotFleetHistoryEntries {
		half := spotFleetHistoryHalfPoint
		copy(records, records[len(records)-half:])
		records = records[:half]
	}

	b.fleetHistory[fleetID] = records
}

// FleetDeletionResult mirrors the real DeleteFleetSuccessItem: the state the
// fleet was in immediately before deletion, alongside its ID. AWS's real
// shape has no plain fleetState member for this op, only
// currentFleetState/previousFleetState (types.go, DeleteFleetSuccessItem).
type FleetDeletionResult struct {
	FleetID            string
	PreviousFleetState string
}

// DeleteFleets deletes the given fleets. terminateInstances mirrors
// DeleteFleetsInput.TerminateInstances (ec2@v1.319.1
// api_op_DeleteFleets.go): when true, every instance the fleet launched is
// terminated too, rather than left running with no owning fleet.
func (b *InMemoryBackend) DeleteFleets(ids []string, terminateInstances bool) []FleetDeletionResult {
	b.mu.Lock("DeleteFleets")
	defer b.mu.Unlock()

	var deleted []FleetDeletionResult

	for _, id := range ids {
		f, ok := b.fleets.Get(id)
		if !ok {
			continue
		}

		prev := f.FleetState
		f.FleetState = tgwRouteStateDeleted

		if terminateInstances {
			for _, instID := range f.InstanceIDs {
				if inst, exists := b.instances.Get(instID); exists {
					inst.State = StateTerminated
					inst.TerminatedAt = time.Now().UTC()
				}
			}
		}

		b.fleets.Delete(id)
		delete(b.tags, id)
		deleted = append(deleted, FleetDeletionResult{FleetID: id, PreviousFleetState: prev})
	}

	return deleted
}

func (b *InMemoryBackend) DescribeFleets(ids []string) []*Fleet {
	b.mu.RLock("DescribeFleets")
	defer b.mu.RUnlock()

	var result []*Fleet

	for _, f := range b.fleets.All() {
		if len(ids) > 0 && !slices.Contains(ids, f.FleetID) {
			continue
		}

		cp := *f
		cp.InstanceIDs = append([]string(nil), f.InstanceIDs...)
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].FleetID < result[j].FleetID
	})

	return result
}

func (b *InMemoryBackend) ModifyFleet(id string, totalTargetCapacity int, excessPolicy string) error {
	b.mu.Lock("ModifyFleet")
	defer b.mu.Unlock()

	f, ok := b.fleets.Get(id)
	if !ok {
		return fmt.Errorf("%w: %s", ErrFleetNotFound, id)
	}

	if totalTargetCapacity > 0 {
		f.TotalTargetCapacity = totalTargetCapacity
	}

	if excessPolicy != "" {
		f.ExcessCapacityTerminationPolicy = excessPolicy
	}

	b.appendEC2FleetHistoryLocked(id, FleetHistoryRecord{
		Timestamp:        time.Now().UTC(),
		EventType:        fleetHistoryEventType,
		EventInformation: fmt.Sprintf("fleet %s target capacity changed to %d", id, f.TotalTargetCapacity),
	})

	return nil
}

// DescribeFleetInstances returns the fleet's running instances, optionally
// narrowed by filters (currently "instance-type", the only filter
// DescribeFleetInstancesInput documents). Fleets of type instant are not
// supported by the real op (api_op_DescribeFleetInstances.go doc comment:
// "use DescribeFleets" instead, where CreateFleetOutput/DescribeFleetsOutput
// already carry an instant fleet's instances) -- this returns an empty set
// for them rather than fabricating support the real API lacks.
func (b *InMemoryBackend) DescribeFleetInstances(
	fleetID string, filters map[string][]string,
) ([]ActiveFleetInstance, error) {
	if fleetID == "" {
		return nil, fmt.Errorf("%w: FleetId is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeFleetInstances")
	defer b.mu.RUnlock()

	f, ok := b.fleets.Get(fleetID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrFleetNotFound, fleetID)
	}

	if f.FleetType == fleetTypeInstant {
		return nil, nil
	}

	result := make([]ActiveFleetInstance, 0, len(f.InstanceIDs))

	for _, id := range f.InstanceIDs {
		inst, exists := b.instances.Get(id)
		if !exists {
			continue
		}

		result = append(result, ActiveFleetInstance{
			InstanceID:     id,
			InstanceType:   inst.InstanceType,
			InstanceHealth: "healthy",
		})
	}

	return applyActiveFleetInstanceFilters(result, filters), nil
}

// DescribeFleetHistory returns the fleet's history events at or after
// startTime, optionally narrowed to a single eventType.
func (b *InMemoryBackend) DescribeFleetHistory(
	fleetID string, startTime time.Time, eventType string,
) ([]FleetHistoryRecord, error) {
	if fleetID == "" {
		return nil, fmt.Errorf("%w: FleetId is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeFleetHistory")
	defer b.mu.RUnlock()

	if _, ok := b.fleets.Get(fleetID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrFleetNotFound, fleetID)
	}

	all := b.fleetHistory[fleetID]
	result := make([]FleetHistoryRecord, 0, len(all))

	for _, rec := range all {
		if rec.Timestamp.Before(startTime) {
			continue
		}

		if eventType != "" && rec.EventType != eventType {
			continue
		}

		result = append(result, rec)
	}

	return result, nil
}

// ---- Network Insights backend methods ----
