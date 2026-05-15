package ec2

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"
)

// SpotFleet allocation strategies.
const (
	SpotFleetAllocationStrategyLowestPrice   = "lowestPrice"
	SpotFleetAllocationStrategyDiversified   = "diversified"
	SpotFleetAllocationStrategyCapacityOpt   = "capacityOptimized"
	SpotFleetAllocationStrategyPriceCapacity = "priceCapacityOptimized"
)

// SpotFleet request states.
const (
	SpotFleetStateActive     = "active"
	SpotFleetStateCancelled  = "cancelled"
	SpotFleetStateCancelling = "cancelled_running"
	SpotFleetStateFailed     = "failed"
	SpotFleetStateModifying  = "modifying"
	SpotFleetStateSubmitted  = "submitted"
)

// SpotFleet activity statuses.
const (
	SpotFleetActivityFulfilled          = "fulfilled"
	SpotFleetActivityPendingFulfillment = "pending_fulfillment"
	SpotFleetActivityPendingTermination = "pending_termination"
	SpotFleetActivityError              = "error"
)

// SpotFleet excess capacity termination policies.
const (
	SpotFleetTerminationDefault       = "default"
	SpotFleetTerminationNoTermination = "noTermination"
)

// Internal spot fleet constants.
const (
	// spotFleetDefaultInstanceType is the fallback instance type for spot fleet instances.
	spotFleetDefaultInstanceType = "m5.large"
	// spotFleetDefaultImageID is the fallback AMI for spot fleet instances.
	spotFleetDefaultImageID = "ami-spot-default"
	// spotFleetMaxInstances is the safety cap on instances per fleet.
	spotFleetMaxInstances = 1000
	// spotFleetHistoryEventType is the event type for fleet history records.
	spotFleetHistoryEventType = "fleetRequestChange"
	// maxSpotFleetHistoryEntries caps the append-only history slice per fleet
	// to prevent unbounded memory growth in long-running tests/servers.
	maxSpotFleetHistoryEntries = 500
)

// SpotFleetLaunchSpecification is a single launch spec within a spot fleet config.
type SpotFleetLaunchSpecification struct {
	ImageID          string  `json:"imageId"`
	InstanceType     string  `json:"instanceType"`
	SubnetID         string  `json:"subnetId"`
	KeyName          string  `json:"keyName"`
	SpotPrice        string  `json:"spotPrice"`
	WeightedCapacity float64 `json:"weightedCapacity"`
}

// SpotFleetRequestConfig is the configuration submitted with RequestSpotFleet.
type SpotFleetRequestConfig struct {
	ValidFrom                        time.Time                      `json:"validFrom"`
	ValidUntil                       time.Time                      `json:"validUntil"`
	SpotPrice                        string                         `json:"spotPrice"`
	AllocationStrategy               string                         `json:"allocationStrategy"`
	ExcessCapacityTerminationPolicy  string                         `json:"excessCapacityTerminationPolicy"`
	IamFleetRole                     string                         `json:"iamFleetRole"`
	Type                             string                         `json:"type"`
	LaunchSpecifications             []SpotFleetLaunchSpecification `json:"launchSpecifications"`
	TargetCapacity                   int                            `json:"targetCapacity"`
	TerminateInstancesWithExpiration bool                           `json:"terminateInstancesWithExpiration"`
	ReplaceUnhealthyInstances        bool                           `json:"replaceUnhealthyInstances"`
}

// SpotFleetRequest represents a Spot Fleet request.
type SpotFleetRequest struct {
	CreateTime                time.Time              `json:"createTime"`
	Tags                      map[string]string      `json:"tags"`
	SpotFleetRequestID        string                 `json:"spotFleetRequestId"`
	SpotFleetRequestState     string                 `json:"spotFleetRequestState"`
	ActivityStatus            string                 `json:"activityStatus"`
	InstanceIDs               []string               `json:"instanceIds"`
	SpotFleetRequestConfig    SpotFleetRequestConfig `json:"spotFleetRequestConfig"`
	FulfilledCapacity         float64                `json:"fulfilledCapacity"`
	OnDemandFulfilledCapacity float64                `json:"onDemandFulfilledCapacity"`
}

// SpotFleetHistoryRecord is a history event for a spot fleet.
type SpotFleetHistoryRecord struct {
	Timestamp        time.Time `json:"timestamp"`
	EventType        string    `json:"eventType"`
	EventInformation string    `json:"eventInformation"`
}

// spawnFleetInstancesLocked spawns instances to fulfill the spot fleet's TargetCapacity.
// Must be called with b.mu held for writing.
// Returns (spawned count, fulfilled capacity).
func (b *InMemoryBackend) spawnFleetInstancesLocked(
	fleet *SpotFleetRequest,
	config SpotFleetRequestConfig,
) (int, float64) {
	spec := config.LaunchSpecifications[0]

	imageID := spec.ImageID
	if imageID == "" {
		imageID = spotFleetDefaultImageID
	}

	instanceType := spec.InstanceType
	if instanceType == "" {
		instanceType = spotFleetDefaultInstanceType
	}

	subnetID := spec.SubnetID
	if subnetID == "" {
		subnetID = b.findDefaultSubnetID()
	} else if _, ok := b.subnets[subnetID]; !ok {
		subnetID = b.findDefaultSubnetID()
	}

	vpcID := ""
	if sub, ok := b.subnets[subnetID]; ok {
		vpcID = sub.VPCID
	}

	weightedCap := spec.WeightedCapacity
	if weightedCap <= 0 {
		weightedCap = 1.0
	}

	spawned := 0
	fulfilled := 0.0

	for fulfilled < float64(config.TargetCapacity) && spawned < spotFleetMaxInstances {
		id := "i-" + uuid.New().String()[:17]
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

		eniID := "eni-" + uuid.New().String()[:17]
		attachID := "eni-attach-" + uuid.New().String()[:8]
		b.networkInterfaces[eniID] = &NetworkInterface{
			ID:              eniID,
			SubnetID:        subnetID,
			VPCID:           vpcID,
			PrivateIP:       inst.PrivateIP,
			InstanceID:      id,
			AttachmentID:    attachID,
			DeviceIndex:     0,
			Status:          stateInUse,
			SourceDestCheck: true,
		}
		b.instances[id] = inst
		b.indexInstanceLocked(inst)
		b.indexENILocked(eniID, b.networkInterfaces[eniID])

		fleet.InstanceIDs = append(fleet.InstanceIDs, id)
		fulfilled += weightedCap
		spawned++
	}

	return spawned, fulfilled
}

// RequestSpotFleet creates a new Spot Fleet request and fulfills it by
// spawning instances up to TargetCapacity.
func (b *InMemoryBackend) RequestSpotFleet(
	config SpotFleetRequestConfig,
) (*SpotFleetRequest, error) {
	if config.TargetCapacity < 0 {
		return nil, fmt.Errorf("%w: TargetCapacity must be >= 0", ErrInvalidParameter)
	}

	if len(config.LaunchSpecifications) == 0 {
		return nil, fmt.Errorf(
			"%w: at least one LaunchSpecification is required",
			ErrInvalidParameter,
		)
	}

	if config.AllocationStrategy == "" {
		config.AllocationStrategy = SpotFleetAllocationStrategyLowestPrice
	}

	if config.ExcessCapacityTerminationPolicy == "" {
		config.ExcessCapacityTerminationPolicy = SpotFleetTerminationDefault
	}

	if config.Type == "" {
		config.Type = "maintain"
	}

	b.mu.Lock("RequestSpotFleet")
	defer b.mu.Unlock()

	fleetID := "sfr-" + uuid.New().String()
	now := time.Now().UTC()

	fleet := &SpotFleetRequest{
		SpotFleetRequestID:     fleetID,
		SpotFleetRequestState:  SpotFleetStateActive,
		ActivityStatus:         SpotFleetActivityFulfilled,
		CreateTime:             now,
		SpotFleetRequestConfig: config,
		Tags:                   make(map[string]string),
		InstanceIDs:            []string{},
	}

	spawned, fulfilled := b.spawnFleetInstancesLocked(fleet, config)

	fleet.FulfilledCapacity = fulfilled
	b.spotFleets[fleetID] = fleet

	// Add initial history record.
	b.spotFleetHistory[fleetID] = []SpotFleetHistoryRecord{
		{
			Timestamp: fleet.CreateTime,
			EventType: spotFleetHistoryEventType,
			EventInformation: fmt.Sprintf(
				"fleet %s moved to active state with %d instances",
				fleetID,
				spawned,
			),
		},
	}

	cp := *fleet
	cp.InstanceIDs = append([]string(nil), fleet.InstanceIDs...)
	cp.Tags = make(map[string]string)
	maps.Copy(cp.Tags, fleet.Tags)

	return &cp, nil
}

// DescribeSpotFleetRequests returns spot fleet requests, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeSpotFleetRequests(
	fleetIDs []string,
) ([]*SpotFleetRequest, error) {
	b.mu.RLock("DescribeSpotFleetRequests")
	defer b.mu.RUnlock()

	wantAll := len(fleetIDs) == 0

	// Build an ID set for fast lookup.
	wantSet := make(map[string]struct{}, len(fleetIDs))
	for _, id := range fleetIDs {
		wantSet[id] = struct{}{}
	}

	results := make([]*SpotFleetRequest, 0, len(b.spotFleets))

	for id, fleet := range b.spotFleets {
		if !wantAll {
			if _, ok := wantSet[id]; !ok {
				continue
			}
		}

		cp := *fleet
		cp.InstanceIDs = append([]string(nil), fleet.InstanceIDs...)
		cp.Tags = make(map[string]string)
		maps.Copy(cp.Tags, fleet.Tags)

		results = append(results, &cp)
	}

	// Check for requested IDs that don't exist.
	if !wantAll {
		for _, id := range fleetIDs {
			if _, ok := b.spotFleets[id]; !ok {
				return nil, fmt.Errorf("%w: %s", ErrSpotFleetNotFound, id)
			}
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].SpotFleetRequestID < results[j].SpotFleetRequestID
	})

	return results, nil
}

// CancelSpotFleetRequests cancels spot fleet requests.
// If terminateInstances is true, spawned instances are also terminated.
func (b *InMemoryBackend) CancelSpotFleetRequests(
	fleetIDs []string,
	terminateInstances bool,
) ([]SpotFleetCancelResult, error) {
	if len(fleetIDs) == 0 {
		return nil, fmt.Errorf(
			"%w: at least one SpotFleetRequestId is required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CancelSpotFleetRequests")
	defer b.mu.Unlock()

	results := make([]SpotFleetCancelResult, 0, len(fleetIDs))

	for _, id := range fleetIDs {
		fleet, ok := b.spotFleets[id]
		if !ok {
			results = append(results, SpotFleetCancelResult{
				SpotFleetRequestID:            id,
				CurrentSpotFleetRequestState:  SpotFleetStateCancelled,
				PreviousSpotFleetRequestState: SpotFleetStateCancelled,
				Error:                         "SpotFleetRequestIdNotFound",
			})

			continue
		}

		prevState := fleet.SpotFleetRequestState

		if terminateInstances {
			for _, instID := range fleet.InstanceIDs {
				if inst, exists := b.instances[instID]; exists {
					inst.State = StateTerminated
					inst.TerminatedAt = time.Now().UTC()
				}
			}

			fleet.SpotFleetRequestState = SpotFleetStateCancelled
			fleet.ActivityStatus = SpotFleetActivityPendingTermination
		} else {
			fleet.SpotFleetRequestState = SpotFleetStateCancelling
			fleet.ActivityStatus = SpotFleetActivityFulfilled
		}

		// Record history event.
		b.appendFleetHistoryLocked(id, SpotFleetHistoryRecord{
			Timestamp:        time.Now().UTC(),
			EventType:        spotFleetHistoryEventType,
			EventInformation: fmt.Sprintf("fleet %s cancelled", id),
		})

		results = append(results, SpotFleetCancelResult{
			SpotFleetRequestID:            id,
			CurrentSpotFleetRequestState:  fleet.SpotFleetRequestState,
			PreviousSpotFleetRequestState: prevState,
		})
	}

	return results, nil
}

// adjustFleetCapacityLocked scales a fleet up or down to reach newTarget.
// Must be called with b.mu held for writing.
func (b *InMemoryBackend) adjustFleetCapacityLocked(
	fleet *SpotFleetRequest,
	oldCapacity, newTarget int,
	excessTermination string,
) {
	config := fleet.SpotFleetRequestConfig

	if newTarget > oldCapacity {
		b.scaleFleetUpLocked(fleet, config, newTarget)
	} else if newTarget < oldCapacity && excessTermination == SpotFleetTerminationDefault {
		b.scaleFleetDownLocked(fleet, config, newTarget)
	}
}

// scaleFleetUpLocked adds instances until FulfilledCapacity reaches newTarget.
func (b *InMemoryBackend) scaleFleetUpLocked(
	fleet *SpotFleetRequest,
	config SpotFleetRequestConfig,
	newTarget int,
) {
	spec := config.LaunchSpecifications[0]

	imageID := spec.ImageID
	if imageID == "" {
		imageID = spotFleetDefaultImageID
	}

	instanceType := spec.InstanceType
	if instanceType == "" {
		instanceType = spotFleetDefaultInstanceType
	}

	subnetID := spec.SubnetID
	if subnetID == "" {
		subnetID = b.findDefaultSubnetID()
	} else if _, ok := b.subnets[subnetID]; !ok {
		subnetID = b.findDefaultSubnetID()
	}

	vpcID := ""
	if sub, ok := b.subnets[subnetID]; ok {
		vpcID = sub.VPCID
	}

	weightedCap := spec.WeightedCapacity
	if weightedCap <= 0 {
		weightedCap = 1.0
	}

	for fleet.FulfilledCapacity < float64(newTarget) {
		id := "i-" + uuid.New().String()[:17]
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

		eniID := "eni-" + uuid.New().String()[:17]
		attachID := "eni-attach-" + uuid.New().String()[:8]
		b.networkInterfaces[eniID] = &NetworkInterface{
			ID:              eniID,
			SubnetID:        subnetID,
			VPCID:           vpcID,
			PrivateIP:       inst.PrivateIP,
			InstanceID:      id,
			AttachmentID:    attachID,
			DeviceIndex:     0,
			Status:          stateInUse,
			SourceDestCheck: true,
		}
		b.instances[id] = inst
		b.indexInstanceLocked(inst)
		b.indexENILocked(eniID, b.networkInterfaces[eniID])

		fleet.InstanceIDs = append(fleet.InstanceIDs, id)
		fleet.FulfilledCapacity += weightedCap
	}
}

// scaleFleetDownLocked terminates excess instances until FulfilledCapacity <= newTarget.
func (b *InMemoryBackend) scaleFleetDownLocked(
	fleet *SpotFleetRequest,
	config SpotFleetRequestConfig,
	newTarget int,
) {
	weightedCap := 1.0

	if len(config.LaunchSpecifications) > 0 {
		if w := config.LaunchSpecifications[0].WeightedCapacity; w > 0 {
			weightedCap = w
		}
	}

	for fleet.FulfilledCapacity > float64(newTarget) && len(fleet.InstanceIDs) > 0 {
		lastIdx := len(fleet.InstanceIDs) - 1
		instID := fleet.InstanceIDs[lastIdx]
		fleet.InstanceIDs = fleet.InstanceIDs[:lastIdx]

		if inst, exists := b.instances[instID]; exists {
			inst.State = StateTerminated
			inst.TerminatedAt = time.Now().UTC()
		}

		fleet.FulfilledCapacity -= weightedCap
	}
}

// ModifySpotFleetRequest updates the target capacity of a spot fleet request.
func (b *InMemoryBackend) ModifySpotFleetRequest(
	fleetID string,
	targetCapacity int,
	excessTermination string,
) (*SpotFleetRequest, error) {
	if fleetID == "" {
		return nil, fmt.Errorf("%w: SpotFleetRequestId is required", ErrInvalidParameter)
	}

	if targetCapacity < 0 {
		return nil, fmt.Errorf("%w: TargetCapacity must be >= 0", ErrInvalidParameter)
	}

	b.mu.Lock("ModifySpotFleetRequest")
	defer b.mu.Unlock()

	fleet, ok := b.spotFleets[fleetID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSpotFleetNotFound, fleetID)
	}

	if fleet.SpotFleetRequestState != SpotFleetStateActive {
		return nil, fmt.Errorf("%w: fleet %s is not in active state", ErrInvalidParameter, fleetID)
	}

	oldCapacity := fleet.SpotFleetRequestConfig.TargetCapacity
	fleet.SpotFleetRequestConfig.TargetCapacity = targetCapacity

	if excessTermination != "" {
		fleet.SpotFleetRequestConfig.ExcessCapacityTerminationPolicy = excessTermination
	}

	// Scale up or down as needed.
	b.adjustFleetCapacityLocked(fleet, oldCapacity, targetCapacity, excessTermination)

	fleet.ActivityStatus = SpotFleetActivityFulfilled

	// Record history event.
	b.appendFleetHistoryLocked(fleetID, SpotFleetHistoryRecord{
		Timestamp: time.Now().UTC(),
		EventType: spotFleetHistoryEventType,
		EventInformation: fmt.Sprintf(
			"fleet %s target capacity changed from %d to %d",
			fleetID,
			oldCapacity,
			targetCapacity,
		),
	})

	cp := *fleet
	cp.InstanceIDs = append([]string(nil), fleet.InstanceIDs...)
	cp.Tags = make(map[string]string)
	maps.Copy(cp.Tags, fleet.Tags)

	return &cp, nil
}

// DescribeSpotFleetInstances returns the instances in a spot fleet.
func (b *InMemoryBackend) DescribeSpotFleetInstances(fleetID string) ([]SpotFleetInstance, error) {
	if fleetID == "" {
		return nil, fmt.Errorf("%w: SpotFleetRequestId is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeSpotFleetInstances")
	defer b.mu.RUnlock()

	fleet, ok := b.spotFleets[fleetID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSpotFleetNotFound, fleetID)
	}

	result := make([]SpotFleetInstance, 0, len(fleet.InstanceIDs))

	for _, instID := range fleet.InstanceIDs {
		inst, exists := b.instances[instID]
		if !exists {
			continue
		}

		instanceType := inst.InstanceType
		if instanceType == "" {
			instanceType = spotFleetDefaultInstanceType
		}

		result = append(result, SpotFleetInstance{
			InstanceID:            instID,
			InstanceType:          instanceType,
			SpotInstanceRequestID: "sir-" + uuid.New().String()[:8],
			InstanceHealth:        "healthy",
		})
	}

	return result, nil
}

// DescribeSpotFleetRequestHistory returns history records for a spot fleet request.
func (b *InMemoryBackend) DescribeSpotFleetRequestHistory(
	fleetID string,
	startTime time.Time,
) ([]SpotFleetHistoryRecord, error) {
	if fleetID == "" {
		return nil, fmt.Errorf("%w: SpotFleetRequestId is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeSpotFleetRequestHistory")
	defer b.mu.RUnlock()

	if _, ok := b.spotFleets[fleetID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrSpotFleetNotFound, fleetID)
	}

	allRecords := b.spotFleetHistory[fleetID]
	result := make([]SpotFleetHistoryRecord, 0, len(allRecords))

	for _, rec := range allRecords {
		if !rec.Timestamp.Before(startTime) {
			result = append(result, rec)
		}
	}

	return result, nil
}

// SpotFleetCancelResult is the result of a single CancelSpotFleetRequests item.
type SpotFleetCancelResult struct {
	SpotFleetRequestID            string `json:"spotFleetRequestId"`
	CurrentSpotFleetRequestState  string `json:"currentSpotFleetRequestState"`
	PreviousSpotFleetRequestState string `json:"previousSpotFleetRequestState"`
	Error                         string `json:"error,omitempty"`
}

// SpotFleetInstance is a single instance within a spot fleet.
type SpotFleetInstance struct {
	InstanceID            string  `json:"instanceId"`
	InstanceType          string  `json:"instanceType"`
	SpotInstanceRequestID string  `json:"spotInstanceRequestId"`
	InstanceHealth        string  `json:"instanceHealth"`
	WeightedCapacity      float64 `json:"weightedCapacity"`
}

// appendFleetHistoryLocked appends a history record for fleetID while capping
// the slice at maxSpotFleetHistoryEntries to prevent unbounded memory growth.
// Must be called with b.mu held for writing.
func (b *InMemoryBackend) appendFleetHistoryLocked(fleetID string, rec SpotFleetHistoryRecord) {
	records := b.spotFleetHistory[fleetID]
	records = append(records, rec)

	if len(records) > maxSpotFleetHistoryEntries {
		// Drop the oldest half to amortise the copy cost.
		half := maxSpotFleetHistoryEntries / 2
		copy(records, records[len(records)-half:])
		records = records[:half]
	}

	b.spotFleetHistory[fleetID] = records
}
