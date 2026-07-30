package ec2

import (
	"fmt"
	"hash/fnv"
	"maps"
	"math"
	"sort"
	"strings"
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
	// spotFleetHistoryHalfPoint is the trim target when the history slice exceeds its cap.
	spotFleetHistoryHalfPoint = maxSpotFleetHistoryEntries / 2
)

// SpotFleetLaunchSpecification is a single launch spec within a spot fleet config.
type SpotFleetLaunchSpecification struct {
	ImageID          string  `json:"imageId,omitempty"`
	InstanceType     string  `json:"instanceType,omitempty"`
	SubnetID         string  `json:"subnetId,omitempty"`
	KeyName          string  `json:"keyName,omitempty"`
	SpotPrice        string  `json:"spotPrice,omitempty"`
	WeightedCapacity float64 `json:"weightedCapacity"`
}

// SpotFleetRequestConfig is the configuration submitted with RequestSpotFleet.
type SpotFleetRequestConfig struct {
	ValidFrom                        time.Time                      `json:"validFrom"`
	ValidUntil                       time.Time                      `json:"validUntil"`
	SpotPrice                        string                         `json:"spotPrice,omitempty"`
	AllocationStrategy               string                         `json:"allocationStrategy,omitempty"`
	ExcessCapacityTerminationPolicy  string                         `json:"excessCapacityTerminationPolicy,omitempty"`
	IamFleetRole                     string                         `json:"iamFleetRole,omitempty"`
	Type                             string                         `json:"type,omitempty"`
	LaunchSpecifications             []SpotFleetLaunchSpecification `json:"launchSpecifications,omitempty"`
	TargetCapacity                   int                            `json:"targetCapacity,omitempty"`
	TerminateInstancesWithExpiration bool                           `json:"terminateInstancesWithExpiration,omitempty"`
	ReplaceUnhealthyInstances        bool                           `json:"replaceUnhealthyInstances,omitempty"`
}

// SpotFleetRequest represents a Spot Fleet request.
type SpotFleetRequest struct {
	CreateTime                time.Time              `json:"createTime"`
	Tags                      map[string]string      `json:"tags,omitempty"`
	SpotFleetRequestID        string                 `json:"spotFleetRequestId,omitempty"`
	SpotFleetRequestState     string                 `json:"spotFleetRequestState,omitempty"`
	ActivityStatus            string                 `json:"activityStatus,omitempty"`
	InstanceIDs               []string               `json:"instanceIds,omitempty"`
	SpotFleetRequestConfig    SpotFleetRequestConfig `json:"spotFleetRequestConfig"`
	FulfilledCapacity         float64                `json:"fulfilledCapacity"`
	OnDemandFulfilledCapacity float64                `json:"onDemandFulfilledCapacity"`
}

// SpotFleetHistoryRecord is a history event for a spot fleet.
type SpotFleetHistoryRecord struct {
	Timestamp        time.Time `json:"timestamp"`
	EventType        string    `json:"eventType,omitempty"`
	EventInformation string    `json:"eventInformation,omitempty"`
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
	} else if _, ok := b.subnets.Get(subnetID); !ok {
		subnetID = b.findDefaultSubnetID()
	}

	vpcID := ""
	if sub, ok := b.subnets.Get(subnetID); ok {
		vpcID = sub.VPCID
	}

	weightedCap := spec.WeightedCapacity
	if weightedCap <= 0 {
		weightedCap = 1.0
	}

	spawned := 0
	fulfilled := 0.0

	for fulfilled < float64(config.TargetCapacity) && spawned < spotFleetMaxInstances {
		b.spawnFleetInstanceLocked(fleet, imageID, instanceType, subnetID, vpcID)
		fulfilled += weightedCap
		spawned++
	}

	return spawned, fulfilled
}

// spawnFleetInstanceLocked creates a single instance (with a matching primary
// ENI) for a spot fleet at the given image/instance-type/subnet/VPC, indexes
// it, and appends its ID to fleet.InstanceIDs. Must be called with b.mu held
// for writing. Shared by spawnFleetInstancesLocked and scaleFleetUpLocked, the
// two fleet-scaling paths that spawn instances the same way.
func (b *InMemoryBackend) spawnFleetInstanceLocked(
	fleet *SpotFleetRequest,
	imageID, instanceType, subnetID, vpcID string,
) {
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
		config.Type = fleetTypeDefault
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
	b.spotFleets.Put(fleet)

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

	results := make([]*SpotFleetRequest, 0, b.spotFleets.Len())

	for _, fleet := range b.spotFleets.All() {
		id := spotFleetsKeyFn(fleet)
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
			if _, ok := b.spotFleets.Get(id); !ok {
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
		fleet, ok := b.spotFleets.Get(id)
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
				if inst, exists := b.instances.Get(instID); exists {
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
	} else if _, ok := b.subnets.Get(subnetID); !ok {
		subnetID = b.findDefaultSubnetID()
	}

	vpcID := ""
	if sub, ok := b.subnets.Get(subnetID); ok {
		vpcID = sub.VPCID
	}

	weightedCap := spec.WeightedCapacity
	if weightedCap <= 0 {
		weightedCap = 1.0
	}

	for fleet.FulfilledCapacity < float64(newTarget) {
		b.spawnFleetInstanceLocked(fleet, imageID, instanceType, subnetID, vpcID)
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

		if inst, exists := b.instances.Get(instID); exists {
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

	fleet, ok := b.spotFleets.Get(fleetID)
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

	fleet, ok := b.spotFleets.Get(fleetID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSpotFleetNotFound, fleetID)
	}

	result := make([]SpotFleetInstance, 0, len(fleet.InstanceIDs))

	for _, instID := range fleet.InstanceIDs {
		inst, exists := b.instances.Get(instID)
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

	if _, ok := b.spotFleets.Get(fleetID); !ok {
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
	SpotFleetRequestID            string `json:"spotFleetRequestId,omitempty"`
	CurrentSpotFleetRequestState  string `json:"currentSpotFleetRequestState,omitempty"`
	PreviousSpotFleetRequestState string `json:"previousSpotFleetRequestState,omitempty"`
	Error                         string `json:"error,omitempty"`
}

// SpotFleetInstance is a single instance within a spot fleet.
type SpotFleetInstance struct {
	InstanceID            string  `json:"instanceId,omitempty"`
	InstanceType          string  `json:"instanceType,omitempty"`
	SpotInstanceRequestID string  `json:"spotInstanceRequestId,omitempty"`
	InstanceHealth        string  `json:"instanceHealth,omitempty"`
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
		half := spotFleetHistoryHalfPoint
		copy(records, records[len(records)-half:])
		records = records[:half]
	}

	b.spotFleetHistory[fleetID] = records
}

// SpotPriceRecord represents a single spot price history data point.
type SpotPriceRecord struct {
	Timestamp          time.Time `json:"timestamp"`
	InstanceType       string    `json:"instanceType,omitempty"`
	AvailabilityZone   string    `json:"availabilityZone,omitempty"`
	ProductDescription string    `json:"productDescription,omitempty"`
	SpotPrice          string    `json:"spotPrice,omitempty"`
}

// spotPriceHashModulus is the hash modulus used to produce a 0–0.40 spread in
// deterministicSpotPrice; combined with spotPriceDivisor it yields a 0.30–0.70 ratio.
const (
	spotPriceHashModulus = 1000
	// spotPriceDivisor maps the 0–999 hash remainder to a 0–0.40 spread.
	spotPriceDivisor = 2500.0
	// spotPriceMinRatio is the base ratio (0.30) before the hash spread is added.
	spotPriceMinRatio = 0.30
	// spotPriceDecimalScale rounds prices to 4 decimal places (× then /).
	spotPriceDecimalScale = 10000
	// spotPriceHistoryBucketHours is the number of hours between synthetic price records.
	spotPriceHistoryBucketHours = 6
)

const (
	spotPlacementScoreFloor = 1
	spotPlacementScoreCeil  = 10
	spotPlacementScoreRange = spotPlacementScoreCeil - spotPlacementScoreFloor + 1
	spotPlacementScoreTopN  = 10
)

// SpotPlacementScoreEntry is a single deterministic Spot placement score
// returned by GetSpotPlacementScores.
type SpotPlacementScoreEntry struct {
	Region             string
	AvailabilityZoneID string
	Score              int32
}

// CreateSpotDatafeedSubscription creates the account-level spot data feed.
func (b *InMemoryBackend) CreateSpotDatafeedSubscription(
	bucket, prefix string,
) (*SpotDatafeed, error) {
	if bucket == "" {
		return nil, fmt.Errorf("%w: Bucket is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateSpotDatafeedSubscription")
	defer b.mu.Unlock()

	b.spotDatafeed = &SpotDatafeed{Bucket: bucket, Prefix: prefix, State: stateActive}

	return b.spotDatafeed, nil
}

// DeleteSpotDatafeedSubscription removes the spot data feed.
func (b *InMemoryBackend) DeleteSpotDatafeedSubscription() {
	b.mu.Lock("DeleteSpotDatafeedSubscription")
	defer b.mu.Unlock()

	b.spotDatafeed = nil
}

// DescribeSpotDatafeedSubscription returns the current spot data feed.
func (b *InMemoryBackend) DescribeSpotDatafeedSubscription() *SpotDatafeed {
	b.mu.RLock("DescribeSpotDatafeedSubscription")
	defer b.mu.RUnlock()

	if b.spotDatafeed == nil {
		return nil
	}
	cp := *b.spotDatafeed

	return &cp
}

// ---- Image lifecycle ----

// deterministicSpotPrice returns a stable spot price for (instanceType, az, product)
// that varies predictably without randomness, keeping tests deterministic.
// Price is ~30–70% of the on-demand base.
func deterministicSpotPrice(instanceType, az, product string) float64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(instanceType + "|" + az + "|" + product))
	ratio := spotPriceMinRatio + float64(h.Sum32()%spotPriceHashModulus)/spotPriceDivisor // 0.30 – 0.70
	base, ok := spotPriceBaseTable[instanceType]

	if !ok {
		base = 0.10
	}

	// Round to 4 decimal places for realism.
	return math.Round(base*ratio*spotPriceDecimalScale) / spotPriceDecimalScale
}

// GenerateSpotPriceHistory produces a deterministic slice of spot price records
// for the given filters. startTime defaults to 24 h ago if zero.
// Each (instanceType, az, product) combination gets one record per hour in the window.
func GenerateSpotPriceHistory(
	instanceTypes, azs, products []string,
	startTime time.Time,
	region string,
) []SpotPriceRecord {
	if startTime.IsZero() {
		startTime = time.Now().UTC().Add(-24 * time.Hour)
	}

	endTime := time.Now().UTC()

	if len(instanceTypes) == 0 {
		instanceTypes = []string{
			instanceTypeT3Micro,
			instanceTypeT3Small,
			spotFleetDefaultInstanceType,
			instanceTypeC5XL,
		}
	}

	if len(azs) == 0 {
		azs = []string{region + "a", region + "b", region + "c"}
	}

	if len(products) == 0 {
		products = []string{"Linux/UNIX"}
	}

	var records []SpotPriceRecord

	for _, it := range instanceTypes {
		for _, az := range azs {
			for _, prod := range products {
				price := deterministicSpotPrice(it, az, prod)
				// One price point per 6-hour bucket in the window.
				for ts := startTime; ts.Before(endTime); ts = ts.Add(spotPriceHistoryBucketHours * time.Hour) {
					records = append(records, SpotPriceRecord{
						InstanceType:       it,
						AvailabilityZone:   az,
						ProductDescription: prod,
						SpotPrice:          fmt.Sprintf("%.4f", price),
						Timestamp:          ts,
					})
				}
			}
		}
	}

	return records
}

// spotPlacementScoreFor derives a deterministic score (1-10) for a seed
// string so repeated queries against the same inputs are stable, since this
// mock has no real capacity telemetry to sample from.
func spotPlacementScoreFor(seed string) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(seed))

	return int32(h.Sum32()%spotPlacementScoreRange) + spotPlacementScoreFloor
}

// GetSpotPlacementScores returns deterministic Spot placement scores for the
// requested instance types across the requested (or default) regions/AZs.
func (b *InMemoryBackend) GetSpotPlacementScores(
	instanceTypes, regionNames []string, singleAZ bool,
) ([]*SpotPlacementScoreEntry, error) {
	if len(instanceTypes) == 0 {
		return nil, fmt.Errorf("%w: InstanceType is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetSpotPlacementScores")
	defer b.mu.RUnlock()

	regions := regionNames
	if len(regions) == 0 {
		regions = stubRegions
	}

	seedSuffix := strings.Join(instanceTypes, ",")
	out := make([]*SpotPlacementScoreEntry, 0, len(regions))

	for _, region := range regions {
		if !singleAZ {
			out = append(out, &SpotPlacementScoreEntry{
				Region: region,
				Score:  spotPlacementScoreFor(region + seedSuffix),
			})

			continue
		}

		for _, az := range b.DescribeAvailabilityZones(region) {
			out = append(out, &SpotPlacementScoreEntry{
				AvailabilityZoneID: az,
				Score:              spotPlacementScoreFor(az + seedSuffix),
			})
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}

		return out[i].Region+out[i].AvailabilityZoneID < out[j].Region+out[j].AvailabilityZoneID
	})

	if len(out) > spotPlacementScoreTopN {
		out = out[:spotPlacementScoreTopN]
	}

	return out, nil
}
