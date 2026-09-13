package ec2

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ErrSpotRequestNotFound is returned when a spot instance request is not found.
var ErrSpotRequestNotFound = errors.New("InvalidSpotInstanceRequestID.NotFound")

// SpotLaunchSpecification holds launch parameters for a spot instance request.
type SpotLaunchSpecification struct {
	ImageID      string `json:"imageID,omitempty"`
	InstanceType string `json:"instanceType,omitempty"`
	SubnetID     string `json:"subnetID,omitempty"`
}

// SpotInstanceRequest represents an EC2 spot instance request.
type SpotInstanceRequest struct {
	CreateTime                   time.Time               `json:"createTime"`
	CancelledAt                  time.Time               `json:"cancelledAt"`
	ValidUntil                   time.Time               `json:"validUntil"`
	LaunchSpec                   SpotLaunchSpecification `json:"launchSpec"`
	ID                           string                  `json:"id,omitempty"`
	InstanceID                   string                  `json:"instanceID,omitempty"`
	State                        string                  `json:"state,omitempty"`
	SpotPrice                    string                  `json:"spotPrice,omitempty"`
	Type                         string                  `json:"type,omitempty"`
	AvailabilityZoneGroup        string                  `json:"availabilityZoneGroup,omitempty"`
	LaunchGroup                  string                  `json:"launchGroup,omitempty"`
	InstanceInterruptionBehavior string                  `json:"instanceInterruptionBehavior,omitempty"`
}

// RequestSpotInstancesOptions carries RequestSpotInstances' remaining
// declare+echo fields, added as a trailing variadic struct to stay
// back-compatible with existing call sites.
type RequestSpotInstancesOptions struct {
	ValidUntil                   time.Time
	AvailabilityZoneGroup        string
	LaunchGroup                  string
	InstanceInterruptionBehavior string
	InstanceCount                int
}

// RequestSpotInstances creates one spot instance request per InstanceCount
// (api_op_RequestSpotInstances.go: "The maximum number of Spot Instances to
// launch" -- one SpotInstanceRequest is created per instance, not one
// request covering all of them) and immediately fulfils each with a running
// instance.
func (b *InMemoryBackend) RequestSpotInstances(
	imageID, instanceType, subnetID, spotPrice string,
	tags map[string]string,
	opts ...RequestSpotInstancesOptions,
) ([]*SpotInstanceRequest, error) {
	if imageID == "" {
		return nil, fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	o := RequestSpotInstancesOptions{InstanceInterruptionBehavior: "terminate"}
	if len(opts) > 0 {
		o = opts[0]
		if o.InstanceInterruptionBehavior == "" {
			o.InstanceInterruptionBehavior = "terminate"
		}
	}

	count := o.InstanceCount
	if count <= 0 {
		count = 1
	}

	b.mu.Lock("RequestSpotInstances")
	defer b.mu.Unlock()

	if subnetID == "" {
		subnetID = b.findDefaultSubnetID()
	} else if _, ok := b.subnets.Get(subnetID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrSubnetNotFound, subnetID)
	}

	// Allocate a backing instance immediately (mock fulfils spot requests instantly).
	vpcID := ""
	if sub, ok := b.subnets.Get(subnetID); ok {
		vpcID = sub.VPCID
	}

	out := make([]*SpotInstanceRequest, 0, count)

	for range count {
		instanceID := newInstanceID()
		inst := &Instance{
			ID:           instanceID,
			ImageID:      imageID,
			InstanceType: instanceType,
			State:        StateRunning,
			VPCID:        vpcID,
			SubnetID:     subnetID,
			LaunchTime:   time.Now(),
			PrivateIP:    b.allocPrivateIP(),
		}
		b.instances.Put(inst)

		reqID := "sir-" + uuid.New().String()[:8]
		req := &SpotInstanceRequest{
			ID:                           reqID,
			InstanceID:                   instanceID,
			State:                        stateActive,
			SpotPrice:                    spotPrice,
			Type:                         "one-time",
			CreateTime:                   time.Now(),
			ValidUntil:                   o.ValidUntil,
			AvailabilityZoneGroup:        o.AvailabilityZoneGroup,
			LaunchGroup:                  o.LaunchGroup,
			InstanceInterruptionBehavior: o.InstanceInterruptionBehavior,
			LaunchSpec: SpotLaunchSpecification{
				ImageID:      imageID,
				InstanceType: instanceType,
				SubnetID:     subnetID,
			},
		}
		b.spotRequests.Put(req)
		b.setTagsLocked(reqID, tags)

		out = append(out, req)
	}

	return out, nil
}

// DescribeSpotInstanceRequests returns spot requests, optionally filtered by IDs.
// When ids are provided, lookups are O(len(ids)) via the spot-request map
// rather than scanning every spot request in the backend.
func (b *InMemoryBackend) DescribeSpotInstanceRequests(ids []string) []*SpotInstanceRequest {
	b.mu.RLock("DescribeSpotInstanceRequests")
	defer b.mu.RUnlock()

	if len(ids) > 0 {
		out := make([]*SpotInstanceRequest, 0, len(ids))

		for _, id := range ids {
			req, ok := b.spotRequests.Get(id)
			if !ok {
				continue
			}

			cp := *req
			out = append(out, &cp)
		}

		return out
	}

	out := make([]*SpotInstanceRequest, 0, b.spotRequests.Len())

	for _, req := range b.spotRequests.All() {
		cp := *req
		out = append(out, &cp)
	}

	return out
}

// CancelSpotInstanceRequests cancels the given spot instance requests (transitions to cancelled).
func (b *InMemoryBackend) CancelSpotInstanceRequests(ids []string) error {
	b.mu.Lock("CancelSpotInstanceRequests")
	defer b.mu.Unlock()

	for _, id := range ids {
		req, ok := b.spotRequests.Get(id)
		if !ok {
			return fmt.Errorf("%w: %s", ErrSpotRequestNotFound, id)
		}

		req.State = stateCancelled
		req.CancelledAt = time.Now()
	}

	return nil
}
