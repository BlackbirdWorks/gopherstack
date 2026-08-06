package outposts

import (
	"errors"
	"fmt"
)

// awsServiceNameEC2 is the only types.AWSServiceName value this backend
// records: every runningInstance is populated by services/ec2's
// RunInstances via ConsumeCapacity below.
const awsServiceNameEC2 = "EC2"

// ErrOutpostNotFound and ErrInsufficientOutpostCapacity back the
// cross-service ConsumeCapacity/ReleaseCapacity contract services/ec2 calls
// into (via its own cross_service.go) from RunInstances/TerminateInstances.
// Exported (unlike this package's wire-shaping errNotFoundSentinel/
// errQuotaExceeded in errors.go) because a sibling package needs
// errors.Is-able values, not this package's own HTTP error rendering --
// services/ec2 translates these into its own EC2-wire-shaped sentinel
// errors rather than surfacing outposts' restjson1 exception shapes.
var (
	ErrOutpostNotFound             = errors.New("outpost does not exist")
	ErrInsufficientOutpostCapacity = errors.New("insufficient configured capacity for instance type")
)

// runningInstance is this backend's own authoritative record of one EC2
// instance currently running on one of its Outposts' assets, populated by
// ConsumeCapacity and removed by ReleaseCapacity. It is NOT a mirror of
// services/ec2's Instance table -- this package cannot import services/ec2
// to read that table directly, since ec2 already imports this package for
// the same cross-service call (services/ec2/cross_service.go) and Go
// forbids the resulting import cycle. Backs ListAssetInstances with real
// data instead of an always-empty result.
type runningInstance struct {
	InstanceID   string
	InstanceType string
	AssetID      string
	AccountID    string
	OutpostID    string
}

// ConsumeCapacity is real AWS's behavior of depleting an Outpost's
// configured instance-type capacity as EC2 instances launch onto it (see
// GetOutpostInstanceTypes's doc comment and PARITY.md's capacity-ledger
// note). Draws entirely from outpostArn's single seeded Asset
// (assets.go's seedAssetForOutpostLocked -- there is no public CreateAsset
// operation among these 43, so every Outpost has exactly one), atomically:
// either every one of instanceIDs is admitted and debited, or none are and
// no capacity is decremented. Called by services/ec2's RunInstances via
// cross_service.go, once per launch batch (instanceIDs are the batch's
// freshly-minted instance IDs, generated before capacity is checked so a
// rejected batch never creates an ec2.Instance at all).
func (b *InMemoryBackend) ConsumeCapacity(outpostArn, instanceType, accountID string, instanceIDs []string) error {
	if len(instanceIDs) == 0 {
		return nil
	}

	b.mu.Lock("ConsumeCapacity")
	defer b.mu.Unlock()

	o, ok := b.resolveOutpostLocked(outpostArn)
	if !ok {
		return fmt.Errorf("%w: %s", ErrOutpostNotFound, outpostArn)
	}

	assets := b.assetsByOutpost.Get(o.ID)
	if len(assets) == 0 || assets[0].ComputeAttributes == nil {
		return fmt.Errorf("%w: %s has no configured capacity for %s",
			ErrInsufficientOutpostCapacity, o.ID, instanceType)
	}

	a := assets[0]
	need := int32(len(instanceIDs)) //nolint:gosec // RunInstances count is always small

	capIdx := -1

	for i := range a.ComputeAttributes.InstanceTypeCapacities {
		if a.ComputeAttributes.InstanceTypeCapacities[i].InstanceType == instanceType {
			capIdx = i

			break
		}
	}

	var available int32
	if capIdx != -1 {
		available = a.ComputeAttributes.InstanceTypeCapacities[capIdx].Count
	}

	if available < need {
		return fmt.Errorf("%w: %s has %d available, %d requested",
			ErrInsufficientOutpostCapacity, instanceType, available, need)
	}

	a.ComputeAttributes.InstanceTypeCapacities[capIdx].Count -= need

	for _, id := range instanceIDs {
		b.runningInstances.Put(&runningInstance{
			InstanceID:   id,
			InstanceType: instanceType,
			AssetID:      a.ID,
			AccountID:    accountID,
			OutpostID:    o.ID,
		})
	}

	return nil
}

// ReleaseCapacity credits capacity back onto the Asset instanceID was
// originally drawn from, mirroring real AWS returning Outpost capacity when
// the EC2 instance that consumed it terminates. Silently no-ops if
// instanceID has no ConsumeCapacity record (it never launched on an
// Outpost, its Outpost/Asset was since deleted, or it was already
// released) -- called unconditionally by services/ec2's TerminateInstances
// via cross_service.go.
func (b *InMemoryBackend) ReleaseCapacity(instanceID string) {
	b.mu.Lock("ReleaseCapacity")
	defer b.mu.Unlock()

	ri, ok := b.runningInstances.Get(instanceID)
	if !ok {
		return
	}

	b.runningInstances.Delete(instanceID)

	a, ok := b.assets.Get(ri.AssetID)
	if !ok {
		return
	}

	if a.ComputeAttributes == nil {
		a.ComputeAttributes = &ComputeAttributes{}
	}

	for i := range a.ComputeAttributes.InstanceTypeCapacities {
		if a.ComputeAttributes.InstanceTypeCapacities[i].InstanceType == ri.InstanceType {
			a.ComputeAttributes.InstanceTypeCapacities[i].Count++

			return
		}
	}

	a.ComputeAttributes.InstanceTypeCapacities = append(
		a.ComputeAttributes.InstanceTypeCapacities,
		InstanceTypeCapacity{InstanceType: ri.InstanceType, Count: 1},
	)
}
