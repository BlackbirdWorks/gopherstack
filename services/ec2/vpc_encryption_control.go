package ec2

import (
	"errors"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ErrVpcEncryptionControlNotFound is returned when a VPC Encryption Control
// configuration is not found.
var ErrVpcEncryptionControlNotFound = errors.New("InvalidVpcEncryptionControlID.NotFound")

// ---- constants ----

const (
	vpcEncryptionControlModeMonitor = "monitor"
	vpcEncryptionControlModeEnforce = "enforce"

	vpcEncryptionControlStateAvailable = "available"
	vpcEncryptionControlStateDeleted   = "deleted"

	vpcEncryptionExclusionEnabled  = "enabled"
	vpcEncryptionExclusionDisabled = "disabled"

	vpcEncryptionControlIDPrefixLength = 17

	// Account-level VPC Encryption Control (parity-4): distinct mode/state
	// vocab from the per-VPC configuration above.
	accountVpcEncryptionControlModeUnmanaged      = "unmanaged"
	accountVpcEncryptionControlModeAttemptMonitor = "attempt-monitor"
	accountVpcEncryptionControlModeAttemptEnforce = "attempt-enforce"
	accountVpcEncryptionControlStateDefault       = "default-state"
	accountVpcEncryptionControlManagedByAccount   = "account"
)

// vpcEncryptionControlValidModes are the AWS-defined VpcEncryptionControlMode values.
//
//nolint:gochecknoglobals // lookup set
var vpcEncryptionControlValidModes = map[string]bool{
	vpcEncryptionControlModeMonitor: true,
	vpcEncryptionControlModeEnforce: true,
}

// accountVpcEncryptionControlValidModes are the AWS-defined
// AccountVpcEncryptionControlMode values, a distinct vocabulary from the
// per-VPC VpcEncryptionControlMode above.
//
//nolint:gochecknoglobals // lookup set
var accountVpcEncryptionControlValidModes = map[string]bool{
	accountVpcEncryptionControlModeUnmanaged:      true,
	accountVpcEncryptionControlModeAttemptMonitor: true,
	accountVpcEncryptionControlModeAttemptEnforce: true,
}

// ---- models ----

// VpcEncryptionControlExclusionState is a single traffic-type's exclusion setting
// within a VPC Encryption Control configuration.
type VpcEncryptionControlExclusionState struct {
	State string `json:"state,omitempty"`
}

// VpcEncryptionControlExclusions holds the per-traffic-type exclusion settings for a
// VPC Encryption Control configuration.
type VpcEncryptionControlExclusions struct {
	EgressOnlyInternetGateway VpcEncryptionControlExclusionState `json:"egressOnlyInternetGateway"`
	ElasticFileSystem         VpcEncryptionControlExclusionState `json:"elasticFileSystem"`
	InternetGateway           VpcEncryptionControlExclusionState `json:"internetGateway"`
	Lambda                    VpcEncryptionControlExclusionState `json:"lambda"`
	NatGateway                VpcEncryptionControlExclusionState `json:"natGateway"`
	VirtualPrivateGateway     VpcEncryptionControlExclusionState `json:"virtualPrivateGateway"`
	VpcLattice                VpcEncryptionControlExclusionState `json:"vpcLattice"`
	VpcPeering                VpcEncryptionControlExclusionState `json:"vpcPeering"`
}

// VpcEncryptionControl represents a per-VPC encryption-in-transit enforcement
// configuration (VPC Encryption Control).
type VpcEncryptionControl struct {
	Tags                   map[string]string              `json:"tags,omitempty"`
	ResourceExclusions     VpcEncryptionControlExclusions `json:"resourceExclusions"`
	VpcEncryptionControlID string                         `json:"vpcEncryptionControlId,omitempty"`
	VpcID                  string                         `json:"vpcId,omitempty"`
	Mode                   string                         `json:"mode,omitempty"`
	State                  string                         `json:"state,omitempty"`
	StateMessage           string                         `json:"stateMessage,omitempty"`
}

// VpcEncryptionNonCompliantResource describes a VPC sub-resource that is blocking
// encryption enforcement for a VPC.
type VpcEncryptionNonCompliantResource struct {
	ID           string `json:"id,omitempty"`
	Type         string `json:"type,omitempty"`
	Description  string `json:"description,omitempty"`
	IsExcludable bool   `json:"isExcludable,omitempty"`
}

func cloneVpcEncryptionControl(vec *VpcEncryptionControl) *VpcEncryptionControl {
	cp := *vec
	cp.Tags = maps.Clone(vec.Tags)

	return &cp
}

// ---- VpcEncryptionControlExclusionModify ----

// VpcEncryptionControlExclusionModify holds the subset of exclusion fields that
// ModifyVpcEncryptionControl may change. Empty string values mean "leave unchanged".
type VpcEncryptionControlExclusionModify struct {
	EgressOnlyInternetGateway string
	ElasticFileSystem         string
	InternetGateway           string
	Lambda                    string
	NatGateway                string
	VirtualPrivateGateway     string
	VpcLattice                string
	VpcPeering                string
}

// vpcEncryptionExclusionInputToState maps the AWS "enable"/"disable" modify input
// value onto the stored "enabled"/"disabled" exclusion state.
func vpcEncryptionExclusionInputToState(input string) string {
	switch input {
	case "enable":
		return vpcEncryptionExclusionEnabled
	case "disable":
		return vpcEncryptionExclusionDisabled
	default:
		return ""
	}
}

// applyVpcEncryptionControlExclusionModify merges non-empty fields of mod onto excl.
func applyVpcEncryptionControlExclusionModify(
	excl *VpcEncryptionControlExclusions,
	mod VpcEncryptionControlExclusionModify,
) {
	fields := []struct {
		out *VpcEncryptionControlExclusionState
		in  string
	}{
		{&excl.EgressOnlyInternetGateway, mod.EgressOnlyInternetGateway},
		{&excl.ElasticFileSystem, mod.ElasticFileSystem},
		{&excl.InternetGateway, mod.InternetGateway},
		{&excl.Lambda, mod.Lambda},
		{&excl.NatGateway, mod.NatGateway},
		{&excl.VirtualPrivateGateway, mod.VirtualPrivateGateway},
		{&excl.VpcLattice, mod.VpcLattice},
		{&excl.VpcPeering, mod.VpcPeering},
	}

	for _, f := range fields {
		if state := vpcEncryptionExclusionInputToState(f.in); state != "" {
			f.out.State = state
		}
	}
}

// ---- CreateVpcEncryptionControl ----

// CreateVpcEncryptionControl creates a VPC Encryption Control configuration for a VPC.
// New configurations start in "monitor" mode with all traffic-type exclusions disabled,
// matching the AWS default; ModifyVpcEncryptionControl subsequently switches to "enforce"
// mode or adjusts exclusions. Only one configuration is allowed per VPC.
func (b *InMemoryBackend) CreateVpcEncryptionControl(
	vpcID string,
	tags map[string]string,
) (*VpcEncryptionControl, error) {
	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateVpcEncryptionControl")
	defer b.mu.Unlock()

	if _, ok := b.vpcs.Get(vpcID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	for _, existing := range b.vpcEncryptionControls.All() {
		if existing.VpcID == vpcID {
			return nil, fmt.Errorf(
				"%w: a VPC Encryption Control configuration already exists for %s",
				ErrInvalidParameter, vpcID,
			)
		}
	}

	disabled := VpcEncryptionControlExclusionState{State: vpcEncryptionExclusionDisabled}
	vec := &VpcEncryptionControl{
		VpcEncryptionControlID: "vpc-ec-" + uuid.New().String()[:vpcEncryptionControlIDPrefixLength],
		VpcID:                  vpcID,
		Mode:                   vpcEncryptionControlModeMonitor,
		State:                  vpcEncryptionControlStateAvailable,
		ResourceExclusions: VpcEncryptionControlExclusions{
			EgressOnlyInternetGateway: disabled,
			ElasticFileSystem:         disabled,
			InternetGateway:           disabled,
			Lambda:                    disabled,
			NatGateway:                disabled,
			VirtualPrivateGateway:     disabled,
			VpcLattice:                disabled,
			VpcPeering:                disabled,
		},
		Tags: maps.Clone(tags),
	}
	b.vpcEncryptionControls.Put(vec)

	return cloneVpcEncryptionControl(vec), nil
}

// DeleteVpcEncryptionControl removes a VPC Encryption Control configuration.
func (b *InMemoryBackend) DeleteVpcEncryptionControl(id string) (*VpcEncryptionControl, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: VpcEncryptionControlId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteVpcEncryptionControl")
	defer b.mu.Unlock()

	vec, ok := b.vpcEncryptionControls.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVpcEncryptionControlNotFound, id)
	}

	vec.State = vpcEncryptionControlStateDeleted
	out := cloneVpcEncryptionControl(vec)
	b.vpcEncryptionControls.Delete(id)
	delete(b.tags, id)

	return out, nil
}

// DescribeVpcEncryptionControls returns VPC Encryption Control configurations,
// optionally filtered by configuration ID and/or VPC ID.
func (b *InMemoryBackend) DescribeVpcEncryptionControls(ids, vpcIDs []string) []*VpcEncryptionControl {
	b.mu.RLock("DescribeVpcEncryptionControls")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	vpcIDSet := make(map[string]bool, len(vpcIDs))
	for _, id := range vpcIDs {
		vpcIDSet[id] = true
	}

	out := make([]*VpcEncryptionControl, 0, b.vpcEncryptionControls.Len())

	for _, vec := range b.vpcEncryptionControls.All() {
		if len(idSet) > 0 && !idSet[vec.VpcEncryptionControlID] {
			continue
		}

		if len(vpcIDSet) > 0 && !vpcIDSet[vec.VpcID] {
			continue
		}

		out = append(out, cloneVpcEncryptionControl(vec))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].VpcEncryptionControlID < out[j].VpcEncryptionControlID })

	return out
}

// ModifyVpcEncryptionControl updates the mode and/or per-traffic-type exclusions of an
// existing VPC Encryption Control configuration.
func (b *InMemoryBackend) ModifyVpcEncryptionControl(
	id, mode string,
	exclusions VpcEncryptionControlExclusionModify,
) (*VpcEncryptionControl, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: VpcEncryptionControlId is required", ErrInvalidParameter)
	}

	if mode != "" && !vpcEncryptionControlValidModes[mode] {
		return nil, fmt.Errorf("%w: Mode %q is invalid", ErrInvalidParameter, mode)
	}

	b.mu.Lock("ModifyVpcEncryptionControl")
	defer b.mu.Unlock()

	vec, ok := b.vpcEncryptionControls.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVpcEncryptionControlNotFound, id)
	}

	if mode != "" {
		vec.Mode = mode
	}

	applyVpcEncryptionControlExclusionModify(&vec.ResourceExclusions, exclusions)

	return cloneVpcEncryptionControl(vec), nil
}

// GetVpcResourcesBlockingEncryptionEnforcement returns the VPC sub-resources that are
// blocking encryption enforcement for a VPC. Enforcement blockers arise from
// unencrypted-traffic-capable resources (e.g. gateways lacking exclusions); this mock's
// resource set has no such non-compliant resources, so it returns an empty, valid list
// once the VPC itself is confirmed to exist.
func (b *InMemoryBackend) GetVpcResourcesBlockingEncryptionEnforcement(
	vpcID string,
) ([]*VpcEncryptionNonCompliantResource, error) {
	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetVpcResourcesBlockingEncryptionEnforcement")
	defer b.mu.RUnlock()

	if _, ok := b.vpcs.Get(vpcID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	return []*VpcEncryptionNonCompliantResource{}, nil
}

// ---- Account-level VPC Encryption Control (parity-4) ----

// AccountVpcEncryptionControl represents the account-wide VPC Encryption
// Control configuration (as distinct from the per-VPC VpcEncryptionControl
// above), field-diffed against aws-sdk-go-v2/service/ec2/types.AccountVpcEncryptionControl.
type AccountVpcEncryptionControl struct {
	LastUpdateTimestamp time.Time                      `json:"lastUpdateTimestamp,omitzero"`
	Mode                string                         `json:"mode,omitempty"`
	State               string                         `json:"state,omitempty"`
	ManagedBy           string                         `json:"managedBy,omitempty"`
	Exclusions          VpcEncryptionControlExclusions `json:"exclusions"`
}

// DescribeAccountVpcEncryptionControl returns the current account-level VPC
// Encryption Control configuration singleton.
func (b *InMemoryBackend) DescribeAccountVpcEncryptionControl() *AccountVpcEncryptionControl {
	b.mu.RLock("DescribeAccountVpcEncryptionControl")
	defer b.mu.RUnlock()

	cp := *b.accountVpcEncryptionControl

	return &cp
}

// ModifyAccountVpcEncryptionControl updates the account-level VPC Encryption
// Control mode and/or per-traffic-type exclusions.
func (b *InMemoryBackend) ModifyAccountVpcEncryptionControl(
	mode string,
	exclusions VpcEncryptionControlExclusionModify,
) (*AccountVpcEncryptionControl, error) {
	if mode != "" && !accountVpcEncryptionControlValidModes[mode] {
		return nil, fmt.Errorf("%w: Mode %q is invalid", ErrInvalidParameter, mode)
	}

	b.mu.Lock("ModifyAccountVpcEncryptionControl")
	defer b.mu.Unlock()

	if mode != "" {
		b.accountVpcEncryptionControl.Mode = mode
	}

	applyVpcEncryptionControlExclusionModify(&b.accountVpcEncryptionControl.Exclusions, exclusions)
	b.accountVpcEncryptionControl.LastUpdateTimestamp = time.Now().UTC()

	cp := *b.accountVpcEncryptionControl

	return &cp, nil
}
