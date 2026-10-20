package s3files

import (
	"context"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

// ENIRequest describes the network interface backing a mount target.
type ENIRequest struct {
	SubnetID       string
	Description    string
	IPv4Address    string
	SecurityGroups []string
}

// ENI is a provisioned network interface.
type ENI struct {
	ID             string
	PrivateIP      string
	SecurityGroups []string
}

// NetworkResolver provisions mount-target network interfaces in EC2. Errors must wrap
// ErrNotFound, ErrConflict or ErrValidation so they reach the wire with the right code.
type NetworkResolver interface {
	// CreateENI creates an interface; empty SecurityGroups selects the subnet VPC's default group.
	CreateENI(req ENIRequest) (ENI, error)
	// SetENISecurityGroups replaces the interface's security groups.
	SetENISecurityGroups(eniID string, sgs []string) error
	DeleteENI(eniID string)
}

// RoleResolver checks that an IAM role exists.
type RoleResolver interface {
	RoleExists(roleARN string) bool
}

// KeyResolver checks that a KMS key exists and is usable.
type KeyResolver interface {
	CheckKey(ctx context.Context, keyID string) error
}

// SetNetworkResolver wires EC2 network interface management; nil mints interface IDs locally.
func (b *InMemoryBackend) SetNetworkResolver(r NetworkResolver) {
	b.mu.Lock("SetNetworkResolver")
	defer b.mu.Unlock()
	b.network = r
}

// SetRoleResolver wires IAM role validation; nil leaves role ARNs shape-checked only.
func (b *InMemoryBackend) SetRoleResolver(r RoleResolver) {
	b.mu.Lock("SetRoleResolver")
	defer b.mu.Unlock()
	b.roles = r
}

// SetKeyResolver wires KMS key validation; nil leaves key IDs unchecked.
func (b *InMemoryBackend) SetKeyResolver(r KeyResolver) {
	b.mu.Lock("SetKeyResolver")
	defer b.mu.Unlock()
	b.keys = r
}

func (b *InMemoryBackend) checkFileSystemDeps(ctx context.Context, in CreateFileSystemInput) error {
	if b.roles != nil && !b.roles.RoleExists(in.RoleARN) {
		return validation("Role " + in.RoleARN + " does not exist")
	}

	if b.keys != nil && in.KMSKeyID != "" {
		if err := b.keys.CheckKey(ctx, in.KMSKeyID); err != nil {
			return validation(err.Error())
		}
	}

	return nil
}

// provisionNetwork gives mt its IP and interface, from EC2 when wired. Callers hold the lock.
func (b *InMemoryBackend) provisionNetwork(
	in CreateMountTargetInput,
	mt *MountTarget,
	info SubnetInfo,
	known bool,
) error {
	if b.network == nil {
		ipv4, err := b.pickIPv4(in, info, known)
		if err != nil {
			return err
		}

		mt.IPv4Address = ipv4
		mt.NetworkInterfaceID = awsid.New("eni-")
		mt.SecurityGroups = slices.Clone(in.SecurityGroups)

		return nil
	}

	eni, err := b.network.CreateENI(ENIRequest{
		SubnetID:       in.SubnetID,
		Description:    "S3 Files mount target for " + mt.FileSystemID + " (" + mt.ID + ")",
		IPv4Address:    in.IPv4Address,
		SecurityGroups: in.SecurityGroups,
	})
	if err != nil {
		return err
	}

	mt.NetworkInterfaceID = eni.ID
	mt.SecurityGroups = eni.SecurityGroups

	if in.IPAddressType != ipv6Only {
		mt.IPv4Address = eni.PrivateIP
	}

	return nil
}

// removeMountTarget drops mt and its network interface. Callers hold the write lock.
func (b *InMemoryBackend) removeMountTarget(mt *MountTarget) {
	if b.network != nil {
		b.network.DeleteENI(mt.NetworkInterfaceID)
	}

	b.mountTargets.Delete(mt.ID)
}
