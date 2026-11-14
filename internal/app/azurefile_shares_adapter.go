package app

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azurefile"
)

// azureFileShares adapts the Azure Files data plane to services/azurearm.FileShares.
type azureFileShares struct {
	b *azurefile.InMemoryBackend
}

var _ azurearm.FileShares = azureFileShares{}

func fileShareErr(err error) error {
	if se, ok := errors.AsType[*azurefile.ShareError](err); ok {
		return &azurearm.FileShareError{Code: se.Code, Message: se.Message, Status: se.Status}
	}

	return err
}

func (a azureFileShares) PutFileShare(
	account, name string, in azurearm.FileShareInput, create bool,
) (azurearm.FileShareView, bool, error) {
	acl := make([]azurefile.ShareACL, 0, len(in.ACL))
	for _, p := range in.ACL {
		acl = append(acl, azurefile.ShareACL(p))
	}

	v, created, err := a.b.PutShare(account, name, azurefile.ShareInput{
		Metadata: in.Metadata, Quota: in.Quota, Iops: in.Iops, Bandwidth: in.Bandwidth, BurstIops: in.BurstIops,
		BurstBandwidth: in.BurstBandwidth, PaidBursting: in.PaidBursting, Tier: in.Tier, Protocols: in.Protocols,
		RootSquash: in.RootSquash, ACL: acl, HasMetadata: in.HasMetadata, HasACL: in.HasACL,
	}, create)
	if err != nil {
		return azurearm.FileShareView{}, false, fileShareErr(err)
	}

	return fileShareView(v), created, nil
}

func (a azureFileShares) GetFileShare(account, name, snapshot string) (azurearm.FileShareView, error) {
	v, err := a.b.GetShare(account, name, snapshot)
	if err != nil {
		return azurearm.FileShareView{}, fileShareErr(err)
	}

	return fileShareView(v), nil
}

func (a azureFileShares) ListFileShares(account string, snapshots, deleted bool) []azurearm.FileShareView {
	views := a.b.ListShares(account, snapshots, deleted)
	out := make([]azurearm.FileShareView, 0, len(views))

	for _, v := range views {
		out = append(out, fileShareView(v))
	}

	return out
}

func (a azureFileShares) DeleteFileShare(account, name, snapshot, include string) (bool, error) {
	deleted, err := a.b.DeleteShare(account, name, snapshot, azurefile.ShareDeleteMode(include))

	return deleted, fileShareErr(err)
}

func (a azureFileShares) RestoreFileShare(account, target, deletedName, deletedVersion string) error {
	_, err := a.b.RestoreShare(account, target, deletedName, deletedVersion)

	return fileShareErr(err)
}

func fileShareView(v azurefile.ShareView) azurearm.FileShareView {
	acl := make([]azurearm.FileShareACL, 0, len(v.ACL))
	for _, p := range v.ACL {
		acl = append(acl, azurearm.FileShareACL(p))
	}

	return azurearm.FileShareView{
		Created: v.Created, LastModified: v.LastModified, TierChanged: v.TierChanged, DeletedAt: v.DeletedAt,
		Metadata: v.Metadata, Iops: v.Iops, Bandwidth: v.Bandwidth, BurstIops: v.BurstIops,
		BurstBandwidth: v.BurstBandwidth, PaidBursting: v.PaidBursting, SnapshotVDir: v.SnapshotVDir, Name: v.Name,
		ETag: v.ETag, Tier: v.Tier, Protocols: v.Protocols, RootSquash: v.RootSquash, Snapshot: v.Snapshot,
		LeaseStatus: v.LeaseStatus, LeaseState: v.LeaseState, LeaseDuration: v.LeaseDuration, Version: v.Version,
		ACL: acl, Usage: v.Usage, RemainingDays: v.RemainingDays, Quota: v.Quota, Deleted: v.Deleted,
	}
}
