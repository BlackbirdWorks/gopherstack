package directoryservice

import "slices"

// describeByID resolves an owned directory or an accepted share by ID.
// Callers must hold b.mu.
func (b *InMemoryBackend) describeByID(region, id string) (Directory, bool) {
	if d, ok := b.directoryGet(region, id); ok {
		return b.describeDirectory(d), true
	}

	return b.consumerDirectory(region, id)
}

// consumerDirectory builds the consumer-side view of an accepted share.
// Callers must hold b.mu.
func (b *InMemoryBackend) consumerDirectory(region, sharedID string) (Directory, bool) {
	sd, ok := b.sharedDirectoryGet(region, sharedID)
	if !ok || ShareStatus(sd.ShareStatus) != ShareStatusShared {
		return Directory{}, false
	}

	owner, ok := b.directoryGet(region, sd.OwnerDirectoryID)
	if !ok {
		return Directory{}, false
	}

	od := b.describeDirectory(owner)

	return Directory{
		LaunchTime:               sd.CreatedDateTime,
		StageLastUpdatedDateTime: sd.LastUpdatedDateTime,
		DirectoryID:              sd.SharedDirectoryID,
		Name:                     od.Name,
		ShortName:                od.ShortName,
		Type:                     DirectoryTypeSharedMicrosoftAD,
		Stage:                    DirectoryStageActive,
		ShareMethod:              ShareMethod(sd.ShareMethod),
		ShareStatus:              ShareStatus(sd.ShareStatus),
		ShareNotes:               sd.ShareNotes,
		OwnerDirectoryDescription: &OwnerDirectoryDescription{
			AccountID:      sd.OwnerAccountID,
			DirectoryID:    sd.OwnerDirectoryID,
			NetworkType:    od.NetworkType,
			RadiusSettings: od.RadiusSettings,
			RadiusStatus:   od.RadiusStatus,
			VpcSettings:    od.VpcSettings,
			DNSIPAddrs:     slices.Clone(od.DNSIPAddrs),
			DNSIPv6Addrs:   slices.Clone(od.DNSIPv6Addrs),
		},
	}, true
}
