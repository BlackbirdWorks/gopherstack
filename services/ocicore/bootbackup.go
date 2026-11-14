package ocicore

// CreateBootVolumeBackupIn is the CreateBootVolumeBackup request body.
type CreateBootVolumeBackupIn struct {
	TagsIn
	RetentionIn
	DisplayName  string `json:"displayName"`
	Type         string `json:"type"`
	BootVolumeID string `json:"bootVolumeId"`
	KmsKeyID     string `json:"kmsKeyId"`
}

// CreateBootVolumeBackup snapshots a boot volume; the first backup of a boot volume is always FULL.
func (b *InMemoryBackend) CreateBootVolumeBackup(in CreateBootVolumeBackupIn) (Resource, string, *APIError) {
	if aerr := backupRequest(in.DisplayName, "bootVolumeId", in.BootVolumeID, in.Type); aerr != nil {
		return nil, "", aerr
	}

	bv, aerr := availableSource[*BootVolume](b, in.BootVolumeID, "boot volume")
	if aerr == nil {
		aerr = b.checkKmsKey(in.KmsKeyID)
	}

	if aerr != nil {
		return nil, "", aerr
	}

	bk, aerr := b.snapshotBootVolume(bv, in, bv.CompartmentID)
	if aerr != nil {
		return nil, "", aerr
	}

	return bk.view(), bk.Internal.Etag, nil
}

func (b *InMemoryBackend) snapshotBootVolume(
	bv *BootVolume, in CreateBootVolumeBackupIn, comp string,
) (*BootVolumeBackup, *APIError) {
	prior := len(liveOf(b, func(x *BootVolumeBackup) bool { return x.BootVolumeID == bv.ID }))
	bk := &BootVolumeBackup{
		Base:         b.newBase(kindBootBackup, comp, b.stamped(in.DisplayName, "bootbackup"), in.TagsIn),
		Type:         backupTypeFor(in.Type, prior),
		BootVolumeID: bv.ID, SourceType: sourceManual, KmsKeyID: in.KmsKeyID, ImageID: bv.ImageID,
		SizeInGBs: bv.SizeInGBs, TimeRequestReceived: b.nowUTC(),
	}

	if aerr := b.commitBackup(bk, &bk.Retention, in.RetentionIn); aerr != nil {
		return nil, aerr
	}

	return bk, nil
}

// UpdateBootVolumeBackupIn is the UpdateBootVolumeBackup request body.
type UpdateBootVolumeBackupIn struct {
	KmsKeyID *string `json:"kmsKeyId"`
	RetentionIn
	UpdateIn
}

// UpdateBootVolumeBackup changes a boot volume backup's name, tags, key and retention settings.
func (b *InMemoryBackend) UpdateBootVolumeBackup(
	id, ifMatch string,
	in UpdateBootVolumeBackupIn,
) (Resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(bk *BootVolumeBackup) *APIError {
		if in.KmsKeyID != nil {
			if aerr := b.checkKmsKey(*in.KmsKeyID); aerr != nil {
				return aerr
			}
		}

		if aerr := bk.apply(in.RetentionIn, bk.TimeCreated); aerr != nil {
			return aerr
		}

		if in.KmsKeyID != nil {
			bk.KmsKeyID = *in.KmsKeyID
		}

		return nil
	})
}
