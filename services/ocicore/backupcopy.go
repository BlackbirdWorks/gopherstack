package ocicore

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

// regionKeys maps OCI region identifiers to the region keys used in OCIDs.
var regionKeys = map[string]string{ //nolint:gochecknoglobals // immutable
	"us-ashburn-1": "iad", "us-phoenix-1": "phx", "us-sanjose-1": "sjc", "ca-toronto-1": "yyz",
	"ca-montreal-1": "yul", "sa-saopaulo-1": "gru", "sa-santiago-1": "scl", "sa-vinhedo-1": "vcp",
	"mx-queretaro-1": "qro", "eu-frankfurt-1": "fra", "eu-zurich-1": "zrh", "eu-amsterdam-1": "ams",
	"eu-marseille-1": "mrs", "eu-milan-1": "lin", "eu-stockholm-1": "arn", "uk-london-1": "lhr",
	"uk-cardiff-1": "cwl", "ap-tokyo-1": "nrt", "ap-osaka-1": "kix", "ap-seoul-1": "icn",
	"ap-chuncheon-1": "yny", "ap-mumbai-1": "bom", "ap-hyderabad-1": "hyd", "ap-singapore-1": "sin",
	"ap-sydney-1": "syd", "ap-melbourne-1": "mel", "me-jeddah-1": "jed", "me-dubai-1": "dxb",
	"il-jerusalem-1": "mtz", "af-johannesburg-1": "jnb",
}

// CopyBackupIn is the body of CopyVolumeBackup, CopyBootVolumeBackup and CopyVolumeGroupBackup.
type CopyBackupIn struct {
	DestinationRegion string `json:"destinationRegion"`
	DisplayName       string `json:"displayName"`
	KmsKeyID          string `json:"kmsKeyId"`
}

// copyRegion validates the destination of a cross-region copy and returns its region key.
func (b *InMemoryBackend) copyRegion(in CopyBackupIn, sourceRegion string) (string, *APIError) {
	if in.DestinationRegion == "" {
		return "", errMissing("destinationRegion is required")
	}

	key, ok := regionKeys[in.DestinationRegion]
	if !ok {
		return "", errParam("destinationRegion " + in.DestinationRegion + " is not a known region")
	}

	if in.DestinationRegion == cmpOrStr(sourceRegion, regionName) {
		return "", errParam("destinationRegion must differ from the region of the backup")
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return "", aerr
	}

	return key, b.checkKmsKey(in.KmsKeyID)
}

func newOCIDIn(kind, regionKey string) string {
	return "ocid1." + kind + ".oc1." + regionKey + "." + awsid.NewN("", ocidTailLen)
}

func (b *InMemoryBackend) copyWorkRequest(op, entity string, bk Resource) string {
	return b.newWorkRequest(op, bk.base().CompartmentID, []WorkResource{{
		ActionType: wrActionCreated, EntityType: entity, Identifier: bk.base().ID,
	}})
}

// CopyVolumeBackup copies an AVAILABLE volume backup to another region; the copy is CREATING until the clock settles.
func (b *InMemoryBackend) CopyVolumeBackup(id string, in CopyBackupIn) (Resource, string, string, *APIError) {
	b.mu.Lock("CopyVolumeBackup")
	defer b.mu.Unlock()

	src, aerr := getLive[*VolumeBackup](b, id)
	if aerr != nil {
		return nil, "", "", aerr
	}

	if src.LifecycleState != statAvailable {
		return nil, "", "", errState("The volume backup " + id + " is " + src.LifecycleState)
	}

	key, aerr := b.copyRegion(in, src.Region)
	if aerr != nil {
		return nil, "", "", aerr
	}

	cp := b.copyVolumeBackup(src, in, key)

	return cp.view(), cp.Internal.Etag, b.copyWorkRequest("COPY_VOLUME_BACKUP", "volumebackup", cp), nil
}

func (b *InMemoryBackend) copyVolumeBackup(src *VolumeBackup, in CopyBackupIn, key string) *VolumeBackup {
	cp := &VolumeBackup{
		Base: b.newBase(kindBackup, src.CompartmentID, cmpOrStr(in.DisplayName, src.DisplayName), TagsIn{}),
		Type: backupFull, SourceType: sourceManual, VolumeID: src.VolumeID, SourceVolumeBackupID: src.ID,
		KmsKeyID: in.KmsKeyID, SizeInGBs: src.SizeInGBs, SizeInMBs: src.SizeInMBs, TimeRequestReceived: b.nowUTC(),
		Region: in.DestinationRegion,
	}
	cp.ID = newOCIDIn(kindBackup, key)
	b.moveTo(cp, statCreating, statAvailable)
	b.add(cp)

	return cp
}

// CopyBootVolumeBackup copies an AVAILABLE boot volume backup to another region.
func (b *InMemoryBackend) CopyBootVolumeBackup(id string, in CopyBackupIn) (Resource, string, string, *APIError) {
	b.mu.Lock("CopyBootVolumeBackup")
	defer b.mu.Unlock()

	src, aerr := getLive[*BootVolumeBackup](b, id)
	if aerr != nil {
		return nil, "", "", aerr
	}

	if src.LifecycleState != statAvailable {
		return nil, "", "", errState("The boot volume backup " + id + " is " + src.LifecycleState)
	}

	key, aerr := b.copyRegion(in, src.Region)
	if aerr != nil {
		return nil, "", "", aerr
	}

	cp := b.copyBootBackup(src, in, key)

	return cp.view(), cp.Internal.Etag, b.copyWorkRequest("COPY_BOOT_VOLUME_BACKUP", "bootvolumebackup", cp), nil
}

func (b *InMemoryBackend) copyBootBackup(src *BootVolumeBackup, in CopyBackupIn, key string) *BootVolumeBackup {
	cp := &BootVolumeBackup{
		Base: b.newBase(kindBootBackup, src.CompartmentID, cmpOrStr(in.DisplayName, src.DisplayName), TagsIn{}),
		Type: backupFull, SourceType: sourceManual, BootVolumeID: src.BootVolumeID, SourceBootVolumeBackupID: src.ID,
		ImageID: src.ImageID, KmsKeyID: in.KmsKeyID, SizeInGBs: src.SizeInGBs, TimeRequestReceived: b.nowUTC(),
		Region: in.DestinationRegion,
	}
	cp.ID = newOCIDIn(kindBootBackup, key)
	b.moveTo(cp, statCreating, statAvailable)
	b.add(cp)

	return cp
}

// CopyVolumeGroupBackup copies a group backup and every member backup to another region.
func (b *InMemoryBackend) CopyVolumeGroupBackup(id string, in CopyBackupIn) (Resource, string, string, *APIError) {
	b.mu.Lock("CopyVolumeGroupBackup")
	defer b.mu.Unlock()

	src, aerr := getLive[*VolumeGroupBackup](b, id)
	if aerr != nil {
		return nil, "", "", aerr
	}

	if src.LifecycleState != statAvailable {
		return nil, "", "", errState("The volume group backup " + id + " is " + src.LifecycleState)
	}

	key, aerr := b.copyRegion(in, src.Region)
	if aerr != nil {
		return nil, "", "", aerr
	}

	cp := &VolumeGroupBackup{
		Base: b.newBase(
			kindGroupBackup,
			src.CompartmentID,
			cmpOrStr(in.DisplayName, src.DisplayName),
			TagsIn{},
		),
		Type:                      backupFull,
		SourceType:                sourceManual,
		VolumeGroupID:             src.VolumeGroupID,
		SourceVolumeGroupBackupID: src.ID,
		SizeInMBs:                 src.SizeInMBs,
		SizeInGBs:                 src.SizeInGBs,
		TimeRequestReceived:       b.nowUTC(),
		Region:                    in.DestinationRegion,
		VolumeBackupIDs:           []string{},
	}
	cp.ID = newOCIDIn(kindGroupBackup, key)

	for _, m := range b.groupBackupMembers(src) {
		switch bk := m.(type) {
		case *VolumeBackup:
			c := b.copyVolumeBackup(
				bk,
				CopyBackupIn{DestinationRegion: in.DestinationRegion, KmsKeyID: in.KmsKeyID},
				key,
			)
			c.VolumeGroupBackupID = cp.ID
			cp.VolumeBackupIDs = append(cp.VolumeBackupIDs, c.ID)
		case *BootVolumeBackup:
			c := b.copyBootBackup(bk, CopyBackupIn{DestinationRegion: in.DestinationRegion, KmsKeyID: in.KmsKeyID}, key)
			c.VolumeGroupBackupID = cp.ID
			cp.VolumeBackupIDs = append(cp.VolumeBackupIDs, c.ID)
		}
	}

	b.moveTo(cp, statCreating, statAvailable)
	b.add(cp)

	return cp.view(), cp.Internal.Etag, b.copyWorkRequest("COPY_VOLUME_GROUP_BACKUP", "volumegroupbackup", cp), nil
}

func copyEntries() []entry {
	return []entry{
		{copyOp((*InMemoryBackend).CopyVolumeBackup, "volumeBackupId"), http.MethodPost,
			idPath("/volumeBackups", "volumeBackupId") + "/actions/copy", "CopyVolumeBackup"},
		{copyOp((*InMemoryBackend).CopyBootVolumeBackup, "bootVolumeBackupId"), http.MethodPost,
			idPath("/bootVolumeBackups", "bootVolumeBackupId") + "/actions/copy", "CopyBootVolumeBackup"},
		{copyOp((*InMemoryBackend).CopyVolumeGroupBackup, "volumeGroupBackupId"), http.MethodPost,
			idPath("/volumeGroupBackups", "volumeGroupBackupId") + "/actions/copy", "CopyVolumeGroupBackup"},
	}
}

func copyOp(
	fn func(*InMemoryBackend, string, CopyBackupIn) (Resource, string, string, *APIError), param string,
) opFunc {
	return func(h *Handler, q *req) error {
		var in CopyBackupIn
		if err := q.decode(&in); err != nil {
			return err
		}

		v, etag, wr, aerr := fn(h.Backend, q.param(param), in)
		if aerr != nil {
			return aerr
		}

		q.header("opc-work-request-id", wr)

		return send(q, v, etag, nil)
	}
}
