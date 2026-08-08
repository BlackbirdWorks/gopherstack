package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

type modifyVolumeResponse struct {
	XMLName            xml.Name               `xml:"ModifyVolumeResponse"`
	RequestID          string                 `xml:"requestId"`
	VolumeModification volumeModificationItem `xml:"volumeModification"`
}

type volumeStatusInfo struct {
	Status string `xml:"status"`
}

type volumeStatusItem struct {
	VolumeID         string           `xml:"volumeId"`
	AvailabilityZone string           `xml:"availabilityZone"`
	VolumeStatus     volumeStatusInfo `xml:"volumeStatus"`
}

type volumeStatusSet struct {
	Items []volumeStatusItem `xml:"item"`
}

type describeVolumeStatusResponse struct {
	XMLName         xml.Name        `xml:"DescribeVolumeStatusResponse"`
	RequestID       string          `xml:"requestId"`
	VolumeStatusSet volumeStatusSet `xml:"volumeStatusSet"`
}

type describeVolumesModificationsResponse struct {
	XMLName               xml.Name `xml:"DescribeVolumesModificationsResponse"`
	RequestID             string   `xml:"requestId"`
	VolumeModificationSet struct {
		Items []volumeModificationItem `xml:"item"`
	} `xml:"volumeModificationSet"`
}

func (h *Handler) handleModifyVolume(vals url.Values, reqID string) (any, error) {
	volumeID := vals.Get("VolumeId")
	volumeType := vals.Get("VolumeType")
	sizeStr := vals.Get("Size")
	iopsStr := vals.Get("Iops")

	var size, iops int
	if sizeStr != "" {
		var parseErr error
		size, parseErr = strconv.Atoi(sizeStr)
		if parseErr != nil {
			return nil, fmt.Errorf("%w: invalid Size value: %s", ErrInvalidParameter, sizeStr)
		}
	}
	if iopsStr != "" {
		var parseErr error
		iops, parseErr = strconv.Atoi(iopsStr)
		if parseErr != nil {
			return nil, fmt.Errorf("%w: invalid Iops value: %s", ErrInvalidParameter, iopsStr)
		}
	}

	mod, err := h.Backend.ModifyVolume(volumeID, volumeType, size, iops)
	if err != nil {
		return nil, err
	}

	return &modifyVolumeResponse{
		RequestID: reqID,
		VolumeModification: volumeModificationItem{
			VolumeID:          mod.VolumeID,
			ModificationState: mod.ModificationState,
			TargetVolumeType:  mod.TargetVolumeType,
			TargetSize:        mod.TargetSize,
			OrigVolumeType:    mod.OrigVolumeType,
			OrigSize:          mod.OrigSize,
			Progress:          mod.Progress,
			StartTime:         mod.StartTime.UTC().Format("2006-01-02T15:04:05.000Z"),
		},
	}, nil
}

func (h *Handler) handleDescribeVolumeStatus(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VolumeId")
	items := h.Backend.DescribeVolumeStatus(ids)

	out := make([]volumeStatusItem, 0, len(items))
	for _, item := range items {
		out = append(out, volumeStatusItem{
			VolumeID:         item.VolumeID,
			AvailabilityZone: item.AvailabilityZone,
			VolumeStatus:     volumeStatusInfo{Status: item.VolumeStatus},
		})
	}

	return &describeVolumeStatusResponse{
		RequestID:       reqID,
		VolumeStatusSet: volumeStatusSet{Items: out},
	}, nil
}

func (h *Handler) handleDescribeVolumesModifications(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VolumeId")
	mods := h.Backend.DescribeVolumesModifications(ids)

	items := make([]volumeModificationItem, 0, len(mods))
	for _, mod := range mods {
		items = append(items, volumeModificationItem{
			VolumeID:          mod.VolumeID,
			ModificationState: mod.ModificationState,
			TargetVolumeType:  mod.TargetVolumeType,
			TargetSize:        mod.TargetSize,
			OrigVolumeType:    mod.OrigVolumeType,
			OrigSize:          mod.OrigSize,
			Progress:          mod.Progress,
			StartTime:         mod.StartTime.UTC().Format("2006-01-02T15:04:05.000Z"),
		})
	}

	resp := &describeVolumesModificationsResponse{RequestID: reqID}
	resp.VolumeModificationSet.Items = items

	return resp, nil
}

type volumeModificationItem struct {
	VolumeID          string `xml:"volumeId"`
	ModificationState string `xml:"modificationState"`
	TargetVolumeType  string `xml:"targetVolumeType"`
	OrigVolumeType    string `xml:"originalVolumeType"`
	StartTime         string `xml:"startTime"`
	Progress          int64  `xml:"progress"`
	TargetSize        int    `xml:"targetSize"`
	OrigSize          int    `xml:"originalSize"`
}

type ebsEncryptionByDefaultResponse struct {
	XMLName                xml.Name `xml:"GetEbsEncryptionByDefaultResponse"`
	RequestID              string   `xml:"requestId"`
	EbsEncryptionByDefault bool     `xml:"ebsEncryptionByDefault"`
}

type enableEbsEncryptionByDefaultResponse struct {
	XMLName                xml.Name `xml:"EnableEbsEncryptionByDefaultResponse"`
	RequestID              string   `xml:"requestId"`
	EbsEncryptionByDefault bool     `xml:"ebsEncryptionByDefault"`
}

type disableEbsEncryptionByDefaultResponse struct {
	XMLName                xml.Name `xml:"DisableEbsEncryptionByDefaultResponse"`
	RequestID              string   `xml:"requestId"`
	EbsEncryptionByDefault bool     `xml:"ebsEncryptionByDefault"`
}

type ebsDefaultKmsKeyResponse struct {
	XMLName   xml.Name `xml:"GetEbsDefaultKmsKeyIdResponse"`
	RequestID string   `xml:"requestId"`
	KmsKeyID  string   `xml:"kmsKeyId"`
}

type modifyEbsDefaultKmsKeyResponse struct {
	XMLName   xml.Name `xml:"ModifyEbsDefaultKmsKeyIdResponse"`
	RequestID string   `xml:"requestId"`
	KmsKeyID  string   `xml:"kmsKeyId"`
}

type snapshotLockItem struct {
	SnapshotID       string `xml:"snapshotId"`
	LockState        string `xml:"lockState"`
	LockCreatedOn    string `xml:"lockCreatedOn"`
	LockExpiresOn    string `xml:"lockExpiresOn,omitempty"`
	LockDurationDays int    `xml:"lockDurationDays,omitempty"`
}

type copyVolumesResponse struct {
	XMLName   xml.Name `xml:"CopyVolumesResponse"`
	RequestID string   `xml:"requestId"`
	VolumeSet struct {
		Items []copyVolumesVolumeItem `xml:"item"`
	} `xml:"volumeSet"`
}

type createReplaceRootVolumeTaskResponse struct {
	XMLName               xml.Name                  `xml:"CreateReplaceRootVolumeTaskResponse"`
	RequestID             string                    `xml:"requestId"`
	ReplaceRootVolumeTask replaceRootVolumeTaskItem `xml:"replaceRootVolumeTask"`
}

type describeReplaceRootVolumeTasksResponse struct {
	XMLName                  xml.Name `xml:"DescribeReplaceRootVolumeTasksResponse"`
	RequestID                string   `xml:"requestId"`
	ReplaceRootVolumeTaskSet struct {
		Items []replaceRootVolumeTaskItem `xml:"item"`
	} `xml:"replaceRootVolumeTaskSet"`
}

type addressTransferDetailItem struct {
	AllocationID        string `xml:"allocationId"`
	PublicIP            string `xml:"publicIp"`
	TransferAccountID   string `xml:"transferAccountId"`
	TransferOfferStatus string `xml:"transferOfferStatus"`
	TransferOfferExpiry string `xml:"transferOfferExpiry"`
}

func (h *Handler) handleEnableEbsEncryptionByDefault(_ url.Values, reqID string) (any, error) {
	h.Backend.EnableEbsEncryptionByDefault()

	return &enableEbsEncryptionByDefaultResponse{
		RequestID:              reqID,
		EbsEncryptionByDefault: true,
	}, nil
}

func (h *Handler) handleDisableEbsEncryptionByDefault(_ url.Values, reqID string) (any, error) {
	h.Backend.DisableEbsEncryptionByDefault()

	return &disableEbsEncryptionByDefaultResponse{
		RequestID:              reqID,
		EbsEncryptionByDefault: false,
	}, nil
}

func (h *Handler) handleGetEbsEncryptionByDefault(_ url.Values, reqID string) (any, error) {
	return &ebsEncryptionByDefaultResponse{
		XMLName:                xml.Name{Local: "GetEbsEncryptionByDefaultResponse"},
		RequestID:              reqID,
		EbsEncryptionByDefault: h.Backend.GetEbsEncryptionByDefault(),
	}, nil
}

func (h *Handler) handleGetEbsDefaultKmsKeyID(_ url.Values, reqID string) (any, error) {
	return &ebsDefaultKmsKeyResponse{
		RequestID: reqID,
		KmsKeyID:  h.Backend.GetEbsDefaultKmsKeyID(),
	}, nil
}

func (h *Handler) handleModifyEbsDefaultKmsKeyID(vals url.Values, reqID string) (any, error) {
	kmsKeyID := vals.Get("KmsKeyId")
	if err := h.Backend.ModifyEbsDefaultKmsKeyID(kmsKeyID); err != nil {
		return nil, err
	}

	return &modifyEbsDefaultKmsKeyResponse{RequestID: reqID, KmsKeyID: kmsKeyID}, nil
}

func (h *Handler) handleEnableVolumeIO(vals url.Values, reqID string) (any, error) {
	volumeID := vals.Get("VolumeId")
	if err := h.Backend.EnableVolumeIO(volumeID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableVolumeIOResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleCopyVolumes(vals url.Values, reqID string) (any, error) {
	volumeIDs := parseMemberList(vals, "VolumeId")
	destRegion := vals.Get("DestinationRegion")

	results, err := h.Backend.CopyVolumes(volumeIDs, destRegion)
	if err != nil {
		return nil, err
	}

	resp := &copyVolumesResponse{RequestID: reqID}
	for _, r := range results {
		resp.VolumeSet.Items = append(resp.VolumeSet.Items, copyVolumesVolumeItem(r))
	}

	return resp, nil
}

func (h *Handler) handleCreateReplaceRootVolumeTask(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("InstanceId")
	snapshotID := vals.Get("SnapshotId")

	task, err := h.Backend.CreateReplaceRootVolumeTask(instanceID, snapshotID)
	if err != nil {
		return nil, err
	}

	item := replaceRootVolumeTaskItem{
		ReplaceRootVolumeTaskID: task.ReplaceRootVolumeTaskID,
		InstanceID:              task.InstanceID,
		TaskState:               task.TaskState,
		StartTime:               task.StartTime.UTC().Format("2006-01-02T15:04:05.000Z"),
		SnapshotID:              task.SnapshotID,
	}
	if !task.CompleteTime.IsZero() {
		item.CompleteTime = task.CompleteTime.UTC().Format("2006-01-02T15:04:05.000Z")
	}

	return &createReplaceRootVolumeTaskResponse{RequestID: reqID, ReplaceRootVolumeTask: item}, nil
}

func (h *Handler) handleDescribeReplaceRootVolumeTasks(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ReplaceRootVolumeTaskId")
	tasks := h.Backend.DescribeReplaceRootVolumeTasks(ids)

	resp := &describeReplaceRootVolumeTasksResponse{RequestID: reqID}
	for _, task := range tasks {
		item := replaceRootVolumeTaskItem{
			ReplaceRootVolumeTaskID: task.ReplaceRootVolumeTaskID,
			InstanceID:              task.InstanceID,
			TaskState:               task.TaskState,
			StartTime:               task.StartTime.UTC().Format("2006-01-02T15:04:05.000Z"),
			SnapshotID:              task.SnapshotID,
		}
		if !task.CompleteTime.IsZero() {
			item.CompleteTime = task.CompleteTime.UTC().Format("2006-01-02T15:04:05.000Z")
		}
		resp.ReplaceRootVolumeTaskSet.Items = append(resp.ReplaceRootVolumeTaskSet.Items, item)
	}

	return resp, nil
}

type listVolumesInRecycleBinResponse struct {
	XMLName   xml.Name `xml:"ListVolumesInRecycleBinResponse"`
	RequestID string   `xml:"requestId"`
	VolumeSet struct {
		Items []recycleBinVolumeItem `xml:"item"`
	} `xml:"volumeSet"`
}

func (h *Handler) handleResetEbsDefaultKmsKeyID(_ url.Values, reqID string) (any, error) {
	h.Backend.ResetEbsDefaultKmsKeyID()

	return &stubResponse{
		XMLName:   xml.Name{Local: "ResetEbsDefaultKmsKeyIdResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleListVolumesInRecycleBin(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VolumeId")
	vols := h.Backend.ListVolumesInRecycleBin(ids)

	resp := &listVolumesInRecycleBinResponse{RequestID: reqID}
	for _, v := range vols {
		resp.VolumeSet.Items = append(
			resp.VolumeSet.Items,
			recycleBinVolumeItem{VolumeID: v.VolumeID},
		)
	}

	return resp, nil
}

func (h *Handler) handleRestoreVolumeFromRecycleBin(vals url.Values, reqID string) (any, error) {
	volumeID := vals.Get("VolumeId")
	if err := h.Backend.RestoreVolumeFromRecycleBin(volumeID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "RestoreVolumeFromRecycleBinResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

// registerVolumesOps registers the Volumes operation handlers.
func registerVolumesOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["ModifyVolume"] = h.handleModifyVolume
	ops["DescribeVolumeStatus"] = h.handleDescribeVolumeStatus
	ops["DescribeVolumesModifications"] = h.handleDescribeVolumesModifications
	ops["EnableEbsEncryptionByDefault"] = h.handleEnableEbsEncryptionByDefault
	ops["DisableEbsEncryptionByDefault"] = h.handleDisableEbsEncryptionByDefault
	ops["GetEbsEncryptionByDefault"] = h.handleGetEbsEncryptionByDefault
	ops["GetEbsDefaultKmsKeyId"] = h.handleGetEbsDefaultKmsKeyID
	ops["ModifyEbsDefaultKmsKeyId"] = h.handleModifyEbsDefaultKmsKeyID
	ops["EnableVolumeIO"] = h.handleEnableVolumeIO
	ops["CopyVolumes"] = h.handleCopyVolumes
	ops["CreateReplaceRootVolumeTask"] = h.handleCreateReplaceRootVolumeTask
	ops["DescribeReplaceRootVolumeTasks"] = h.handleDescribeReplaceRootVolumeTasks
	ops["ResetEbsDefaultKmsKeyId"] = h.handleResetEbsDefaultKmsKeyID
	ops["ListVolumesInRecycleBin"] = h.handleListVolumesInRecycleBin
	ops["RestoreVolumeFromRecycleBin"] = h.handleRestoreVolumeFromRecycleBin
}

// volumesSupportedOperations lists the operation names registered by
// registerVolumesOps, for GetSupportedOperations().
func volumesSupportedOperations() []string {
	return []string{
		"ModifyVolume",
		"DescribeVolumeStatus",
		"DescribeVolumesModifications",
		"EnableEbsEncryptionByDefault",
		"DisableEbsEncryptionByDefault",
		"GetEbsEncryptionByDefault",
		"GetEbsDefaultKmsKeyId",
		"ModifyEbsDefaultKmsKeyId",
		"EnableVolumeIO",
		"CopyVolumes",
		"CreateReplaceRootVolumeTask",
		"DescribeReplaceRootVolumeTasks",
		"ResetEbsDefaultKmsKeyId",
		"ListVolumesInRecycleBin",
		"RestoreVolumeFromRecycleBin",
	}
}

const (
	// gp2 IOPS scaling: 3 IOPS per GB, min 100, max 16 000.
	gp2IOPSPerGB = 3
	gp2IOPSMin   = 100
	gp2IOPSMax   = 16000

	// gp3 defaults per AWS documentation.
	gp3DefaultIOPS       = 3000
	gp3DefaultThroughput = 125

	// gp3 provisioning bounds and coupling ratios per AWS documentation.
	// IOPS must be 3 000–16 000; throughput 125–1 000 MiB/s.
	gp3MinIOPS       = 3000
	gp3MaxIOPS       = 16000
	gp3MinThroughput = 125
	gp3MaxThroughput = 1000
	// gp3MaxIOPSPerGiB caps provisioned IOPS above the free 3 000 baseline at
	// 500 IOPS per GiB of volume size.
	gp3MaxIOPSPerGiB = 500
	// gp3IOPSPerThroughput encodes the max throughput-to-IOPS ratio of 0.25
	// MiB/s per provisioned IOPS (i.e. throughput * 4 must not exceed IOPS).
	gp3IOPSPerThroughput = 4
)

type volumeItem struct {
	Attachment *attachmentItem `xml:"attachmentSet>item,omitempty"`
	VolumeID   string          `xml:"volumeId"`
	AZ         string          `xml:"availabilityZone"`
	VolumeType string          `xml:"volumeType"`
	State      string          `xml:"status"`
	CreateTime string          `xml:"createTime"`
	KmsKeyID   string          `xml:"kmsKeyId,omitempty"`
	SnapshotID string          `xml:"snapshotId,omitempty"`
	Size       int             `xml:"size"`
	Iops       int             `xml:"iops,omitempty"`
	Throughput int             `xml:"throughput,omitempty"`
	Encrypted  bool            `xml:"encrypted"`
}

type attachmentItem struct {
	VolumeID   string `xml:"volumeId"`
	InstanceID string `xml:"instanceId"`
	Device     string `xml:"device"`
	State      string `xml:"status"`
	AttachTime string `xml:"attachTime"`
}

type volumeItemSet struct {
	Items []volumeItem `xml:"item"`
}

type describeVolumesResponse struct {
	XMLName   xml.Name      `xml:"DescribeVolumesResponse"`
	Xmlns     string        `xml:"xmlns,attr"`
	RequestID string        `xml:"requestId"`
	VolumeSet volumeItemSet `xml:"volumeSet"`
}

type createVolumeResponse struct {
	XMLName    xml.Name `xml:"CreateVolumeResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	RequestID  string   `xml:"requestId"`
	VolumeID   string   `xml:"volumeId"`
	AZ         string   `xml:"availabilityZone"`
	VolumeType string   `xml:"volumeType"`
	State      string   `xml:"status"`
	CreateTime string   `xml:"createTime"`
	KmsKeyID   string   `xml:"kmsKeyId,omitempty"`
	SnapshotID string   `xml:"snapshotId,omitempty"`
	Size       int      `xml:"size"`
	Iops       int      `xml:"iops,omitempty"`
	Throughput int      `xml:"throughput,omitempty"`
	Encrypted  bool     `xml:"encrypted"`
}

type deleteVolumeResponse struct {
	XMLName   xml.Name `xml:"DeleteVolumeResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type attachVolumeResponse struct {
	XMLName    xml.Name `xml:"AttachVolumeResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	RequestID  string   `xml:"requestId"`
	VolumeID   string   `xml:"volumeId"`
	InstanceID string   `xml:"instanceId"`
	Device     string   `xml:"device"`
	State      string   `xml:"status"`
	AttachTime string   `xml:"attachTime"`
}

type detachVolumeResponse struct {
	XMLName    xml.Name `xml:"DetachVolumeResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	RequestID  string   `xml:"requestId"`
	VolumeID   string   `xml:"volumeId"`
	InstanceID string   `xml:"instanceId"`
	Device     string   `xml:"device"`
	State      string   `xml:"status"`
}

func toVolumeItem(vol *Volume) volumeItem {
	item := volumeItem{
		VolumeID:   vol.ID,
		Size:       vol.Size,
		AZ:         vol.AZ,
		VolumeType: vol.VolumeType,
		State:      vol.State,
		CreateTime: vol.CreateTime.UTC().Format("2006-01-02T15:04:05.000Z"),
		Encrypted:  vol.Encrypted,
		KmsKeyID:   vol.KmsKeyID,
		SnapshotID: vol.SnapshotID,
		Iops:       vol.Iops,
		Throughput: vol.Throughput,
	}

	if vol.Attachment != nil {
		item.Attachment = &attachmentItem{
			VolumeID:   vol.Attachment.VolumeID,
			InstanceID: vol.Attachment.InstanceID,
			Device:     vol.Attachment.Device,
			State:      vol.Attachment.State,
			AttachTime: vol.Attachment.AttachTime.UTC().Format("2006-01-02T15:04:05.000Z"),
		}
	}

	return item
}

// parsePositiveInt parses s as a positive integer; returns an error wrapping
// ErrInvalidParameter if the string is present but invalid or non-positive.
func parsePositiveInt(s, field string) (int, error) {
	if s == "" {
		return 0, nil
	}

	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("%w: invalid %s value: %s", ErrInvalidParameter, field, s)
	}

	return v, nil
}

// defaultIOPSForType returns the IOPS to use when the caller did not specify,
// based on volume type and size. Returns 0 for types without an IOPS concept.
func defaultIOPSForType(volType string, size int) int {
	switch volType {
	case volTypeGP3:
		return gp3DefaultIOPS
	case volTypeDefaultGP2:
		effectiveSize := size
		if effectiveSize <= 0 {
			effectiveSize = 8
		}

		return max(gp2IOPSMin, min(effectiveSize*gp2IOPSPerGB, gp2IOPSMax))
	}

	return 0
}

// parseVolumePerf parses and validates the Iops and Throughput form fields,
// enforcing AWS rules: io1/io2 require Iops; gp3/gp2 get type-based defaults.
func parseVolumePerf(iopsStr, throughputStr, volType string, size int) (int, int, error) {
	iops, err := parsePositiveInt(iopsStr, "Iops")
	if err != nil {
		return 0, 0, err
	}

	throughput, err := parsePositiveInt(throughputStr, "Throughput")
	if err != nil {
		return 0, 0, err
	}

	effectiveVolType := volType
	if effectiveVolType == "" {
		effectiveVolType = volTypeDefaultGP2
	}

	// io1 and io2 require an explicit Iops value.
	if (effectiveVolType == "io1" || effectiveVolType == "io2") && iops == 0 {
		return 0, 0, fmt.Errorf("%w: The parameter Iops is not optional for volume type %s",
			ErrInvalidParameter, effectiveVolType)
	}

	if iops == 0 {
		iops = defaultIOPSForType(effectiveVolType, size)
	}

	if throughput == 0 && effectiveVolType == volTypeGP3 {
		throughput = gp3DefaultThroughput
	}

	if effectiveVolType == volTypeGP3 {
		if verr := validateGP3Coupling(iops, throughput, size); verr != nil {
			return 0, 0, verr
		}
	}

	return iops, throughput, nil
}

// validateGP3Coupling enforces the AWS gp3 iops/throughput coupling rules on a
// volume create: IOPS in [3000,16000], throughput in [125,1000] MiB/s, IOPS
// above the free 3000 baseline capped at 500 IOPS/GiB, and throughput capped at
// 0.25 MiB/s per provisioned IOPS. size <= 0 (unspecified) skips the size-ratio
// check because CreateVolume applies a default size downstream.
func validateGP3Coupling(iops, throughput, size int) error {
	if iops < gp3MinIOPS || iops > gp3MaxIOPS {
		return fmt.Errorf(
			"%w: Iops must be between %d and %d for gp3 volumes",
			ErrInvalidParameter, gp3MinIOPS, gp3MaxIOPS,
		)
	}

	if throughput < gp3MinThroughput || throughput > gp3MaxThroughput {
		return fmt.Errorf(
			"%w: Throughput must be between %d and %d MiB/s for gp3 volumes",
			ErrInvalidParameter, gp3MinThroughput, gp3MaxThroughput,
		)
	}

	// IOPS above the free 3000 baseline may not exceed 500 IOPS per GiB.
	if size > 0 && iops > gp3MinIOPS && iops > size*gp3MaxIOPSPerGiB {
		return fmt.Errorf(
			"%w: Iops of %d exceeds the maximum ratio of %d IOPS per GiB for a %d GiB gp3 volume",
			ErrInvalidParameter, iops, gp3MaxIOPSPerGiB, size,
		)
	}

	// Throughput may not exceed 0.25 MiB/s per provisioned IOPS.
	if throughput*gp3IOPSPerThroughput > iops {
		return fmt.Errorf(
			"%w: Throughput of %d MiB/s exceeds the maximum ratio of 0.25 MiB/s per provisioned IOPS (Iops=%d)",
			ErrInvalidParameter, throughput, iops,
		)
	}

	return nil
}

func (h *Handler) handleCreateVolume(vals url.Values, reqID string) (any, error) {
	az := vals.Get("AvailabilityZone")
	volType := vals.Get("VolumeType")
	sizeStr := vals.Get("Size")
	encryptedStr := vals.Get("Encrypted")
	kmsKeyID := vals.Get("KmsKeyID")
	snapshotID := vals.Get("SnapshotId")

	size := 0
	if sizeStr != "" {
		// If parsing fails, size defaults to 0 and CreateVolume will use the default size.
		_, _ = fmt.Sscan(sizeStr, &size)
	}

	iops, throughput, err := parseVolumePerf(
		vals.Get("Iops"),
		vals.Get("Throughput"),
		volType,
		size,
	)
	if err != nil {
		return nil, err
	}

	vol, err := h.Backend.CreateVolume(az, volType, size, snapshotID)
	if err != nil {
		return nil, err
	}

	// Apply encryption if requested (gap 13).
	if encryptedStr == ec2BooleanTrue {
		if encErr := h.Backend.SetVolumeEncryption(vol.ID, true, kmsKeyID); encErr != nil {
			return nil, encErr
		}

		vol.Encrypted = true
		vol.KmsKeyID = kmsKeyID
		if vol.KmsKeyID == "" {
			vol.KmsKeyID = "alias/aws/ebs"
		}
	}

	// Apply IOPS and throughput.
	if iops > 0 || throughput > 0 {
		if perfErr := h.Backend.SetVolumePerformance(vol.ID, iops, throughput); perfErr != nil {
			return nil, perfErr
		}

		vol.Iops = iops
		vol.Throughput = throughput
	}

	return &createVolumeResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		VolumeID:   vol.ID,
		Size:       vol.Size,
		AZ:         vol.AZ,
		VolumeType: vol.VolumeType,
		State:      vol.State,
		CreateTime: vol.CreateTime.UTC().Format("2006-01-02T15:04:05.000Z"),
		Encrypted:  vol.Encrypted,
		KmsKeyID:   vol.KmsKeyID,
		SnapshotID: vol.SnapshotID,
		Iops:       vol.Iops,
		Throughput: vol.Throughput,
	}, nil
}

func (h *Handler) handleDescribeVolumes(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VolumeId")
	vols := h.Backend.DescribeVolumes(ids)

	filters := parseEC2Filters(vals)
	vols = applyVolumeFilters(vols, filters, h.Backend)

	items := make([]volumeItem, 0, len(vols))
	for _, vol := range vols {
		items = append(items, toVolumeItem(vol))
	}

	return &describeVolumesResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		VolumeSet: volumeItemSet{Items: items},
	}, nil
}

func (h *Handler) handleDeleteVolume(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VolumeId")
	if id == "" {
		return nil, fmt.Errorf("%w: VolumeId is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteVolume(id); err != nil {
		return nil, err
	}

	return &deleteVolumeResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleAttachVolume(vals url.Values, reqID string) (any, error) {
	volumeID := vals.Get("VolumeId")
	instanceID := vals.Get("InstanceId")
	device := vals.Get("Device")

	if volumeID == "" || instanceID == "" {
		return nil, fmt.Errorf("%w: VolumeId and InstanceId are required", ErrInvalidParameter)
	}

	att, err := h.Backend.AttachVolume(volumeID, instanceID, device)
	if err != nil {
		return nil, err
	}

	return &attachVolumeResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		VolumeID:   att.VolumeID,
		InstanceID: att.InstanceID,
		Device:     att.Device,
		State:      att.State,
		AttachTime: att.AttachTime.UTC().Format("2006-01-02T15:04:05.000Z"),
	}, nil
}

func (h *Handler) handleDetachVolume(vals url.Values, reqID string) (any, error) {
	volumeID := vals.Get("VolumeId")
	if volumeID == "" {
		return nil, fmt.Errorf("%w: VolumeId is required", ErrInvalidParameter)
	}

	forceStr := vals.Get("Force")
	force, _ := strconv.ParseBool(forceStr)

	att, err := h.Backend.DetachVolume(volumeID, force)
	if err != nil {
		return nil, err
	}

	return &detachVolumeResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		VolumeID:   att.VolumeID,
		InstanceID: att.InstanceID,
		Device:     att.Device,
		State:      att.State,
	}, nil
}

type describeVolumeAttributeResponse struct {
	XMLName   xml.Name `xml:"DescribeVolumeAttributeResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	VolumeID  string   `xml:"volumeId"`
	Attribute namedBoolAttr
}

type modifyVolumeAttributeResponse struct {
	XMLName   xml.Name `xml:"ModifyVolumeAttributeResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func (h *Handler) handleDescribeVolumeAttribute(vals url.Values, reqID string) (any, error) {
	volumeID := vals.Get("VolumeId")
	if volumeID == "" {
		return nil, fmt.Errorf("%w: VolumeId is required", ErrInvalidParameter)
	}

	attr := vals.Get("Attribute")
	if attr == "" {
		return nil, fmt.Errorf("%w: Attribute is required", ErrInvalidParameter)
	}

	return &describeVolumeAttributeResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		VolumeID:  volumeID,
		Attribute: namedBoolAttr{XMLName: xml.Name{Local: attr}, Value: ec2BooleanFalse},
	}, nil
}

// handleModifyVolumeAttribute is a stub that accepts any attribute modification and returns success.
func (h *Handler) handleModifyVolumeAttribute(vals url.Values, reqID string) (any, error) {
	if vals.Get("VolumeId") == "" {
		return nil, fmt.Errorf("%w: VolumeId is required", ErrInvalidParameter)
	}

	return &modifyVolumeAttributeResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

// handleDescribeSnapshotAttribute returns stub snapshot attribute data.
