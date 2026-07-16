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
			StartTime:         mod.StartTime.Format("2006-01-02T15:04:05.000Z"),
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
			StartTime:         mod.StartTime.Format("2006-01-02T15:04:05.000Z"),
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
		StartTime:               task.StartTime.Format("2006-01-02T15:04:05.000Z"),
		SnapshotID:              task.SnapshotID,
	}
	if !task.CompleteTime.IsZero() {
		item.CompleteTime = task.CompleteTime.Format("2006-01-02T15:04:05.000Z")
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
			StartTime:               task.StartTime.Format("2006-01-02T15:04:05.000Z"),
			SnapshotID:              task.SnapshotID,
		}
		if !task.CompleteTime.IsZero() {
			item.CompleteTime = task.CompleteTime.Format("2006-01-02T15:04:05.000Z")
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
