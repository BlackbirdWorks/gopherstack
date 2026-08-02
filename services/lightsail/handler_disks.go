package lightsail

import "context"

// diskOps returns the dispatch table for family I+J (12 ops).
func (h *Handler) diskOps() map[string]opFunc {
	return map[string]opFunc{
		"AttachDisk":             h.handleAttachDisk,
		"DetachDisk":             h.handleDetachDisk,
		"CreateDisk":             h.handleCreateDisk,
		"CreateDiskFromSnapshot": h.handleCreateDiskFromSnapshot,
		"DeleteDisk":             h.handleDeleteDisk,
		"GetDisk":                h.handleGetDisk,
		"GetDisks":               h.handleGetDisks,
		"CreateDiskSnapshot":     h.handleCreateDiskSnapshot,
		"DeleteDiskSnapshot":     h.handleDeleteDiskSnapshot,
		"GetDiskSnapshot":        h.handleGetDiskSnapshot,
		"GetDiskSnapshots":       h.handleGetDiskSnapshots,
		"CopySnapshot":           h.handleCopySnapshot,
	}
}

type diskWire struct {
	CreatedAt       *float64              `json:"createdAt,omitempty"`
	Location        *resourceLocationWire `json:"location,omitempty"`
	Path            string                `json:"path,omitempty"`
	ResourceType    string                `json:"resourceType,omitempty"`
	AutoMountStatus string                `json:"autoMountStatus,omitempty"`
	AttachedTo      string                `json:"attachedTo,omitempty"`
	AttachmentState string                `json:"attachmentState,omitempty"`
	SupportCode     string                `json:"supportCode,omitempty"`
	State           string                `json:"state,omitempty"`
	Arn             string                `json:"arn,omitempty"`
	Name            string                `json:"name,omitempty"`
	Tags            []tagWire             `json:"tags,omitempty"`
	AddOns          []addOnWire           `json:"addOns,omitempty"`
	GbInUse         int32                 `json:"gbInUse,omitempty"`
	SizeInGb        int32                 `json:"sizeInGb,omitempty"`
	Iops            int32                 `json:"iops,omitempty"`
	IsSystemDisk    bool                  `json:"isSystemDisk,omitempty"`
	IsAttached      bool                  `json:"isAttached,omitempty"`
}

func diskToWire(d *Disk) diskWire {
	return diskWire{
		AddOns: addOnsToWire(d.AddOns), Arn: d.Arn, AttachedTo: d.AttachedTo, AttachmentState: d.AttachmentState,
		AutoMountStatus: d.AutoMountStatus, CreatedAt: epochPtr(d.CreatedAt), GbInUse: d.GbInUse, Iops: d.Iops,
		IsAttached: d.IsAttached, IsSystemDisk: d.IsSystemDisk, Location: locationToWire(d.Location),
		Name: d.Name, Path: d.Path, ResourceType: ResourceTypeDisk, SizeInGb: d.SizeInGb, State: d.State,
		SupportCode: d.SupportCode, Tags: mapFromTags(d.Tags),
	}
}

type createDiskRequest struct {
	AvailabilityZone string             `json:"availabilityZone,omitempty"`
	DiskName         string             `json:"diskName"`
	AddOns           []addOnRequestWire `json:"addOns,omitempty"`
	Tags             []tagWire          `json:"tags,omitempty"`
	SizeInGb         int32              `json:"sizeInGb"`
}

func (h *Handler) handleCreateDisk(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createDiskRequest](body)
	if err != nil {
		return nil, err
	}

	ops, createErr := h.Backend.CreateDisk(
		req.DiskName, req.AvailabilityZone, req.SizeInGb, addOnRequestsFromWire(req.AddOns), tagsFromWire(req.Tags),
	)
	if createErr != nil {
		return nil, createErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type createDiskFromSnapshotRequest struct {
	AvailabilityZone                string             `json:"availabilityZone,omitempty"`
	DiskName                        string             `json:"diskName"`
	DiskSnapshotName                string             `json:"diskSnapshotName,omitempty"`
	RestoreDate                     string             `json:"restoreDate,omitempty"`
	SourceDiskName                  string             `json:"sourceDiskName,omitempty"`
	AddOns                          []addOnRequestWire `json:"addOns,omitempty"`
	Tags                            []tagWire          `json:"tags,omitempty"`
	SizeInGb                        int32              `json:"sizeInGb"`
	UseLatestRestorableAutoSnapshot bool               `json:"useLatestRestorableAutoSnapshot,omitempty"`
}

func (h *Handler) handleCreateDiskFromSnapshot(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createDiskFromSnapshotRequest](body)
	if err != nil {
		return nil, err
	}

	ops, createErr := h.Backend.CreateDiskFromSnapshot(
		req.DiskName, req.AvailabilityZone, req.DiskSnapshotName, req.SizeInGb,
		addOnRequestsFromWire(req.AddOns), tagsFromWire(req.Tags),
	)
	if createErr != nil {
		return nil, createErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type attachDiskRequest struct {
	DiskName     string `json:"diskName"`
	DiskPath     string `json:"diskPath"`
	InstanceName string `json:"instanceName"`
	AutoMounting bool   `json:"autoMounting,omitempty"`
}

func (h *Handler) handleAttachDisk(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[attachDiskRequest](body)
	if err != nil {
		return nil, err
	}

	op, attachErr := h.Backend.AttachDisk(req.DiskName, req.InstanceName, req.DiskPath, req.AutoMounting)
	if attachErr != nil {
		return nil, attachErr
	}

	return marshalResponse(opsEnvelope([]Operation{*op}))
}

type diskNameRequest struct {
	DiskName string `json:"diskName"`
}

func (h *Handler) handleDetachDisk(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[diskNameRequest](body)
	if err != nil {
		return nil, err
	}

	op, detachErr := h.Backend.DetachDisk(req.DiskName)
	if detachErr != nil {
		return nil, detachErr
	}

	return marshalResponse(opsEnvelope([]Operation{*op}))
}

type deleteDiskRequest struct {
	DiskName          string `json:"diskName"`
	ForceDeleteAddOns bool   `json:"forceDeleteAddOns,omitempty"`
}

func (h *Handler) handleDeleteDisk(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[deleteDiskRequest](body)
	if err != nil {
		return nil, err
	}

	ops, delErr := h.Backend.DeleteDisk(req.DiskName)
	if delErr != nil {
		return nil, delErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type diskEnvelope struct {
	Disk *diskWire `json:"disk,omitempty"`
}

func (h *Handler) handleGetDisk(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[diskNameRequest](body)
	if err != nil {
		return nil, err
	}

	d, getErr := h.Backend.GetDisk(req.DiskName)
	if getErr != nil {
		return nil, getErr
	}

	w := diskToWire(d)

	return marshalResponse(diskEnvelope{Disk: &w})
}

type disksListResponse struct {
	NextPageToken string     `json:"nextPageToken,omitempty"`
	Disks         []diskWire `json:"disks,omitempty"`
}

func (h *Handler) handleGetDisks(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[pageTokenRequest](body)
	if err != nil {
		return nil, err
	}

	pg, pgErr := h.Backend.GetDisks(req.PageToken)
	if pgErr != nil {
		return nil, pgErr
	}

	out := make([]diskWire, len(pg.Data))
	for i, d := range pg.Data {
		out[i] = diskToWire(d)
	}

	return marshalResponse(disksListResponse{Disks: out, NextPageToken: pg.Next})
}

type diskSnapshotWire struct {
	Location           *resourceLocationWire `json:"location,omitempty"`
	CreatedAt          *float64              `json:"createdAt,omitempty"`
	Progress           string                `json:"progress,omitempty"`
	FromDiskName       string                `json:"fromDiskName,omitempty"`
	FromInstanceArn    string                `json:"fromInstanceArn,omitempty"`
	FromInstanceName   string                `json:"fromInstanceName,omitempty"`
	FromDiskArn        string                `json:"fromDiskArn,omitempty"`
	Name               string                `json:"name,omitempty"`
	Arn                string                `json:"arn,omitempty"`
	ResourceType       string                `json:"resourceType,omitempty"`
	State              string                `json:"state,omitempty"`
	SupportCode        string                `json:"supportCode,omitempty"`
	Tags               []tagWire             `json:"tags,omitempty"`
	SizeInGb           int32                 `json:"sizeInGb,omitempty"`
	IsFromAutoSnapshot bool                  `json:"isFromAutoSnapshot,omitempty"`
}

func diskSnapshotToWire(s *DiskSnapshot) diskSnapshotWire {
	return diskSnapshotWire{
		Arn:                s.Arn,
		CreatedAt:          epochPtr(s.CreatedAt),
		FromDiskArn:        s.FromDiskArn,
		FromDiskName:       s.FromDiskName,
		FromInstanceArn:    s.FromInstanceArn,
		FromInstanceName:   s.FromInstanceName,
		IsFromAutoSnapshot: s.IsFromAutoSnapshot,
		Location: locationToWire(
			s.Location,
		),
		Name:         s.Name,
		Progress:     s.Progress,
		ResourceType: ResourceTypeDiskSnapshot,
		SizeInGb:     s.SizeInGb,
		State:        s.State,
		SupportCode:  s.SupportCode,
		Tags:         mapFromTags(s.Tags),
	}
}

type createDiskSnapshotRequest struct {
	DiskName         string    `json:"diskName,omitempty"`
	DiskSnapshotName string    `json:"diskSnapshotName"`
	InstanceName     string    `json:"instanceName,omitempty"`
	Tags             []tagWire `json:"tags,omitempty"`
}

func (h *Handler) handleCreateDiskSnapshot(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createDiskSnapshotRequest](body)
	if err != nil {
		return nil, err
	}

	ops, createErr := h.Backend.CreateDiskSnapshot(
		req.DiskName,
		req.InstanceName,
		req.DiskSnapshotName,
		tagsFromWire(req.Tags),
	)
	if createErr != nil {
		return nil, createErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type diskSnapshotNameRequest struct {
	DiskSnapshotName string `json:"diskSnapshotName"`
}

func (h *Handler) handleDeleteDiskSnapshot(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[diskSnapshotNameRequest](body)
	if err != nil {
		return nil, err
	}

	ops, delErr := h.Backend.DeleteDiskSnapshot(req.DiskSnapshotName)
	if delErr != nil {
		return nil, delErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type diskSnapshotEnvelope struct {
	DiskSnapshot *diskSnapshotWire `json:"diskSnapshot,omitempty"`
}

func (h *Handler) handleGetDiskSnapshot(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[diskSnapshotNameRequest](body)
	if err != nil {
		return nil, err
	}

	snap, getErr := h.Backend.GetDiskSnapshot(req.DiskSnapshotName)
	if getErr != nil {
		return nil, getErr
	}

	w := diskSnapshotToWire(snap)

	return marshalResponse(diskSnapshotEnvelope{DiskSnapshot: &w})
}

type diskSnapshotsListResponse struct {
	NextPageToken string             `json:"nextPageToken,omitempty"`
	DiskSnapshots []diskSnapshotWire `json:"diskSnapshots,omitempty"`
}

func (h *Handler) handleGetDiskSnapshots(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[pageTokenRequest](body)
	if err != nil {
		return nil, err
	}

	pg, pgErr := h.Backend.GetDiskSnapshots(req.PageToken)
	if pgErr != nil {
		return nil, pgErr
	}

	out := make([]diskSnapshotWire, len(pg.Data))
	for i, s := range pg.Data {
		out[i] = diskSnapshotToWire(s)
	}

	return marshalResponse(diskSnapshotsListResponse{DiskSnapshots: out, NextPageToken: pg.Next})
}

type copySnapshotRequest struct {
	RestoreDate                     string `json:"restoreDate,omitempty"`
	SourceRegion                    string `json:"sourceRegion,omitempty"`
	SourceResourceName              string `json:"sourceResourceName,omitempty"`
	SourceSnapshotName              string `json:"sourceSnapshotName,omitempty"`
	TargetSnapshotName              string `json:"targetSnapshotName"`
	UseLatestRestorableAutoSnapshot bool   `json:"useLatestRestorableAutoSnapshot,omitempty"`
}

func (h *Handler) handleCopySnapshot(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[copySnapshotRequest](body)
	if err != nil {
		return nil, err
	}

	source := req.SourceSnapshotName
	if source == "" {
		source = req.SourceResourceName
	}

	ops, copyErr := h.Backend.CopySnapshot(req.SourceRegion, source, req.TargetSnapshotName)
	if copyErr != nil {
		return nil, copyErr
	}

	return marshalResponse(opsEnvelope(ops))
}
