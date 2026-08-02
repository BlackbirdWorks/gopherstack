package lightsail

import "context"

// instanceExtrasOps returns the dispatch table for family D+E+F (10 ops).
func (h *Handler) instanceExtrasOps() map[string]opFunc {
	return map[string]opFunc{
		"CreateInstanceSnapshot":        h.handleCreateInstanceSnapshot,
		"DeleteInstanceSnapshot":        h.handleDeleteInstanceSnapshot,
		"GetInstanceSnapshot":           h.handleGetInstanceSnapshot,
		"GetInstanceSnapshots":          h.handleGetInstanceSnapshots,
		"GetInstanceMetricData":         h.handleGetInstanceMetricData,
		"UpdateInstanceMetadataOptions": h.handleUpdateInstanceMetadataOptions,
		"GetAutoSnapshots":              h.handleGetAutoSnapshots,
		"DeleteAutoSnapshot":            h.handleDeleteAutoSnapshot,
		"EnableAddOn":                   h.handleEnableAddOn,
		"DisableAddOn":                  h.handleDisableAddOn,
	}
}

type attachedDiskWire struct {
	Path     string `json:"path,omitempty"`
	SizeInGb int32  `json:"sizeInGb,omitempty"`
}

type instanceSnapshotWire struct {
	Location           *resourceLocationWire `json:"location,omitempty"`
	CreatedAt          *float64              `json:"createdAt,omitempty"`
	FromBundleID       string                `json:"fromBundleId,omitempty"`
	Name               string                `json:"name,omitempty"`
	Arn                string                `json:"arn,omitempty"`
	FromInstanceArn    string                `json:"fromInstanceArn,omitempty"`
	FromInstanceName   string                `json:"fromInstanceName,omitempty"`
	SupportCode        string                `json:"supportCode,omitempty"`
	State              string                `json:"state,omitempty"`
	FromBlueprintID    string                `json:"fromBlueprintId,omitempty"`
	Progress           string                `json:"progress,omitempty"`
	ResourceType       string                `json:"resourceType,omitempty"`
	FromAttachedDisks  []attachedDiskWire    `json:"fromAttachedDisks,omitempty"`
	Tags               []tagWire             `json:"tags,omitempty"`
	SizeInGb           int32                 `json:"sizeInGb,omitempty"`
	IsFromAutoSnapshot bool                  `json:"isFromAutoSnapshot,omitempty"`
}

func instanceSnapshotToWire(s *InstanceSnapshot) instanceSnapshotWire {
	disks := make([]attachedDiskWire, len(s.FromAttachedDisks))
	for i, d := range s.FromAttachedDisks {
		disks[i] = attachedDiskWire(d)
	}

	return instanceSnapshotWire{
		Arn: s.Arn, CreatedAt: epochPtr(s.CreatedAt), FromAttachedDisks: disks,
		FromBlueprintID: s.FromBlueprintID, FromBundleID: s.FromBundleID, FromInstanceArn: s.FromInstanceArn,
		FromInstanceName: s.FromInstanceName, IsFromAutoSnapshot: s.IsFromAutoSnapshot,
		Location: locationToWire(
			s.Location,
		), Name: s.Name, Progress: s.Progress, ResourceType: ResourceTypeInstanceSnapshot,
		SizeInGb: s.SizeInGb, State: s.State, SupportCode: s.SupportCode, Tags: mapFromTags(s.Tags),
	}
}

type createInstanceSnapshotRequest struct {
	InstanceName         string    `json:"instanceName"`
	InstanceSnapshotName string    `json:"instanceSnapshotName"`
	Tags                 []tagWire `json:"tags,omitempty"`
}

func (h *Handler) handleCreateInstanceSnapshot(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createInstanceSnapshotRequest](body)
	if err != nil {
		return nil, err
	}

	ops, createErr := h.Backend.CreateInstanceSnapshot(
		req.InstanceName,
		req.InstanceSnapshotName,
		tagsFromWire(req.Tags),
	)
	if createErr != nil {
		return nil, createErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type instanceSnapshotNameRequest struct {
	InstanceSnapshotName string `json:"instanceSnapshotName"`
}

func (h *Handler) handleDeleteInstanceSnapshot(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[instanceSnapshotNameRequest](body)
	if err != nil {
		return nil, err
	}

	ops, delErr := h.Backend.DeleteInstanceSnapshot(req.InstanceSnapshotName)
	if delErr != nil {
		return nil, delErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type instanceSnapshotEnvelope struct {
	InstanceSnapshot *instanceSnapshotWire `json:"instanceSnapshot,omitempty"`
}

func (h *Handler) handleGetInstanceSnapshot(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[instanceSnapshotNameRequest](body)
	if err != nil {
		return nil, err
	}

	snap, getErr := h.Backend.GetInstanceSnapshot(req.InstanceSnapshotName)
	if getErr != nil {
		return nil, getErr
	}

	w := instanceSnapshotToWire(snap)

	return marshalResponse(instanceSnapshotEnvelope{InstanceSnapshot: &w})
}

type instanceSnapshotsListResponse struct {
	NextPageToken     string                 `json:"nextPageToken,omitempty"`
	InstanceSnapshots []instanceSnapshotWire `json:"instanceSnapshots,omitempty"`
}

func (h *Handler) handleGetInstanceSnapshots(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[pageTokenRequest](body)
	if err != nil {
		return nil, err
	}

	pg, pgErr := h.Backend.GetInstanceSnapshots(req.PageToken)
	if pgErr != nil {
		return nil, pgErr
	}

	out := make([]instanceSnapshotWire, len(pg.Data))
	for i, s := range pg.Data {
		out[i] = instanceSnapshotToWire(s)
	}

	return marshalResponse(instanceSnapshotsListResponse{InstanceSnapshots: out, NextPageToken: pg.Next})
}

type getInstanceMetricDataResponse struct {
	MetricName string     `json:"metricName,omitempty"`
	MetricData []struct{} `json:"metricData"`
}

type getInstanceMetricDataRequest struct {
	InstanceName string `json:"instanceName"`
	MetricName   string `json:"metricName,omitempty"`
}

func (h *Handler) handleGetInstanceMetricData(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[getInstanceMetricDataRequest](body)
	if err != nil {
		return nil, err
	}

	if getErr := h.Backend.GetInstanceMetricData(req.InstanceName); getErr != nil {
		return nil, getErr
	}

	return marshalResponse(getInstanceMetricDataResponse{MetricData: []struct{}{}, MetricName: req.MetricName})
}

type updateInstanceMetadataOptionsRequest struct {
	HTTPEndpoint            string `json:"httpEndpoint,omitempty"`
	HTTPProtocolIpv6        string `json:"httpProtocolIpv6,omitempty"`
	HTTPTokens              string `json:"httpTokens,omitempty"`
	InstanceName            string `json:"instanceName"`
	HTTPPutResponseHopLimit int32  `json:"httpPutResponseHopLimit,omitempty"`
}

func (h *Handler) handleUpdateInstanceMetadataOptions(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[updateInstanceMetadataOptionsRequest](body)
	if err != nil {
		return nil, err
	}

	op, updateErr := h.Backend.UpdateInstanceMetadataOptions(req.InstanceName, InstanceMetadataOptions{
		HTTPEndpoint: req.HTTPEndpoint, HTTPProtocolIpv6: req.HTTPProtocolIpv6,
		HTTPPutResponseHopLimit: req.HTTPPutResponseHopLimit, HTTPTokens: req.HTTPTokens,
	})
	if updateErr != nil {
		return nil, updateErr
	}

	return marshalResponse(opEnvelope(op))
}

type resourceNameRequest struct {
	ResourceName string `json:"resourceName"`
}

type autoSnapshotDetailsWire struct {
	CreatedAt         *float64           `json:"createdAt,omitempty"`
	Date              string             `json:"date,omitempty"`
	Status            string             `json:"status,omitempty"`
	FromAttachedDisks []attachedDiskWire `json:"fromAttachedDisks,omitempty"`
}

type getAutoSnapshotsResponse struct {
	ResourceName  string                    `json:"resourceName,omitempty"`
	ResourceType  string                    `json:"resourceType,omitempty"`
	AutoSnapshots []autoSnapshotDetailsWire `json:"autoSnapshots,omitempty"`
}

func (h *Handler) handleGetAutoSnapshots(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[resourceNameRequest](body)
	if err != nil {
		return nil, err
	}

	snaps, kind, getErr := h.Backend.GetAutoSnapshots(req.ResourceName)
	if getErr != nil {
		return nil, getErr
	}

	sortAutoSnapshots(snaps)

	out := make([]autoSnapshotDetailsWire, len(snaps))

	for i, s := range snaps {
		disks := make([]attachedDiskWire, len(s.FromAttachedDisks))
		for j, d := range s.FromAttachedDisks {
			disks[j] = attachedDiskWire(d)
		}

		out[i] = autoSnapshotDetailsWire{
			CreatedAt:         epochPtr(s.CreatedAt),
			Date:              s.Date,
			FromAttachedDisks: disks,
			Status:            s.Status,
		}
	}

	return marshalResponse(
		getAutoSnapshotsResponse{AutoSnapshots: out, ResourceName: req.ResourceName, ResourceType: kind},
	)
}

type deleteAutoSnapshotRequest struct {
	Date         string `json:"date"`
	ResourceName string `json:"resourceName"`
}

func (h *Handler) handleDeleteAutoSnapshot(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[deleteAutoSnapshotRequest](body)
	if err != nil {
		return nil, err
	}

	ops, delErr := h.Backend.DeleteAutoSnapshot(req.ResourceName, req.Date)
	if delErr != nil {
		return nil, delErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type enableAddOnRequest struct {
	AddOnRequest addOnRequestWire `json:"addOnRequest"`
	ResourceName string           `json:"resourceName"`
}

func (h *Handler) handleEnableAddOn(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[enableAddOnRequest](body)
	if err != nil {
		return nil, err
	}

	reqs := addOnRequestsFromWire([]addOnRequestWire{req.AddOnRequest})
	if len(reqs) == 0 {
		return nil, validationError("AddOnRequest is required")
	}

	ops, enableErr := h.Backend.EnableAddOn(req.ResourceName, reqs[0])
	if enableErr != nil {
		return nil, enableErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type disableAddOnRequest struct {
	AddOnType    string `json:"addOnType"`
	ResourceName string `json:"resourceName"`
}

func (h *Handler) handleDisableAddOn(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[disableAddOnRequest](body)
	if err != nil {
		return nil, err
	}

	ops, disableErr := h.Backend.DisableAddOn(req.ResourceName, req.AddOnType)
	if disableErr != nil {
		return nil, disableErr
	}

	return marshalResponse(opsEnvelope(ops))
}
