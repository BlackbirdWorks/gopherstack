package lightsail

import "context"

// exportCfnOps returns the dispatch table for family K (4 ops).
func (h *Handler) exportCfnOps() map[string]opFunc {
	return map[string]opFunc{
		"ExportSnapshot":                h.handleExportSnapshot,
		"GetExportSnapshotRecords":      h.handleGetExportSnapshotRecords,
		"CreateCloudFormationStack":     h.handleCreateCloudFormationStack,
		"GetCloudFormationStackRecords": h.handleGetCloudFormationStackRecords,
	}
}

type exportSnapshotRequest struct {
	SourceSnapshotName string `json:"sourceSnapshotName"`
}

func (h *Handler) handleExportSnapshot(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[exportSnapshotRequest](body)
	if err != nil {
		return nil, err
	}

	ops, exportErr := h.Backend.ExportSnapshot(req.SourceSnapshotName)
	if exportErr != nil {
		return nil, exportErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type destinationInfoWire struct {
	ID      string `json:"id,omitempty"`
	Service string `json:"service,omitempty"`
}

type exportSnapshotRecordWire struct {
	Arn             string                `json:"arn,omitempty"`
	CreatedAt       *float64              `json:"createdAt,omitempty"`
	DestinationInfo *destinationInfoWire  `json:"destinationInfo,omitempty"`
	Location        *resourceLocationWire `json:"location,omitempty"`
	Name            string                `json:"name,omitempty"`
	ResourceType    string                `json:"resourceType,omitempty"`
	State           string                `json:"state,omitempty"`
}

type exportSnapshotRecordsListResponse struct {
	NextPageToken         string                     `json:"nextPageToken,omitempty"`
	ExportSnapshotRecords []exportSnapshotRecordWire `json:"exportSnapshotRecords,omitempty"`
}

func (h *Handler) handleGetExportSnapshotRecords(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[pageTokenRequest](body)
	if err != nil {
		return nil, err
	}

	pg, pgErr := h.Backend.GetExportSnapshotRecords(req.PageToken)
	if pgErr != nil {
		return nil, pgErr
	}

	out := make([]exportSnapshotRecordWire, len(pg.Data))

	for i, r := range pg.Data {
		out[i] = exportSnapshotRecordWire{
			Arn: r.Arn, CreatedAt: epochPtr(r.CreatedAt), Location: locationToWire(r.Location),
			Name: r.Name, ResourceType: ResourceTypeExportSnapshotRecord, State: r.State,
			DestinationInfo: &destinationInfoWire{ID: r.SourceName, Service: r.SourceResourceType},
		}
	}

	return marshalResponse(exportSnapshotRecordsListResponse{ExportSnapshotRecords: out, NextPageToken: pg.Next})
}

type instanceEntryWire struct {
	AvailabilityZone string `json:"availabilityZone,omitempty"`
	InstanceType     string `json:"instanceType,omitempty"`
	PortInfoSource   string `json:"portInfoSource,omitempty"`
	SourceName       string `json:"sourceName"`
	UserData         string `json:"userData,omitempty"`
}

type createCloudFormationStackRequest struct {
	Instances []instanceEntryWire `json:"instances"`
}

func (h *Handler) handleCreateCloudFormationStack(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createCloudFormationStackRequest](body)
	if err != nil {
		return nil, err
	}

	entries := make([]InstanceEntry, len(req.Instances))
	for i, e := range req.Instances {
		entries[i] = InstanceEntry{
			SourceName: e.SourceName, AvailabilityZone: e.AvailabilityZone,
			InstanceType: e.InstanceType, PortInfoSource: e.PortInfoSource, UserData: e.UserData,
		}
	}

	ops, createErr := h.Backend.CreateCloudFormationStack(entries)
	if createErr != nil {
		return nil, createErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type cloudFormationStackRecordWire struct {
	Arn             string                `json:"arn,omitempty"`
	CreatedAt       *float64              `json:"createdAt,omitempty"`
	DestinationInfo *destinationInfoWire  `json:"destinationInfo,omitempty"`
	Location        *resourceLocationWire `json:"location,omitempty"`
	Name            string                `json:"name,omitempty"`
	ResourceType    string                `json:"resourceType,omitempty"`
	State           string                `json:"state,omitempty"`
}

type cloudFormationStackRecordsListResponse struct {
	NextPageToken              string                          `json:"nextPageToken,omitempty"`
	CloudFormationStackRecords []cloudFormationStackRecordWire `json:"cloudFormationStackRecords,omitempty"`
}

func (h *Handler) handleGetCloudFormationStackRecords(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[pageTokenRequest](body)
	if err != nil {
		return nil, err
	}

	pg, pgErr := h.Backend.GetCloudFormationStackRecords(req.PageToken)
	if pgErr != nil {
		return nil, pgErr
	}

	out := make([]cloudFormationStackRecordWire, len(pg.Data))

	for i, r := range pg.Data {
		out[i] = cloudFormationStackRecordWire{
			Arn: r.Arn, CreatedAt: epochPtr(r.CreatedAt), Location: locationToWire(r.Location),
			Name: r.Name, ResourceType: ResourceTypeCloudFormationStackRecord, State: r.State,
			DestinationInfo: &destinationInfoWire{ID: r.DestinationInfoID, Service: "CloudFormation"},
		}
	}

	return marshalResponse(
		cloudFormationStackRecordsListResponse{CloudFormationStackRecords: out, NextPageToken: pg.Next},
	)
}
