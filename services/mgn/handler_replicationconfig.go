package mgn

import (
	"context"
	"net/http"
)

func (h *Handler) handleGetReplicationConfiguration(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req getReplicationConfigurationRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	rc, err := h.Backend.GetReplicationConfiguration(req.SourceServerID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toReplicationConfigurationWire(rc))
}

func (h *Handler) handleUpdateReplicationConfiguration(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req updateReplicationConfigurationRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	in := UpdateReplicationConfigurationInput{
		StorageConfiguration:                fromStorageConfigurationWire(req.StorageConfiguration),
		StagingAreaTags:                     req.StagingAreaTags,
		ReplicatedDisks:                     fromReplicatedDisksWireOrNil(req.ReplicatedDisks),
		ReplicationServersSecurityGroupsIDs: req.ReplicationServersSecurityGroupsIDs,
		AssociateDefaultSecurityGroup:       req.AssociateDefaultSecurityGroup,
		BandwidthThrottling:                 req.BandwidthThrottling,
		CreatePublicIP:                      req.CreatePublicIP,
		DataPlaneRouting:                    req.DataPlaneRouting,
		DefaultLargeStagingDiskType:         req.DefaultLargeStagingDiskType,
		EbsEncryption:                       req.EbsEncryption,
		EbsEncryptionKeyArn:                 req.EbsEncryptionKeyArn,
		InternetProtocol:                    req.InternetProtocol,
		Name:                                req.Name,
		ReplicationServerInstanceType:       req.ReplicationServerInstanceType,
		StagingAreaSubnetID:                 req.StagingAreaSubnetID,
		StoreSnapshotOnLocalZone:            req.StoreSnapshotOnLocalZone,
		UseDedicatedReplicationServer:       req.UseDedicatedReplicationServer,
		UseFipsEndpoint:                     req.UseFipsEndpoint,
	}

	rc, err := h.Backend.UpdateReplicationConfiguration(req.SourceServerID, in)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toReplicationConfigurationWire(rc))
}

// fromReplicatedDisksWireOrNil returns nil (not an empty non-nil slice)
// when ds is empty, so UpdateReplicationConfigurationInput.ReplicatedDisks'
// nil-means-"don't touch" convention (see replicationconfig.go's
// applyReplicationConfigUpdateShapes) is preserved for a caller that omits
// the field entirely.
func fromReplicatedDisksWireOrNil(ds []replicatedDiskWire) []ReplicationConfigurationReplicatedDisk {
	if ds == nil {
		return nil
	}

	return fromReplicatedDisksWire(ds)
}

func (h *Handler) handleCreateReplicationConfigurationTemplate(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req createReplicationConfigurationTemplateRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	in := CreateReplicationConfigurationTemplateInput{
		StorageConfiguration:                fromStorageConfigurationWire(req.StorageConfiguration),
		Tags:                                req.Tags,
		StagingAreaTags:                     req.StagingAreaTags,
		ReplicationServersSecurityGroupsIDs: req.ReplicationServersSecurityGroupsIDs,
		DataPlaneRouting:                    req.DataPlaneRouting,
		DefaultLargeStagingDiskType:         req.DefaultLargeStagingDiskType,
		EbsEncryption:                       req.EbsEncryption,
		EbsEncryptionKeyArn:                 req.EbsEncryptionKeyArn,
		InternetProtocol:                    req.InternetProtocol,
		ReplicationServerInstanceType:       req.ReplicationServerInstanceType,
		StagingAreaSubnetID:                 req.StagingAreaSubnetID,
		AssociateDefaultSecurityGroup:       req.AssociateDefaultSecurityGroup,
		CreatePublicIP:                      req.CreatePublicIP,
		StoreSnapshotOnLocalZone:            req.StoreSnapshotOnLocalZone,
		UseDedicatedReplicationServer:       req.UseDedicatedReplicationServer,
		UseFipsEndpoint:                     req.UseFipsEndpoint,
		BandwidthThrottling:                 req.BandwidthThrottling,
	}

	tmpl, err := h.Backend.CreateReplicationConfigurationTemplate(in)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toReplicationConfigurationTemplateWire(tmpl))
}

func (h *Handler) handleDeleteReplicationConfigurationTemplate(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req replicationConfigurationTemplateIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteReplicationConfigurationTemplate(req.ReplicationConfigurationTemplateID); err != nil {
		return nil, err
	}

	return marshalResponse(struct{}{})
}

func (h *Handler) handleDescribeReplicationConfigurationTemplates(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req describeReplicationConfigurationTemplatesRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	pg, err := h.Backend.DescribeReplicationConfigurationTemplates(
		req.ReplicationConfigurationTemplateIDs, req.NextToken, int(req.MaxResults),
	)
	if err != nil {
		return nil, err
	}

	items := make([]replicationConfigurationTemplateWire, len(pg.Data))
	for i, t := range pg.Data {
		items[i] = toReplicationConfigurationTemplateWire(t)
	}

	return marshalResponse(describeReplicationConfigurationTemplatesResponse{Items: items, NextToken: pg.Next})
}

func (h *Handler) handleUpdateReplicationConfigurationTemplate(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req updateReplicationConfigurationTemplateRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	in := UpdateReplicationConfigurationTemplateInput{
		StorageConfiguration:                fromStorageConfigurationWire(req.StorageConfiguration),
		StagingAreaTags:                     req.StagingAreaTags,
		ReplicationServersSecurityGroupsIDs: req.ReplicationServersSecurityGroupsIDs,
		DataPlaneRouting:                    req.DataPlaneRouting,
		DefaultLargeStagingDiskType:         req.DefaultLargeStagingDiskType,
		EbsEncryption:                       req.EbsEncryption,
		EbsEncryptionKeyArn:                 req.EbsEncryptionKeyArn,
		InternetProtocol:                    req.InternetProtocol,
		ReplicationServerInstanceType:       req.ReplicationServerInstanceType,
		StagingAreaSubnetID:                 req.StagingAreaSubnetID,
		AssociateDefaultSecurityGroup:       req.AssociateDefaultSecurityGroup,
		CreatePublicIP:                      req.CreatePublicIP,
		StoreSnapshotOnLocalZone:            req.StoreSnapshotOnLocalZone,
		UseDedicatedReplicationServer:       req.UseDedicatedReplicationServer,
		UseFipsEndpoint:                     req.UseFipsEndpoint,
		BandwidthThrottling:                 req.BandwidthThrottling,
	}

	tmpl, err := h.Backend.UpdateReplicationConfigurationTemplate(req.ReplicationConfigurationTemplateID, in)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toReplicationConfigurationTemplateWire(tmpl))
}
