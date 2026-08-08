package neptune

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
)

func (h *Handler) handleDescribeGlobalClusters(ctx context.Context, _ url.Values) (any, error) {
	gcs := h.Backend.DescribeGlobalClusters(ctx)
	members := make([]xmlGlobalCluster, 0, len(gcs))
	for _, gc := range gcs {
		cp := gc
		members = append(members, toXMLGlobalCluster(&cp))
	}

	return &describeGlobalClustersResponse{
		Xmlns: neptuneXMLNS,
		Result: describeGlobalClustersResult{
			GlobalClusters: xmlGlobalClusterList{Members: members},
		},
	}, nil
}

func (h *Handler) handleCreateGlobalCluster(ctx context.Context, vals url.Values) (any, error) {
	globalClusterID := vals.Get("GlobalClusterIdentifier")
	sourceDBClusterID := vals.Get("SourceDBClusterIdentifier")
	tags := parseTagEntries(vals)
	if err := validateTagEntries(tags); err != nil {
		return nil, err
	}
	gc, err := h.Backend.CreateGlobalCluster(ctx, globalClusterID, sourceDBClusterID)
	if err != nil {
		return nil, err
	}
	if len(tags) > 0 {
		_ = h.Backend.AddTagsToResource(ctx, gc.GlobalClusterArn, tags)
	}

	return &createGlobalClusterResponse{
		Xmlns:         neptuneXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleDeleteGlobalCluster(ctx context.Context, vals url.Values) (any, error) {
	globalClusterID := vals.Get("GlobalClusterIdentifier")
	gc, err := h.Backend.DeleteGlobalCluster(ctx, globalClusterID)
	if err != nil {
		return nil, err
	}

	return &deleteGlobalClusterResponse{
		Xmlns:         neptuneXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleFailoverGlobalCluster(ctx context.Context, vals url.Values) (any, error) {
	globalClusterID := vals.Get("GlobalClusterIdentifier")
	targetDBClusterID := vals.Get("TargetDbClusterIdentifier")
	gc, err := h.Backend.FailoverGlobalCluster(ctx, globalClusterID, targetDBClusterID)
	if err != nil {
		return nil, err
	}

	return &failoverGlobalClusterResponse{
		Xmlns:         neptuneXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleModifyGlobalCluster(ctx context.Context, vals url.Values) (any, error) {
	globalClusterID := vals.Get("GlobalClusterIdentifier")
	rawDP := vals.Get("DeletionProtection")
	opts := GlobalClusterModifyOptions{
		NewGlobalClusterIdentifier: vals.Get("NewGlobalClusterIdentifier"),
		EngineVersion:              vals.Get("EngineVersion"),
		DeletionProtection:         rawDP == formTrue,
		DeletionProtectionSet:      rawDP != "",
		AllowMajorVersionUpgrade:   vals.Get("AllowMajorVersionUpgrade") == formTrue,
	}
	gc, err := h.Backend.ModifyGlobalCluster(ctx, globalClusterID, opts)
	if err != nil {
		return nil, err
	}

	return &modifyGlobalClusterResponse{
		Xmlns:         neptuneXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleRemoveFromGlobalCluster(ctx context.Context, vals url.Values) (any, error) {
	globalClusterID := vals.Get("GlobalClusterIdentifier")
	dbClusterARN := vals.Get("DbClusterIdentifier")
	gc, err := h.Backend.RemoveFromGlobalCluster(ctx, globalClusterID, dbClusterARN)
	if err != nil {
		return nil, err
	}

	return &removeFromGlobalClusterResponse{
		Xmlns:         neptuneXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleSwitchoverGlobalCluster(ctx context.Context, vals url.Values) (any, error) {
	globalClusterID := vals.Get("GlobalClusterIdentifier")
	targetDBClusterID := vals.Get("TargetDbClusterIdentifier")
	gc, err := h.Backend.SwitchoverGlobalCluster(ctx, globalClusterID, targetDBClusterID)
	if err != nil {
		return nil, err
	}

	return &switchoverGlobalClusterResponse{
		Xmlns:         neptuneXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func toXMLGlobalCluster(gc *GlobalCluster) xmlGlobalCluster {
	members := make([]xmlGlobalClusterMember, 0, len(gc.GlobalClusterMembers))
	for _, m := range gc.GlobalClusterMembers {
		members = append(members, xmlGlobalClusterMember(m))
	}

	return xmlGlobalCluster{
		GlobalClusterIdentifier: gc.GlobalClusterIdentifier,
		GlobalClusterArn:        gc.GlobalClusterArn,
		GlobalClusterResourceID: gc.GlobalClusterResourceID,
		Status:                  gc.Status,
		Engine:                  gc.Engine,
		EngineVersion:           gc.EngineVersion,
		GlobalClusterMembers:    xmlGlobalClusterMemberList{Members: members},
		StorageEncrypted:        gc.StorageEncrypted,
		DeletionProtection:      gc.DeletionProtection,
	}
}

type describeGlobalClustersResult struct {
	GlobalClusters xmlGlobalClusterList `xml:"GlobalClusters"`
}

type describeGlobalClustersResponse struct {
	XMLName xml.Name                     `xml:"DescribeGlobalClustersResponse"`
	Xmlns   string                       `xml:"xmlns,attr"`
	Result  describeGlobalClustersResult `xml:"DescribeGlobalClustersResult"`
}

type xmlGlobalClusterMember struct {
	DBClusterARN string `xml:"DBClusterArn"`
	IsWriter     bool   `xml:"IsWriter"`
}

type xmlGlobalClusterMemberList struct {
	Members []xmlGlobalClusterMember `xml:"GlobalClusterMember"`
}

type xmlGlobalClusterList struct {
	Members []xmlGlobalCluster `xml:"GlobalCluster"`
}

type xmlGlobalCluster struct {
	GlobalClusterIdentifier string                     `xml:"GlobalClusterIdentifier"`
	GlobalClusterArn        string                     `xml:"GlobalClusterArn,omitempty"`
	GlobalClusterResourceID string                     `xml:"GlobalClusterResourceId,omitempty"`
	Status                  string                     `xml:"Status"`
	Engine                  string                     `xml:"Engine,omitempty"`
	EngineVersion           string                     `xml:"EngineVersion,omitempty"`
	GlobalClusterMembers    xmlGlobalClusterMemberList `xml:"GlobalClusterMembers"`
	StorageEncrypted        bool                       `xml:"StorageEncrypted"`
	DeletionProtection      bool                       `xml:"DeletionProtection"`
}

type createGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"CreateGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"CreateGlobalClusterResult>GlobalCluster"`
}

type deleteGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"DeleteGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"DeleteGlobalClusterResult>GlobalCluster"`
}

type failoverGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"FailoverGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"FailoverGlobalClusterResult>GlobalCluster"`
}

type modifyGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"ModifyGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"ModifyGlobalClusterResult>GlobalCluster"`
}

type removeFromGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"RemoveFromGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"RemoveFromGlobalClusterResult>GlobalCluster"`
}

type switchoverGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"SwitchoverGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"SwitchoverGlobalClusterResult>GlobalCluster"`
}

// dispatchGlobalClusterAndTagAction handles GlobalCluster actions plus the
// generic resource-tagging actions; it is the last link in dispatch's chain,
// so an unmatched action here is a genuinely invalid Neptune action.
func (h *Handler) dispatchGlobalClusterAndTagAction(
	ctx context.Context, action string, vals url.Values,
) (any, error) {
	switch action {
	case "DescribeGlobalClusters":
		return h.handleDescribeGlobalClusters(ctx, vals)
	case "CreateGlobalCluster":
		return h.handleCreateGlobalCluster(ctx, vals)
	case "DeleteGlobalCluster":
		return h.handleDeleteGlobalCluster(ctx, vals)
	case "FailoverGlobalCluster":
		return h.handleFailoverGlobalCluster(ctx, vals)
	case "ModifyGlobalCluster":
		return h.handleModifyGlobalCluster(ctx, vals)
	case "RemoveFromGlobalCluster":
		return h.handleRemoveFromGlobalCluster(ctx, vals)
	case "SwitchoverGlobalCluster":
		return h.handleSwitchoverGlobalCluster(ctx, vals)
	case "ListTagsForResource":
		return h.handleListTagsForResource(ctx, vals)
	case "AddTagsToResource":
		return h.handleAddTagsToResource(ctx, vals)
	case "RemoveTagsFromResource":
		return h.handleRemoveTagsFromResource(ctx, vals)
	default:
		return nil, fmt.Errorf("%w: %s is not a valid Neptune action", ErrUnknownAction, action)
	}
}
