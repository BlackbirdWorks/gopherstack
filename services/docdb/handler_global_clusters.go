package docdb

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func (h *Handler) handleDescribeGlobalClusters(ctx context.Context, vals url.Values) (any, error) {
	gcs := h.Backend.DescribeGlobalClusters(ctx, vals.Get("GlobalClusterIdentifier"))
	gcs, err := filterGlobalClusters(vals, gcs)
	if err != nil {
		return nil, err
	}
	members := make([]xmlGlobalCluster, 0, len(gcs))
	for _, gc := range gcs {
		cp := gc
		members = append(members, h.globalClusterXML(ctx, &cp))
	}

	return &describeGlobalClustersResponse{
		Xmlns:          docdbXMLNS,
		GlobalClusters: xmlGlobalClusterList{Members: members},
	}, nil
}

func (h *Handler) handleCreateGlobalCluster(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("GlobalClusterIdentifier")
	sourceDBClusterID := vals.Get("SourceDBClusterIdentifier")
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")
	gc, err := h.Backend.CreateGlobalCluster(ctx, id, sourceDBClusterID, engine, engineVersion,
		CreateGlobalClusterOptions{
			DatabaseName:       vals.Get("DatabaseName"),
			DeletionProtection: parseBoolParam(vals, "DeletionProtection"),
			StorageEncrypted:   parseBoolParam(vals, "StorageEncrypted"),
		})
	if err != nil {
		return nil, err
	}

	return &createGlobalClusterResponse{
		Xmlns:         docdbXMLNS,
		GlobalCluster: h.globalClusterXML(ctx, gc),
	}, nil
}

func (h *Handler) handleDeleteGlobalCluster(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("GlobalClusterIdentifier")
	gc, err := h.Backend.DeleteGlobalCluster(ctx, id)
	if err != nil {
		return nil, err
	}

	return &deleteGlobalClusterResponse{
		Xmlns:         docdbXMLNS,
		GlobalCluster: h.globalClusterXML(ctx, gc),
	}, nil
}

func (h *Handler) handleModifyGlobalCluster(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("GlobalClusterIdentifier")
	newID := vals.Get("NewGlobalClusterIdentifier")
	deletionProtection := parseBoolParam(vals, "DeletionProtection")
	gc, err := h.Backend.ModifyGlobalCluster(ctx, id, newID, deletionProtection)
	if err != nil {
		return nil, err
	}

	return &modifyGlobalClusterResponse{
		Xmlns:         docdbXMLNS,
		GlobalCluster: h.globalClusterXML(ctx, gc),
	}, nil
}

func (h *Handler) handleFailoverGlobalCluster(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("GlobalClusterIdentifier")
	targetDBClusterID := vals.Get("TargetDbClusterIdentifier")
	if vals.Get("AllowDataLoss") == stringTrue && vals.Get("Switchover") == stringTrue {
		return nil, fmt.Errorf(
			"%w: AllowDataLoss and Switchover cannot be specified together", ErrInvalidParameterCombination,
		)
	}
	gc, err := h.Backend.FailoverGlobalCluster(ctx, id, targetDBClusterID)
	if err != nil {
		return nil, err
	}

	return &failoverGlobalClusterResponse{
		Xmlns:         docdbXMLNS,
		GlobalCluster: h.globalClusterXML(ctx, gc),
	}, nil
}

func (h *Handler) handleRemoveFromGlobalCluster(ctx context.Context, vals url.Values) (any, error) {
	globalClusterID := vals.Get("GlobalClusterIdentifier")
	dbClusterID := vals.Get("DbClusterIdentifier")
	gc, err := h.Backend.RemoveFromGlobalCluster(ctx, globalClusterID, dbClusterID)
	if err != nil {
		return nil, err
	}

	return &removeFromGlobalClusterResponse{
		Xmlns:         docdbXMLNS,
		GlobalCluster: h.globalClusterXML(ctx, gc),
	}, nil
}

func (h *Handler) handleSwitchoverGlobalCluster(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("GlobalClusterIdentifier")
	targetDBClusterID := vals.Get("TargetDbClusterIdentifier")
	gc, err := h.Backend.SwitchoverGlobalCluster(ctx, id, targetDBClusterID)
	if err != nil {
		return nil, err
	}

	return &switchoverGlobalClusterResponse{
		Xmlns:         docdbXMLNS,
		GlobalCluster: h.globalClusterXML(ctx, gc),
	}, nil
}

// docdb@v1.51.4 deserializers.go:14551 wraps each entry in
// <GlobalClusterMember>, not <GlobalCluster>.
type xmlGlobalClusterList struct {
	Members []xmlGlobalCluster `xml:"GlobalClusterMember"`
}

type describeGlobalClustersResponse struct {
	XMLName        xml.Name             `xml:"DescribeGlobalClustersResponse"`
	Xmlns          string               `xml:"xmlns,attr"`
	GlobalClusters xmlGlobalClusterList `xml:"DescribeGlobalClustersResult>GlobalClusters"`
}

// xmlGlobalClusterMember mirrors types.GlobalClusterMember's wire shape:
// DBClusterArn/IsWriter/Readers/SynchronizationStatus (see
// awsAwsquery_deserializeDocumentGlobalClusterMember). Readers uses the
// generic "member" list-item wrapper (awsAwsquery_deserializeDocumentReadersArnList).
type xmlGlobalClusterMember struct {
	DBClusterArn          string         `xml:"DBClusterArn,omitempty"`
	SynchronizationStatus string         `xml:"SynchronizationStatus,omitempty"`
	Readers               xmlReadersList `xml:"Readers"`
	IsWriter              bool           `xml:"IsWriter"`
}

type xmlReadersList struct {
	Members []string `xml:"member"`
}

type xmlGlobalClusterMemberList struct {
	Members []xmlGlobalClusterMember `xml:"GlobalClusterMember"`
}

// xmlGlobalCluster mirrors the real types.GlobalCluster response shape
// (confirmed against awsAwsquery_deserializeDocumentGlobalCluster,
// docdb@v1.51.4 deserializers.go). Note: SourceDBClusterIdentifier is a
// CreateGlobalClusterInput REQUEST member only -- the real GlobalCluster
// response type has no such member -- so gc.SourceDBClusterID (retained on
// the backend model for CreateGlobalCluster's own membership bootstrap) is
// deliberately not emitted here.
type xmlGlobalCluster struct {
	GlobalClusterIdentifier string                     `xml:"GlobalClusterIdentifier"`
	Engine                  string                     `xml:"Engine,omitempty"`
	EngineVersion           string                     `xml:"EngineVersion,omitempty"`
	GlobalClusterArn        string                     `xml:"GlobalClusterArn,omitempty"`
	Status                  string                     `xml:"Status"`
	DatabaseName            string                     `xml:"DatabaseName,omitempty"`
	GlobalClusterResourceID string                     `xml:"GlobalClusterResourceId,omitempty"`
	TagList                 *xmlTagList                `xml:"TagList,omitempty"`
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

type modifyGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"ModifyGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"ModifyGlobalClusterResult>GlobalCluster"`
}

type failoverGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"FailoverGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"FailoverGlobalClusterResult>GlobalCluster"`
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

// globalClusterXML adds the ARN-keyed tags (AddTagsToResource) as TagList.
func (h *Handler) globalClusterXML(ctx context.Context, gc *GlobalCluster) xmlGlobalCluster {
	x := toXMLGlobalCluster(gc)
	if tags := h.Backend.ListTagsForResource(ctx, gc.GlobalClusterArn); len(tags) > 0 {
		list := &xmlTagList{Members: make([]svcTags.KV, 0, len(tags))}
		for _, t := range tags {
			list.Members = append(list.Members, svcTags.KV(t))
		}
		x.TagList = list
	}

	return x
}

func toXMLGlobalCluster(gc *GlobalCluster) xmlGlobalCluster {
	members := make([]xmlGlobalClusterMember, 0, len(gc.GlobalClusterMembers))
	for _, m := range gc.GlobalClusterMembers {
		readers := make([]string, len(m.Readers))
		copy(readers, m.Readers)
		members = append(members, xmlGlobalClusterMember{
			DBClusterArn:          m.DBClusterArn,
			SynchronizationStatus: m.SynchronizationStatus,
			Readers:               xmlReadersList{Members: readers},
			IsWriter:              m.IsWriter,
		})
	}

	return xmlGlobalCluster{
		GlobalClusterIdentifier: gc.GlobalClusterIdentifier,
		Engine:                  gc.Engine,
		EngineVersion:           gc.EngineVersion,
		GlobalClusterArn:        gc.GlobalClusterArn,
		Status:                  gc.Status,
		GlobalClusterMembers:    xmlGlobalClusterMemberList{Members: members},
		StorageEncrypted:        gc.StorageEncrypted,
		DeletionProtection:      gc.DeletionProtection,
		DatabaseName:            gc.DatabaseName,
		GlobalClusterResourceID: gc.GlobalClusterResourceID,
	}
}
