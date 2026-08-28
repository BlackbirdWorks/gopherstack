package rds

import (
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleDescribeGlobalClusters(vals url.Values) (any, error) {
	id := vals.Get("GlobalClusterIdentifier")
	clusters, err := h.Backend.DescribeGlobalClusters(id)
	if err != nil {
		return nil, err
	}
	members := make([]xmlGlobalCluster, 0, len(clusters))
	for _, gc := range clusters {
		cp := gc
		members = append(members, toXMLGlobalCluster(&cp))
	}

	return &describeGlobalClustersResponse{
		Xmlns:          rdsXMLNS,
		GlobalClusters: xmlGlobalClusterList{Members: members},
	}, nil
}

type xmlGlobalClusterMember struct {
	DBClusterArn                string `xml:"DBClusterArn"`
	GlobalWriteForwardingStatus string `xml:"GlobalWriteForwardingStatus"`
	IsWriter                    bool   `xml:"IsWriter"`
}

// globalWriteForwardingStatusEnabled and globalWriteForwardingStatusDisabled
// are two of the five types.WriteForwardingStatus enum members (rds@v1.124.1
// types/enums.go); this backend does not implement write forwarding, so a
// member's status is always one of these two, never enabling/disabling/unknown.
const (
	globalWriteForwardingStatusEnabled  = "enabled"
	globalWriteForwardingStatusDisabled = "disabled"
)

type xmlGlobalClusterMemberList struct {
	Members []xmlGlobalClusterMember `xml:"GlobalClusterMember"`
}

type xmlGlobalCluster struct {
	GlobalClusterMembers    *xmlGlobalClusterMemberList `xml:"GlobalClusterMembers,omitempty"`
	GlobalClusterIdentifier string                      `xml:"GlobalClusterIdentifier"`
	GlobalClusterArn        string                      `xml:"GlobalClusterArn,omitempty"`
	Engine                  string                      `xml:"Engine,omitempty"`
	EngineVersion           string                      `xml:"EngineVersion,omitempty"`
	Status                  string                      `xml:"Status,omitempty"`
	PrimaryRegion           string                      `xml:"PrimaryRegion,omitempty"`
	StorageEncrypted        bool                        `xml:"StorageEncrypted,omitempty"`
	DeletionProtection      bool                        `xml:"DeletionProtection,omitempty"`
}

type xmlGlobalClusterList struct {
	Members []xmlGlobalCluster `xml:"GlobalClusterMember"`
}

type describeGlobalClustersResponse struct {
	XMLName        xml.Name             `xml:"DescribeGlobalClustersResponse"`
	Xmlns          string               `xml:"xmlns,attr"`
	GlobalClusters xmlGlobalClusterList `xml:"DescribeGlobalClustersResult>GlobalClusters"`
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

func (h *Handler) handleCreateGlobalCluster(vals url.Values) (any, error) {
	id := vals.Get("GlobalClusterIdentifier")
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")
	storageEncrypted := vals.Get("StorageEncrypted") == formTrue
	deletionProtection := vals.Get("DeletionProtection") == formTrue

	gc, err := h.Backend.CreateGlobalCluster(id, engine, engineVersion, storageEncrypted, deletionProtection)
	if err != nil {
		return nil, err
	}

	h.applyCreateTags(vals, gc.GlobalClusterArn)

	return &createGlobalClusterResponse{
		Xmlns:         rdsXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleDeleteGlobalCluster(vals url.Values) (any, error) {
	id := vals.Get("GlobalClusterIdentifier")

	gc, err := h.Backend.DeleteGlobalCluster(id)
	if err != nil {
		return nil, err
	}

	return &deleteGlobalClusterResponse{
		Xmlns:         rdsXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleModifyGlobalCluster(vals url.Values) (any, error) {
	id := vals.Get("GlobalClusterIdentifier")
	newID := vals.Get("NewGlobalClusterIdentifier")
	engineVersion := vals.Get("EngineVersion")

	var deletionProtection *bool
	if dp := vals.Get("DeletionProtection"); dp != "" {
		v := dp == formTrue
		deletionProtection = &v
	}

	gc, err := h.Backend.ModifyGlobalCluster(id, newID, engineVersion, deletionProtection)
	if err != nil {
		return nil, err
	}

	return &modifyGlobalClusterResponse{
		Xmlns:         rdsXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

// AddGlobalClusterMemberInternal appends a member directly to an existing
// global cluster, bypassing normal validation. Used for seeding tests: no
// gopherstack API currently populates GlobalClusterMembers (CreateDBCluster
// never wires a DB cluster into a global cluster's membership).
func (b *InMemoryBackend) AddGlobalClusterMemberInternal(globalClusterID string, member GlobalClusterMember) {
	b.mu.Lock("AddGlobalClusterMemberInternal")
	defer b.mu.Unlock()

	gc, ok := b.globalClusters.Get(globalClusterID)
	if !ok {
		return
	}
	gc.GlobalClusterMembers = append(gc.GlobalClusterMembers, member)
}

func toXMLGlobalCluster(gc *GlobalCluster) xmlGlobalCluster {
	x := xmlGlobalCluster{
		GlobalClusterIdentifier: gc.GlobalClusterIdentifier,
		GlobalClusterArn:        gc.GlobalClusterArn,
		Engine:                  gc.Engine,
		EngineVersion:           gc.EngineVersion,
		Status:                  gc.Status,
		PrimaryRegion:           gc.PrimaryRegion,
		StorageEncrypted:        gc.StorageEncrypted,
		DeletionProtection:      gc.DeletionProtection,
	}

	if len(gc.GlobalClusterMembers) > 0 {
		members := make([]xmlGlobalClusterMember, 0, len(gc.GlobalClusterMembers))
		for _, m := range gc.GlobalClusterMembers {
			status := globalWriteForwardingStatusDisabled
			if m.GlobalWriteForwarding {
				status = globalWriteForwardingStatusEnabled
			}
			members = append(members, xmlGlobalClusterMember{
				DBClusterArn:                m.DBClusterArn,
				GlobalWriteForwardingStatus: status,
				IsWriter:                    m.IsWriter,
			})
		}

		x.GlobalClusterMembers = &xmlGlobalClusterMemberList{Members: members}
	}

	return x
}

type removeFromGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"RemoveFromGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"RemoveFromGlobalClusterResult>GlobalCluster"`
}

type failoverGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"FailoverGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"FailoverGlobalClusterResult>GlobalCluster"`
}

type switchoverGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"SwitchoverGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"SwitchoverGlobalClusterResult>GlobalCluster"`
}

func (h *Handler) handleRemoveFromGlobalCluster(vals url.Values) (any, error) {
	globalClusterID := vals.Get("GlobalClusterIdentifier")
	dbClusterARN := vals.Get("DbClusterIdentifier")
	gc, err := h.Backend.RemoveFromGlobalCluster(globalClusterID, dbClusterARN)
	if err != nil {
		return nil, err
	}

	return &removeFromGlobalClusterResponse{
		Xmlns:         rdsXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleFailoverGlobalCluster(vals url.Values) (any, error) {
	globalClusterID := vals.Get("GlobalClusterIdentifier")
	targetDB := vals.Get("TargetDbClusterIdentifier")
	gc, err := h.Backend.FailoverGlobalCluster(globalClusterID, targetDB)
	if err != nil {
		return nil, err
	}

	return &failoverGlobalClusterResponse{
		Xmlns:         rdsXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleSwitchoverGlobalCluster(vals url.Values) (any, error) {
	globalClusterID := vals.Get("GlobalClusterIdentifier")
	targetDB := vals.Get("TargetDbClusterIdentifier")
	gc, err := h.Backend.SwitchoverGlobalCluster(globalClusterID, targetDB)
	if err != nil {
		return nil, err
	}

	return &switchoverGlobalClusterResponse{
		Xmlns:         rdsXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}
