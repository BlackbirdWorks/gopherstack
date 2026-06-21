package redshift

import (
	"encoding/xml"
	"net/url"
)

// ---- CreateClusterSnapshot ----

type createClusterSnapshotResponse struct {
	XMLName  xml.Name    `xml:"CreateClusterSnapshotResponse"`
	Xmlns    string      `xml:"xmlns,attr"`
	Snapshot xmlSnapshot `xml:"CreateClusterSnapshotResult>Snapshot"`
}

func (h *Handler) handleCreateClusterSnapshot(vals url.Values) (any, error) {
	snapshotID := vals.Get("SnapshotIdentifier")
	clusterID := vals.Get("ClusterIdentifier")

	snap, err := h.Backend.CreateClusterSnapshot(snapshotID, clusterID)
	if err != nil {
		return nil, err
	}

	return &createClusterSnapshotResponse{
		Xmlns:    redshiftXMLNS,
		Snapshot: snapshotToXML(snap),
	}, nil
}

// ---- DeleteClusterSnapshot ----

type deleteClusterSnapshotResponse struct {
	XMLName  xml.Name    `xml:"DeleteClusterSnapshotResponse"`
	Xmlns    string      `xml:"xmlns,attr"`
	Snapshot xmlSnapshot `xml:"DeleteClusterSnapshotResult>Snapshot"`
}

func (h *Handler) handleDeleteClusterSnapshot(vals url.Values) (any, error) {
	snapshotID := vals.Get("SnapshotIdentifier")

	snap, err := h.Backend.DeleteClusterSnapshot(snapshotID)
	if err != nil {
		return nil, err
	}

	return &deleteClusterSnapshotResponse{
		Xmlns:    redshiftXMLNS,
		Snapshot: snapshotToXML(snap),
	}, nil
}

// ---- DescribeClusterSnapshots ----

type xmlSnapshotList struct {
	Members []xmlSnapshot `xml:"Snapshot"`
}

type describeClusterSnapshotsResponse struct {
	XMLName   xml.Name        `xml:"DescribeClusterSnapshotsResponse"`
	Xmlns     string          `xml:"xmlns,attr"`
	Snapshots xmlSnapshotList `xml:"DescribeClusterSnapshotsResult>Snapshots"`
}

func (h *Handler) handleDescribeClusterSnapshots(vals url.Values) (any, error) {
	snapshotID := vals.Get("SnapshotIdentifier")
	clusterID := vals.Get("ClusterIdentifier")
	snapshotType := vals.Get("SnapshotType")

	snaps, err := h.Backend.DescribeClusterSnapshots(snapshotID, clusterID, snapshotType)
	if err != nil {
		return nil, err
	}

	members := make([]xmlSnapshot, 0, len(snaps))
	for _, s := range snaps {
		sp := s
		members = append(members, snapshotToXML(&sp))
	}

	return &describeClusterSnapshotsResponse{
		Xmlns:     redshiftXMLNS,
		Snapshots: xmlSnapshotList{Members: members},
	}, nil
}

// ---- CopyClusterSnapshot ----

type copyClusterSnapshotResponse struct {
	XMLName  xml.Name    `xml:"CopyClusterSnapshotResponse"`
	Xmlns    string      `xml:"xmlns,attr"`
	Snapshot xmlSnapshot `xml:"CopyClusterSnapshotResult>Snapshot"`
}

func (h *Handler) handleCopyClusterSnapshot(vals url.Values) (any, error) {
	sourceSnapshotID := vals.Get("SourceSnapshotIdentifier")
	destinationSnapshotID := vals.Get("TargetSnapshotIdentifier")

	snap, err := h.Backend.CopyClusterSnapshot(sourceSnapshotID, destinationSnapshotID)
	if err != nil {
		return nil, err
	}

	return &copyClusterSnapshotResponse{
		Xmlns:    redshiftXMLNS,
		Snapshot: snapshotToXML(snap),
	}, nil
}

// ---- RestoreFromClusterSnapshot ----

type restoreFromClusterSnapshotResponse struct {
	XMLName xml.Name   `xml:"RestoreFromClusterSnapshotResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Cluster xmlCluster `xml:"RestoreFromClusterSnapshotResult>Cluster"`
}

func (h *Handler) handleRestoreFromClusterSnapshot(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	snapshotID := vals.Get("SnapshotIdentifier")

	cluster, err := h.Backend.RestoreFromClusterSnapshot(clusterID, snapshotID)
	if err != nil {
		return nil, err
	}

	return &restoreFromClusterSnapshotResponse{
		Xmlns:   redshiftXMLNS,
		Cluster: toXMLCluster(cluster),
	}, nil
}
