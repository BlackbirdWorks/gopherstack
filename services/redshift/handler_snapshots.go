package redshift

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
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
	Marker    string          `xml:"DescribeClusterSnapshotsResult>Marker,omitempty"`
	Snapshots xmlSnapshotList `xml:"DescribeClusterSnapshotsResult>Snapshots"`
}

const (
	defaultSnapshotPageSize = 100
	maxSnapshotPageSize     = 100
)

func (h *Handler) handleDescribeClusterSnapshots(vals url.Values) (any, error) {
	snapshotID := vals.Get("SnapshotIdentifier")
	clusterID := vals.Get("ClusterIdentifier")
	snapshotType := vals.Get("SnapshotType")
	markerStr := vals.Get("Marker")
	maxRecordsStr := vals.Get("MaxRecords")

	snaps, err := h.Backend.DescribeClusterSnapshots(snapshotID, clusterID, snapshotType)
	if err != nil {
		return nil, err
	}

	pageSize := defaultSnapshotPageSize
	if maxRecordsStr != "" {
		n, parseErr := strconv.Atoi(maxRecordsStr)
		if parseErr != nil || n < 20 || n > maxSnapshotPageSize {
			return nil, fmt.Errorf(
				"%w: MaxRecords must be between 20 and %d", ErrInvalidParameter, maxSnapshotPageSize,
			)
		}

		pageSize = n
	}

	startIdx := 0

	if markerStr != "" {
		decoded, decErr := base64.StdEncoding.DecodeString(markerStr)
		if decErr != nil {
			return nil, fmt.Errorf("%w: invalid Marker", ErrInvalidParameter)
		}

		afterID := string(decoded)

		for i, s := range snaps {
			if s.SnapshotIdentifier == afterID {
				startIdx = i + 1

				break
			}
		}
	}

	end := min(startIdx+pageSize, len(snaps))

	page := snaps[startIdx:end]

	var nextMarker string

	if end < len(snaps) {
		nextMarker = base64.StdEncoding.EncodeToString([]byte(snaps[end-1].SnapshotIdentifier))
	}

	members := make([]xmlSnapshot, 0, len(page))
	for _, s := range page {
		sp := s
		members = append(members, snapshotToXML(&sp))
	}

	return &describeClusterSnapshotsResponse{
		Xmlns:     redshiftXMLNS,
		Marker:    nextMarker,
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
