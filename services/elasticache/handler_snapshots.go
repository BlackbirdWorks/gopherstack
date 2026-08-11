package elasticache

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/labstack/echo/v5"
)

// snapshotXML is the XML representation of a cache snapshot. Durability
// (deserializers.go:24609, awsAwsquery_deserializeDocumentSnapshot) was added
// by the SDK after this service's last field diff (gopherstack-31dm). Real
// AWS captures it from the source replication group's Durability at
// snapshot time, but CreateSnapshotInput/CopySnapshotInput have no Durability
// member, and this domain model's CacheSnapshot has no source-durability
// data to copy (snapshots may also come from a plain CacheCluster, which has
// no Durability concept at all) -- deliberately left always empty rather than
// guessed, per parity-principles.md's no-fabrication rule.
type snapshotXML struct {
	ARN                string `xml:"ARN"`
	SnapshotName       string `xml:"SnapshotName"`
	CacheClusterID     string `xml:"CacheClusterId,omitempty"`
	ReplicationGroupID string `xml:"ReplicationGroupId,omitempty"`
	SnapshotStatus     string `xml:"SnapshotStatus"`
	Engine             string `xml:"Engine,omitempty"`
	EngineVersion      string `xml:"EngineVersion,omitempty"`
	CacheNodeType      string `xml:"CacheNodeType,omitempty"`
	SnapshotSource     string `xml:"SnapshotSource"`
	Durability         string `xml:"Durability,omitempty"`
	SnapshotCreateTime string `xml:"SnapshotCreateTime,omitempty"`
}

func snapshotToXML(snap *CacheSnapshot) snapshotXML {
	return snapshotXML{
		ARN:                snap.ARN,
		SnapshotName:       snap.SnapshotName,
		CacheClusterID:     snap.CacheClusterID,
		ReplicationGroupID: snap.ReplicationGroupID,
		SnapshotStatus:     snap.Status,
		Engine:             snap.Engine,
		EngineVersion:      snap.EngineVersion,
		CacheNodeType:      snap.NodeType,
		SnapshotSource:     snap.SnapshotSource,
		SnapshotCreateTime: snap.CreatedAt.UTC().Format(time.RFC3339),
	}
}

func (h *Handler) createSnapshot(ctx context.Context, c *echo.Context, form url.Values) error {
	snapshotName := form.Get("SnapshotName")
	clusterID := form.Get("CacheClusterId")
	replicationGroupID := form.Get("ReplicationGroupId")

	snap, err := h.Backend.CreateSnapshot(ctx, snapshotName, clusterID, replicationGroupID)
	if err != nil {
		if errors.Is(err, ErrInvalidSnapshotSource) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"InvalidParameterCombination",
				ErrInvalidSnapshotSource.Error(),
			)
		}
		if errors.Is(err, ErrSnapshotAlreadyExists) {
			return xmlError(c, http.StatusBadRequest, "SnapshotAlreadyExistsFault", "Snapshot already exists")
		}
		if errors.Is(err, ErrClusterNotFound) {
			return xmlError(c, http.StatusNotFound, "CacheClusterNotFound", "Cache cluster not found")
		}
		if errors.Is(err, ErrReplicationGroupNotFound) {
			return xmlError(c, http.StatusNotFound, "ReplicationGroupNotFoundFault", "Replication group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	h.applyCreateTimeTags(ctx, form, snap.ARN)

	type result struct {
		XMLName  xml.Name    `xml:"CreateSnapshotResponse"`
		Xmlns    string      `xml:"xmlns,attr"`
		Snapshot snapshotXML `xml:"CreateSnapshotResult>Snapshot"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:    elasticacheNS,
		Snapshot: snapshotToXML(snap),
	})
}

func (h *Handler) deleteSnapshot(ctx context.Context, c *echo.Context, form url.Values) error {
	snapshotName := form.Get("SnapshotName")

	snap, err := h.Backend.DeleteSnapshot(ctx, snapshotName)
	if err != nil {
		if errors.Is(err, ErrSnapshotNotFound) {
			return xmlError(c, http.StatusNotFound, "SnapshotNotFoundFault", "Snapshot not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName  xml.Name    `xml:"DeleteSnapshotResponse"`
		Xmlns    string      `xml:"xmlns,attr"`
		Snapshot snapshotXML `xml:"DeleteSnapshotResult>Snapshot"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:    elasticacheNS,
		Snapshot: snapshotToXML(snap),
	})
}

func (h *Handler) describeSnapshots(ctx context.Context, c *echo.Context, form url.Values) error {
	snapshotName := form.Get("SnapshotName")
	clusterID := form.Get("CacheClusterId")
	replicationGroupID := form.Get("ReplicationGroupId")
	snapshotSource := form.Get("SnapshotSource")
	marker, maxRecords, err := parsePaginationChecked(c, form)
	if err != nil {
		return err
	}

	p, err := h.Backend.DescribeSnapshots(
		ctx, snapshotName, clusterID, replicationGroupID, snapshotSource, marker, maxRecords,
	)
	if err != nil {
		if errors.Is(err, ErrSnapshotNotFound) {
			return xmlError(c, http.StatusNotFound, "SnapshotNotFoundFault", "Snapshot not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type snapshots struct {
		Snapshot []snapshotXML `xml:"Snapshot"`
	}
	type result struct {
		XMLName   xml.Name  `xml:"DescribeSnapshotsResponse"`
		Xmlns     string    `xml:"xmlns,attr"`
		Marker    string    `xml:"DescribeSnapshotsResult>Marker,omitempty"`
		Snapshots snapshots `xml:"DescribeSnapshotsResult>Snapshots"`
	}

	items := make([]snapshotXML, 0, len(p.Data))
	for i := range p.Data {
		items = append(items, snapshotToXML(&p.Data[i]))
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:     elasticacheNS,
		Marker:    p.Next,
		Snapshots: snapshots{Snapshot: items},
	})
}

func (h *Handler) copySnapshot(ctx context.Context, c *echo.Context, form url.Values) error {
	sourceSnapshotName := form.Get("SourceSnapshotName")
	targetSnapshotName := form.Get("TargetSnapshotName")

	snap, err := h.Backend.CopySnapshot(ctx, sourceSnapshotName, targetSnapshotName)
	if err != nil {
		if errors.Is(err, ErrSnapshotNotFound) {
			return xmlError(c, http.StatusNotFound, "SnapshotNotFoundFault", "Source snapshot not found")
		}
		if errors.Is(err, ErrSnapshotAlreadyExists) {
			return xmlError(c, http.StatusBadRequest, "SnapshotAlreadyExistsFault", "Target snapshot already exists")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName  xml.Name    `xml:"CopySnapshotResponse"`
		Xmlns    string      `xml:"xmlns,attr"`
		Snapshot snapshotXML `xml:"CopySnapshotResult>Snapshot"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:    elasticacheNS,
		Snapshot: snapshotToXML(snap),
	})
}
