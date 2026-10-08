package elasticache

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

// snapshotXML is the XML representation of a cache snapshot.
type snapshotXML struct {
	NodeSnapshots               *nodeSnapshotsXML `xml:"NodeSnapshots,omitempty"`
	ARN                         string            `xml:"ARN"`
	SnapshotName                string            `xml:"SnapshotName"`
	CacheClusterID              string            `xml:"CacheClusterId,omitempty"`
	ReplicationGroupID          string            `xml:"ReplicationGroupId,omitempty"`
	ReplicationGroupDescription string            `xml:"ReplicationGroupDescription,omitempty"`
	SnapshotStatus              string            `xml:"SnapshotStatus"`
	Engine                      string            `xml:"Engine,omitempty"`
	EngineVersion               string            `xml:"EngineVersion,omitempty"`
	CacheNodeType               string            `xml:"CacheNodeType,omitempty"`
	SnapshotSource              string            `xml:"SnapshotSource"`
	AutomaticFailover           string            `xml:"AutomaticFailover,omitempty"`
	Durability                  string            `xml:"Durability,omitempty"`
	CacheClusterCreateTime      string            `xml:"CacheClusterCreateTime,omitempty"`
	KmsKeyID                    string            `xml:"KmsKeyId,omitempty"`
	NumCacheNodes               int               `xml:"NumCacheNodes,omitempty"`
	NumNodeGroups               int               `xml:"NumNodeGroups,omitempty"`
}

type nodeSnapshotsXML struct {
	NodeSnapshot []nodeSnapshotXML `xml:"NodeSnapshot"`
}

type nodeSnapshotXML struct {
	NodeGroupConfiguration *nodeGroupConfigXML `xml:"NodeGroupConfiguration,omitempty"`
	CacheClusterID         string              `xml:"CacheClusterId,omitempty"`
	CacheNodeID            string              `xml:"CacheNodeId,omitempty"`
	NodeGroupID            string              `xml:"NodeGroupId,omitempty"`
	CacheNodeCreateTime    string              `xml:"CacheNodeCreateTime,omitempty"`
	SnapshotCreateTime     string              `xml:"SnapshotCreateTime,omitempty"`
}

type nodeGroupConfigXML struct {
	ReplicaAvailabilityZones *replicaAZsXML `xml:"ReplicaAvailabilityZones,omitempty"`
	NodeGroupID              string         `xml:"NodeGroupId,omitempty"`
	Slots                    string         `xml:"Slots,omitempty"`
	PrimaryAvailabilityZone  string         `xml:"PrimaryAvailabilityZone,omitempty"`
	ReplicaCount             int            `xml:"ReplicaCount"`
}

type replicaAZsXML struct {
	AvailabilityZone []string `xml:"AvailabilityZone"`
}

func nodeGroupConfigToXML(ng NodeGroup) *nodeGroupConfigXML {
	cfg := &nodeGroupConfigXML{NodeGroupID: ng.NodeGroupID, Slots: ng.Slots, ReplicaCount: len(ng.Replicas)}

	if ng.PrimaryNode != nil {
		cfg.PrimaryAvailabilityZone = ng.PrimaryNode.PreferredAvailabilityZone
	}

	if len(ng.Replicas) > 0 {
		cfg.ReplicaAvailabilityZones = &replicaAZsXML{}

		for _, r := range ng.Replicas {
			cfg.ReplicaAvailabilityZones.AvailabilityZone = append(cfg.ReplicaAvailabilityZones.AvailabilityZone,
				r.PreferredAvailabilityZone)
		}
	}

	return cfg
}

// nodeSnapshotsToXML lists the per-node entries of a snapshot; node group
// configuration is included only when the caller asked for it.
func nodeSnapshotsToXML(snap *CacheSnapshot, showConfig bool) *nodeSnapshotsXML {
	created := snap.CreatedAt.UTC().Format(time.RFC3339)
	out := &nodeSnapshotsXML{}

	for _, ng := range snap.NodeGroups {
		members := slices.Clone(ng.Replicas)
		if ng.PrimaryNode != nil {
			members = slices.Insert(members, 0, *ng.PrimaryNode)
		}

		for _, m := range members {
			ns := nodeSnapshotXML{
				CacheClusterID: m.CacheClusterID, CacheNodeID: m.CacheNodeID,
				NodeGroupID: ng.NodeGroupID, SnapshotCreateTime: created,
			}
			if showConfig {
				ns.NodeGroupConfiguration = nodeGroupConfigToXML(ng)
			}

			out.NodeSnapshot = append(out.NodeSnapshot, ns)
		}
	}

	for _, id := range snap.CacheNodeIDs {
		out.NodeSnapshot = append(out.NodeSnapshot, nodeSnapshotXML{
			CacheClusterID: snap.CacheClusterID, CacheNodeID: id, SnapshotCreateTime: created,
			CacheNodeCreateTime: snap.SourceClusterCreatedAt.UTC().Format(time.RFC3339),
		})
	}

	if len(out.NodeSnapshot) == 0 {
		return nil
	}

	return out
}

func snapshotToXML(snap *CacheSnapshot, showNodeGroupConfig bool) snapshotXML {
	x := snapshotXML{
		ARN:                         snap.ARN,
		SnapshotName:                snap.SnapshotName,
		CacheClusterID:              snap.CacheClusterID,
		ReplicationGroupID:          snap.ReplicationGroupID,
		ReplicationGroupDescription: snap.ReplicationGroupDescription,
		SnapshotStatus:              snap.Status,
		Engine:                      snap.Engine,
		EngineVersion:               snap.EngineVersion,
		CacheNodeType:               snap.NodeType,
		SnapshotSource:              snap.SnapshotSource,
		AutomaticFailover:           snap.AutomaticFailover,
		Durability:                  snap.Durability,
		KmsKeyID:                    snap.KmsKeyID,
		NumCacheNodes:               len(snap.CacheNodeIDs),
		NumNodeGroups:               len(snap.NodeGroups),
		NodeSnapshots:               nodeSnapshotsToXML(snap, showNodeGroupConfig),
	}
	if !snap.SourceClusterCreatedAt.IsZero() {
		x.CacheClusterCreateTime = snap.SourceClusterCreatedAt.UTC().Format(time.RFC3339)
	}

	return x
}

func (h *Handler) createSnapshot(ctx context.Context, c *echo.Context, form url.Values) error {
	snapshotName := form.Get("SnapshotName")
	clusterID := form.Get("CacheClusterId")
	replicationGroupID := form.Get("ReplicationGroupId")

	snap, err := h.Backend.CreateSnapshotFull(ctx, snapshotName, clusterID, replicationGroupID, form.Get("KmsKeyId"))
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
		Snapshot: snapshotToXML(snap, false),
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
		Snapshot: snapshotToXML(snap, false),
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

	showConfig := strings.EqualFold(form.Get("ShowNodeGroupConfig"), "true")
	items := make([]snapshotXML, 0, len(p.Data))
	for i := range p.Data {
		items = append(items, snapshotToXML(&p.Data[i], showConfig))
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

	snap, err := h.Backend.CopySnapshotFull(ctx, sourceSnapshotName, targetSnapshotName, form.Get("KmsKeyId"))
	if err != nil {
		if errors.Is(err, ErrSnapshotNotFound) {
			return xmlError(c, http.StatusNotFound, "SnapshotNotFoundFault", "Source snapshot not found")
		}
		if errors.Is(err, ErrSnapshotAlreadyExists) {
			return xmlError(c, http.StatusBadRequest, "SnapshotAlreadyExistsFault", "Target snapshot already exists")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	h.applyCreateTimeTags(ctx, form, snap.ARN)

	type result struct {
		XMLName  xml.Name    `xml:"CopySnapshotResponse"`
		Xmlns    string      `xml:"xmlns,attr"`
		Snapshot snapshotXML `xml:"CopySnapshotResult>Snapshot"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:    elasticacheNS,
		Snapshot: snapshotToXML(snap, false),
	})
}

// takeFinalSnapshot creates the snapshot named by FinalSnapshotIdentifier before a delete and returns an
// undo func that removes it again when the delete then fails.
func (h *Handler) takeFinalSnapshot(
	ctx context.Context, form url.Values, clusterID, replicationGroupID string,
) (func(), error) {
	name := form.Get("FinalSnapshotIdentifier")
	if name == "" {
		return func() {}, nil
	}

	if _, err := h.Backend.CreateSnapshot(ctx, name, clusterID, replicationGroupID); err != nil {
		return nil, err
	}

	return func() { _, _ = h.Backend.DeleteSnapshot(ctx, name) }, nil
}

func finalSnapshotErrorResponse(c *echo.Context, err error) error {
	if errors.Is(err, ErrSnapshotAlreadyExists) {
		return xmlError(c, http.StatusBadRequest, "SnapshotAlreadyExistsFault", "Snapshot already exists")
	}

	return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
}
