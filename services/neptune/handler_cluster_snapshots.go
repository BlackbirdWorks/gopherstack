package neptune

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
)

func (h *Handler) handleCreateDBClusterSnapshot(ctx context.Context, vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	snap, err := h.Backend.CreateDBClusterSnapshot(ctx, snapshotID, clusterID)
	if err != nil {
		return nil, err
	}

	return &createDBClusterSnapshotResponse{
		Xmlns:             neptuneXMLNS,
		DBClusterSnapshot: toXMLClusterSnapshot(snap),
	}, nil
}

func (h *Handler) handleDescribeDBClusterSnapshots(
	ctx context.Context,
	vals url.Values,
) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	snapshotType := vals.Get("SnapshotType")
	snaps, err := h.Backend.DescribeDBClusterSnapshots(ctx, snapshotID, clusterID, snapshotType)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBClusterSnapshot, 0, len(snaps))
	for _, snap := range snaps {
		cp := snap
		members = append(members, toXMLClusterSnapshot(&cp))
	}

	members, nextMarker := applyNeptuneMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBClusterSnapshotsResponse{
		Xmlns: neptuneXMLNS,
		Result: describeDBClusterSnapshotsResult{
			DBClusterSnapshots: xmlDBClusterSnapshotList{Members: members},
			Marker:             nextMarker,
		},
	}, nil
}

func (h *Handler) handleDeleteDBClusterSnapshot(ctx context.Context, vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	snap, err := h.Backend.DeleteDBClusterSnapshot(ctx, snapshotID)
	if err != nil {
		return nil, err
	}

	return &deleteDBClusterSnapshotResponse{
		Xmlns:             neptuneXMLNS,
		DBClusterSnapshot: toXMLClusterSnapshot(snap),
	}, nil
}

func (h *Handler) handleCopyDBClusterSnapshot(ctx context.Context, vals url.Values) (any, error) {
	sourceSnapshotID := vals.Get("SourceDBClusterSnapshotIdentifier")
	targetSnapshotID := vals.Get("TargetDBClusterSnapshotIdentifier")
	snap, err := h.Backend.CopyDBClusterSnapshot(ctx, sourceSnapshotID, targetSnapshotID)
	if err != nil {
		return nil, err
	}

	return &copyDBClusterSnapshotResponse{
		Xmlns:             neptuneXMLNS,
		DBClusterSnapshot: toXMLClusterSnapshot(snap),
	}, nil
}

// toXMLClusterSnapshotAttributesResult builds the DBClusterSnapshotAttributesResult
// AWS returns from both DescribeDBClusterSnapshotAttributes and
// ModifyDBClusterSnapshotAttribute. AWS always includes an entry for the
// "restore" attribute (empty AttributeValues when nothing is shared), never
// an empty list -- see the real API's describe-db-cluster-snapshot-attributes
// output shape.
func toXMLClusterSnapshotAttributesResult(snap *DBClusterSnapshot) xmlDBClusterSnapshotAttributesResult {
	values := make([]string, len(snap.RestoreAttributeValues))
	copy(values, snap.RestoreAttributeValues)

	return xmlDBClusterSnapshotAttributesResult{
		DBClusterSnapshotIdentifier: snap.DBClusterSnapshotIdentifier,
		DBClusterSnapshotAttributes: xmlDBClusterSnapshotAttrList{
			Members: []xmlDBClusterSnapshotAttribute{
				{
					AttributeName:   dbClusterSnapshotRestoreAttribute,
					AttributeValues: xmlAttributeValueList{Members: values},
				},
			},
		},
	}
}

func (h *Handler) handleDescribeDBClusterSnapshotAttributes(
	ctx context.Context,
	vals url.Values,
) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	snaps, err := h.Backend.DescribeDBClusterSnapshots(ctx, snapshotID, "", "")
	if err != nil {
		return nil, err
	}
	snap := snaps[0]

	return &describeDBClusterSnapshotAttributesResponse{
		Xmlns: neptuneXMLNS,
		Result: describeDBClusterSnapshotAttributesResult{
			DBClusterSnapshotAttributesResult: toXMLClusterSnapshotAttributesResult(&snap),
		},
	}, nil
}

func (h *Handler) handleModifyDBClusterSnapshotAttribute(
	ctx context.Context,
	vals url.Values,
) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	attributeName := vals.Get("AttributeName")
	// The wire item name here is "AttributeValue" (see
	// awsAwsquery_serializeDocumentAttributeValueList in the SDK's
	// serializers.go), not the generic "member" most other Neptune list
	// params use -- ValuesToAdd.AttributeValue.1, not ValuesToAdd.member.1.
	valuesToAdd := parseMemberList(vals, "ValuesToAdd.AttributeValue")
	valuesToRemove := parseMemberList(vals, "ValuesToRemove.AttributeValue")
	snap, err := h.Backend.ModifyDBClusterSnapshotAttribute(
		ctx, snapshotID, attributeName, valuesToAdd, valuesToRemove,
	)
	if err != nil {
		return nil, err
	}

	return &modifyDBClusterSnapshotAttributeResponse{
		Xmlns: neptuneXMLNS,
		Result: modifyDBClusterSnapshotAttributeResult{
			DBClusterSnapshotAttributesResult: toXMLClusterSnapshotAttributesResult(snap),
		},
	}, nil
}

func toXMLClusterSnapshot(snap *DBClusterSnapshot) xmlDBClusterSnapshot {
	return xmlDBClusterSnapshot{
		DBClusterSnapshotIdentifier:      snap.DBClusterSnapshotIdentifier,
		DBClusterSnapshotArn:             snap.DBClusterSnapshotArn,
		DBClusterIdentifier:              snap.DBClusterIdentifier,
		Engine:                           snap.Engine,
		EngineVersion:                    snap.EngineVersion,
		Status:                           snap.Status,
		StorageEncrypted:                 snap.StorageEncrypted,
		SnapshotType:                     snap.SnapshotType,
		SnapshotCreateTime:               snap.SnapshotCreateTime,
		ClusterCreateTime:                snap.ClusterCreateTime,
		KmsKeyID:                         snap.KmsKeyID,
		VpcID:                            snap.VpcID,
		IAMDatabaseAuthenticationEnabled: snap.IAMDatabaseAuthenticationEnabled,
		Port:                             snap.Port,
		PercentProgress:                  snap.PercentProgress,
		AllocatedStorage:                 snap.AllocatedStorage,
	}
}

type xmlDBClusterSnapshot struct {
	DBClusterSnapshotIdentifier      string `xml:"DBClusterSnapshotIdentifier"`
	DBClusterSnapshotArn             string `xml:"DBClusterSnapshotArn,omitempty"`
	DBClusterIdentifier              string `xml:"DBClusterIdentifier"`
	Engine                           string `xml:"Engine"`
	EngineVersion                    string `xml:"EngineVersion,omitempty"`
	Status                           string `xml:"Status"`
	SnapshotType                     string `xml:"SnapshotType,omitempty"`
	SnapshotCreateTime               string `xml:"SnapshotCreateTime,omitempty"`
	ClusterCreateTime                string `xml:"ClusterCreateTime,omitempty"`
	KmsKeyID                         string `xml:"KmsKeyId,omitempty"`
	VpcID                            string `xml:"VpcId,omitempty"`
	StorageEncrypted                 bool   `xml:"StorageEncrypted"`
	IAMDatabaseAuthenticationEnabled bool   `xml:"IAMDatabaseAuthenticationEnabled"`
	Port                             int    `xml:"Port,omitempty"`
	PercentProgress                  int    `xml:"PercentProgress,omitempty"`
	AllocatedStorage                 int    `xml:"AllocatedStorage,omitempty"`
}

type xmlDBClusterSnapshotList struct {
	Members []xmlDBClusterSnapshot `xml:"DBClusterSnapshot"`
}

type createDBClusterSnapshotResponse struct {
	XMLName           xml.Name             `xml:"CreateDBClusterSnapshotResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	DBClusterSnapshot xmlDBClusterSnapshot `xml:"CreateDBClusterSnapshotResult>DBClusterSnapshot"`
}

type describeDBClusterSnapshotsResult struct {
	Marker             string                   `xml:"Marker,omitempty"`
	DBClusterSnapshots xmlDBClusterSnapshotList `xml:"DBClusterSnapshots"`
}

type describeDBClusterSnapshotsResponse struct {
	XMLName xml.Name                         `xml:"DescribeDBClusterSnapshotsResponse"`
	Xmlns   string                           `xml:"xmlns,attr"`
	Result  describeDBClusterSnapshotsResult `xml:"DescribeDBClusterSnapshotsResult"`
}

type deleteDBClusterSnapshotResponse struct {
	XMLName           xml.Name             `xml:"DeleteDBClusterSnapshotResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	DBClusterSnapshot xmlDBClusterSnapshot `xml:"DeleteDBClusterSnapshotResult>DBClusterSnapshot"`
}

type copyDBClusterSnapshotResponse struct {
	XMLName           xml.Name             `xml:"CopyDBClusterSnapshotResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	DBClusterSnapshot xmlDBClusterSnapshot `xml:"CopyDBClusterSnapshotResult>DBClusterSnapshot"`
}

type xmlAttributeValueList struct {
	Members []string `xml:"AttributeValue"`
}

type xmlDBClusterSnapshotAttribute struct {
	AttributeName   string                `xml:"AttributeName"`
	AttributeValues xmlAttributeValueList `xml:"AttributeValues"`
}

type xmlDBClusterSnapshotAttrList struct {
	Members []xmlDBClusterSnapshotAttribute `xml:"DBClusterSnapshotAttribute"`
}

type xmlDBClusterSnapshotAttributesResult struct {
	DBClusterSnapshotIdentifier string                       `xml:"DBClusterSnapshotIdentifier"`
	DBClusterSnapshotAttributes xmlDBClusterSnapshotAttrList `xml:"DBClusterSnapshotAttributes"`
}

type describeDBClusterSnapshotAttributesResult struct {
	DBClusterSnapshotAttributesResult xmlDBClusterSnapshotAttributesResult `xml:"DBClusterSnapshotAttributesResult"`
}

type describeDBClusterSnapshotAttributesResponse struct {
	XMLName xml.Name                                  `xml:"DescribeDBClusterSnapshotAttributesResponse"`
	Xmlns   string                                    `xml:"xmlns,attr"`
	Result  describeDBClusterSnapshotAttributesResult `xml:"DescribeDBClusterSnapshotAttributesResult"`
}

type modifyDBClusterSnapshotAttributeResult struct {
	DBClusterSnapshotAttributesResult xmlDBClusterSnapshotAttributesResult `xml:"DBClusterSnapshotAttributesResult"`
}

type modifyDBClusterSnapshotAttributeResponse struct {
	XMLName xml.Name                               `xml:"ModifyDBClusterSnapshotAttributeResponse"`
	Xmlns   string                                 `xml:"xmlns,attr"`
	Result  modifyDBClusterSnapshotAttributeResult `xml:"ModifyDBClusterSnapshotAttributeResult"`
}

// dispatchSnapshotAndEndpointAction handles DBClusterSnapshot and
// DBClusterEndpoint actions together to keep each switch's cyclomatic
// complexity within lint limits; see dispatch's doc comment for the chaining
// rationale.
func (h *Handler) dispatchSnapshotAndEndpointAction(
	ctx context.Context, action string, vals url.Values,
) (any, error) {
	switch action {
	case "CreateDBClusterSnapshot":
		return h.handleCreateDBClusterSnapshot(ctx, vals)
	case "DescribeDBClusterSnapshots":
		return h.handleDescribeDBClusterSnapshots(ctx, vals)
	case "DeleteDBClusterSnapshot":
		return h.handleDeleteDBClusterSnapshot(ctx, vals)
	case "CopyDBClusterSnapshot":
		return h.handleCopyDBClusterSnapshot(ctx, vals)
	case "DescribeDBClusterSnapshotAttributes":
		return h.handleDescribeDBClusterSnapshotAttributes(ctx, vals)
	case "ModifyDBClusterSnapshotAttribute":
		return h.handleModifyDBClusterSnapshotAttribute(ctx, vals)
	case "CreateDBClusterEndpoint":
		return h.handleCreateDBClusterEndpoint(ctx, vals)
	case "DeleteDBClusterEndpoint":
		return h.handleDeleteDBClusterEndpoint(ctx, vals)
	case "DescribeDBClusterEndpoints":
		return h.handleDescribeDBClusterEndpoints(ctx, vals)
	case "ModifyDBClusterEndpoint":
		return h.handleModifyDBClusterEndpoint(ctx, vals)
	default:
		return h.dispatchEventSubscriptionAction(ctx, action, vals)
	}
}
