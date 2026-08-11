package docdb

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
)

func (h *Handler) handleCreateDBClusterSnapshot(ctx context.Context, vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	tags := parseTags(vals)
	snap, err := h.Backend.CreateDBClusterSnapshot(ctx, snapshotID, clusterID, tags)
	if err != nil {
		return nil, err
	}

	return &createDBClusterSnapshotResponse{
		Xmlns:             docdbXMLNS,
		DBClusterSnapshot: toXMLClusterSnapshot(snap),
	}, nil
}

func (h *Handler) handleDescribeDBClusterSnapshots(ctx context.Context, vals url.Values) (any, error) {
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

	members, nextMarker := applyDocDBMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBClusterSnapshotsResponse{
		Xmlns: docdbXMLNS,
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
		Xmlns:             docdbXMLNS,
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
		Xmlns:             docdbXMLNS,
		DBClusterSnapshot: toXMLClusterSnapshot(snap),
	}, nil
}

func (h *Handler) handleDescribeDBClusterSnapshotAttributes(ctx context.Context, vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	result, err := h.Backend.DescribeDBClusterSnapshotAttributes(ctx, snapshotID)
	if err != nil {
		return nil, err
	}
	attrs := make([]xmlDBClusterSnapshotAttribute, 0, len(result.Attributes))
	for _, a := range result.Attributes {
		values := make([]string, len(a.AttributeValues))
		copy(values, a.AttributeValues)
		attrs = append(attrs, xmlDBClusterSnapshotAttribute{
			AttributeName:   a.AttributeName,
			AttributeValues: xmlAttributeValueList{Members: values},
		})
	}

	return &describeDBClusterSnapshotAttributesResponse{
		Xmlns: docdbXMLNS,
		Result: describeDBClusterSnapshotAttributesResult{
			DBClusterSnapshotAttributesResult: xmlDBClusterSnapshotAttributesResult{
				DBClusterSnapshotIdentifier: result.DBClusterSnapshotIdentifier,
				DBClusterSnapshotAttributes: xmlDBClusterSnapshotAttributeList{Members: attrs},
			},
		},
	}, nil
}

func (h *Handler) handleModifyDBClusterSnapshotAttribute(ctx context.Context, vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	attributeName := vals.Get("AttributeName")
	valuesToAdd := parseAttributeValueMembers(vals, "ValuesToAdd")
	valuesToRemove := parseAttributeValueMembers(vals, "ValuesToRemove")
	result, err := h.Backend.ModifyDBClusterSnapshotAttribute(
		ctx,
		snapshotID,
		attributeName,
		valuesToAdd,
		valuesToRemove,
	)
	if err != nil {
		return nil, err
	}
	attrs := make([]xmlDBClusterSnapshotAttribute, 0, len(result.Attributes))
	for _, a := range result.Attributes {
		attrCopy := make([]string, len(a.AttributeValues))
		copy(attrCopy, a.AttributeValues)
		attrs = append(attrs, xmlDBClusterSnapshotAttribute{
			AttributeName:   a.AttributeName,
			AttributeValues: xmlAttributeValueList{Members: attrCopy},
		})
	}

	return &modifyDBClusterSnapshotAttributeResponse{
		Xmlns: docdbXMLNS,
		Result: modifyDBClusterSnapshotAttributeResult{
			DBClusterSnapshotAttributesResult: xmlDBClusterSnapshotAttributesResult{
				DBClusterSnapshotIdentifier: result.DBClusterSnapshotIdentifier,
				DBClusterSnapshotAttributes: xmlDBClusterSnapshotAttributeList{Members: attrs},
			},
		},
	}, nil
}

func toXMLClusterSnapshot(snap *DBClusterSnapshot) xmlDBClusterSnapshot {
	return xmlDBClusterSnapshot{
		DBClusterSnapshotIdentifier: snap.DBClusterSnapshotIdentifier,
		DBClusterIdentifier:         snap.DBClusterIdentifier,
		DBClusterArn:                snap.DBClusterArn,
		DBClusterSnapshotArn:        snap.DBClusterSnapshotArn,
		Engine:                      snap.Engine,
		Status:                      snap.Status,
		SnapshotType:                snap.SnapshotType,
		SnapshotCreateTime:          snap.SnapshotCreateTime,
		EngineVersion:               snap.EngineVersion,
		PercentProgress:             snap.PercentProgress,
		StorageEncrypted:            snap.StorageEncrypted,
	}
}

type xmlDBClusterSnapshot struct {
	DBClusterSnapshotIdentifier string `xml:"DBClusterSnapshotIdentifier"`
	DBClusterIdentifier         string `xml:"DBClusterIdentifier"`
	DBClusterArn                string `xml:"DBClusterArn,omitempty"`
	DBClusterSnapshotArn        string `xml:"DBClusterSnapshotArn,omitempty"`
	Engine                      string `xml:"Engine"`
	Status                      string `xml:"Status"`
	SnapshotType                string `xml:"SnapshotType,omitempty"`
	SnapshotCreateTime          string `xml:"SnapshotCreateTime,omitempty"`
	EngineVersion               string `xml:"EngineVersion,omitempty"`
	PercentProgress             int    `xml:"PercentProgress"`
	StorageEncrypted            bool   `xml:"StorageEncrypted"`
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

type xmlDBClusterSnapshotAttributeList struct {
	Members []xmlDBClusterSnapshotAttribute `xml:"DBClusterSnapshotAttribute"`
}

type xmlDBClusterSnapshotAttributesResult struct {
	DBClusterSnapshotIdentifier string                            `xml:"DBClusterSnapshotIdentifier"`
	DBClusterSnapshotAttributes xmlDBClusterSnapshotAttributeList `xml:"DBClusterSnapshotAttributes"`
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

func parseAttributeValueMembers(vals url.Values, prefix string) []string {
	var values []string
	for i := 1; ; i++ {
		v := vals.Get(fmt.Sprintf("%s.AttributeValue.%d", prefix, i))
		if v == "" {
			return values
		}
		values = append(values, v)
	}
}
