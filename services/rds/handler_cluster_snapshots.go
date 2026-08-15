package rds

import (
	"encoding/xml"
	"net/url"
	"time"
)

func (h *Handler) handleCreateDBClusterSnapshot(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	snap, err := h.Backend.CreateDBClusterSnapshot(snapshotID, clusterID)
	if err != nil {
		return nil, err
	}

	h.applyCreateTags(vals, snap.DBClusterSnapshotArn)

	return &createDBClusterSnapshotResponse{
		Xmlns:             rdsXMLNS,
		DBClusterSnapshot: toXMLClusterSnapshot(snap),
	}, nil
}

func (h *Handler) handleDescribeDBClusterSnapshots(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	snaps, err := h.Backend.DescribeDBClusterSnapshots(snapshotID, clusterID)
	if err != nil {
		return nil, err
	}
	snaps, err = applyDBClusterSnapshotFilters(vals, snaps)
	if err != nil {
		return nil, err
	}
	members, marker, err := paginateDescribe(vals, snaps, func(a, b DBClusterSnapshot) bool {
		return a.DBClusterSnapshotIdentifier < b.DBClusterSnapshotIdentifier
	}, func(item DBClusterSnapshot) xmlDBClusterSnapshot {
		cp := item

		return toXMLClusterSnapshot(&cp)
	})
	if err != nil {
		return nil, err
	}

	return &describeDBClusterSnapshotsResponse{
		Xmlns:              rdsXMLNS,
		DBClusterSnapshots: xmlDBClusterSnapshotList{Members: members},
		Marker:             marker,
	}, nil
}

func (h *Handler) handleDeleteDBClusterSnapshot(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	snap, err := h.Backend.DeleteDBClusterSnapshot(snapshotID)
	if err != nil {
		return nil, err
	}

	return &deleteDBClusterSnapshotResponse{
		Xmlns:             rdsXMLNS,
		DBClusterSnapshot: toXMLClusterSnapshot(snap),
	}, nil
}

func (h *Handler) handleCopyDBClusterSnapshot(vals url.Values) (any, error) {
	sourceSnapshotID := vals.Get("SourceDBClusterSnapshotIdentifier")
	targetSnapshotID := vals.Get("TargetDBClusterSnapshotIdentifier")
	snap, err := h.Backend.CopyDBClusterSnapshot(sourceSnapshotID, targetSnapshotID)
	if err != nil {
		return nil, err
	}

	return &copyDBClusterSnapshotResponse{
		Xmlns:             rdsXMLNS,
		DBClusterSnapshot: toXMLClusterSnapshot(snap),
	}, nil
}

func toXMLClusterSnapshot(s *DBClusterSnapshot) xmlDBClusterSnapshot {
	var snapshotCreateTime string
	if !s.SnapshotCreateTime.IsZero() {
		snapshotCreateTime = s.SnapshotCreateTime.UTC().Format(time.RFC3339)
	}

	return xmlDBClusterSnapshot{
		DBClusterSnapshotIdentifier: s.DBClusterSnapshotIdentifier,
		DBClusterSnapshotArn:        s.DBClusterSnapshotArn,
		DBClusterIdentifier:         s.DBClusterIdentifier,
		DBClusterResourceID:         s.DBClusterResourceID,
		Engine:                      s.Engine,
		EngineVersion:               s.EngineVersion,
		Status:                      s.Status,
		SnapshotType:                s.SnapshotType,
		SnapshotCreateTime:          snapshotCreateTime,
		PercentProgress:             s.PercentProgress,
		StorageEncrypted:            s.StorageEncrypted,
	}
}

type xmlDBClusterSnapshot struct {
	DBClusterSnapshotIdentifier string `xml:"DBClusterSnapshotIdentifier"`
	DBClusterSnapshotArn        string `xml:"DBClusterSnapshotArn,omitempty"`
	DBClusterIdentifier         string `xml:"DBClusterIdentifier"`
	DBClusterResourceID         string `xml:"DbClusterResourceId,omitempty"`
	Engine                      string `xml:"Engine"`
	EngineVersion               string `xml:"EngineVersion,omitempty"`
	Status                      string `xml:"Status"`
	SnapshotType                string `xml:"SnapshotType,omitempty"`
	SnapshotCreateTime          string `xml:"SnapshotCreateTime,omitempty"`
	PercentProgress             int    `xml:"PercentProgress,omitempty"`
	StorageEncrypted            bool   `xml:"StorageEncrypted,omitempty"`
}

type xmlDBClusterSnapshotList struct {
	Members []xmlDBClusterSnapshot `xml:"DBClusterSnapshot"`
}

type createDBClusterSnapshotResponse struct {
	XMLName           xml.Name             `xml:"CreateDBClusterSnapshotResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	DBClusterSnapshot xmlDBClusterSnapshot `xml:"CreateDBClusterSnapshotResult>DBClusterSnapshot"`
}

type describeDBClusterSnapshotsResponse struct {
	XMLName            xml.Name                 `xml:"DescribeDBClusterSnapshotsResponse"`
	Xmlns              string                   `xml:"xmlns,attr"`
	Marker             string                   `xml:"DescribeDBClusterSnapshotsResult>Marker,omitempty"`
	DBClusterSnapshots xmlDBClusterSnapshotList `xml:"DescribeDBClusterSnapshotsResult>DBClusterSnapshots"`
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

// xmlDBClusterSnapshotAttributeList's member tag is "DBClusterSnapshotAttribute",
// distinct from the plain-snapshot xmlDBSnapshotAttributeList's "DBSnapshotAttribute"
// (rds@v1.124.1 deserializers.go: awsAwsquery_deserializeDocumentDBClusterSnapshotAttributeList).
type xmlDBClusterSnapshotAttributeList struct {
	Members []xmlDBSnapshotAttribute `xml:"DBClusterSnapshotAttribute"`
}

type xmlDBClusterSnapshotAttributesResult struct {
	DBClusterSnapshotIdentifier string                            `xml:"DBClusterSnapshotIdentifier"`
	DBClusterSnapshotAttributes xmlDBClusterSnapshotAttributeList `xml:"DBClusterSnapshotAttributes"`
}

type xmlClusterSnapshotAttrWrapper struct {
	Result xmlDBClusterSnapshotAttributesResult `xml:"DBClusterSnapshotAttributesResult"`
}

type describeDBClusterSnapshotAttributesResponse struct {
	XMLName xml.Name                      `xml:"DescribeDBClusterSnapshotAttributesResponse"`
	Xmlns   string                        `xml:"xmlns,attr"`
	Wrapper xmlClusterSnapshotAttrWrapper `xml:"DescribeDBClusterSnapshotAttributesResult"`
}

type modifyDBClusterSnapshotAttributeResponse struct {
	XMLName xml.Name                      `xml:"ModifyDBClusterSnapshotAttributeResponse"`
	Xmlns   string                        `xml:"xmlns,attr"`
	Wrapper xmlClusterSnapshotAttrWrapper `xml:"ModifyDBClusterSnapshotAttributeResult"`
}

func (h *Handler) handleDescribeDBClusterSnapshotAttributes(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	result, err := h.Backend.DescribeDBClusterSnapshotAttributes(snapshotID)
	if err != nil {
		return nil, err
	}

	return &describeDBClusterSnapshotAttributesResponse{
		Xmlns:   rdsXMLNS,
		Wrapper: xmlClusterSnapshotAttrWrapper{Result: toXMLClusterSnapshotAttributesResult(result)},
	}, nil
}

func (h *Handler) handleModifyDBClusterSnapshotAttribute(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	attributeName := vals.Get("AttributeName")
	// Real wire key per rds@v1.124.1 serializers.go's
	// awsAwsquery_serializeDocumentAttributeValueList: the list member's
	// locationName is "AttributeValue", not the query-protocol default "member".
	valuesToAdd := extractMemberList(vals, "ValuesToAdd.AttributeValue.")
	valuesToRemove := extractMemberList(vals, "ValuesToRemove.AttributeValue.")
	result, err := h.Backend.ModifyDBClusterSnapshotAttribute(snapshotID, attributeName, valuesToAdd, valuesToRemove)
	if err != nil {
		return nil, err
	}

	return &modifyDBClusterSnapshotAttributeResponse{
		Xmlns:   rdsXMLNS,
		Wrapper: xmlClusterSnapshotAttrWrapper{Result: toXMLClusterSnapshotAttributesResult(result)},
	}, nil
}

func toXMLClusterSnapshotAttributesResult(r *DBClusterSnapshotAttributesResult) xmlDBClusterSnapshotAttributesResult {
	attrs := make([]xmlDBSnapshotAttribute, 0, len(r.DBClusterSnapshotAttributes))
	for _, a := range r.DBClusterSnapshotAttributes {
		vals := make([]string, len(a.AttributeValues))
		copy(vals, a.AttributeValues)
		attrs = append(attrs, xmlDBSnapshotAttribute{
			AttributeName:   a.AttributeName,
			AttributeValues: xmlStringValues{Members: vals},
		})
	}

	return xmlDBClusterSnapshotAttributesResult{
		DBClusterSnapshotIdentifier: r.DBClusterSnapshotIdentifier,
		DBClusterSnapshotAttributes: xmlDBClusterSnapshotAttributeList{Members: attrs},
	}
}
