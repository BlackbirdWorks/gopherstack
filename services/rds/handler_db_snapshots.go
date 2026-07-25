package rds

import (
	"encoding/xml"
	"net/url"
	"time"
)

func (h *Handler) handleCreateDBSnapshot(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBSnapshotIdentifier")
	instanceID := vals.Get("DBInstanceIdentifier")

	snap, err := h.Backend.CreateDBSnapshot(snapshotID, instanceID)
	if err != nil {
		return nil, err
	}

	return &createDBSnapshotResponse{
		Xmlns:      rdsXMLNS,
		DBSnapshot: toXMLSnapshot(snap),
	}, nil
}

func (h *Handler) handleDescribeDBSnapshots(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBSnapshotIdentifier")
	instanceID := vals.Get("DBInstanceIdentifier")

	snaps, err := h.Backend.DescribeDBSnapshots(snapshotID, instanceID)
	if err != nil {
		return nil, err
	}
	snaps, err = applyDBSnapshotFilters(vals, snaps)
	if err != nil {
		return nil, err
	}
	members, marker, err := paginateDescribe(vals, snaps, func(a, b DBSnapshot) bool {
		return a.DBSnapshotIdentifier < b.DBSnapshotIdentifier
	}, func(item DBSnapshot) xmlDBSnapshot {
		cp := item

		return toXMLSnapshot(&cp)
	})
	if err != nil {
		return nil, err
	}

	return &describeDBSnapshotsResponse{
		Xmlns:       rdsXMLNS,
		DBSnapshots: xmlDBSnapshotList{Members: members},
		Marker:      marker,
	}, nil
}

func (h *Handler) handleDeleteDBSnapshot(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBSnapshotIdentifier")

	snap, err := h.Backend.DeleteDBSnapshot(snapshotID)
	if err != nil {
		return nil, err
	}

	return &deleteDBSnapshotResponse{
		Xmlns:      rdsXMLNS,
		DBSnapshot: toXMLSnapshot(snap),
	}, nil
}

func toXMLSnapshot(snap *DBSnapshot) xmlDBSnapshot {
	var snapshotCreateTime string
	if !snap.SnapshotCreateTime.IsZero() {
		snapshotCreateTime = snap.SnapshotCreateTime.UTC().Format(time.RFC3339)
	}

	return xmlDBSnapshot{
		DBSnapshotIdentifier: snap.DBSnapshotIdentifier,
		DBInstanceIdentifier: snap.DBInstanceIdentifier,
		DbiResourceID:        snap.DbiResourceID,
		Engine:               snap.Engine,
		EngineVersion:        snap.EngineVersion,
		Status:               snap.Status,
		AllocatedStorage:     snap.AllocatedStorage,
		Port:                 snap.Port,
		StorageType:          snap.StorageType,
		OptionGroupName:      snap.OptionGroupName,
		KmsKeyID:             snap.KmsKeyID,
		SourceRegion:         snap.SourceRegion,
		SnapshotType:         snap.SnapshotType,
		SnapshotCreateTime:   snapshotCreateTime,
		PercentProgress:      snap.PercentProgress,
		CopyTagsToSnapshot:   snap.CopyTagsToSnapshot,
		Encrypted:            snap.StorageEncrypted,
	}
}

type xmlDBSnapshot struct {
	DBSnapshotIdentifier string `xml:"DBSnapshotIdentifier"`
	DBInstanceIdentifier string `xml:"DBInstanceIdentifier"`
	DbiResourceID        string `xml:"DbiResourceId,omitempty"`
	Engine               string `xml:"Engine"`
	EngineVersion        string `xml:"EngineVersion,omitempty"`
	Status               string `xml:"Status"`
	StorageType          string `xml:"StorageType,omitempty"`
	OptionGroupName      string `xml:"OptionGroupName,omitempty"`
	KmsKeyID             string `xml:"KmsKeyId,omitempty"`
	SourceRegion         string `xml:"SourceRegion,omitempty"`
	SnapshotType         string `xml:"SnapshotType,omitempty"`
	SnapshotCreateTime   string `xml:"SnapshotCreateTime,omitempty"`
	AllocatedStorage     int    `xml:"AllocatedStorage,omitempty"`
	Port                 int    `xml:"Port,omitempty"`
	PercentProgress      int    `xml:"PercentProgress,omitempty"`
	Encrypted            bool   `xml:"Encrypted,omitempty"`
	CopyTagsToSnapshot   bool   `xml:"CopyTagsToSnapshot,omitempty"`
}

type xmlDBSnapshotList struct {
	Members []xmlDBSnapshot `xml:"DBSnapshot"`
}

type createDBSnapshotResponse struct {
	XMLName    xml.Name      `xml:"CreateDBSnapshotResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBSnapshot xmlDBSnapshot `xml:"CreateDBSnapshotResult>DBSnapshot"`
}

type deleteDBSnapshotResponse struct {
	XMLName    xml.Name      `xml:"DeleteDBSnapshotResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBSnapshot xmlDBSnapshot `xml:"DeleteDBSnapshotResult>DBSnapshot"`
}

type describeDBSnapshotsResponse struct {
	XMLName     xml.Name          `xml:"DescribeDBSnapshotsResponse"`
	Xmlns       string            `xml:"xmlns,attr"`
	Marker      string            `xml:"DescribeDBSnapshotsResult>Marker,omitempty"`
	DBSnapshots xmlDBSnapshotList `xml:"DescribeDBSnapshotsResult>DBSnapshots"`
}

type restoreDBInstanceFromDBSnapshotResponse struct {
	XMLName    xml.Name      `xml:"RestoreDBInstanceFromDBSnapshotResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"RestoreDBInstanceFromDBSnapshotResult>DBInstance"`
}

type copyDBSnapshotResponse struct {
	XMLName    xml.Name      `xml:"CopyDBSnapshotResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBSnapshot xmlDBSnapshot `xml:"CopyDBSnapshotResult>DBSnapshot"`
}

func (h *Handler) handleRestoreDBInstanceFromDBSnapshot(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	snapshotID := vals.Get("DBSnapshotIdentifier")
	opts := DBInstanceOptions{
		MultiAZ:            vals.Get("MultiAZ") == formTrue,
		DeletionProtection: vals.Get("DeletionProtection") == formTrue,
		StorageType:        vals.Get("StorageType"),
		AvailabilityZone:   vals.Get("AvailabilityZone"),
	}

	inst, err := h.Backend.RestoreDBInstanceFromDBSnapshot(id, snapshotID, opts)
	if err != nil {
		return nil, err
	}

	return &restoreDBInstanceFromDBSnapshotResponse{
		Xmlns:      rdsXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleCopyDBSnapshot(vals url.Values) (any, error) {
	sourceSnapshotID := vals.Get("SourceDBSnapshotIdentifier")
	targetSnapshotID := vals.Get("TargetDBSnapshotIdentifier")

	opts := CopyDBSnapshotOptions{
		KmsKeyID:     vals.Get("KmsKeyId"),
		SourceRegion: vals.Get("SourceRegion"),
		CopyTags:     vals.Get("CopyTags") == formTrue,
	}

	snap, err := h.Backend.CopyDBSnapshot(sourceSnapshotID, targetSnapshotID, opts)
	if err != nil {
		return nil, err
	}

	return &copyDBSnapshotResponse{
		Xmlns:      rdsXMLNS,
		DBSnapshot: toXMLSnapshot(snap),
	}, nil
}

type xmlDBSnapshotAttribute struct {
	AttributeName   string          `xml:"AttributeName"`
	AttributeValues xmlStringValues `xml:"AttributeValues"`
}

type xmlStringValues struct {
	Members []string `xml:"AttributeValue"`
}

type xmlDBSnapshotAttributeList struct {
	Members []xmlDBSnapshotAttribute `xml:"DBSnapshotAttribute"`
}

type xmlDBSnapshotAttributesResult struct {
	DBSnapshotIdentifier string                     `xml:"DBSnapshotIdentifier"`
	DBSnapshotAttributes xmlDBSnapshotAttributeList `xml:"DBSnapshotAttributes"`
}

type describeDBSnapshotAttributesResponse struct {
	XMLName xml.Name                      `xml:"DescribeDBSnapshotAttributesResponse"`
	Xmlns   string                        `xml:"xmlns,attr"`
	Result  xmlDBSnapshotAttributesResult `xml:"DescribeDBSnapshotAttributesResult>DBSnapshotAttributesResult"`
}

type modifyDBSnapshotResponse struct {
	XMLName    xml.Name      `xml:"ModifyDBSnapshotResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBSnapshot xmlDBSnapshot `xml:"ModifyDBSnapshotResult>DBSnapshot"`
}

type modifyDBSnapshotAttributeResponse struct {
	XMLName xml.Name                      `xml:"ModifyDBSnapshotAttributeResponse"`
	Xmlns   string                        `xml:"xmlns,attr"`
	Result  xmlDBSnapshotAttributesResult `xml:"ModifyDBSnapshotAttributeResult>DBSnapshotAttributesResult"`
}

func (h *Handler) handleDescribeDBSnapshotAttributes(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBSnapshotIdentifier")
	result, err := h.Backend.DescribeDBSnapshotAttributes(snapshotID)
	if err != nil {
		return nil, err
	}

	return &describeDBSnapshotAttributesResponse{
		Xmlns:  rdsXMLNS,
		Result: toXMLSnapshotAttributesResult(result),
	}, nil
}

func (h *Handler) handleModifyDBSnapshot(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBSnapshotIdentifier")
	optionGroupName := vals.Get("OptionGroupName")
	engineVersion := vals.Get("EngineVersion")
	snap, err := h.Backend.ModifyDBSnapshot(snapshotID, optionGroupName, engineVersion)
	if err != nil {
		return nil, err
	}

	return &modifyDBSnapshotResponse{
		Xmlns:      rdsXMLNS,
		DBSnapshot: toXMLSnapshot(snap),
	}, nil
}

func (h *Handler) handleModifyDBSnapshotAttribute(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBSnapshotIdentifier")
	attributeName := vals.Get("AttributeName")
	valuesToAdd := extractMemberList(vals, "ValuesToAdd.member.")
	valuesToRemove := extractMemberList(vals, "ValuesToRemove.member.")
	result, err := h.Backend.ModifyDBSnapshotAttribute(snapshotID, attributeName, valuesToAdd, valuesToRemove)
	if err != nil {
		return nil, err
	}

	return &modifyDBSnapshotAttributeResponse{
		Xmlns:  rdsXMLNS,
		Result: toXMLSnapshotAttributesResult(result),
	}, nil
}

func toXMLSnapshotAttributesResult(r *DBSnapshotAttributesResult) xmlDBSnapshotAttributesResult {
	attrs := make([]xmlDBSnapshotAttribute, 0, len(r.DBSnapshotAttributes))
	for _, a := range r.DBSnapshotAttributes {
		vals := make([]string, len(a.AttributeValues))
		copy(vals, a.AttributeValues)
		attrs = append(attrs, xmlDBSnapshotAttribute{
			AttributeName:   a.AttributeName,
			AttributeValues: xmlStringValues{Members: vals},
		})
	}

	return xmlDBSnapshotAttributesResult{
		DBSnapshotIdentifier: r.DBSnapshotIdentifier,
		DBSnapshotAttributes: xmlDBSnapshotAttributeList{Members: attrs},
	}
}
