package rds

import (
	"encoding/xml"
	"net/url"
)

type xmlTenantDatabase struct {
	MasterUserSecret     *xmlMasterUserSecret `xml:"MasterUserSecret,omitempty"`
	TenantDatabaseName   string               `xml:"TenantDBName"`
	TenantDatabaseARN    string               `xml:"TenantDatabaseARN,omitempty"`
	DBInstanceIdentifier string               `xml:"DBInstanceIdentifier,omitempty"`
	Status               string               `xml:"Status,omitempty"`
}

func toXMLTenantDatabase(tdb *TenantDatabase) xmlTenantDatabase {
	return xmlTenantDatabase{
		TenantDatabaseName:   tdb.TenantDBName,
		TenantDatabaseARN:    tdb.TenantDatabaseARN,
		DBInstanceIdentifier: tdb.DBInstanceIdentifier,
		Status:               tdb.Status,
		MasterUserSecret:     tdb.MasterSecret.toXML(),
	}
}

type xmlTenantDatabaseList struct {
	Members []xmlTenantDatabase `xml:"TenantDatabase"`
}

type createTenantDatabaseResponse struct {
	TenantDatabase xmlTenantDatabase `xml:"CreateTenantDatabaseResult>TenantDatabase"`
	XMLName        xml.Name          `xml:"CreateTenantDatabaseResponse"`
	Xmlns          string            `xml:"xmlns,attr"`
}

type deleteTenantDatabaseResponse struct {
	TenantDatabase xmlTenantDatabase `xml:"DeleteTenantDatabaseResult>TenantDatabase"`
	XMLName        xml.Name          `xml:"DeleteTenantDatabaseResponse"`
	Xmlns          string            `xml:"xmlns,attr"`
}

type describeTenantDatabasesResponse struct {
	XMLName         xml.Name              `xml:"DescribeTenantDatabasesResponse"`
	Xmlns           string                `xml:"xmlns,attr"`
	Marker          string                `xml:"DescribeTenantDatabasesResult>Marker,omitempty"`
	TenantDatabases xmlTenantDatabaseList `xml:"DescribeTenantDatabasesResult>TenantDatabases"`
}

type modifyTenantDatabaseResponse struct {
	TenantDatabase xmlTenantDatabase `xml:"ModifyTenantDatabaseResult>TenantDatabase"`
	XMLName        xml.Name          `xml:"ModifyTenantDatabaseResponse"`
	Xmlns          string            `xml:"xmlns,attr"`
}

type xmlDBSnapshotTenantDatabase struct {
	DBSnapshotIdentifier string `xml:"DBSnapshotIdentifier"`
	TenantDatabaseName   string `xml:"TenantDBName,omitempty"`
}

type xmlDBSnapshotTenantDatabaseList struct {
	Members []xmlDBSnapshotTenantDatabase `xml:"DBSnapshotTenantDatabase"`
}

type describeDBSnapshotTenantDatabasesResult struct {
	DBSnapshotTenantDatabases xmlDBSnapshotTenantDatabaseList `xml:"DBSnapshotTenantDatabases"`
}

type describeDBSnapshotTenantDatabasesResponse struct {
	XMLName xml.Name                                `xml:"DescribeDBSnapshotTenantDatabasesResponse"`
	Xmlns   string                                  `xml:"xmlns,attr"`
	Result  describeDBSnapshotTenantDatabasesResult `xml:"DescribeDBSnapshotTenantDatabasesResult"`
}

func (h *Handler) handleCreateTenantDatabase(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")
	tenantDBName := vals.Get("TenantDBName")
	masterUsername := vals.Get("MasterUsername")

	tdb, err := h.Backend.CreateTenantDatabaseWithSecret(
		instanceID, tenantDBName, masterUsername, vals.Get("MasterUserPassword"), parseMasterSecretRequest(vals),
	)
	if err != nil {
		return nil, err
	}

	h.applyCreateTags(vals, tdb.TenantDatabaseARN)

	return &createTenantDatabaseResponse{
		Xmlns:          rdsXMLNS,
		TenantDatabase: toXMLTenantDatabase(tdb),
	}, nil
}

func (h *Handler) handleDeleteTenantDatabase(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")
	tenantDBName := vals.Get("TenantDBName")

	tdb, err := h.Backend.DeleteTenantDatabase(instanceID, tenantDBName, DeleteTenantDatabaseOptions{
		FinalDBSnapshotIdentifier: vals.Get("FinalDBSnapshotIdentifier"),
		SkipFinalSnapshot:         vals.Get("SkipFinalSnapshot") == formTrue,
	})
	if err != nil {
		return nil, err
	}

	return &deleteTenantDatabaseResponse{
		Xmlns:          rdsXMLNS,
		TenantDatabase: toXMLTenantDatabase(tdb),
	}, nil
}

func (h *Handler) handleDescribeTenantDatabases(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")
	tenantDBName := vals.Get("TenantDBName")

	tdbs, err := h.Backend.DescribeTenantDatabases(instanceID, tenantDBName)
	if err != nil {
		return nil, err
	}
	tdbs, err = applyTenantDatabaseFilters(vals, tdbs)
	if err != nil {
		return nil, err
	}

	members, marker, err := paginateDescribe(
		vals, tdbs,
		func(a, b TenantDatabase) bool {
			ka := a.DBInstanceIdentifier + "/" + a.TenantDBName
			kb := b.DBInstanceIdentifier + "/" + b.TenantDBName

			return ka < kb
		},
		func(tdb TenantDatabase) xmlTenantDatabase {
			return toXMLTenantDatabase(&tdb)
		},
	)
	if err != nil {
		return nil, err
	}

	return &describeTenantDatabasesResponse{
		Xmlns:           rdsXMLNS,
		Marker:          marker,
		TenantDatabases: xmlTenantDatabaseList{Members: members},
	}, nil
}

func (h *Handler) handleModifyTenantDatabase(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")
	tenantDBName := vals.Get("TenantDBName")
	newTenantDBName := vals.Get("NewTenantDBName")

	tdb, err := h.Backend.ModifyTenantDatabaseWithSecret(
		instanceID, tenantDBName, newTenantDBName, vals.Get("MasterUserPassword"), parseMasterSecretRequest(vals),
	)
	if err != nil {
		return nil, err
	}

	return &modifyTenantDatabaseResponse{
		Xmlns:          rdsXMLNS,
		TenantDatabase: toXMLTenantDatabase(tdb),
	}, nil
}

func (h *Handler) handleDescribeDBSnapshotTenantDatabases(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBSnapshotIdentifier")
	instanceID := vals.Get("DBInstanceIdentifier")

	entries := h.Backend.DescribeDBSnapshotTenantDatabases(snapshotID, instanceID)
	entries, err := applyDBSnapshotTenantDatabaseFilters(vals, entries)
	if err != nil {
		return nil, err
	}

	members := make([]xmlDBSnapshotTenantDatabase, 0, len(entries))
	for _, e := range entries {
		members = append(members, xmlDBSnapshotTenantDatabase{
			DBSnapshotIdentifier: e.DBSnapshotIdentifier,
			TenantDatabaseName:   e.TenantDatabaseName,
		})
	}

	return &describeDBSnapshotTenantDatabasesResponse{
		Xmlns: rdsXMLNS,
		Result: describeDBSnapshotTenantDatabasesResult{
			DBSnapshotTenantDatabases: xmlDBSnapshotTenantDatabaseList{Members: members},
		},
	}, nil
}
