package rds

import (
	"encoding/xml"
	"net/url"
)

type xmlDBClusterAutomatedBackup struct {
	DBClusterIdentifier   string `xml:"DBClusterIdentifier"`
	DBClusterResourceID   string `xml:"DbClusterResourceId,omitempty"`
	Engine                string `xml:"Engine,omitempty"`
	EngineVersion         string `xml:"EngineVersion,omitempty"`
	Region                string `xml:"Region,omitempty"`
	Status                string `xml:"Status,omitempty"`
	BackupRetentionPeriod int    `xml:"BackupRetentionPeriod,omitempty"`
	StorageEncrypted      bool   `xml:"StorageEncrypted,omitempty"`
}

type xmlDBClusterAutomatedBackupList struct {
	Members []xmlDBClusterAutomatedBackup `xml:"DBClusterAutomatedBackup"`
}

type deleteDBClusterAutomatedBackupResult struct {
	DBClusterAutomatedBackup xmlDBClusterAutomatedBackup `xml:"DBClusterAutomatedBackup"`
}

type deleteDBClusterAutomatedBackupResponse struct {
	XMLName xml.Name                             `xml:"DeleteDBClusterAutomatedBackupResponse"`
	Xmlns   string                               `xml:"xmlns,attr"`
	Result  deleteDBClusterAutomatedBackupResult `xml:"DeleteDBClusterAutomatedBackupResult"`
}

type describeDBClusterAutomatedBackupsResult struct {
	DBClusterAutomatedBackups xmlDBClusterAutomatedBackupList `xml:"DBClusterAutomatedBackups"`
}

type describeDBClusterAutomatedBackupsResponse struct {
	XMLName xml.Name                                `xml:"DescribeDBClusterAutomatedBackupsResponse"`
	Xmlns   string                                  `xml:"xmlns,attr"`
	Result  describeDBClusterAutomatedBackupsResult `xml:"DescribeDBClusterAutomatedBackupsResult"`
}

type xmlDBInstanceAutomatedBackup struct {
	DBInstanceIdentifier  string `xml:"DBInstanceIdentifier"`
	DbiResourceID         string `xml:"DbiResourceId,omitempty"`
	Engine                string `xml:"Engine,omitempty"`
	EngineVersion         string `xml:"EngineVersion,omitempty"`
	DBInstanceArn         string `xml:"DBInstanceArn,omitempty"`
	Region                string `xml:"Region,omitempty"`
	Status                string `xml:"Status,omitempty"`
	AllocatedStorage      int    `xml:"AllocatedStorage,omitempty"`
	BackupRetentionPeriod int    `xml:"BackupRetentionPeriod,omitempty"`
}

type xmlDBInstanceAutomatedBackupList struct {
	Members []xmlDBInstanceAutomatedBackup `xml:"DBInstanceAutomatedBackup"`
}

type deleteDBInstanceAutomatedBackupResult struct {
	DBInstanceAutomatedBackup xmlDBInstanceAutomatedBackup `xml:"DBInstanceAutomatedBackup"`
}

type deleteDBInstanceAutomatedBackupResponse struct {
	XMLName xml.Name                              `xml:"DeleteDBInstanceAutomatedBackupResponse"`
	Xmlns   string                                `xml:"xmlns,attr"`
	Result  deleteDBInstanceAutomatedBackupResult `xml:"DeleteDBInstanceAutomatedBackupResult"`
}

type describeDBInstanceAutomatedBackupsResult struct {
	DBInstanceAutomatedBackups xmlDBInstanceAutomatedBackupList `xml:"DBInstanceAutomatedBackups"`
}

type describeDBInstanceAutomatedBackupsResponse struct {
	XMLName xml.Name                                 `xml:"DescribeDBInstanceAutomatedBackupsResponse"`
	Xmlns   string                                   `xml:"xmlns,attr"`
	Result  describeDBInstanceAutomatedBackupsResult `xml:"DescribeDBInstanceAutomatedBackupsResult"`
}

type startDBInstanceAutomatedBackupsReplicationResult struct {
	DBInstanceAutomatedBackup xmlDBInstanceAutomatedBackup `xml:"DBInstanceAutomatedBackup"`
}

type startDBInstanceAutomatedBackupsReplicationResponse struct {
	XMLName xml.Name                                         `xml:"StartDBInstanceAutomatedBackupsReplicationResponse"`
	Xmlns   string                                           `xml:"xmlns,attr"`
	Result  startDBInstanceAutomatedBackupsReplicationResult `xml:"StartDBInstanceAutomatedBackupsReplicationResult"`
}

type stopDBInstanceAutomatedBackupsReplicationResult struct {
	DBInstanceAutomatedBackup xmlDBInstanceAutomatedBackup `xml:"DBInstanceAutomatedBackup"`
}

type stopDBInstanceAutomatedBackupsReplicationResponse struct {
	XMLName xml.Name                                        `xml:"StopDBInstanceAutomatedBackupsReplicationResponse"`
	Xmlns   string                                          `xml:"xmlns,attr"`
	Result  stopDBInstanceAutomatedBackupsReplicationResult `xml:"StopDBInstanceAutomatedBackupsReplicationResult"`
}

func (h *Handler) handleDeleteDBClusterAutomatedBackup(vals url.Values) (any, error) {
	resourceID := vals.Get("DbClusterResourceId")
	if resourceID == "" {
		resourceID = vals.Get("DBClusterIdentifier")
	}

	backup, err := h.Backend.DeleteDBClusterAutomatedBackup(resourceID)
	if err != nil {
		return nil, err
	}

	return &deleteDBClusterAutomatedBackupResponse{
		Xmlns: rdsXMLNS,
		Result: deleteDBClusterAutomatedBackupResult{
			DBClusterAutomatedBackup: toXMLClusterBackup(backup),
		},
	}, nil
}

func toXMLInstanceBackup(ab *DBInstanceAutomatedBackup) xmlDBInstanceAutomatedBackup {
	return xmlDBInstanceAutomatedBackup{
		DBInstanceIdentifier:  ab.DBInstanceIdentifier,
		DbiResourceID:         ab.DbiResourceID,
		Engine:                ab.Engine,
		EngineVersion:         ab.EngineVersion,
		DBInstanceArn:         ab.DBInstanceArn,
		Region:                ab.Region,
		Status:                ab.Status,
		AllocatedStorage:      ab.AllocatedStorage,
		BackupRetentionPeriod: ab.BackupRetentionPeriod,
	}
}

func (h *Handler) handleDescribeDBClusterAutomatedBackups(vals url.Values) (any, error) {
	clusterID := vals.Get("DBClusterIdentifier")
	backups := h.Backend.DescribeDBClusterAutomatedBackups(clusterID)

	members := make([]xmlDBClusterAutomatedBackup, 0, len(backups))
	for i := range backups {
		members = append(members, toXMLClusterBackup(&backups[i]))
	}

	return &describeDBClusterAutomatedBackupsResponse{
		Xmlns: rdsXMLNS,
		Result: describeDBClusterAutomatedBackupsResult{
			DBClusterAutomatedBackups: xmlDBClusterAutomatedBackupList{Members: members},
		},
	}, nil
}

func (h *Handler) handleDeleteDBInstanceAutomatedBackup(vals url.Values) (any, error) {
	resourceID := vals.Get("DbiResourceId")
	if resourceID == "" {
		resourceID = vals.Get("DBInstanceIdentifier")
	}

	backup, err := h.Backend.DeleteDBInstanceAutomatedBackup(resourceID)
	if err != nil {
		return nil, err
	}

	return &deleteDBInstanceAutomatedBackupResponse{
		Xmlns: rdsXMLNS,
		Result: deleteDBInstanceAutomatedBackupResult{
			DBInstanceAutomatedBackup: toXMLInstanceBackup(backup),
		},
	}, nil
}

func (h *Handler) handleDescribeDBInstanceAutomatedBackups(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")
	backups := h.Backend.DescribeDBInstanceAutomatedBackups(instanceID)
	members := make([]xmlDBInstanceAutomatedBackup, 0, len(backups))

	for i := range backups {
		members = append(members, toXMLInstanceBackup(&backups[i]))
	}

	return &describeDBInstanceAutomatedBackupsResponse{
		Xmlns: rdsXMLNS,
		Result: describeDBInstanceAutomatedBackupsResult{
			DBInstanceAutomatedBackups: xmlDBInstanceAutomatedBackupList{Members: members},
		},
	}, nil
}

func (h *Handler) handleStartDBInstanceAutomatedBackupsReplication(vals url.Values) (any, error) {
	sourceARN := vals.Get("SourceDBInstanceArn")
	retentionPeriod := parseInt(vals.Get("BackupRetentionPeriod"))

	backup, err := h.Backend.StartDBInstanceAutomatedBackupsReplication(sourceARN, retentionPeriod)
	if err != nil {
		return nil, err
	}

	return &startDBInstanceAutomatedBackupsReplicationResponse{
		Xmlns: rdsXMLNS,
		Result: startDBInstanceAutomatedBackupsReplicationResult{
			DBInstanceAutomatedBackup: toXMLInstanceBackup(backup),
		},
	}, nil
}

func (h *Handler) handleStopDBInstanceAutomatedBackupsReplication(vals url.Values) (any, error) {
	sourceARN := vals.Get("SourceDBInstanceArn")

	backup, err := h.Backend.StopDBInstanceAutomatedBackupsReplication(sourceARN)
	if err != nil {
		return nil, err
	}

	return &stopDBInstanceAutomatedBackupsReplicationResponse{
		Xmlns: rdsXMLNS,
		Result: stopDBInstanceAutomatedBackupsReplicationResult{
			DBInstanceAutomatedBackup: toXMLInstanceBackup(backup),
		},
	}, nil
}
