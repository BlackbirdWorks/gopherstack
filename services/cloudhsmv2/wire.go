package cloudhsmv2

// Wire shapes mirror cloudhsmv2 v1.43.4 types; timestamps are epoch-seconds numbers (awsjson1.1).

type wireTag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type wireRetention struct {
	Type  string `json:"Type,omitempty"`
	Value string `json:"Value,omitempty"`
}

type wireCertificates struct {
	ClusterCertificate string `json:"ClusterCertificate,omitempty"`
	ClusterCsr         string `json:"ClusterCsr,omitempty"`
}

type wireHsm struct {
	HsmID        string `json:"HsmId"`
	AZ           string `json:"AvailabilityZone,omitempty"`
	ClusterID    string `json:"ClusterId,omitempty"`
	EniID        string `json:"EniId,omitempty"`
	EniIP        string `json:"EniIp,omitempty"`
	HsmType      string `json:"HsmType,omitempty"`
	State        string `json:"State,omitempty"`
	StateMessage string `json:"StateMessage,omitempty"`
	SubnetID     string `json:"SubnetId,omitempty"`
}

type wireCluster struct {
	BackupRetentionPolicy     *wireRetention    `json:"BackupRetentionPolicy,omitempty"`
	Certificates              *wireCertificates `json:"Certificates,omitempty"`
	SubnetMapping             map[string]string `json:"SubnetMapping,omitempty"`
	BackupPolicy              string            `json:"BackupPolicy,omitempty"`
	ClusterID                 string            `json:"ClusterId"`
	HsmType                   string            `json:"HsmType,omitempty"`
	Mode                      string            `json:"Mode,omitempty"`
	NetworkType               string            `json:"NetworkType,omitempty"`
	SecurityGroup             string            `json:"SecurityGroup,omitempty"`
	SourceBackupID            string            `json:"SourceBackupId,omitempty"`
	State                     string            `json:"State,omitempty"`
	StateMessage              string            `json:"StateMessage,omitempty"`
	VpcID                     string            `json:"VpcId,omitempty"`
	Hsms                      []wireHsm         `json:"Hsms"`
	TagList                   []wireTag         `json:"TagList"`
	CreateTimestamp           float64           `json:"CreateTimestamp,omitempty"`
	HsmTypeRollbackExpiration float64           `json:"HsmTypeRollbackExpiration,omitempty"`
}

type wireBackup struct {
	BackupARN       string    `json:"BackupArn,omitempty"`
	BackupID        string    `json:"BackupId"`
	BackupState     string    `json:"BackupState,omitempty"`
	ClusterID       string    `json:"ClusterId,omitempty"`
	HsmType         string    `json:"HsmType,omitempty"`
	Mode            string    `json:"Mode,omitempty"`
	SourceBackup    string    `json:"SourceBackup,omitempty"`
	SourceCluster   string    `json:"SourceCluster,omitempty"`
	SourceRegion    string    `json:"SourceRegion,omitempty"`
	TagList         []wireTag `json:"TagList"`
	CopyTimestamp   float64   `json:"CopyTimestamp,omitempty"`
	CreateTimestamp float64   `json:"CreateTimestamp,omitempty"`
	DeleteTimestamp float64   `json:"DeleteTimestamp,omitempty"`
	NeverExpires    bool      `json:"NeverExpires"`
}

type wireDestinationBackup struct {
	SourceBackup    string  `json:"SourceBackup,omitempty"`
	SourceCluster   string  `json:"SourceCluster,omitempty"`
	SourceRegion    string  `json:"SourceRegion,omitempty"`
	CreateTimestamp float64 `json:"CreateTimestamp,omitempty"`
}
