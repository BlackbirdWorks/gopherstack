package cloudhsmv2

import "time"

const (
	stateCreateInProgress     = "CREATE_IN_PROGRESS"
	stateUninitialized        = "UNINITIALIZED"
	stateInitializeInProgress = "INITIALIZE_IN_PROGRESS"
	stateInitialized          = "INITIALIZED"
	stateActive               = "ACTIVE"
	stateModifyInProgress     = "MODIFY_IN_PROGRESS"
	stateDeleteInProgress     = "DELETE_IN_PROGRESS"
	stateDeleted              = "DELETED"
	stateReady                = "READY"
	statePendingDeletion      = "PENDING_DELETION"

	hsmTypeV1 = "hsm1.medium"
	hsmTypeV2 = "hsm2m.medium"

	modeFIPS    = "FIPS"
	modeNonFIPS = "NON_FIPS"

	networkIPv4      = "IPV4"
	networkDualstack = "DUALSTACK"

	retentionDays        = "DAYS"
	defaultRetentionDays = 90
	minRetentionDays     = 7
	maxRetentionDays     = 379
	backupRecoveryWindow = 7 * 24 * time.Hour
	rollbackWindow       = 7 * 24 * time.Hour
)

// Cluster is a stored CloudHSM cluster.
type Cluster struct {
	Created        time.Time         `json:"created"`
	ReadyAt        time.Time         `json:"readyAt"`
	InitUntil      time.Time         `json:"initUntil"`
	ModifyUntil    time.Time         `json:"modifyUntil"`
	RollbackExpiry time.Time         `json:"rollbackExpiry"`
	DeletedAt      time.Time         `json:"deletedAt"`
	DeleteUntil    time.Time         `json:"deleteUntil"`
	SubnetMapping  map[string]string `json:"subnetMapping"`
	Tags           map[string]string `json:"tags"`
	ID             string            `json:"id"`
	Account        string            `json:"account"`
	Region         string            `json:"region"`
	HsmType        string            `json:"hsmType"`
	Mode           string            `json:"mode"`
	NetworkType    string            `json:"networkType"`
	VpcID          string            `json:"vpcId"`
	SecurityGroup  string            `json:"securityGroup"`
	SourceBackupID string            `json:"sourceBackupId,omitempty"`
	CSR            string            `json:"csr"`
	ClusterCert    string            `json:"clusterCert,omitempty"`
	RetentionDays  int               `json:"retentionDays"`
	Initialized    bool              `json:"initialized"`
}

// Hsm is a stored HSM.
type Hsm struct {
	Created     time.Time `json:"created"`
	ReadyAt     time.Time `json:"readyAt"`
	DeletedAt   time.Time `json:"deletedAt"`
	DeleteUntil time.Time `json:"deleteUntil"`
	ID          string    `json:"id"`
	ClusterID   string    `json:"clusterId"`
	Account     string    `json:"account"`
	Region      string    `json:"region"`
	Zone        string    `json:"zone"`
	SubnetID    string    `json:"subnetId"`
	EniID       string    `json:"eniId"`
	EniIP       string    `json:"eniIp"`
	HsmType     string    `json:"hsmType"`
}

// Backup is a stored cluster backup.
type Backup struct {
	Created       time.Time         `json:"created"`
	ReadyAt       time.Time         `json:"readyAt"`
	CopyTime      time.Time         `json:"copyTime"`
	DeleteAt      time.Time         `json:"deleteAt"`
	Tags          map[string]string `json:"tags"`
	ID            string            `json:"id"`
	Account       string            `json:"account"`
	Region        string            `json:"region"`
	ClusterID     string            `json:"clusterId"`
	HsmType       string            `json:"hsmType"`
	Mode          string            `json:"mode"`
	ClusterCert   string            `json:"clusterCert,omitempty"`
	SourceBackup  string            `json:"sourceBackup,omitempty"`
	SourceCluster string            `json:"sourceCluster,omitempty"`
	SourceRegion  string            `json:"sourceRegion,omitempty"`
	Policy        string            `json:"policy,omitempty"`
	RetentionDays int               `json:"retentionDays"`
	NeverExpires  bool              `json:"neverExpires"`
}

func (b *Backup) expiresAt() time.Time {
	if b.NeverExpires {
		return time.Time{}
	}

	return b.Created.AddDate(0, 0, b.RetentionDays)
}
