package gcpsql

import "time"

const (
	baseLink = "https://sqladmin.googleapis.com/sql/v1beta4"

	kindInstance   = "sql#instance"
	kindOperation  = "sql#operation"
	kindDatabase   = "sql#database"
	kindUser       = "sql#user"
	kindBackupRun  = "sql#backupRun"
	kindSslCert    = "sql#sslCert"
	kindSettings   = "sql#settings"
	kindConnect    = "sql#connectSettings"
	timeLayout     = "2006-01-02T15:04:05.000Z"
	stateRunnable  = "RUNNABLE"
	statePending   = "PENDING_CREATE"
	stateDeleting  = "PENDING_DELETE"
	stateSuspended = "SUSPENDED"
	stateFailed    = "FAILED"
	keyKind        = "kind"
	policyNever    = "NEVER"
	fileCSV        = "CSV"
	opPending      = "PENDING"
	opRunning      = "RUNNING"
	opDone         = "DONE"
)

func stamp(t time.Time) string { return t.UTC().Format(timeLayout) }

// IPMapping is one address of an instance.
type IPMapping struct {
	Type      string `json:"type"`
	IPAddress string `json:"ipAddress"`
}

// SslCert is a server CA, client or ephemeral certificate.
type SslCert struct {
	Kind             string `json:"kind"`
	CertSerialNumber string `json:"certSerialNumber,omitempty"`
	Cert             string `json:"cert,omitempty"`
	CommonName       string `json:"commonName,omitempty"`
	Sha1Fingerprint  string `json:"sha1Fingerprint,omitempty"`
	Instance         string `json:"instance,omitempty"`
	CreateTime       string `json:"createTime,omitempty"`
	ExpirationTime   string `json:"expirationTime,omitempty"`
	SelfLink         string `json:"selfLink,omitempty"`
}

// Instance is the sql#instance resource.
type Instance struct {
	Settings                   map[string]any `json:"settings,omitempty"`
	ServerCaCert               *SslCert       `json:"serverCaCert,omitempty"`
	Kind                       string         `json:"kind"`
	State                      string         `json:"state"`
	DatabaseVersion            string         `json:"databaseVersion"`
	Etag                       string         `json:"etag"`
	InstanceType               string         `json:"instanceType"`
	Project                    string         `json:"project"`
	ServiceAccountEmailAddress string         `json:"serviceAccountEmailAddress,omitempty"`
	BackendType                string         `json:"backendType"`
	SelfLink                   string         `json:"selfLink"`
	ConnectionName             string         `json:"connectionName"`
	Name                       string         `json:"name"`
	Region                     string         `json:"region"`
	GceZone                    string         `json:"gceZone,omitempty"`
	CreateTime                 string         `json:"createTime"`
	MaintenanceVersion         string         `json:"maintenanceVersion,omitempty"`
	SQLNetworkArchitecture     string         `json:"sqlNetworkArchitecture,omitempty"`
	GopherstackEndpoint        string         `json:"gopherstackEndpoint,omitempty"`
	MasterInstanceName         string         `json:"masterInstanceName,omitempty"`
	ReplicaConfiguration       map[string]any `json:"replicaConfiguration,omitempty"`
	IPAddresses                []IPMapping    `json:"ipAddresses,omitempty"`
	ReplicaNames               []string       `json:"replicaNames,omitempty"`
}

// OperationError is one error of a failed operation.
type OperationError struct {
	Kind    string `json:"kind"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// OperationErrors is the error payload of a failed operation.
type OperationErrors struct {
	Kind   string           `json:"kind"`
	Errors []OperationError `json:"errors"`
}

// Operation is the sql#operation resource.
type Operation struct {
	Error         *OperationErrors `json:"error,omitempty"`
	ExportContext map[string]any   `json:"exportContext,omitempty"`
	ImportContext map[string]any   `json:"importContext,omitempty"`
	BackupContext map[string]any   `json:"backupContext,omitempty"`
	Kind          string           `json:"kind"`
	Name          string           `json:"name"`
	OperationType string           `json:"operationType"`
	Status        string           `json:"status"`
	TargetID      string           `json:"targetId"`
	TargetLink    string           `json:"targetLink"`
	TargetProject string           `json:"targetProject"`
	SelfLink      string           `json:"selfLink"`
	User          string           `json:"user,omitempty"`
	InsertTime    string           `json:"insertTime"`
	StartTime     string           `json:"startTime,omitempty"`
	EndTime       string           `json:"endTime,omitempty"`
	seq           int64
}

// Database is the sql#database resource.
type Database struct {
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Instance  string `json:"instance"`
	Project   string `json:"project"`
	Charset   string `json:"charset,omitempty"`
	Collation string `json:"collation,omitempty"`
	Etag      string `json:"etag"`
	SelfLink  string `json:"selfLink"`
	System    bool   `json:"-"`
}

// User is the sql#user resource; the password is never serialised.
type User struct {
	PasswordPolicy map[string]any `json:"passwordPolicy,omitempty"`
	Kind           string         `json:"kind"`
	Name           string         `json:"name"`
	Host           string         `json:"host,omitempty"`
	Instance       string         `json:"instance"`
	Project        string         `json:"project"`
	Type           string         `json:"type,omitempty"`
	Etag           string         `json:"etag"`
	DatabaseRoles  []string       `json:"databaseRoles,omitempty"`
}

// BackupRun is the sql#backupRun resource.
type BackupRun struct {
	Error           *OperationError `json:"error,omitempty"`
	EndTime         string          `json:"endTime"`
	SelfLink        string          `json:"selfLink"`
	Type            string          `json:"type"`
	BackupKind      string          `json:"backupKind"`
	Description     string          `json:"description,omitempty"`
	EnqueuedTime    string          `json:"enqueuedTime"`
	Status          string          `json:"status"`
	WindowStartTime string          `json:"windowStartTime"`
	StartTime       string          `json:"startTime"`
	Instance        string          `json:"instance"`
	Kind            string          `json:"kind"`
	Location        string          `json:"location"`
	DatabaseVersion string          `json:"databaseVersion"`
	TimeZone        string          `json:"timeZone"`
	ID              string          `json:"id"`
}

func instanceLink(project, name string) string {
	return baseLink + "/projects/" + project + "/instances/" + name
}
