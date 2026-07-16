package redshift

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"
)

// ---------------------------------------------------------------------------
// Sentinel errors — Redshift Serverless
// ---------------------------------------------------------------------------

var (
	// ErrNamespaceNotFound is returned when a serverless namespace does not exist.
	ErrNamespaceNotFound = errors.New("ResourceNotFoundException")
	// ErrNamespaceAlreadyExists is returned when a serverless namespace already exists.
	ErrNamespaceAlreadyExists = errors.New("ConflictException")
	// ErrWorkgroupNotFound is returned when a serverless workgroup does not exist.
	ErrWorkgroupNotFound = errors.New("ResourceNotFoundException")
	// ErrWorkgroupAlreadyExists is returned when a serverless workgroup already exists.
	ErrWorkgroupAlreadyExists = errors.New("ConflictException")
	// ErrServerlessSnapshotNotFound is returned when a serverless snapshot does not exist.
	ErrServerlessSnapshotNotFound = errors.New("ResourceNotFoundException")
	// ErrServerlessConflict is returned when a serverless resource already exists.
	ErrServerlessConflict = errors.New("ConflictException")
	// ErrUsageLimitSLNotFound is returned when a serverless usage limit does not exist.
	ErrUsageLimitSLNotFound = errors.New("ResourceNotFoundException")
	// ErrScheduledActionSLNotFound is returned when a serverless scheduled action does not exist.
	ErrScheduledActionSLNotFound = errors.New("ResourceNotFoundException")
)

// ---------------------------------------------------------------------------
// Serverless models
// ---------------------------------------------------------------------------

// Namespace represents a Redshift Serverless namespace.
type Namespace struct {
	CreationDate  time.Time `json:"creationDate"`
	NamespaceArn  string    `json:"namespaceArn"`
	NamespaceID   string    `json:"namespaceId"`
	NamespaceName string    `json:"namespaceName"`
	AdminUsername string    `json:"adminUsername,omitempty"`
	DBName        string    `json:"dbName,omitempty"`
	KmsKeyID      string    `json:"kmsKeyId,omitempty"`
	Status        string    `json:"status"`
	IamRoles      []string  `json:"iamRoles,omitempty"`
	LogExports    []string  `json:"logExports,omitempty"`
}

// Workgroup represents a Redshift Serverless workgroup.
type Workgroup struct {
	CreationDate     time.Time         `json:"creationDate"`
	WorkgroupArn     string            `json:"workgroupArn"`
	WorkgroupID      string            `json:"workgroupId"`
	WorkgroupName    string            `json:"workgroupName"`
	NamespaceName    string            `json:"namespaceName"`
	Status           string            `json:"status"`
	Endpoint         WorkgroupEndpoint `json:"endpoint"`
	SubnetIDs        []string          `json:"subnetIds,omitempty"`
	SecurityGroupIDs []string          `json:"securityGroupIds,omitempty"`
	BaseCapacity     int               `json:"baseCapacity"`
}

// WorkgroupEndpoint holds the endpoint address and port.
type WorkgroupEndpoint struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
}

// ServerlessSnapshot represents a Redshift Serverless namespace snapshot.
type ServerlessSnapshot struct {
	SnapshotCreateTime        time.Time `json:"snapshotCreateTime"`
	SnapshotArn               string    `json:"snapshotArn"`
	SnapshotName              string    `json:"snapshotName"`
	NamespaceName             string    `json:"namespaceName"`
	NamespaceArn              string    `json:"namespaceArn"`
	Status                    string    `json:"status"`
	AdminUsername             string    `json:"adminUsername,omitempty"`
	AccountsWithRestoreAccess []string  `json:"accountsWithRestoreAccess,omitempty"`
}

// ServerlessUsageLimit represents a serverless usage limit.
type ServerlessUsageLimit struct {
	UsageLimitArn string `json:"usageLimitArn"`
	UsageLimitID  string `json:"usageLimitId"`
	ResourceArn   string `json:"resourceArn"`
	UsageType     string `json:"usageType"`
	Period        string `json:"period"`
	BreachAction  string `json:"breachAction"`
	Amount        int64  `json:"amount"`
}

// ServerlessScheduledAction represents a serverless scheduled action.
type ServerlessScheduledAction struct {
	ScheduledActionArn  string    `json:"scheduledActionArn"`
	ScheduledActionName string    `json:"scheduledActionName"`
	NamespaceName       string    `json:"namespaceName"`
	Schedule            string    `json:"schedule"`
	StartTime           time.Time `json:"startTime"`
	EndTime             time.Time `json:"endTime"`
	Status              string    `json:"status"`
	TargetAction        string    `json:"targetAction"`
}

// ---------------------------------------------------------------------------
// Status constants for serverless resources
// ---------------------------------------------------------------------------

const (
	slStatusAvailable = "AVAILABLE"
	slStatusActive    = dataShareStatusActive // reuse existing constant

	// Magic number constants for serverless operations.
	slIDHexBytes          = 8    // bytes for random resource IDs (produces 16-char hex)
	slEndpointHexBytes    = 6    // bytes for random endpoint suffix (produces 12-char hex)
	slDefaultBaseCapacity = 32   // default RPU if not specified
	slServerlessPort      = 5439 // default Redshift serverless port
	slCredTokenHexBytes   = 4    // bytes for credential token suffix
	slCredSecretHexBytes  = 20   // bytes for credential secret
	slCredExpiryMinutes   = 15   // credential TTL in minutes
	slDefaultPageSize     = 100  // default max results per page
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}

func serverlessDefaultPageSize() int { return slDefaultPageSize }
