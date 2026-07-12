package athena

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	idChars  = "abcdef0123456789"
	idLength = 10

	defaultWorkGroup = "primary"
	awsDataCatalog   = "AwsDataCatalog"
	millisToSeconds  = 1000.0

	stateAuto       = "AUTO"
	stateSucceeded  = "SUCCEEDED"
	stateFailed     = "FAILED"
	stateCancelled  = "CANCELLED"
	stateCancelling = "CANCELLING"

	workGroupStateEnabled  = "ENABLED"
	workGroupStateDisabled = "DISABLED"

	columnTypeString = "string"

	tableTypeExternal = "EXTERNAL_TABLE"
)

// AWS Athena models a small, fixed set of API exceptions. The gopherstack
// sentinels below each map to one of these codes; handleError translates the
// sentinel into the __type field the SDK deserializes. See
// aws-sdk-go/service/athena/errors.go for the authoritative list.
const (
	errTypeInternalServer      = "InternalServerException"
	errTypeInvalidRequestExc   = "InvalidRequestException"
	errTypeMetadataExc         = "MetadataException"
	errTypeResourceNotFoundExc = "ResourceNotFoundException"
	errTypeSessionExistsExc    = "SessionAlreadyExistsException"
	errTypeTooManyRequestsExc  = "TooManyRequestsException"
)

var (
	// ErrNotFound is returned when a requested resource that AWS reports via
	// InvalidRequestException does not exist (workgroups, named queries, data
	// catalogs, query executions, notebooks, capacity reservations). AWS Athena
	// returns InvalidRequestException — not a dedicated NotFound code — for these.
	ErrNotFound = errors.New(errTypeInvalidRequestExc)
	// ErrAlreadyExists is returned when a resource already exists. AWS Athena
	// reports duplicate workgroups/catalogs/notebooks as InvalidRequestException.
	ErrAlreadyExists = errors.New(errTypeInvalidRequestExc)
	// ErrProtected is returned when an operation is not allowed on a protected
	// resource (e.g. deleting the primary workgroup). AWS returns
	// InvalidRequestException for these.
	ErrProtected = errors.New(errTypeInvalidRequestExc)
	// ErrValidation is returned when input fails validation
	// (InvalidRequestException).
	ErrValidation = errors.New(errTypeInvalidRequestExc)
	// ErrResourceNotFound is returned for resources AWS reports with the
	// dedicated ResourceNotFoundException code: sessions, calculation
	// executions, and prepared statements.
	ErrResourceNotFound = errors.New(errTypeResourceNotFoundExc)
	// ErrMetadata is returned when a metadata lookup against the (emulated Glue)
	// catalog fails — missing databases and tables — which AWS surfaces as
	// MetadataException.
	ErrMetadata = errors.New(errTypeMetadataExc)
	// ErrSessionExists is returned when a session with the same identity already
	// exists (SessionAlreadyExistsException).
	ErrSessionExists = errors.New(errTypeSessionExistsExc)
)

// validDataCatalogTypes is the set of accepted DataCatalog type values.
func isValidDataCatalogType(t string) bool {
	switch t {
	case "LAMBDA", "GLUE", "HIVE", "FEDERATED":
		return true
	default:
		return false
	}
}

// EncryptionConfiguration holds encryption settings for query results.
type EncryptionConfiguration struct {
	EncryptionOption string `json:"EncryptionOption,omitempty"`
	KmsKey           string `json:"KmsKey,omitempty"`
}

// ACLConfiguration controls S3 canned ACL for query results.
type ACLConfiguration struct {
	S3AclOption string `json:"S3AclOption,omitempty"`
}

// CustomerEncCfg holds KMS key for user data encryption.
type CustomerEncCfg struct {
	KmsKey string `json:"KmsKey,omitempty"`
}

// ResultConfiguration holds the configuration for where query results are stored.
type ResultConfiguration struct {
	// ACLConfiguration is tagged "AclConfiguration" (not "ACLConfiguration") to
	// match the real Athena wire shape: aws-sdk-go-v2's generated deserializer
	// switches on the exact-case JSON key "AclConfiguration", so a mismatched
	// tag here would make the SDK silently drop the field.
	ACLConfiguration        *ACLConfiguration       `json:"AclConfiguration,omitempty"`
	EncryptionConfiguration EncryptionConfiguration `json:"EncryptionConfiguration,omitzero"`
	ExpectedBucketOwner     string                  `json:"ExpectedBucketOwner,omitempty"`
	OutputLocation          string                  `json:"OutputLocation,omitempty"`
}

// ResultReuseByAgeConfiguration controls result reuse by result age.
type ResultReuseByAgeConfiguration struct {
	Enabled         bool  `json:"Enabled"`
	MaxAgeInMinutes int32 `json:"MaxAgeInMinutes,omitempty"`
}

// ResultReuseConfiguration controls whether previous query results can be reused.
type ResultReuseConfiguration struct {
	ResultReuseByAgeConfiguration *ResultReuseByAgeConfiguration `json:"ResultReuseByAgeConfiguration,omitempty"`
}

// EngineVersion holds the engine version configuration for a workgroup.
type EngineVersion struct {
	SelectedEngineVersion  string `json:"SelectedEngineVersion,omitempty"`
	EffectiveEngineVersion string `json:"EffectiveEngineVersion,omitempty"`
}

// WorkGroupConfiguration holds configuration for a workgroup.
type WorkGroupConfiguration struct {
	CustomerContentEncryptionConfiguration *CustomerEncCfg     `json:"CustomerContentEncryptionConfiguration,omitempty"`
	ResultConfiguration                    ResultConfiguration `json:"ResultConfiguration,omitzero"`
	EngineVersion                          EngineVersion       `json:"EngineVersion,omitzero"`
	AdditionalConfiguration                string              `json:"AdditionalConfiguration,omitempty"`
	ExecutionRole                          string              `json:"ExecutionRole,omitempty"`
	BytesScannedCutoffPerQuery             int64               `json:"BytesScannedCutoffPerQuery,omitempty"`
	EnableMinEnc                           bool                `json:"EnableMinimumEncryptionConfiguration,omitempty"`
	EnforceWGCfg                           bool                `json:"EnforceWorkGroupConfiguration,omitempty"`
	PublishCWMetrics                       bool                `json:"PublishCloudWatchMetricsEnabled,omitempty"`
	RequesterPays                          bool                `json:"RequesterPaysEnabled,omitempty"`
}

// WorkGroup represents an Athena workgroup.
type WorkGroup struct {
	Name          string                 `json:"Name"`
	Description   string                 `json:"Description,omitempty"`
	State         string                 `json:"State"`
	Tags          map[string]string      `json:"Tags,omitempty"`
	Configuration WorkGroupConfiguration `json:"Configuration,omitzero"`
	CreationTime  float64                `json:"CreationTime,omitempty"`
}

// WorkGroupSummary is a reduced view of a WorkGroup for list responses.
type WorkGroupSummary struct {
	EngineVersion *EngineVersion `json:"EngineVersion,omitempty"`
	Name          string         `json:"Name"`
	Description   string         `json:"Description,omitempty"`
	State         string         `json:"State"`
	CreationTime  float64        `json:"CreationTime,omitempty"`
}

// NamedQuery represents a saved Athena query.
type NamedQuery struct {
	NamedQueryID string `json:"NamedQueryId"`
	Name         string `json:"Name"`
	Description  string `json:"Description,omitempty"`
	Database     string `json:"Database"`
	QueryString  string `json:"QueryString"`
	WorkGroup    string `json:"WorkGroup,omitempty"`
}

// DataCatalog represents an Athena data catalog.
type DataCatalog struct {
	Parameters     map[string]string `json:"Parameters,omitempty"`
	Tags           map[string]string `json:"Tags,omitempty"`
	Name           string            `json:"Name"`
	Type           string            `json:"Type"`
	Description    string            `json:"Description,omitempty"`
	ConnectionType string            `json:"ConnectionType,omitempty"`
	Error          string            `json:"Error,omitempty"`
	Status         string            `json:"Status,omitempty"`
}

// DataCatalogSummary is a reduced view of a DataCatalog for list responses.
type DataCatalogSummary struct {
	CatalogName    string `json:"CatalogName"`
	Type           string `json:"Type"`
	ConnectionType string `json:"ConnectionType,omitempty"`
	Error          string `json:"Error,omitempty"`
	Status         string `json:"Status,omitempty"`
}

// QueryExecutionContext holds the database and catalog for a query execution.
type QueryExecutionContext struct {
	Database string `json:"Database,omitempty"`
	Catalog  string `json:"Catalog,omitempty"`
}

// QueryExecutionStatus holds the status of a query execution.
type QueryExecutionStatus struct {
	AthenaError        *QueryExecutionError `json:"AthenaError,omitempty"`
	State              string               `json:"State"`
	StateChangeReason  string               `json:"StateChangeReason,omitempty"`
	SubmissionDateTime float64              `json:"SubmissionDateTime,omitempty"`
	CompletionDateTime float64              `json:"CompletionDateTime,omitempty"`
}

// QueryExecutionError describes why a query execution reached the FAILED state,
// mirroring the AthenaError shape AWS returns inside QueryExecutionStatus (the
// JSON field is still "AthenaError").
type QueryExecutionError struct {
	ErrorMessage  string `json:"ErrorMessage,omitempty"`
	ErrorCategory int32  `json:"ErrorCategory,omitempty"`
	ErrorType     int32  `json:"ErrorType,omitempty"`
	Retryable     bool   `json:"Retryable,omitempty"`
}

// athenaErrorCategoryUser is the AthenaError.ErrorCategory value AWS uses for
// failures caused by the submitted query (as opposed to platform errors).
const athenaErrorCategoryUser = 2

// QueryExecutionStatistics holds statistics for a query execution.
type QueryExecutionStatistics struct {
	DataManifestLocation             string  `json:"DataManifestLocation,omitempty"`
	DpuCount                         float64 `json:"DpuCount,omitempty"`
	EngineExecutionTimeInMillis      int64   `json:"EngineExecutionTimeInMillis,omitempty"`
	DataScannedInBytes               int64   `json:"DataScannedInBytes,omitempty"`
	QueryPlanningTimeInMillis        int64   `json:"QueryPlanningTimeInMillis,omitempty"`
	QueryQueueTimeInMillis           int64   `json:"QueryQueueTimeInMillis,omitempty"`
	ServicePreProcessingTimeInMillis int64   `json:"ServicePreProcessingTimeInMillis,omitempty"`
	ServiceProcessingTimeInMillis    int64   `json:"ServiceProcessingTimeInMillis,omitempty"`
	TotalExecutionTimeInMillis       int64   `json:"TotalExecutionTimeInMillis,omitempty"`
	ReusedPreviousResult             bool    `json:"ReusedPreviousResult,omitempty"`
}

// QueryExecution represents an Athena query execution.
type QueryExecution struct {
	ResultConfiguration      ResultConfiguration       `json:"ResultConfiguration,omitzero"`
	QueryExecutionContext    QueryExecutionContext     `json:"QueryExecutionContext,omitzero"`
	ResultReuseConfiguration *ResultReuseConfiguration `json:"ResultReuseConfiguration,omitempty"`
	EngineVersion            *EngineVersion            `json:"EngineVersion,omitempty"`
	QueryExecutionID         string                    `json:"QueryExecutionId"`
	Query                    string                    `json:"Query"`
	WorkGroup                string                    `json:"WorkGroup,omitempty"`
	StatementType            string                    `json:"StatementType,omitempty"`
	ExecutionParameters      []string                  `json:"ExecutionParameters,omitempty"`
	Status                   QueryExecutionStatus      `json:"Status"`
	Statistics               QueryExecutionStatistics  `json:"Statistics,omitzero"`
}

// Tag is a key-value pair.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// PreparedStatement represents an Athena prepared statement.
type PreparedStatement struct {
	StatementName    string  `json:"StatementName"`
	WorkGroupName    string  `json:"WorkGroupName"`
	QueryStatement   string  `json:"QueryStatement"`
	Description      string  `json:"Description,omitempty"`
	LastModifiedTime float64 `json:"LastModifiedTime,omitempty"`
}

// PreparedStatementSummary is a reduced view returned by ListPreparedStatements.
type PreparedStatementSummary struct {
	StatementName    string  `json:"StatementName"`
	LastModifiedTime float64 `json:"LastModifiedTime,omitempty"`
}

// UnprocessedPreparedStatementName describes a prepared statement that could not be retrieved.
type UnprocessedPreparedStatementName struct {
	StatementName string `json:"StatementName"`
	ErrorMessage  string `json:"ErrorMessage"`
}

// UnprocessedNamedQueryID describes a named query that could not be retrieved.
type UnprocessedNamedQueryID struct {
	NamedQueryID string `json:"NamedQueryId"`
	ErrorCode    string `json:"ErrorCode,omitempty"`
	ErrorMessage string `json:"ErrorMessage,omitempty"`
}

// UnprocessedQueryExecutionID describes a query execution that could not be retrieved.
type UnprocessedQueryExecutionID struct {
	QueryExecutionID string `json:"QueryExecutionId"`
	ErrorCode        string `json:"ErrorCode,omitempty"`
	ErrorMessage     string `json:"ErrorMessage,omitempty"`
}

// CapacityAllocation describes a single capacity allocation attempt.
type CapacityAllocation struct {
	Status                string  `json:"Status,omitempty"`
	StatusMessage         string  `json:"StatusMessage,omitempty"`
	RequestTime           float64 `json:"RequestTime,omitempty"`
	RequestCompletionTime float64 `json:"RequestCompletionTime,omitempty"`
}

// CapacityReservation represents an Athena capacity reservation.
type CapacityReservation struct {
	Tags                         map[string]string   `json:"Tags,omitempty"`
	LastAllocation               *CapacityAllocation `json:"LastAllocation,omitempty"`
	Name                         string              `json:"Name"`
	Status                       string              `json:"Status"`
	CreationTime                 float64             `json:"CreationTime,omitempty"`
	LastSuccessfulAllocationTime float64             `json:"LastSuccessfulAllocationTime,omitempty"`
	TargetDpus                   int32               `json:"TargetDpus"`
	AllocatedDpus                int32               `json:"AllocatedDpus"`
}

// NotebookMetadata holds metadata for an Athena notebook.
type NotebookMetadata struct {
	NotebookID       string  `json:"NotebookId"`
	Name             string  `json:"Name"`
	WorkGroup        string  `json:"WorkGroup"`
	Type             string  `json:"Type"`
	CreationTime     float64 `json:"CreationTime,omitempty"`
	LastModifiedTime float64 `json:"LastModifiedTime,omitempty"`
}

// Notebook represents an Athena notebook with its content.
type Notebook struct {
	NotebookID       string  `json:"NotebookId"`
	Name             string  `json:"Name"`
	WorkGroup        string  `json:"WorkGroup"`
	Type             string  `json:"Type"`
	Content          string  `json:"Content"`
	CreationTime     float64 `json:"CreationTime,omitempty"`
	LastModifiedTime float64 `json:"LastModifiedTime,omitempty"`
}

// StorageBackend is the interface for the Athena in-memory store.
type StorageBackend interface {
	// WorkGroups
	CreateWorkGroup(
		name, description, state string,
		cfg WorkGroupConfiguration,
		tags map[string]string,
	) error
	GetWorkGroup(name string) (*WorkGroup, error)
	ListWorkGroups(nextToken string, maxResults int) ([]*WorkGroupSummary, string, error)
	UpdateWorkGroup(name, description, state string, cfg *WorkGroupConfiguration) error
	DeleteWorkGroup(name string) error

	// Named Queries
	CreateNamedQuery(name, description, database, queryString, workGroup string) (string, error)
	GetNamedQuery(id string) (*NamedQuery, error)
	ListNamedQueries(workGroup, nextToken string, maxResults int) ([]string, string, error)
	BatchGetNamedQuery(ids []string) ([]NamedQuery, []UnprocessedNamedQueryID)
	DeleteNamedQuery(id string) error

	// Data Catalogs
	CreateDataCatalog(
		name, catalogType, description, connectionType string,
		params, tags map[string]string,
	) error
	GetDataCatalog(name string) (*DataCatalog, error)
	ListDataCatalogs(nextToken string, maxResults int) ([]*DataCatalogSummary, string, error)
	UpdateDataCatalog(
		name, catalogType, description, connectionType string,
		params map[string]string,
	) error
	DeleteDataCatalog(name string) error

	// Query Executions
	StartQueryExecution(
		query, workGroup string,
		ctx QueryExecutionContext,
		rc ResultConfiguration,
		execParams []string,
		reuseCfg *ResultReuseConfiguration,
	) (string, error)
	GetQueryExecution(id string) (*QueryExecution, error)
	GetQueryResults(id, nextToken string, maxResults int) (*sqlResultPage, error)
	ListQueryExecutions(workGroup string) ([]string, error)
	StopQueryExecution(id string) error
	BatchGetQueryExecution(ids []string) ([]QueryExecution, []UnprocessedQueryExecutionID)

	// Tags
	TagResource(arn string, tags map[string]string) error
	UntagResource(arn string, keys []string) error
	ListTagsForResource(arn string) ([]Tag, error)

	// Prepared Statements
	BatchGetPreparedStatement(
		workGroup string,
		names []string,
	) ([]PreparedStatement, []UnprocessedPreparedStatementName)
	CreatePreparedStatement(name, description, workGroup, queryStatement string) error
	DeletePreparedStatement(name, workGroup string) error
	GetPreparedStatement(name, workGroup string) (*PreparedStatement, error)
	ListPreparedStatements(
		workGroup, nextToken string,
		maxResults int,
	) ([]PreparedStatementSummary, string, error)

	// Capacity Reservations
	CancelCapacityReservation(name string) error
	CreateCapacityReservation(name string, targetDPUs int32, tags map[string]string) error
	DeleteCapacityReservation(name string) error

	// Notebooks
	CreateNotebook(workGroup, name string, tags map[string]string) (string, error)
	CreatePresignedNotebookURL(sessionID string) (url, authToken string, authTokenExpiration float64, err error)
	DeleteNotebook(notebookID string) error
	ExportNotebook(notebookID string) (NotebookMetadata, string, error)
	GetNotebookMetadata(notebookID string) (*NotebookMetadata, error)
	ListNotebookMetadata(workGroup, namePrefix string) ([]NotebookMetadata, error)
	ImportNotebook(workGroup, name, payload, notebookType string) (string, error)
	UpdateNotebook(notebookID, payload, notebookType, sessionID string) error
	UpdateNotebookMetadata(notebookID, newName string) error

	// Sessions
	StartSession(
		workGroup, description, notebookVersion string,
		engineCfg EngineConfiguration,
		sessionCfg SessionConfiguration,
		notebookID string,
	) (string, string, error)
	GetSession(id string) (*Session, error)
	GetSessionStatus(id string) (SessionStatus, error)
	GetSessionEndpoint(id string) (url, authToken string, authTokenExpiration float64, err error)
	TerminateSession(id string) (string, error)
	ListSessions(workGroup, stateFilter string) ([]SessionSummary, error)
	ListNotebookSessions(notebookID string) ([]SessionSummary, error)

	// Calculations
	StartCalculationExecution(sessionID, description, codeBlock string) (string, string, error)
	GetCalculationExecution(id string) (*CalculationExecution, error)
	GetCalculationExecutionStatus(id string) (CalculationStatus, CalculationStatistics, error)
	GetCalculationExecutionCode(id string) (string, error)
	StopCalculationExecution(id string) (string, error)
	ListCalculationExecutions(sessionID, stateFilter string) ([]CalculationSummary, error)

	// Capacity reservations (extended)
	GetCapacityReservation(name string) (*CapacityReservation, error)
	ListCapacityReservations() ([]CapacityReservation, error)
	UpdateCapacityReservation(name string, targetDPUs int32) error
	PutCapacityAssignmentConfiguration(name string, assignments []CapacityAssignment) error
	GetCapacityAssignmentConfiguration(name string) (*CapacityAssignmentConfiguration, error)

	// Database / table metadata
	GetDatabase(catalog, name string) (*Database, error)
	ListDatabases(catalog string) ([]Database, error)
	GetTableMetadata(catalog, database, table string) (*TableMetadata, error)
	ListTableMetadata(catalog, database, expr string) ([]TableMetadata, error)

	// Misc updates / runtime stats
	UpdateNamedQuery(id, name, description, queryString string) error
	UpdatePreparedStatement(name, workGroup, queryStatement, description string) error
	GetQueryRuntimeStatistics(id string) (*QueryRuntimeStatistics, error)
	GetResourceDashboard(resourceARN string) (string, error)

	// Engine version / executor / DPU listings
	ListEngineVersions() []EngineVersionDescriptor
	ListApplicationDPUSizes() []ApplicationDPUSizes
	ListExecutors(sessionID, stateFilter string) ([]Executor, error)
}

// InMemoryBackend implements StorageBackend using in-memory maps.
//
// Every AWS resource collection is a *store.Table[T] registered on registry
// (see store_setup.go for the full split between tables registered directly
// and the two "dirty" tables handled separately); queryResults, tableData,
// and resourceTags remain plain maps -- see the store_setup.go file doc for
// why each is left as-is.
type InMemoryBackend struct {
	registry                      *store.Registry
	workGroups                    *store.Table[WorkGroup]
	namedQueries                  *store.Table[NamedQuery]
	namedQueriesByWorkGroup       *store.Index[NamedQuery]
	dataCatalogs                  *store.Table[DataCatalog]
	notebooks                     *store.Table[Notebook]
	notebooksByName               *store.Index[Notebook]
	queryResults                  map[string]*sqlResult
	tableData                     map[string][]map[string]any
	resourceTags                  map[string]map[string]string
	preparedStatements            *store.Table[PreparedStatement]
	preparedStatementsByWorkGroup *store.Index[PreparedStatement]
	queryExecutions               *store.Table[QueryExecution]
	queryExecutionsByWorkGroup    *store.Index[QueryExecution]
	sessions                      *store.Table[Session]
	calculations                  *store.Table[CalculationExecution]
	capacityReservations          *store.Table[CapacityReservation]
	capacityAssignments           *store.Table[CapacityAssignmentConfiguration]
	databases                     *store.Table[Database]
	databasesByCatalog            *store.Index[Database]
	tables                        *store.Table[TableMetadata]
	tablesByDatabase              *store.Index[TableMetadata]
	mu                            *lockmetrics.RWMutex
	accountID                     string
	region                        string
}

// NewInMemoryBackend creates a new InMemoryBackend and seeds the default "primary" workgroup.
func NewInMemoryBackend(region, accountID string) *InMemoryBackend {
	if region == "" {
		region = config.DefaultRegion
	}

	if accountID == "" {
		accountID = config.DefaultAccountID
	}

	b := &InMemoryBackend{
		registry:     store.NewRegistry(),
		queryResults: make(map[string]*sqlResult),
		tableData:    make(map[string][]map[string]any),
		resourceTags: make(map[string]map[string]string),
		region:       region,
		accountID:    accountID,
		mu:           lockmetrics.New("athena"),
	}

	registerAllTables(b)

	b.workGroups.Put(&WorkGroup{
		Name:  defaultWorkGroup,
		State: workGroupStateEnabled,
	})

	b.seedDefaultMetadata()

	return b
}

// seedDefaultMetadata seeds AwsDataCatalog and example database/table metadata
// so that metadata operations return useful sample data without explicit setup.
func (b *InMemoryBackend) seedDefaultMetadata() {
	const database = "default"

	b.dataCatalogs.Put(&DataCatalog{
		Name:   awsDataCatalog,
		Type:   "GLUE",
		Status: "CREATE_COMPLETE",
	})

	b.databases.Put(&Database{
		Catalog:     awsDataCatalog,
		Name:        database,
		Description: "Default Athena database",
	})

	b.tables.Put(&TableMetadata{
		Catalog:   awsDataCatalog,
		Database:  database,
		Name:      "sample_table",
		TableType: tableTypeExternal,
		Columns: []Column{
			{Name: "id", Type: "bigint"},
			{Name: "value", Type: columnTypeString},
		},
	})
}

// randomID generates a cryptographically random 10-character hex ID.
func randomID() string {
	b := make([]byte, idLength)
	charCount := uint64(len(idChars))

	for i := range b {
		var v [8]byte
		_, _ = rand.Read(v[:])
		b[i] = idChars[binary.BigEndian.Uint64(v[:])%charCount]
	}

	return string(b)
}

// sessionAuthTokenTTL is the validity window gopherstack assigns to the
// synthesized bearer tokens returned by GetSessionEndpoint and
// CreatePresignedNotebookUrl. AWS documents CreatePresignedNotebookUrl's token
// as needing a refresh roughly every 10 minutes; gopherstack reuses that
// window for both operations since neither backs a real authentication
// system.
const sessionAuthTokenTTL = 10 * time.Minute

// newSessionAuthToken synthesizes an opaque bearer token and its
// epoch-seconds expiration time for GetSessionEndpoint and
// CreatePresignedNotebookUrl, both of which require an AuthToken +
// AuthTokenExpirationTime pair on the wire despite gopherstack modeling no
// real authentication.
func newSessionAuthToken() (string, float64) {
	return randomID(), float64(time.Now().Add(sessionAuthTokenTTL).UnixMilli()) / millisToSeconds
}

func (b *InMemoryBackend) workGroupARN(name string) string {
	return arn.Build("athena", b.region, b.accountID, fmt.Sprintf("workgroup/%s", name))
}

func (b *InMemoryBackend) dataCatalogARN(name string) string {
	return arn.Build("athena", b.region, b.accountID, fmt.Sprintf("datacatalog/%s", name))
}

// --- WorkGroups ---

// CreateWorkGroup creates a new workgroup.
// athenaMinBytesScannedCutoff is the minimum value AWS Athena permits for the
// per-query data-scan limit (10 MB).
const athenaMinBytesScannedCutoff int64 = 10 * 1024 * 1024

// validateWorkGroupState reports an error if state is non-empty and not one of
// the two valid AWS values ("ENABLED" or "DISABLED"). An empty string is
// accepted where the caller treats it as "use default".
func validateWorkGroupState(state string) error {
	if state == "" || state == workGroupStateEnabled || state == workGroupStateDisabled {
		return nil
	}

	return fmt.Errorf(
		"%w: State %q is invalid; must be %s or %s",
		ErrValidation, state, workGroupStateEnabled, workGroupStateDisabled,
	)
}

// validateWorkGroupConfiguration enforces AWS-documented bounds for workgroup
// configuration knobs. Currently this only checks BytesScannedCutoffPerQuery
// (a positive value < 10 MiB is rejected; zero means "unlimited" and is
// permitted).
func validateWorkGroupConfiguration(cfg WorkGroupConfiguration) error {
	if cfg.BytesScannedCutoffPerQuery > 0 &&
		cfg.BytesScannedCutoffPerQuery < athenaMinBytesScannedCutoff {
		return fmt.Errorf(
			"%w: BytesScannedCutoffPerQuery must be at least %d bytes (10 MB)",
			ErrValidation, athenaMinBytesScannedCutoff,
		)
	}

	return nil
}

func (b *InMemoryBackend) CreateWorkGroup(
	name, description, state string,
	cfg WorkGroupConfiguration,
	tags map[string]string,
) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if err := validateWorkGroupState(state); err != nil {
		return err
	}

	if err := validateWorkGroupConfiguration(cfg); err != nil {
		return err
	}

	b.mu.Lock("CreateWorkGroup")
	defer b.mu.Unlock()

	if b.workGroups.Has(name) {
		return fmt.Errorf("%w: workgroup %q already exists", ErrAlreadyExists, name)
	}

	if state == "" {
		state = workGroupStateEnabled
	}

	now := float64(time.Now().UnixMilli()) / millisToSeconds
	b.workGroups.Put(&WorkGroup{
		Name:          name,
		Description:   description,
		State:         state,
		Tags:          maps.Clone(tags),
		Configuration: cfg,
		CreationTime:  now,
	})

	arn := b.workGroupARN(name)
	if len(tags) > 0 {
		b.resourceTags[arn] = copyTags(tags)
	}

	return nil
}

// GetWorkGroup retrieves a workgroup by name.
func (b *InMemoryBackend) GetWorkGroup(name string) (*WorkGroup, error) {
	b.mu.RLock("GetWorkGroup")
	defer b.mu.RUnlock()

	wg, ok := b.workGroups.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: workgroup %q not found", ErrNotFound, name)
	}

	cp := *wg
	cp.Tags = maps.Clone(wg.Tags)

	return &cp, nil
}

// ListWorkGroups returns summaries of all workgroups with optional NextToken/MaxResults pagination.
func (b *InMemoryBackend) ListWorkGroups(
	nextToken string,
	maxResults int,
) ([]*WorkGroupSummary, string, error) {
	b.mu.RLock("ListWorkGroups")
	defer b.mu.RUnlock()

	all := make([]*WorkGroupSummary, 0, b.workGroups.Len())
	for _, wg := range b.workGroups.All() {
		sum := &WorkGroupSummary{
			Name:         wg.Name,
			Description:  wg.Description,
			State:        wg.State,
			CreationTime: wg.CreationTime,
		}
		if ev := wg.Configuration.EngineVersion; ev.SelectedEngineVersion != "" ||
			ev.EffectiveEngineVersion != "" {
			cp := ev
			sum.EngineVersion = &cp
		}
		all = append(all, sum)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name < all[j].Name
	})

	const defaultMaxResults = 50
	limit := defaultMaxResults
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	start := 0
	if nextToken != "" {
		for i, s := range all {
			if s.Name == nextToken {
				start = i

				break
			}
		}
	}

	all = all[start:]

	outToken := ""
	if len(all) > limit {
		outToken = all[limit].Name
		all = all[:limit]
	}

	return all, outToken, nil
}

// UpdateWorkGroup updates an existing workgroup.
func (b *InMemoryBackend) UpdateWorkGroup(
	name, description, state string,
	cfg *WorkGroupConfiguration,
) error {
	if err := validateWorkGroupState(state); err != nil {
		return err
	}

	if cfg != nil {
		if err := validateWorkGroupConfiguration(*cfg); err != nil {
			return err
		}
	}

	b.mu.Lock("UpdateWorkGroup")
	defer b.mu.Unlock()

	wg, ok := b.workGroups.Get(name)
	if !ok {
		return fmt.Errorf("%w: workgroup %q not found", ErrNotFound, name)
	}

	if description != "" {
		wg.Description = description
	}

	if state != "" {
		wg.State = state
	}

	if cfg != nil {
		wg.Configuration = *cfg
	}

	return nil
}

// DeleteWorkGroup removes a workgroup by name. The "primary" workgroup cannot be deleted.
func (b *InMemoryBackend) DeleteWorkGroup(name string) error {
	b.mu.Lock("DeleteWorkGroup")
	defer b.mu.Unlock()

	if name == defaultWorkGroup {
		return fmt.Errorf("%w: cannot delete the primary workgroup", ErrProtected)
	}

	if !b.workGroups.Has(name) {
		return fmt.Errorf("%w: workgroup %q not found", ErrNotFound, name)
	}

	b.workGroups.Delete(name)
	delete(b.resourceTags, b.workGroupARN(name))

	return nil
}

// --- Named Queries ---

// CreateNamedQuery creates a new named query and returns its ID.
func (b *InMemoryBackend) CreateNamedQuery(
	name, description, database, queryString, workGroup string,
) (string, error) {
	switch {
	case name == "":
		return "", fmt.Errorf("%w: Name is required", ErrValidation)
	case database == "":
		return "", fmt.Errorf("%w: Database is required", ErrValidation)
	case queryString == "":
		return "", fmt.Errorf("%w: QueryString is required", ErrValidation)
	}

	if workGroup == "" {
		workGroup = defaultWorkGroup
	}

	b.mu.Lock("CreateNamedQuery")
	defer b.mu.Unlock()

	if !b.workGroups.Has(workGroup) {
		return "", fmt.Errorf("%w: workgroup %q not found", ErrNotFound, workGroup)
	}

	id := randomID()
	b.namedQueries.Put(&NamedQuery{
		NamedQueryID: id,
		Name:         name,
		Description:  description,
		Database:     database,
		QueryString:  queryString,
		WorkGroup:    workGroup,
	})

	return id, nil
}

// GetNamedQuery retrieves a named query by ID.
func (b *InMemoryBackend) GetNamedQuery(id string) (*NamedQuery, error) {
	b.mu.RLock("GetNamedQuery")
	defer b.mu.RUnlock()

	q, ok := b.namedQueries.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: named query %q not found", ErrNotFound, id)
	}

	cp := *q

	return &cp, nil
}

// ListNamedQueries returns named query IDs, optionally filtered by workgroup, with pagination.
func (b *InMemoryBackend) ListNamedQueries(
	workGroup, nextToken string,
	maxResults int,
) ([]string, string, error) {
	b.mu.RLock("ListNamedQueries")
	defer b.mu.RUnlock()

	var matches []*NamedQuery
	if workGroup == "" {
		matches = b.namedQueries.All()
	} else {
		matches = b.namedQueriesByWorkGroup.Get(workGroup)
	}

	ids := make([]string, 0, len(matches))
	for _, q := range matches {
		ids = append(ids, q.NamedQueryID)
	}

	sort.Strings(ids)

	const defaultMaxResults = 50
	limit := defaultMaxResults
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	start := 0
	if nextToken != "" {
		for i, id := range ids {
			if id == nextToken {
				start = i

				break
			}
		}
	}

	ids = ids[start:]

	outToken := ""
	if len(ids) > limit {
		outToken = ids[limit]
		ids = ids[:limit]
	}

	return ids, outToken, nil
}

// BatchGetNamedQuery retrieves multiple named queries by ID.
func (b *InMemoryBackend) BatchGetNamedQuery(
	ids []string,
) ([]NamedQuery, []UnprocessedNamedQueryID) {
	b.mu.RLock("BatchGetNamedQuery")
	defer b.mu.RUnlock()

	found := make([]NamedQuery, 0, len(ids))
	unprocessed := make([]UnprocessedNamedQueryID, 0, len(ids))

	for _, id := range ids {
		q, ok := b.namedQueries.Get(id)
		if ok {
			found = append(found, *q)
		} else {
			unprocessed = append(unprocessed, UnprocessedNamedQueryID{
				NamedQueryID: id,
				ErrorCode:    "InvalidRequestException",
				ErrorMessage: fmt.Sprintf("named query %q not found", id),
			})
		}
	}

	return found, unprocessed
}

// DeleteNamedQuery removes a named query by ID.
func (b *InMemoryBackend) DeleteNamedQuery(id string) error {
	b.mu.Lock("DeleteNamedQuery")
	defer b.mu.Unlock()

	if !b.namedQueries.Has(id) {
		return fmt.Errorf("%w: named query %q not found", ErrNotFound, id)
	}

	b.namedQueries.Delete(id)

	return nil
}

// --- Data Catalogs ---

// CreateDataCatalog creates a new data catalog.
func (b *InMemoryBackend) CreateDataCatalog(
	name, catalogType, description, connectionType string,
	params, tags map[string]string,
) error {
	switch {
	case name == "":
		return fmt.Errorf("%w: Name is required", ErrValidation)
	case catalogType == "":
		return fmt.Errorf("%w: Type is required", ErrValidation)
	}

	if !isValidDataCatalogType(catalogType) {
		return fmt.Errorf(
			"%w: Type %q is invalid; must be one of LAMBDA, GLUE, HIVE, FEDERATED",
			ErrValidation,
			catalogType,
		)
	}

	b.mu.Lock("CreateDataCatalog")
	defer b.mu.Unlock()

	if b.dataCatalogs.Has(name) {
		return fmt.Errorf("%w: data catalog %q already exists", ErrAlreadyExists, name)
	}

	status := "CREATE_COMPLETE"
	if catalogType == "FEDERATED" {
		status = "CREATE_IN_PROGRESS"
	}

	b.dataCatalogs.Put(&DataCatalog{
		Name:           name,
		Type:           catalogType,
		Description:    description,
		ConnectionType: connectionType,
		Parameters:     maps.Clone(params),
		Tags:           maps.Clone(tags),
		Status:         status,
	})

	arn := b.dataCatalogARN(name)
	if len(tags) > 0 {
		b.resourceTags[arn] = copyTags(tags)
	}

	return nil
}

// GetDataCatalog retrieves a data catalog by name.
func (b *InMemoryBackend) GetDataCatalog(name string) (*DataCatalog, error) {
	b.mu.RLock("GetDataCatalog")
	defer b.mu.RUnlock()

	dc, ok := b.dataCatalogs.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: data catalog %q not found", ErrNotFound, name)
	}

	cp := *dc
	cp.Tags = maps.Clone(dc.Tags)
	cp.Parameters = maps.Clone(dc.Parameters)

	return &cp, nil
}

// ListDataCatalogs returns summaries of all data catalogs with optional NextToken/MaxResults pagination.
func (b *InMemoryBackend) ListDataCatalogs(
	nextToken string,
	maxResults int,
) ([]*DataCatalogSummary, string, error) {
	b.mu.RLock("ListDataCatalogs")
	defer b.mu.RUnlock()

	all := make([]*DataCatalogSummary, 0, b.dataCatalogs.Len())
	for _, dc := range b.dataCatalogs.All() {
		all = append(all, &DataCatalogSummary{
			CatalogName:    dc.Name,
			Type:           dc.Type,
			ConnectionType: dc.ConnectionType,
			Status:         dc.Status,
			Error:          dc.Error,
		})
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].CatalogName < all[j].CatalogName
	})

	const defaultMaxResults = 50
	limit := defaultMaxResults
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	start := 0
	if nextToken != "" {
		for i, s := range all {
			if s.CatalogName == nextToken {
				start = i

				break
			}
		}
	}

	all = all[start:]

	outToken := ""
	if len(all) > limit {
		outToken = all[limit].CatalogName
		all = all[:limit]
	}

	return all, outToken, nil
}

// UpdateDataCatalog updates an existing data catalog.
func (b *InMemoryBackend) UpdateDataCatalog(
	name, catalogType, description, connectionType string,
	params map[string]string,
) error {
	b.mu.Lock("UpdateDataCatalog")
	defer b.mu.Unlock()

	dc, ok := b.dataCatalogs.Get(name)
	if !ok {
		return fmt.Errorf("%w: data catalog %q not found", ErrNotFound, name)
	}

	if catalogType != "" && catalogType != dc.Type {
		return fmt.Errorf("%w: cannot change the Type of an existing data catalog", ErrValidation)
	}

	if description != "" {
		dc.Description = description
	}

	if connectionType != "" {
		dc.ConnectionType = connectionType
	}

	if params != nil {
		dc.Parameters = params
	}

	return nil
}

// DeleteDataCatalog removes a data catalog by name.
// The built-in AwsDataCatalog cannot be deleted.
func (b *InMemoryBackend) DeleteDataCatalog(name string) error {
	if name == awsDataCatalog {
		return fmt.Errorf(
			"%w: cannot delete the built-in data catalog %s",
			ErrProtected,
			awsDataCatalog,
		)
	}

	b.mu.Lock("DeleteDataCatalog")
	defer b.mu.Unlock()

	if !b.dataCatalogs.Has(name) {
		return fmt.Errorf("%w: data catalog %q not found", ErrNotFound, name)
	}

	b.dataCatalogs.Delete(name)
	delete(b.resourceTags, b.dataCatalogARN(name))

	return nil
}

// --- Query Executions ---

// StartQueryExecution records a new query execution and returns its ID.
//
//nolint:cyclop,funlen // lifecycle setup: enforce workgroup config + result reuse
func (b *InMemoryBackend) StartQueryExecution(
	query, workGroup string,
	ctx QueryExecutionContext,
	rc ResultConfiguration,
	execParams []string,
	reuseCfg *ResultReuseConfiguration,
) (string, error) {
	if query == "" {
		return "", fmt.Errorf("%w: QueryString is required", ErrValidation)
	}

	if workGroup == "" {
		workGroup = defaultWorkGroup
	}

	b.mu.Lock("StartQueryExecution")

	wg, ok := b.workGroups.Get(workGroup)
	if !ok {
		b.mu.Unlock()

		return "", fmt.Errorf("%w: workgroup %q not found", ErrNotFound, workGroup)
	}

	if wg.State == "DISABLED" {
		b.mu.Unlock()

		return "", fmt.Errorf("%w: workgroup %q is disabled", ErrValidation, workGroup)
	}

	// EnforceWorkGroupConfiguration: workgroup settings override per-query settings.
	if wg.Configuration.EnforceWGCfg {
		wgRC := wg.Configuration.ResultConfiguration
		if wgRC.OutputLocation != "" {
			rc.OutputLocation = wgRC.OutputLocation
		}

		if wgRC.EncryptionConfiguration.EncryptionOption != "" {
			rc.EncryptionConfiguration = wgRC.EncryptionConfiguration
		}
	}

	// Result reuse: check for a recent succeeded execution with the same query.
	reused := false
	if reuseCfg != nil &&
		reuseCfg.ResultReuseByAgeConfiguration != nil &&
		reuseCfg.ResultReuseByAgeConfiguration.Enabled {
		maxAge := reuseCfg.ResultReuseByAgeConfiguration.MaxAgeInMinutes
		cutoff := float64(
			time.Now().Add(-time.Duration(maxAge)*time.Minute).UnixMilli(),
		) / millisToSeconds

		for _, prev := range b.queryExecutionsByWorkGroup.Get(workGroup) {
			if prev.Query == query &&
				prev.Status.State == stateSucceeded &&
				prev.Status.CompletionDateTime >= cutoff &&
				!prev.Statistics.ReusedPreviousResult {
				reused = true

				break
			}
		}
	}

	id := randomID()
	now := float64(time.Now().UnixMilli()) / millisToSeconds

	const mockEngineMs int64 = 100

	qe := &QueryExecution{
		QueryExecutionID:         id,
		Query:                    query,
		ResultConfiguration:      rc,
		QueryExecutionContext:    ctx,
		ResultReuseConfiguration: reuseCfg,
		WorkGroup:                workGroup,
		StatementType:            inferStatementType(query),
		ExecutionParameters:      execParams,
		EngineVersion: &EngineVersion{
			SelectedEngineVersion:  stateAuto,
			EffectiveEngineVersion: athenaEngineV3,
		},
		Status: QueryExecutionStatus{
			State:              stateSucceeded,
			SubmissionDateTime: now,
			CompletionDateTime: now,
		},
		Statistics: QueryExecutionStatistics{
			EngineExecutionTimeInMillis:   mockEngineMs,
			TotalExecutionTimeInMillis:    mockEngineMs,
			ServiceProcessingTimeInMillis: 1,
			DataScannedInBytes:            0,
			ReusedPreviousResult:          reused,
		},
	}

	b.queryExecutions.Put(qe)
	b.mu.Unlock()

	// Execute the statement outside the write-lock. executeStatement acquires
	// its own locks: an RLock for SELECT projection and a write-lock for DDL/DML
	// catalog mutations. It returns the (possibly nil) result set plus an outcome
	// describing catalog effects or a failure reason.
	result, outcome := b.executeStatement(query, ctx)

	b.mu.Lock("StartQueryExecution-storeResult")
	b.finalizeExecution(id, result, outcome)
	b.mu.Unlock()

	return id, nil
}

// finalizeExecution stores the result of a query execution and, when the
// statement failed, transitions the execution to FAILED with an AWS-shaped
// AthenaError. Failed queries carry no result set. Callers must hold b.mu.
func (b *InMemoryBackend) finalizeExecution(id string, result *sqlResult, outcome statementOutcome) {
	qe, ok := b.queryExecutions.Get(id)
	if !ok {
		// Evicted between dispatch and finalize; nothing to store.
		return
	}

	if outcome.failed {
		now := float64(time.Now().UnixMilli()) / millisToSeconds
		qe.Status.State = stateFailed
		qe.Status.StateChangeReason = outcome.reason
		qe.Status.CompletionDateTime = now
		qe.Status.AthenaError = &QueryExecutionError{
			ErrorCategory: athenaErrorCategoryUser,
			ErrorType:     outcome.errorType,
			ErrorMessage:  outcome.reason,
		}

		delete(b.queryResults, id)

		return
	}

	b.queryResults[id] = result
}

// inferStatementType returns the Athena StatementType for a query string.
func inferStatementType(query string) string {
	q := strings.ToUpper(strings.TrimSpace(query))
	switch {
	case strings.HasPrefix(q, "SELECT"),
		strings.HasPrefix(q, "INSERT"),
		strings.HasPrefix(q, "CREATE TABLE AS"),
		strings.HasPrefix(q, "UNLOAD"):
		return "DML"
	case strings.HasPrefix(q, "CREATE"),
		strings.HasPrefix(q, "DROP"),
		strings.HasPrefix(q, "ALTER"),
		strings.HasPrefix(q, "MSCK"):
		return "DDL"
	default:
		return "UTILITY"
	}
}

// GetQueryExecution retrieves a query execution by ID.
func (b *InMemoryBackend) GetQueryExecution(id string) (*QueryExecution, error) {
	b.mu.RLock("GetQueryExecution")
	defer b.mu.RUnlock()

	qe, ok := b.queryExecutions.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: query execution %q not found", ErrNotFound, id)
	}

	cp := *qe

	return &cp, nil
}

// ListQueryExecutions returns query execution IDs, optionally filtered by workgroup.
func (b *InMemoryBackend) ListQueryExecutions(workGroup string) ([]string, error) {
	b.mu.RLock("ListQueryExecutions")
	defer b.mu.RUnlock()

	var matches []*QueryExecution
	if workGroup == "" {
		matches = b.queryExecutions.All()
	} else {
		matches = b.queryExecutionsByWorkGroup.Get(workGroup)
	}

	ids := make([]string, 0, len(matches))
	for _, qe := range matches {
		ids = append(ids, qe.QueryExecutionID)
	}

	sort.Strings(ids)

	return ids, nil
}

// isTerminalState reports whether a query execution state is terminal (cannot be stopped).
func isTerminalState(state string) bool {
	switch state {
	case stateSucceeded, stateFailed, stateCancelled:
		return true
	default:
		return false
	}
}

// StopQueryExecution marks a query execution as cancelled.
func (b *InMemoryBackend) StopQueryExecution(id string) error {
	b.mu.Lock("StopQueryExecution")
	defer b.mu.Unlock()

	qe, ok := b.queryExecutions.Get(id)
	if !ok {
		return fmt.Errorf("%w: query execution %q not found", ErrNotFound, id)
	}

	if isTerminalState(qe.Status.State) {
		return fmt.Errorf(
			"%w: query execution %q is already in terminal state %q",
			ErrValidation,
			id,
			qe.Status.State,
		)
	}

	qe.Status.State = stateCancelled

	return nil
}

// BatchGetQueryExecution retrieves multiple query executions by ID.
func (b *InMemoryBackend) BatchGetQueryExecution(
	ids []string,
) ([]QueryExecution, []UnprocessedQueryExecutionID) {
	b.mu.RLock("BatchGetQueryExecution")
	defer b.mu.RUnlock()

	found := make([]QueryExecution, 0, len(ids))
	unprocessed := make([]UnprocessedQueryExecutionID, 0, len(ids))

	for _, id := range ids {
		qe, ok := b.queryExecutions.Get(id)
		if ok {
			found = append(found, *qe)
		} else {
			unprocessed = append(unprocessed, UnprocessedQueryExecutionID{
				QueryExecutionID: id,
				ErrorCode:        "InvalidRequestException",
				ErrorMessage:     fmt.Sprintf("query execution %q not found", id),
			})
		}
	}

	return found, unprocessed
}

// --- Tags ---

// TagResource adds tags to a resource identified by ARN.
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if _, ok := b.resourceTags[arn]; !ok {
		b.resourceTags[arn] = make(map[string]string)
	}

	maps.Copy(b.resourceTags[arn], tags)

	return nil
}

// UntagResource removes tags from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(arn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	existing := b.resourceTags[arn]
	for _, k := range keys {
		delete(existing, k)
	}

	return nil
}

// ListTagsForResource returns all tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(arn string) ([]Tag, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	existing := b.resourceTags[arn]
	result := make([]Tag, 0, len(existing))

	for k, v := range existing {
		result = append(result, Tag{Key: k, Value: v})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	return result, nil
}

// copyTags returns a shallow copy of the given tag map.
func copyTags(tags map[string]string) map[string]string {
	cp := make(map[string]string, len(tags))
	maps.Copy(cp, tags)

	return cp
}

// preparedStatementKey returns the map key for a prepared statement.
func preparedStatementKey(workGroup, name string) string {
	return workGroup + "/" + name
}

// notebookNameKey returns the composite key for notebook name uniqueness.
func notebookNameKey(workGroup, name string) string {
	return workGroup + "/" + name
}

// canDeleteCapacityReservation reports whether a capacity reservation status allows deletion.
func canDeleteCapacityReservation(status string) bool {
	return status == stateCancelled || status == stateCancelling
}

// --- Prepared Statements ---

// CreatePreparedStatement creates a new prepared statement in a workgroup.
func (b *InMemoryBackend) CreatePreparedStatement(
	name, description, workGroup, queryStatement string,
) error {
	switch {
	case name == "":
		return fmt.Errorf("%w: StatementName is required", ErrValidation)
	case workGroup == "":
		return fmt.Errorf("%w: WorkGroup is required", ErrValidation)
	case queryStatement == "":
		return fmt.Errorf("%w: QueryStatement is required", ErrValidation)
	}

	b.mu.Lock("CreatePreparedStatement")
	defer b.mu.Unlock()

	key := preparedStatementKey(workGroup, name)
	if b.preparedStatements.Has(key) {
		return fmt.Errorf(
			"%w: prepared statement %q already exists in workgroup %q",
			ErrAlreadyExists,
			name,
			workGroup,
		)
	}

	now := float64(time.Now().UnixMilli()) / millisToSeconds
	b.preparedStatements.Put(&PreparedStatement{
		StatementName:    name,
		WorkGroupName:    workGroup,
		QueryStatement:   queryStatement,
		Description:      description,
		LastModifiedTime: now,
	})

	return nil
}

// GetPreparedStatement retrieves a prepared statement by name and workgroup.
func (b *InMemoryBackend) GetPreparedStatement(name, workGroup string) (*PreparedStatement, error) {
	b.mu.RLock("GetPreparedStatement")
	defer b.mu.RUnlock()

	key := preparedStatementKey(workGroup, name)
	ps, ok := b.preparedStatements.Get(key)
	if !ok {
		return nil, fmt.Errorf(
			"%w: prepared statement %q not found in workgroup %q",
			ErrResourceNotFound,
			name,
			workGroup,
		)
	}

	cp := *ps

	return &cp, nil
}

// maxListPreparedStatements is the AWS-documented maximum page size for ListPreparedStatements.
const maxListPreparedStatements = 256

// ListPreparedStatements returns summary views of prepared statements in a workgroup, sorted by name,
// with optional NextToken/MaxResults pagination.
func (b *InMemoryBackend) ListPreparedStatements(
	workGroup, nextToken string,
	maxResults int,
) ([]PreparedStatementSummary, string, error) {
	b.mu.RLock("ListPreparedStatements")
	defer b.mu.RUnlock()

	matches := b.preparedStatementsByWorkGroup.Get(workGroup)
	result := make([]PreparedStatementSummary, 0, len(matches))

	for _, ps := range matches {
		result = append(result, PreparedStatementSummary{
			StatementName:    ps.StatementName,
			LastModifiedTime: ps.LastModifiedTime,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].StatementName < result[j].StatementName
	})

	limit := maxListPreparedStatements
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	start := 0
	if nextToken != "" {
		for i, s := range result {
			if s.StatementName == nextToken {
				start = i

				break
			}
		}
	}

	result = result[start:]

	outToken := ""
	if len(result) > limit {
		outToken = result[limit].StatementName
		result = result[:limit]
	}

	return result, outToken, nil
}

// BatchGetPreparedStatement retrieves multiple prepared statements by name within a workgroup.
func (b *InMemoryBackend) BatchGetPreparedStatement(
	workGroup string,
	names []string,
) ([]PreparedStatement, []UnprocessedPreparedStatementName) {
	b.mu.RLock("BatchGetPreparedStatement")
	defer b.mu.RUnlock()

	found := make([]PreparedStatement, 0, len(names))
	unprocessed := make([]UnprocessedPreparedStatementName, 0, len(names))

	for _, name := range names {
		key := preparedStatementKey(workGroup, name)
		ps, ok := b.preparedStatements.Get(key)
		if ok {
			found = append(found, *ps)
		} else {
			unprocessed = append(unprocessed, UnprocessedPreparedStatementName{
				StatementName: name,
				ErrorMessage:  fmt.Sprintf("prepared statement %q not found in workgroup %q", name, workGroup),
			})
		}
	}

	return found, unprocessed
}

// DeletePreparedStatement removes a prepared statement by name and workgroup.
func (b *InMemoryBackend) DeletePreparedStatement(name, workGroup string) error {
	b.mu.Lock("DeletePreparedStatement")
	defer b.mu.Unlock()

	key := preparedStatementKey(workGroup, name)
	if !b.preparedStatements.Has(key) {
		return fmt.Errorf(
			"%w: prepared statement %q not found in workgroup %q",
			ErrResourceNotFound,
			name,
			workGroup,
		)
	}

	b.preparedStatements.Delete(key)

	return nil
}

// --- Capacity Reservations ---

const minCapacityDPUs int32 = 24

// CreateCapacityReservation creates a new capacity reservation.
func (b *InMemoryBackend) CreateCapacityReservation(
	name string,
	targetDPUs int32,
	tags map[string]string,
) error {
	switch {
	case name == "":
		return fmt.Errorf("%w: Name is required", ErrValidation)
	case targetDPUs < minCapacityDPUs:
		return fmt.Errorf("%w: TargetDpus minimum is 24", ErrValidation)
	}

	b.mu.Lock("CreateCapacityReservation")
	defer b.mu.Unlock()

	if b.capacityReservations.Has(name) {
		return fmt.Errorf("%w: capacity reservation %q already exists", ErrAlreadyExists, name)
	}

	now := float64(time.Now().UnixMilli()) / millisToSeconds
	b.capacityReservations.Put(&CapacityReservation{
		Name:          name,
		Status:        "ACTIVE",
		TargetDpus:    targetDPUs,
		AllocatedDpus: targetDPUs,
		Tags:          maps.Clone(tags),
		CreationTime:  now,
		LastAllocation: &CapacityAllocation{
			RequestTime:           now,
			RequestCompletionTime: now,
			Status:                stateSucceeded,
		},
		LastSuccessfulAllocationTime: now,
	})

	return nil
}

// CancelCapacityReservation cancels an active capacity reservation.
func (b *InMemoryBackend) CancelCapacityReservation(name string) error {
	b.mu.Lock("CancelCapacityReservation")
	defer b.mu.Unlock()

	cr, ok := b.capacityReservations.Get(name)
	if !ok {
		return fmt.Errorf("%w: capacity reservation %q not found", ErrNotFound, name)
	}

	cr.Status = stateCancelling

	return nil
}

// DeleteCapacityReservation removes a capacity reservation.
// The reservation must be in CANCELLING or CANCELLED status.
func (b *InMemoryBackend) DeleteCapacityReservation(name string) error {
	b.mu.Lock("DeleteCapacityReservation")
	defer b.mu.Unlock()

	cr, ok := b.capacityReservations.Get(name)
	if !ok {
		return fmt.Errorf("%w: capacity reservation %q not found", ErrNotFound, name)
	}

	if !canDeleteCapacityReservation(cr.Status) {
		return fmt.Errorf(
			"%w: capacity reservation %q must be cancelled before deletion (current status: %s)",
			ErrValidation,
			name,
			cr.Status,
		)
	}

	b.capacityReservations.Delete(name)

	return nil
}

// --- Notebooks ---

// CreateNotebook creates a new Athena notebook and returns its ID.
func (b *InMemoryBackend) CreateNotebook(
	workGroup, name string,
	tags map[string]string,
) (string, error) {
	switch {
	case workGroup == "":
		return "", fmt.Errorf("%w: WorkGroup is required", ErrValidation)
	case name == "":
		return "", fmt.Errorf("%w: Name is required", ErrValidation)
	}

	b.mu.Lock("CreateNotebook")
	defer b.mu.Unlock()

	nameKey := notebookNameKey(workGroup, name)
	if len(b.notebooksByName.Get(nameKey)) > 0 {
		return "", fmt.Errorf(
			"%w: notebook %q already exists in workgroup %q",
			ErrAlreadyExists,
			name,
			workGroup,
		)
	}

	id := randomID()
	now := float64(time.Now().UnixMilli()) / millisToSeconds
	b.notebooks.Put(&Notebook{
		NotebookID:       id,
		Name:             name,
		WorkGroup:        workGroup,
		Type:             "IPYNB",
		CreationTime:     now,
		LastModifiedTime: now,
		Content:          "",
	})

	if len(tags) > 0 {
		notebookARN := arn.Build("athena", b.region, b.accountID, fmt.Sprintf("notebook/%s", id))
		b.resourceTags[notebookARN] = copyTags(tags)
	}

	return id, nil
}

// CreatePresignedNotebookURL generates a presigned notebook URL plus the
// AuthToken/AuthTokenExpirationTime pair the real CreatePresignedNotebookUrl
// response carries alongside it.
func (b *InMemoryBackend) CreatePresignedNotebookURL(sessionID string) (string, string, float64, error) {
	url := fmt.Sprintf(
		"https://athena.%s.amazonaws.com/notebooks/presigned/%s",
		b.region,
		sessionID,
	)
	authToken, authTokenExpiration := newSessionAuthToken()

	return url, authToken, authTokenExpiration, nil
}

// DeleteNotebook removes a notebook by its ID.
func (b *InMemoryBackend) DeleteNotebook(notebookID string) error {
	b.mu.Lock("DeleteNotebook")
	defer b.mu.Unlock()

	if !b.notebooks.Has(notebookID) {
		return fmt.Errorf("%w: notebook %q not found", ErrNotFound, notebookID)
	}

	b.notebooks.Delete(notebookID)

	return nil
}

// ExportNotebook returns the notebook metadata and content for the given notebook ID.
func (b *InMemoryBackend) ExportNotebook(notebookID string) (NotebookMetadata, string, error) {
	b.mu.RLock("ExportNotebook")
	defer b.mu.RUnlock()

	nb, ok := b.notebooks.Get(notebookID)
	if !ok {
		return NotebookMetadata{}, "", fmt.Errorf(
			"%w: notebook %q not found",
			ErrNotFound,
			notebookID,
		)
	}

	meta := NotebookMetadata{
		NotebookID:       nb.NotebookID,
		Name:             nb.Name,
		WorkGroup:        nb.WorkGroup,
		Type:             nb.Type,
		CreationTime:     nb.CreationTime,
		LastModifiedTime: nb.LastModifiedTime,
	}

	return meta, nb.Content, nil
}
