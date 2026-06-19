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

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
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

	columnTypeString = "string"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = errors.New("InvalidRequestException")
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = errors.New("InvalidRequestException")
	// ErrProtected is returned when an operation is not allowed on a protected resource.
	ErrProtected = errors.New("InvalidRequestException")
	// ErrValidation is returned when input fails validation.
	ErrValidation = errors.New("InvalidRequestException")
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
	ACLConfiguration        *ACLConfiguration       `json:"ACLConfiguration,omitempty"`
	EncryptionConfiguration EncryptionConfiguration `json:"EncryptionConfiguration,omitzero"`
	ExpectedBucketOwner     string                  `json:"ExpectedBucketOwner,omitempty"`
	OutputLocation          string                  `json:"OutputLocation,omitempty"`
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
	State              string  `json:"State"`
	StateChangeReason  string  `json:"StateChangeReason,omitempty"`
	SubmissionDateTime float64 `json:"SubmissionDateTime,omitempty"`
	CompletionDateTime float64 `json:"CompletionDateTime,omitempty"`
}

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
}

// QueryExecution represents an Athena query execution.
type QueryExecution struct {
	ResultConfiguration   ResultConfiguration      `json:"ResultConfiguration,omitzero"`
	QueryExecutionContext QueryExecutionContext    `json:"QueryExecutionContext,omitzero"`
	EngineVersion         *EngineVersion           `json:"EngineVersion,omitempty"`
	QueryExecutionID      string                   `json:"QueryExecutionId"`
	Query                 string                   `json:"Query"`
	WorkGroup             string                   `json:"WorkGroup,omitempty"`
	StatementType         string                   `json:"StatementType,omitempty"`
	ExecutionParameters   []string                 `json:"ExecutionParameters,omitempty"`
	Status                QueryExecutionStatus     `json:"Status"`
	Statistics            QueryExecutionStatistics `json:"Statistics,omitzero"`
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
	CreateWorkGroup(name, description, state string, cfg WorkGroupConfiguration, tags map[string]string) error
	GetWorkGroup(name string) (*WorkGroup, error)
	ListWorkGroups() ([]WorkGroupSummary, error)
	UpdateWorkGroup(name, description, state string, cfg *WorkGroupConfiguration) error
	DeleteWorkGroup(name string) error

	// Named Queries
	CreateNamedQuery(name, description, database, queryString, workGroup string) (string, error)
	GetNamedQuery(id string) (*NamedQuery, error)
	ListNamedQueries(workGroup string) ([]string, error)
	BatchGetNamedQuery(ids []string) ([]NamedQuery, []UnprocessedNamedQueryID)
	DeleteNamedQuery(id string) error

	// Data Catalogs
	CreateDataCatalog(name, catalogType, description, connectionType string, params, tags map[string]string) error
	GetDataCatalog(name string) (*DataCatalog, error)
	ListDataCatalogs() ([]DataCatalogSummary, error)
	UpdateDataCatalog(name, catalogType, description, connectionType string, params map[string]string) error
	DeleteDataCatalog(name string) error

	// Query Executions
	StartQueryExecution(
		query, workGroup string,
		ctx QueryExecutionContext,
		rc ResultConfiguration,
		execParams []string,
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
	ListPreparedStatements(workGroup string) ([]PreparedStatementSummary, error)

	// Capacity Reservations
	CancelCapacityReservation(name string) error
	CreateCapacityReservation(name string, targetDPUs int32, tags map[string]string) error
	DeleteCapacityReservation(name string) error

	// Notebooks
	CreateNotebook(workGroup, name string, tags map[string]string) (string, error)
	CreatePresignedNotebookURL(sessionID string) (string, error)
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
	GetSessionEndpoint(id string) (string, error)
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

	// Engine version / executor / DPU listings
	ListEngineVersions() []EngineVersionDescriptor
	ListApplicationDPUSizes() []ApplicationDPUSizes
	ListExecutors(sessionID, stateFilter string) ([]Executor, error)
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	capacityReservations map[string]*CapacityReservation
	notebookNames        map[string]struct{}
	dataCatalogs         map[string]*DataCatalog
	notebooks            map[string]*Notebook
	queryResults         map[string]*sqlResult
	tableData            map[string][]map[string]any
	resourceTags         map[string]map[string]string
	preparedStatements   map[string]*PreparedStatement
	namedQueries         map[string]*NamedQuery
	workGroups           map[string]*WorkGroup
	queryExecutions      map[string]*QueryExecution
	sessions             map[string]*Session
	calculations         map[string]*CalculationExecution
	capacityAssignments  map[string]*CapacityAssignmentConfiguration
	databases            map[string]map[string]*Database
	tables               map[string]map[string]*TableMetadata
	mu                   *lockmetrics.RWMutex
	accountID            string
	region               string
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
		workGroups:           make(map[string]*WorkGroup),
		namedQueries:         make(map[string]*NamedQuery),
		dataCatalogs:         make(map[string]*DataCatalog),
		queryExecutions:      make(map[string]*QueryExecution),
		queryResults:         make(map[string]*sqlResult),
		tableData:            make(map[string][]map[string]any),
		resourceTags:         make(map[string]map[string]string),
		preparedStatements:   make(map[string]*PreparedStatement),
		capacityReservations: make(map[string]*CapacityReservation),
		notebooks:            make(map[string]*Notebook),
		notebookNames:        make(map[string]struct{}),
		sessions:             make(map[string]*Session),
		calculations:         make(map[string]*CalculationExecution),
		capacityAssignments:  make(map[string]*CapacityAssignmentConfiguration),
		databases:            make(map[string]map[string]*Database),
		tables:               make(map[string]map[string]*TableMetadata),
		region:               region,
		accountID:            accountID,
		mu:                   lockmetrics.New("athena"),
	}

	b.workGroups[defaultWorkGroup] = &WorkGroup{
		Name:  defaultWorkGroup,
		State: "ENABLED",
	}

	b.seedDefaultMetadata()

	return b
}

// seedDefaultMetadata seeds AwsDataCatalog and example database/table metadata
// so that metadata operations return useful sample data without explicit setup.
func (b *InMemoryBackend) seedDefaultMetadata() {
	const database = "default"

	b.dataCatalogs[awsDataCatalog] = &DataCatalog{
		Name:   awsDataCatalog,
		Type:   "GLUE",
		Status: "CREATE_COMPLETE",
	}

	b.databases[awsDataCatalog] = map[string]*Database{
		database: {
			Name:        database,
			Description: "Default Athena database",
		},
	}

	b.tables[awsDataCatalog+"/"+database] = map[string]*TableMetadata{
		"sample_table": {
			Name:      "sample_table",
			TableType: "EXTERNAL_TABLE",
			Columns: []Column{
				{Name: "id", Type: "bigint"},
				{Name: "value", Type: columnTypeString},
			},
		},
	}
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

func (b *InMemoryBackend) workGroupARN(name string) string {
	return fmt.Sprintf("arn:aws:athena:%s:%s:workgroup/%s", b.region, b.accountID, name)
}

func (b *InMemoryBackend) dataCatalogARN(name string) string {
	return fmt.Sprintf("arn:aws:athena:%s:%s:datacatalog/%s", b.region, b.accountID, name)
}

// --- WorkGroups ---

// CreateWorkGroup creates a new workgroup.
// athenaMinBytesScannedCutoff is the minimum value AWS Athena permits for the
// per-query data-scan limit (10 MB).
const athenaMinBytesScannedCutoff int64 = 10 * 1024 * 1024

// validateWorkGroupConfiguration enforces AWS-documented bounds for workgroup
// configuration knobs. Currently this only checks BytesScannedCutoffPerQuery
// (a positive value < 10 MiB is rejected; zero means "unlimited" and is
// permitted).
func validateWorkGroupConfiguration(cfg WorkGroupConfiguration) error {
	if cfg.BytesScannedCutoffPerQuery > 0 && cfg.BytesScannedCutoffPerQuery < athenaMinBytesScannedCutoff {
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

	if err := validateWorkGroupConfiguration(cfg); err != nil {
		return err
	}

	b.mu.Lock("CreateWorkGroup")
	defer b.mu.Unlock()

	if _, ok := b.workGroups[name]; ok {
		return fmt.Errorf("%w: workgroup %q already exists", ErrAlreadyExists, name)
	}

	if state == "" {
		state = "ENABLED"
	}

	now := float64(time.Now().UnixMilli()) / millisToSeconds
	b.workGroups[name] = &WorkGroup{
		Name:          name,
		Description:   description,
		State:         state,
		Tags:          maps.Clone(tags),
		Configuration: cfg,
		CreationTime:  now,
	}

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

	wg, ok := b.workGroups[name]
	if !ok {
		return nil, fmt.Errorf("%w: workgroup %q not found", ErrNotFound, name)
	}

	cp := *wg
	cp.Tags = maps.Clone(wg.Tags)

	return &cp, nil
}

// ListWorkGroups returns summaries of all workgroups.
func (b *InMemoryBackend) ListWorkGroups() ([]WorkGroupSummary, error) {
	b.mu.RLock("ListWorkGroups")
	defer b.mu.RUnlock()

	result := make([]WorkGroupSummary, 0, len(b.workGroups))
	for _, wg := range b.workGroups {
		sum := WorkGroupSummary{
			Name:         wg.Name,
			Description:  wg.Description,
			State:        wg.State,
			CreationTime: wg.CreationTime,
		}
		if ev := wg.Configuration.EngineVersion; ev.SelectedEngineVersion != "" || ev.EffectiveEngineVersion != "" {
			cp := ev
			sum.EngineVersion = &cp
		}
		result = append(result, sum)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// UpdateWorkGroup updates an existing workgroup.
func (b *InMemoryBackend) UpdateWorkGroup(name, description, state string, cfg *WorkGroupConfiguration) error {
	if cfg != nil {
		if err := validateWorkGroupConfiguration(*cfg); err != nil {
			return err
		}
	}

	b.mu.Lock("UpdateWorkGroup")
	defer b.mu.Unlock()

	wg, ok := b.workGroups[name]
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

	if _, ok := b.workGroups[name]; !ok {
		return fmt.Errorf("%w: workgroup %q not found", ErrNotFound, name)
	}

	delete(b.workGroups, name)
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

	if _, ok := b.workGroups[workGroup]; !ok {
		return "", fmt.Errorf("%w: workgroup %q not found", ErrNotFound, workGroup)
	}

	id := randomID()
	b.namedQueries[id] = &NamedQuery{
		NamedQueryID: id,
		Name:         name,
		Description:  description,
		Database:     database,
		QueryString:  queryString,
		WorkGroup:    workGroup,
	}

	return id, nil
}

// GetNamedQuery retrieves a named query by ID.
func (b *InMemoryBackend) GetNamedQuery(id string) (*NamedQuery, error) {
	b.mu.RLock("GetNamedQuery")
	defer b.mu.RUnlock()

	q, ok := b.namedQueries[id]
	if !ok {
		return nil, fmt.Errorf("%w: named query %q not found", ErrNotFound, id)
	}

	cp := *q

	return &cp, nil
}

// ListNamedQueries returns named query IDs, optionally filtered by workgroup.
func (b *InMemoryBackend) ListNamedQueries(workGroup string) ([]string, error) {
	b.mu.RLock("ListNamedQueries")
	defer b.mu.RUnlock()

	ids := make([]string, 0, len(b.namedQueries))
	for id, q := range b.namedQueries {
		if workGroup == "" || q.WorkGroup == workGroup {
			ids = append(ids, id)
		}
	}

	sort.Strings(ids)

	return ids, nil
}

// BatchGetNamedQuery retrieves multiple named queries by ID.
func (b *InMemoryBackend) BatchGetNamedQuery(ids []string) ([]NamedQuery, []UnprocessedNamedQueryID) {
	b.mu.RLock("BatchGetNamedQuery")
	defer b.mu.RUnlock()

	found := make([]NamedQuery, 0, len(ids))
	unprocessed := make([]UnprocessedNamedQueryID, 0, len(ids))

	for _, id := range ids {
		q, ok := b.namedQueries[id]
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

	if _, ok := b.namedQueries[id]; !ok {
		return fmt.Errorf("%w: named query %q not found", ErrNotFound, id)
	}

	delete(b.namedQueries, id)

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

	if _, ok := b.dataCatalogs[name]; ok {
		return fmt.Errorf("%w: data catalog %q already exists", ErrAlreadyExists, name)
	}

	status := "CREATE_COMPLETE"
	if catalogType == "FEDERATED" {
		status = "CREATE_IN_PROGRESS"
	}

	b.dataCatalogs[name] = &DataCatalog{
		Name:           name,
		Type:           catalogType,
		Description:    description,
		ConnectionType: connectionType,
		Parameters:     maps.Clone(params),
		Tags:           maps.Clone(tags),
		Status:         status,
	}

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

	dc, ok := b.dataCatalogs[name]
	if !ok {
		return nil, fmt.Errorf("%w: data catalog %q not found", ErrNotFound, name)
	}

	cp := *dc
	cp.Tags = maps.Clone(dc.Tags)
	cp.Parameters = maps.Clone(dc.Parameters)

	return &cp, nil
}

// ListDataCatalogs returns summaries of all data catalogs.
func (b *InMemoryBackend) ListDataCatalogs() ([]DataCatalogSummary, error) {
	b.mu.RLock("ListDataCatalogs")
	defer b.mu.RUnlock()

	result := make([]DataCatalogSummary, 0, len(b.dataCatalogs))
	for _, dc := range b.dataCatalogs {
		result = append(result, DataCatalogSummary{
			CatalogName:    dc.Name,
			Type:           dc.Type,
			ConnectionType: dc.ConnectionType,
			Status:         dc.Status,
			Error:          dc.Error,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CatalogName < result[j].CatalogName
	})

	return result, nil
}

// UpdateDataCatalog updates an existing data catalog.
func (b *InMemoryBackend) UpdateDataCatalog(
	name, catalogType, description, connectionType string,
	params map[string]string,
) error {
	b.mu.Lock("UpdateDataCatalog")
	defer b.mu.Unlock()

	dc, ok := b.dataCatalogs[name]
	if !ok {
		return fmt.Errorf("%w: data catalog %q not found", ErrNotFound, name)
	}

	if catalogType != "" {
		dc.Type = catalogType
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
		return fmt.Errorf("%w: cannot delete the built-in data catalog %s", ErrProtected, awsDataCatalog)
	}

	b.mu.Lock("DeleteDataCatalog")
	defer b.mu.Unlock()

	if _, ok := b.dataCatalogs[name]; !ok {
		return fmt.Errorf("%w: data catalog %q not found", ErrNotFound, name)
	}

	delete(b.dataCatalogs, name)
	delete(b.resourceTags, b.dataCatalogARN(name))

	return nil
}

// --- Query Executions ---

// StartQueryExecution records a new query execution and returns its ID.
func (b *InMemoryBackend) StartQueryExecution(
	query, workGroup string,
	ctx QueryExecutionContext,
	rc ResultConfiguration,
	execParams []string,
) (string, error) {
	if workGroup == "" {
		workGroup = defaultWorkGroup
	}

	b.mu.Lock("StartQueryExecution")

	if _, ok := b.workGroups[workGroup]; !ok {
		b.mu.Unlock()

		return "", fmt.Errorf("%w: workgroup %q not found", ErrNotFound, workGroup)
	}

	id := randomID()
	now := float64(time.Now().UnixMilli()) / millisToSeconds

	const mockEngineMs int64 = 100

	qe := &QueryExecution{
		QueryExecutionID:      id,
		Query:                 query,
		ResultConfiguration:   rc,
		QueryExecutionContext: ctx,
		WorkGroup:             workGroup,
		StatementType:         inferStatementType(query),
		ExecutionParameters:   execParams,
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
		},
	}

	b.queryExecutions[id] = qe
	b.mu.Unlock()

	// Execute SQL outside the write-lock: executeSQL acquires its own RLock.
	result := b.executeSQL(query, ctx)

	b.mu.Lock("StartQueryExecution-storeResult")
	b.queryResults[id] = result
	b.mu.Unlock()

	return id, nil
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

	qe, ok := b.queryExecutions[id]
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

	ids := make([]string, 0, len(b.queryExecutions))
	for id, qe := range b.queryExecutions {
		if workGroup == "" || qe.WorkGroup == workGroup {
			ids = append(ids, id)
		}
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

	qe, ok := b.queryExecutions[id]
	if !ok {
		return fmt.Errorf("%w: query execution %q not found", ErrNotFound, id)
	}

	if isTerminalState(qe.Status.State) {
		return fmt.Errorf("%w: query execution %q is already in terminal state %q", ErrValidation, id, qe.Status.State)
	}

	qe.Status.State = stateCancelled

	return nil
}

// BatchGetQueryExecution retrieves multiple query executions by ID.
func (b *InMemoryBackend) BatchGetQueryExecution(ids []string) ([]QueryExecution, []UnprocessedQueryExecutionID) {
	b.mu.RLock("BatchGetQueryExecution")
	defer b.mu.RUnlock()

	found := make([]QueryExecution, 0, len(ids))
	unprocessed := make([]UnprocessedQueryExecutionID, 0, len(ids))

	for _, id := range ids {
		qe, ok := b.queryExecutions[id]
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
	return status == stateCancelling || status == stateCancelled
}

// --- Prepared Statements ---

// CreatePreparedStatement creates a new prepared statement in a workgroup.
func (b *InMemoryBackend) CreatePreparedStatement(name, description, workGroup, queryStatement string) error {
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
	if _, ok := b.preparedStatements[key]; ok {
		return fmt.Errorf("%w: prepared statement %q already exists in workgroup %q", ErrAlreadyExists, name, workGroup)
	}

	now := float64(time.Now().UnixMilli()) / millisToSeconds
	b.preparedStatements[key] = &PreparedStatement{
		StatementName:    name,
		WorkGroupName:    workGroup,
		QueryStatement:   queryStatement,
		Description:      description,
		LastModifiedTime: now,
	}

	return nil
}

// GetPreparedStatement retrieves a prepared statement by name and workgroup.
func (b *InMemoryBackend) GetPreparedStatement(name, workGroup string) (*PreparedStatement, error) {
	b.mu.RLock("GetPreparedStatement")
	defer b.mu.RUnlock()

	key := preparedStatementKey(workGroup, name)
	ps, ok := b.preparedStatements[key]
	if !ok {
		return nil, fmt.Errorf("%w: prepared statement %q not found in workgroup %q", ErrNotFound, name, workGroup)
	}

	cp := *ps

	return &cp, nil
}

// ListPreparedStatements returns summary views of prepared statements in a workgroup, sorted by name.
func (b *InMemoryBackend) ListPreparedStatements(workGroup string) ([]PreparedStatementSummary, error) {
	b.mu.RLock("ListPreparedStatements")
	defer b.mu.RUnlock()

	result := make([]PreparedStatementSummary, 0, len(b.preparedStatements))
	for key, ps := range b.preparedStatements {
		prefix := workGroup + "/"
		if strings.HasPrefix(key, prefix) {
			result = append(result, PreparedStatementSummary{
				StatementName:    ps.StatementName,
				LastModifiedTime: ps.LastModifiedTime,
			})
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].StatementName < result[j].StatementName
	})

	return result, nil
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
		ps, ok := b.preparedStatements[key]
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
	if _, ok := b.preparedStatements[key]; !ok {
		return fmt.Errorf("%w: prepared statement %q not found in workgroup %q", ErrNotFound, name, workGroup)
	}

	delete(b.preparedStatements, key)

	return nil
}

// --- Capacity Reservations ---

// CreateCapacityReservation creates a new capacity reservation.
func (b *InMemoryBackend) CreateCapacityReservation(name string, targetDPUs int32, tags map[string]string) error {
	switch {
	case name == "":
		return fmt.Errorf("%w: Name is required", ErrValidation)
	case targetDPUs <= 0:
		return fmt.Errorf("%w: TargetDpus must be greater than 0", ErrValidation)
	}

	b.mu.Lock("CreateCapacityReservation")
	defer b.mu.Unlock()

	if _, ok := b.capacityReservations[name]; ok {
		return fmt.Errorf("%w: capacity reservation %q already exists", ErrAlreadyExists, name)
	}

	now := float64(time.Now().UnixMilli()) / millisToSeconds
	b.capacityReservations[name] = &CapacityReservation{
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
	}

	return nil
}

// CancelCapacityReservation cancels an active capacity reservation.
func (b *InMemoryBackend) CancelCapacityReservation(name string) error {
	b.mu.Lock("CancelCapacityReservation")
	defer b.mu.Unlock()

	cr, ok := b.capacityReservations[name]
	if !ok {
		return fmt.Errorf("%w: capacity reservation %q not found", ErrNotFound, name)
	}

	cr.Status = stateCancelling

	return nil
}

// DeleteCapacityReservation removes a capacity reservation.
// The reservation must have been cancelled first (status CANCELLING or CANCELLED).
func (b *InMemoryBackend) DeleteCapacityReservation(name string) error {
	b.mu.Lock("DeleteCapacityReservation")
	defer b.mu.Unlock()

	cr, ok := b.capacityReservations[name]
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

	delete(b.capacityReservations, name)

	return nil
}

// --- Notebooks ---

// CreateNotebook creates a new Athena notebook and returns its ID.
func (b *InMemoryBackend) CreateNotebook(workGroup, name string, tags map[string]string) (string, error) {
	switch {
	case workGroup == "":
		return "", fmt.Errorf("%w: WorkGroup is required", ErrValidation)
	case name == "":
		return "", fmt.Errorf("%w: Name is required", ErrValidation)
	}

	b.mu.Lock("CreateNotebook")
	defer b.mu.Unlock()

	nameKey := notebookNameKey(workGroup, name)
	if _, exists := b.notebookNames[nameKey]; exists {
		return "", fmt.Errorf("%w: notebook %q already exists in workgroup %q", ErrAlreadyExists, name, workGroup)
	}

	id := randomID()
	now := float64(time.Now().UnixMilli()) / millisToSeconds
	b.notebooks[id] = &Notebook{
		NotebookID:       id,
		Name:             name,
		WorkGroup:        workGroup,
		Type:             "IPYNB",
		CreationTime:     now,
		LastModifiedTime: now,
		Content:          "",
	}
	b.notebookNames[nameKey] = struct{}{}

	if len(tags) > 0 {
		notebookARN := fmt.Sprintf("arn:aws:athena:%s:%s:notebook/%s", b.region, b.accountID, id)
		b.resourceTags[notebookARN] = copyTags(tags)
	}

	return id, nil
}

// CreatePresignedNotebookURL generates a presigned URL for a notebook session.
func (b *InMemoryBackend) CreatePresignedNotebookURL(sessionID string) (string, error) {
	return fmt.Sprintf("https://athena.%s.amazonaws.com/notebooks/presigned/%s", b.region, sessionID), nil
}

// DeleteNotebook removes a notebook by its ID.
func (b *InMemoryBackend) DeleteNotebook(notebookID string) error {
	b.mu.Lock("DeleteNotebook")
	defer b.mu.Unlock()

	nb, ok := b.notebooks[notebookID]
	if !ok {
		return fmt.Errorf("%w: notebook %q not found", ErrNotFound, notebookID)
	}

	nameKey := notebookNameKey(nb.WorkGroup, nb.Name)
	delete(b.notebookNames, nameKey)
	delete(b.notebooks, notebookID)

	return nil
}

// ExportNotebook returns the notebook metadata and content for the given notebook ID.
func (b *InMemoryBackend) ExportNotebook(notebookID string) (NotebookMetadata, string, error) {
	b.mu.RLock("ExportNotebook")
	defer b.mu.RUnlock()

	nb, ok := b.notebooks[notebookID]
	if !ok {
		return NotebookMetadata{}, "", fmt.Errorf("%w: notebook %q not found", ErrNotFound, notebookID)
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
