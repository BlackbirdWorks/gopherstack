package athena

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	idChars  = "abcdef0123456789"
	idLength = 10

	defaultWorkGroup = "primary"
	arnRegion        = "us-east-1"
	arnAccount       = "000000000000"
	millisToSeconds  = 1000.0
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = errors.New("InvalidRequestException")
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = errors.New("InvalidRequestException")
	// ErrProtected is returned when an operation is not allowed on a protected resource.
	ErrProtected = errors.New("InvalidRequestException")
)

// EncryptionConfiguration holds encryption settings for query results.
type EncryptionConfiguration struct {
	EncryptionOption string `json:"EncryptionOption,omitempty"`
	KmsKey           string `json:"KmsKey,omitempty"`
}

// ResultConfiguration holds the configuration for where query results are stored.
type ResultConfiguration struct {
	OutputLocation          string                  `json:"OutputLocation,omitempty"`
	EncryptionConfiguration EncryptionConfiguration `json:"EncryptionConfiguration,omitzero"`
}

// EngineVersion holds the engine version configuration for a workgroup.
type EngineVersion struct {
	SelectedEngineVersion  string `json:"SelectedEngineVersion,omitempty"`
	EffectiveEngineVersion string `json:"EffectiveEngineVersion,omitempty"`
}

// WorkGroupConfiguration holds configuration for a workgroup.
type WorkGroupConfiguration struct {
	ResultConfiguration             ResultConfiguration `json:"ResultConfiguration,omitzero"`
	EngineVersion                   EngineVersion       `json:"EngineVersion,omitzero"`
	BytesScannedCutoffPerQuery      int64               `json:"BytesScannedCutoffPerQuery,omitempty"`
	PublishCloudWatchMetricsEnabled bool                `json:"PublishCloudWatchMetricsEnabled,omitempty"`
	RequesterPaysEnabled            bool                `json:"RequesterPaysEnabled,omitempty"`
}

// WorkGroup represents an Athena workgroup.
type WorkGroup struct {
	Name          string                 `json:"Name"`
	Description   string                 `json:"Description,omitempty"`
	State         string                 `json:"State"`
	Tags          map[string]string      `json:"Tags,omitempty"`
	Configuration WorkGroupConfiguration `json:"Configuration,omitzero"`
}

// WorkGroupSummary is a reduced view of a WorkGroup for list responses.
type WorkGroupSummary struct {
	Name  string `json:"Name"`
	State string `json:"State"`
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
	Parameters  map[string]string `json:"Parameters,omitempty"`
	Tags        map[string]string `json:"Tags,omitempty"`
	Name        string            `json:"Name"`
	Type        string            `json:"Type"`
	Description string            `json:"Description,omitempty"`
}

// DataCatalogSummary is a reduced view of a DataCatalog for list responses.
type DataCatalogSummary struct {
	CatalogName string `json:"CatalogName"`
	Type        string `json:"Type"`
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
	EngineExecutionTimeInMillis int64 `json:"EngineExecutionTimeInMillis,omitempty"`
	DataScannedInBytes          int64 `json:"DataScannedInBytes,omitempty"`
}

// QueryExecution represents an Athena query execution.
type QueryExecution struct {
	ResultConfiguration   ResultConfiguration      `json:"ResultConfiguration,omitzero"`
	QueryExecutionContext QueryExecutionContext    `json:"QueryExecutionContext,omitzero"`
	QueryExecutionID      string                   `json:"QueryExecutionId"`
	Query                 string                   `json:"Query"`
	WorkGroup             string                   `json:"WorkGroup,omitempty"`
	Status                QueryExecutionStatus     `json:"Status"`
	Statistics            QueryExecutionStatistics `json:"Statistics,omitzero"`
}

// Tag is a key-value pair.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
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
	BatchGetNamedQuery(ids []string) ([]NamedQuery, []string)
	DeleteNamedQuery(id string) error

	// Data Catalogs
	CreateDataCatalog(name, catalogType, description string, params, tags map[string]string) error
	GetDataCatalog(name string) (*DataCatalog, error)
	ListDataCatalogs() ([]DataCatalogSummary, error)
	UpdateDataCatalog(name, catalogType, description string, params map[string]string) error
	DeleteDataCatalog(name string) error

	// Query Executions
	StartQueryExecution(query, workGroup string, ctx QueryExecutionContext, rc ResultConfiguration) (string, error)
	GetQueryExecution(id string) (*QueryExecution, error)
	ListQueryExecutions(workGroup string) ([]string, error)
	StopQueryExecution(id string) error
	BatchGetQueryExecution(ids []string) ([]QueryExecution, []string)

	// Tags
	TagResource(arn string, tags map[string]string) error
	UntagResource(arn string, keys []string) error
	ListTagsForResource(arn string) ([]Tag, error)
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	workGroups      map[string]*WorkGroup
	namedQueries    map[string]*NamedQuery
	dataCatalogs    map[string]*DataCatalog
	queryExecutions map[string]*QueryExecution
	resourceTags    map[string]map[string]string
	mu              *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend and seeds the default "primary" workgroup.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		workGroups:      make(map[string]*WorkGroup),
		namedQueries:    make(map[string]*NamedQuery),
		dataCatalogs:    make(map[string]*DataCatalog),
		queryExecutions: make(map[string]*QueryExecution),
		resourceTags:    make(map[string]map[string]string),
		mu:              lockmetrics.New("athena"),
	}

	b.workGroups[defaultWorkGroup] = &WorkGroup{
		Name:  defaultWorkGroup,
		State: "ENABLED",
	}

	return b
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

func workGroupARN(name string) string {
	return fmt.Sprintf("arn:aws:athena:%s:%s:workgroup/%s", arnRegion, arnAccount, name)
}

func dataCatalogARN(name string) string {
	return fmt.Sprintf("arn:aws:athena:%s:%s:datacatalog/%s", arnRegion, arnAccount, name)
}

// --- WorkGroups ---

// CreateWorkGroup creates a new workgroup.
func (b *InMemoryBackend) CreateWorkGroup(
	name, description, state string,
	cfg WorkGroupConfiguration,
	tags map[string]string,
) error {
	b.mu.Lock("CreateWorkGroup")
	defer b.mu.Unlock()

	if _, ok := b.workGroups[name]; ok {
		return fmt.Errorf("%w: workgroup %q already exists", ErrAlreadyExists, name)
	}

	if state == "" {
		state = "ENABLED"
	}

	b.workGroups[name] = &WorkGroup{
		Name:          name,
		Description:   description,
		State:         state,
		Tags:          tags,
		Configuration: cfg,
	}

	arn := workGroupARN(name)
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

	return &cp, nil
}

// ListWorkGroups returns summaries of all workgroups.
func (b *InMemoryBackend) ListWorkGroups() ([]WorkGroupSummary, error) {
	b.mu.RLock("ListWorkGroups")
	defer b.mu.RUnlock()

	result := make([]WorkGroupSummary, 0, len(b.workGroups))
	for _, wg := range b.workGroups {
		result = append(result, WorkGroupSummary{Name: wg.Name, State: wg.State})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// UpdateWorkGroup updates an existing workgroup.
func (b *InMemoryBackend) UpdateWorkGroup(name, description, state string, cfg *WorkGroupConfiguration) error {
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
	delete(b.resourceTags, workGroupARN(name))

	return nil
}

// --- Named Queries ---

// CreateNamedQuery creates a new named query and returns its ID.
func (b *InMemoryBackend) CreateNamedQuery(
	name, description, database, queryString, workGroup string,
) (string, error) {
	b.mu.Lock("CreateNamedQuery")
	defer b.mu.Unlock()

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

	ids := make([]string, 0)
	for id, q := range b.namedQueries {
		if workGroup == "" || q.WorkGroup == workGroup {
			ids = append(ids, id)
		}
	}

	sort.Strings(ids)

	return ids, nil
}

// BatchGetNamedQuery retrieves multiple named queries by ID.
func (b *InMemoryBackend) BatchGetNamedQuery(ids []string) ([]NamedQuery, []string) {
	b.mu.RLock("BatchGetNamedQuery")
	defer b.mu.RUnlock()

	found := make([]NamedQuery, 0)
	unprocessed := make([]string, 0)

	for _, id := range ids {
		q, ok := b.namedQueries[id]
		if ok {
			found = append(found, *q)
		} else {
			unprocessed = append(unprocessed, id)
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
	name, catalogType, description string,
	params, tags map[string]string,
) error {
	b.mu.Lock("CreateDataCatalog")
	defer b.mu.Unlock()

	if _, ok := b.dataCatalogs[name]; ok {
		return fmt.Errorf("%w: data catalog %q already exists", ErrAlreadyExists, name)
	}

	b.dataCatalogs[name] = &DataCatalog{
		Name:        name,
		Type:        catalogType,
		Description: description,
		Parameters:  params,
		Tags:        tags,
	}

	arn := dataCatalogARN(name)
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

	return &cp, nil
}

// ListDataCatalogs returns summaries of all data catalogs.
func (b *InMemoryBackend) ListDataCatalogs() ([]DataCatalogSummary, error) {
	b.mu.RLock("ListDataCatalogs")
	defer b.mu.RUnlock()

	result := make([]DataCatalogSummary, 0, len(b.dataCatalogs))
	for _, dc := range b.dataCatalogs {
		result = append(result, DataCatalogSummary{CatalogName: dc.Name, Type: dc.Type})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CatalogName < result[j].CatalogName
	})

	return result, nil
}

// UpdateDataCatalog updates an existing data catalog.
func (b *InMemoryBackend) UpdateDataCatalog(name, catalogType, description string, params map[string]string) error {
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

	if params != nil {
		dc.Parameters = params
	}

	return nil
}

// DeleteDataCatalog removes a data catalog by name.
func (b *InMemoryBackend) DeleteDataCatalog(name string) error {
	b.mu.Lock("DeleteDataCatalog")
	defer b.mu.Unlock()

	if _, ok := b.dataCatalogs[name]; !ok {
		return fmt.Errorf("%w: data catalog %q not found", ErrNotFound, name)
	}

	delete(b.dataCatalogs, name)
	delete(b.resourceTags, dataCatalogARN(name))

	return nil
}

// --- Query Executions ---

// StartQueryExecution records a new query execution and returns its ID.
func (b *InMemoryBackend) StartQueryExecution(
	query, workGroup string,
	ctx QueryExecutionContext,
	rc ResultConfiguration,
) (string, error) {
	b.mu.Lock("StartQueryExecution")
	defer b.mu.Unlock()

	id := randomID()
	now := float64(time.Now().UnixMilli()) / millisToSeconds

	b.queryExecutions[id] = &QueryExecution{
		QueryExecutionID:      id,
		Query:                 query,
		ResultConfiguration:   rc,
		QueryExecutionContext: ctx,
		WorkGroup:             workGroup,
		Status: QueryExecutionStatus{
			State:              "SUCCEEDED",
			SubmissionDateTime: now,
			CompletionDateTime: now,
		},
		Statistics: QueryExecutionStatistics{
			EngineExecutionTimeInMillis: 100, //nolint:mnd // mock execution time
			DataScannedInBytes:          0,
		},
	}

	return id, nil
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

	ids := make([]string, 0)
	for id, qe := range b.queryExecutions {
		if workGroup == "" || qe.WorkGroup == workGroup {
			ids = append(ids, id)
		}
	}

	sort.Strings(ids)

	return ids, nil
}

// StopQueryExecution marks a query execution as cancelled.
func (b *InMemoryBackend) StopQueryExecution(id string) error {
	b.mu.Lock("StopQueryExecution")
	defer b.mu.Unlock()

	qe, ok := b.queryExecutions[id]
	if !ok {
		return fmt.Errorf("%w: query execution %q not found", ErrNotFound, id)
	}

	qe.Status.State = "CANCELLED"

	return nil
}

// BatchGetQueryExecution retrieves multiple query executions by ID.
func (b *InMemoryBackend) BatchGetQueryExecution(ids []string) ([]QueryExecution, []string) {
	b.mu.RLock("BatchGetQueryExecution")
	defer b.mu.RUnlock()

	found := make([]QueryExecution, 0)
	unprocessed := make([]string, 0)

	for _, id := range ids {
		qe, ok := b.queryExecutions[id]
		if ok {
			found = append(found, *qe)
		} else {
			unprocessed = append(unprocessed, id)
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
