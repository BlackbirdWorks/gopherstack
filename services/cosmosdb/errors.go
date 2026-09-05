package cosmosdb

import "errors"

// Sentinel errors for Azure Cosmos DB (Core/SQL API) operations.
var (
	ErrDatabaseNotFound       = errors.New("cosmosdb: database not found")
	ErrDatabaseAlreadyExists  = errors.New("cosmosdb: database already exists")
	ErrContainerNotFound      = errors.New("cosmosdb: container not found")
	ErrContainerAlreadyExists = errors.New("cosmosdb: container already exists")
	ErrDocumentNotFound       = errors.New("cosmosdb: document not found")
	ErrDocumentAlreadyExists  = errors.New("cosmosdb: document already exists")
	ErrETagMismatch           = errors.New("cosmosdb: etag mismatch")

	// ErrInvalidDocument is returned when a document body is not a JSON
	// object, or its "id" field is present but not a non-empty string.
	ErrInvalidDocument = errors.New("cosmosdb: invalid document body")

	// ErrInvalidPartitionKeyPath is returned when a container is created
	// with a partitionKey.paths that is empty or not exactly one path --
	// hierarchical (multi-path) partition keys are out of scope for this
	// milestone; see PARITY.md's deferred section.
	ErrInvalidPartitionKeyPath = errors.New("cosmosdb: partitionKey.paths must contain exactly one path")

	// ErrQueryParse and ErrQueryTooDeep are returned by ParseQuery. A parse
	// error always surfaces as 400 BadRequest, never a panic -- see
	// document_ops.go's handleQuery.
	ErrQueryParse   = errors.New("cosmosdb: query parse error")
	ErrQueryTooDeep = errors.New("cosmosdb: query expression nested too deeply")

	// ErrSnapshotNull* are returned by Restore when a snapshot's nested map
	// holds a JSON null entry, which decodes to a nil pointer that would
	// panic on first dereference if stored as-is. See persistence.go.
	ErrSnapshotDatabaseNull  = errors.New("cosmosdb: restore snapshot: database is null")
	ErrSnapshotContainerNull = errors.New("cosmosdb: restore snapshot: container is null")
	ErrSnapshotDocumentNull  = errors.New("cosmosdb: restore snapshot: document is null")
)
