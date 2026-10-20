// Package cosmosdb emulates Azure Cosmos DB over its REST+JSON gateway protocol: the Core (SQL) API (databases,
// containers with hierarchical partition keys and TTL, documents, a SQL query engine with joins, subqueries,
// aggregates and built-ins, continuation tokens, request-unit accounting) and the Table API. Master-key auth and
// TLS are opt-in. See PARITY.md for scope and known gaps.
package cosmosdb

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// StorageBackend defines the interface for an Azure Cosmos DB (Core/SQL API)
// backend. Shaped after services/azuretable's StorageBackend: a narrow,
// testable seam between the wire handler and storage, so handler tests can
// substitute a fake.
//
// ifMatch on ReplaceDocument/DeleteDocument threads through the same
// If-Match states services/azuretable's StorageBackend documents:
//   - "" (no If-Match header): unconditional mutation.
//   - any other string: the document must exist AND its current ETag must
//     equal this value, else ErrETagMismatch.
//
// partitionKey parameters are the caller-supplied partition key value's
// canonical JSON encoding (see canonicalPartitionKeyJSON), extracted from the
// mandatory x-ms-documentdb-partitionkey header on every point operation.
type StorageBackend interface {
	CreateDatabase(id string) (DatabaseInfo, error)
	GetDatabase(id string) (DatabaseInfo, error)
	ListDatabases() []DatabaseInfo
	DeleteDatabase(id string) error

	CreateUser(dbID, id string) (UserInfo, error)
	GetUser(dbID, id string) (UserInfo, error)
	ListUsers(dbID string) ([]UserInfo, error)
	ReplaceUser(dbID, id, newID string) (UserInfo, error)
	DeleteUser(dbID, id string) error

	CreatePermission(dbID, userID string, spec PermissionSpec) (PermissionInfo, error)
	GetPermission(dbID, userID, id string) (PermissionInfo, error)
	ListPermissions(dbID, userID string) ([]PermissionInfo, error)
	ReplacePermission(dbID, userID, id string, spec PermissionSpec) (PermissionInfo, error)
	DeletePermission(dbID, userID, id string) error

	CreateContainer(dbID string, spec ContainerSpec) (ContainerInfo, error)
	GetContainer(dbID, containerID string) (ContainerInfo, error)
	ListContainers(dbID string) ([]ContainerInfo, error)
	DeleteContainer(dbID, containerID string) error

	// CreateDocument creates (or, if upsert, inserts-or-replaces) a document
	// in containerID. The document's partition key value is derived from
	// body per the container's declared partition key path; body's "id"
	// field is used verbatim if present (a non-string "id" is rejected), or
	// generated if absent.
	CreateDocument(dbID, containerID string, body map[string]any, upsert bool) (DocumentInfo, error)
	GetDocument(dbID, containerID, partitionKey, id string) (DocumentInfo, error)
	ReplaceDocument(
		dbID, containerID, partitionKey, id string, body map[string]any, ifMatch string,
	) (DocumentInfo, error)
	DeleteDocument(dbID, containerID, partitionKey, id, ifMatch string) error
	// ListDocuments returns every document in containerID, ordered by ID,
	// for both the read-feed (GET .../docs) and the SQL query engine's FROM
	// clause. Cross-partition: it is not filtered by partition key.
	ListDocuments(dbID, containerID string) ([]DocumentInfo, error)

	// Reset clears all in-memory state. Used by the
	// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
	Reset()
}
