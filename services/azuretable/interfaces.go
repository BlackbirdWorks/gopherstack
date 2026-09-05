// Package azuretable provides a local, in-memory emulation of Azure Table
// Storage's REST+JSON/OData wire protocol (table CRUD plus entity
// insert/get/query/replace/merge/delete, including a hand-written $filter
// lexer/parser/evaluator), Azurite-compatible enough for unmodified
// azure-sdk-for-go clients to operate against. See AZURE.md and PARITY.md
// for scope and known gaps.
//
// Unlike services/azurequeue and services/azureblob, this package has no
// janitor.go: Table Storage entities carry no TTL/expiry concept (there is
// no message-visibility or blob-lease analogue), so there is nothing for a
// background sweep to do.
package azuretable

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// IfMatchAny is the wildcard If-Match value ("*") meaning "must currently
// exist, but match unconditionally on ETag" -- as opposed to an empty
// ifMatch (no header at all, meaning upsert semantics) or a specific ETag
// string (optimistic-concurrency match required).
const IfMatchAny = "*"

// StorageBackend defines the interface for an Azure Table Storage backend.
// Shaped after services/azurequeue's StorageBackend: a narrow, testable seam
// between the wire handler and storage, so handler tests can substitute a
// fake.
//
// The ifMatch parameter on ReplaceEntity/MergeEntity/DeleteEntity threads
// through the three If-Match states the wire protocol distinguishes:
//   - "" (no If-Match header): upsert semantics for Replace/MergeEntity
//     (create if absent, otherwise mutate unconditionally); DeleteEntity's
//     caller (handler.go) never passes "" -- an absent If-Match on Delete is
//     rejected at the handler layer before the backend is ever called.
//   - IfMatchAny ("*"): the entity must exist, but any current ETag matches.
//   - any other string: the entity must exist AND its current ETag must
//     equal this value, else ErrETagMismatch.
type StorageBackend interface {
	CreateTable(name string) error
	DeleteTable(name string) error
	ListTables() []TableInfo

	InsertEntity(table, partitionKey, rowKey string, props map[string]EntityProperty) (EntityInfo, error)
	GetEntity(table, partitionKey, rowKey string) (EntityInfo, error)
	// QueryEntities returns entities in table matching filter (nil matches
	// everything), ordered by (PartitionKey, RowKey), capped at top results
	// (top <= 0 means unlimited).
	QueryEntities(table string, filter Node, top int) ([]EntityInfo, error)
	ReplaceEntity(
		table, partitionKey, rowKey string,
		props map[string]EntityProperty,
		ifMatch string,
	) (EntityInfo, error)
	MergeEntity(table, partitionKey, rowKey string, props map[string]EntityProperty, ifMatch string) (EntityInfo, error)
	DeleteEntity(table, partitionKey, rowKey, ifMatch string) error

	// Reset clears all in-memory state. Used by the
	// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
	Reset()
}
