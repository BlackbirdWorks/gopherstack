package azuretable

import "errors"

// Sentinel errors for Azure Table Storage operations.
var (
	ErrTableNotFound       = errors.New("azuretable: table not found")
	ErrTableAlreadyExists  = errors.New("azuretable: table already exists")
	ErrEntityNotFound      = errors.New("azuretable: entity not found")
	ErrEntityAlreadyExists = errors.New("azuretable: entity already exists")
	ErrETagMismatch        = errors.New("azuretable: etag mismatch")

	// ErrInvalidEntityKey is returned when a request omits PartitionKey or
	// RowKey entirely. Empty-string keys are accepted (matching real Azure
	// Table Storage); only an absent key is rejected. See entity_ops.go.
	ErrInvalidEntityKey = errors.New("azuretable: PartitionKey and RowKey are required")

	// ErrInvalidEntityProperty is returned when an entity property's JSON
	// value cannot be decoded under its (explicit or inferred) EDM type.
	ErrInvalidEntityProperty = errors.New("azuretable: invalid entity property value")

	// ErrFilterParse and ErrFilterTooDeep are returned by ParseFilter. A
	// parse error always surfaces as 400 InvalidInput, never a panic or 500
	// -- see handler.go's queryEntities.
	ErrFilterParse   = errors.New("azuretable: $filter parse error")
	ErrFilterTooDeep = errors.New("azuretable: $filter expression nested too deeply")

	// ErrSnapshotTableNull and ErrSnapshotEntityNull are returned by Restore
	// when a snapshot's "tables" map (or a table's "Entities" map) holds a
	// JSON null entry, which decodes to a nil pointer that would panic on
	// first dereference if stored as-is. See persistence.go.
	ErrSnapshotTableNull  = errors.New("azuretable: restore snapshot: table is null")
	ErrSnapshotEntityNull = errors.New("azuretable: restore snapshot: entity is null")

	// ErrSnapshotTableNameMismatch is returned by Restore when a snapshot's
	// "tables" map key differs from that entry's storedTable.Name. Table
	// operations all key off the map, while ListTables reads Name -- a
	// mismatch would let those two views disagree about a table's identity.
	// See persistence.go.
	ErrSnapshotTableNameMismatch = errors.New("azuretable: restore snapshot: table map key does not match Name")
)
