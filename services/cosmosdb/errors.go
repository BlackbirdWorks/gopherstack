package cosmosdb

import "errors"

// Sentinel errors for Azure Cosmos DB (Core/SQL API) operations.
var (
	errEmptyID = errors.New("id must not be empty")

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

	// ErrInvalidPartitionKeyPath is returned for a container partition key definition that is empty, unprefixed,
	// duplicated, or has more than three (hierarchical) paths.
	ErrInvalidPartitionKeyPath = errors.New(
		"cosmosdb: partitionKey.paths must contain between one and three distinct \"/\"-prefixed paths",
	)

	// ErrInvalidDefaultTTL is returned for a container defaultTtl that is neither -1 nor a positive integer.
	ErrInvalidDefaultTTL = errors.New("cosmosdb: defaultTtl must be -1 or a positive integer")

	// ErrPartitionKeyImmutable is returned when a container replace tries to change the partition key.
	ErrPartitionKeyImmutable = errors.New("cosmosdb: the partition key of an existing container cannot be changed")

	// ErrInvalidContainerPolicy is returned for a malformed unique-key, geospatial, vector or computed-property setting.
	ErrInvalidContainerPolicy = errors.New("cosmosdb: invalid container policy")

	// ErrUniqueKeyPolicyImmutable is returned when a container replace tries to change the unique key policy.
	ErrUniqueKeyPolicyImmutable = errors.New(
		"cosmosdb: the unique key policy of an existing container cannot be changed",
	)

	// ErrUniqueKeyViolation is returned when a write would duplicate a unique key value within a logical partition.
	ErrUniqueKeyViolation = errors.New("cosmosdb: unique index constraint violation")

	// ErrInvalidContinuation is returned for a malformed x-ms-continuation token.
	ErrInvalidContinuation = errors.New("cosmosdb: invalid continuation token")

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

	ErrSnapshotUserNull       = errors.New("cosmosdb: restore snapshot: user is null")
	ErrSnapshotPermissionNull = errors.New("cosmosdb: restore snapshot: permission is null")

	// ErrPartitionKeyMismatch is returned by ReplaceDocument when the
	// replacement body's own partition-key-path field is present but
	// disagrees with the caller-supplied partition key (the
	// x-ms-documentdb-partitionkey header value, canonicalized). Without
	// this check, a document keyed under partition "a" could be silently
	// rewritten to claim partition "b" in its own body while remaining
	// stored (and only ever findable) under "a" -- an internally
	// inconsistent document real Cosmos DB never allows to exist.
	ErrPartitionKeyMismatch = errors.New(
		"cosmosdb: replacement body's partition key does not match the request's partition key",
	)

	// ErrSnapshotDocumentNullBody is returned by storedDocument.UnmarshalJSON
	// when a persisted document's "Body" field is itself JSON null.
	// encoding/json decodes a null-into-map with no error (it just sets the
	// map to nil), so without this explicit check a document with a null
	// Body would silently restore as an empty document, discarding every
	// field it used to have -- not a decode failure, so nothing would ever
	// surface the data loss.
	ErrSnapshotDocumentNullBody = errors.New("cosmosdb: restore snapshot: document body is null")

	// ErrUnexpectedTrailingJSON is returned by rejectTrailingJSON when a
	// second decode off the same stream succeeds instead of hitting EOF,
	// meaning the input carried more than the one JSON value callers
	// require.
	ErrUnexpectedTrailingJSON = errors.New("cosmosdb: unexpected additional JSON value")

	ErrInvalidPatch         = errors.New("cosmosdb: invalid patch request")
	ErrPatchConditionFailed = errors.New("cosmosdb: patch condition not satisfied")
	ErrBatchTooLarge        = errors.New("cosmosdb: batch has more operations than the 100 allowed")
	ErrScriptNotFound       = errors.New("cosmosdb: script not found")
	ErrScriptAlreadyExists  = errors.New("cosmosdb: script already exists")
	ErrInvalidScript        = errors.New("cosmosdb: invalid script definition")
	ErrScriptFailed         = errors.New("cosmosdb: script execution failed")
	ErrScriptTimeout        = errors.New("cosmosdb: script execution timed out")
	ErrInvalidTrigger       = errors.New("cosmosdb: trigger not usable for this operation")
	ErrInvalidPKRange       = errors.New("cosmosdb: unknown partition key range")
	ErrPKRangeGone          = errors.New("cosmosdb: partition key range was split")
	// ErrChangeFeedStart is returned when an all-versions-and-deletes read names no start.
	ErrChangeFeedStart = errors.New(
		"cosmosdb: all versions and deletes change feed starts from now or from a continuation, not from the beginning " +
			"or a point in time",
	)
	// ErrChangeFeedBackup is returned for an all-versions-and-deletes read on an account without continuous backup.
	ErrChangeFeedBackup = errors.New(
		"cosmosdb: all versions and deletes change feed requires continuous backup on the account",
	)
	// ErrChangeFeedRetention is returned when the start position is older than the continuous backup window.
	ErrChangeFeedRetention = errors.New(
		"cosmosdb: the change feed position is outside the continuous backup retention window",
	)
)
