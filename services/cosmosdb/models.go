package cosmosdb

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"time"
)

// System property names auto-populated on every Cosmos resource. Named
// constants (rather than repeated literals) so goconst doesn't flag the
// wire-protocol JSON keys duplicated across database_ops.go/
// container_ops.go/document_ops.go.
const (
	sysPropRID         = "_rid"
	sysPropETag        = "_etag"
	sysPropSelf        = "_self"
	sysPropTS          = "_ts"
	sysPropAttachments = "_attachments"
	sysPropCount       = "_count"
	collsSegment       = "colls"
)

// stripSystemProperties deletes every system property key from m in place,
// so a client-supplied value for one of them is silently dropped rather
// than stored, mirroring services/azuretable's treatment of "Timestamp" --
// the server always wins.
func stripSystemProperties(m map[string]any) {
	for _, k := range [...]string{sysPropRID, sysPropETag, sysPropSelf, sysPropTS, sysPropAttachments} {
		delete(m, k)
	}
}

// fakeRID derives a deterministic, opaque, base64-shaped fake Cosmos
// resource ID from name (e.g. "dbs/mydb" or "dbs/mydb/colls/mycoll"). Real
// Cosmos _rid values are base64-encoded internal numeric IDs; SDKs treat
// them as opaque strings, so any stable, distinct-per-resource string
// satisfies the wire contract. Deterministic (rather than random) so tests
// can assert on it directly.
func fakeRID(name string) string {
	sum := sha256.Sum256([]byte(name))

	return base64.StdEncoding.EncodeToString(sum[:8])
}

// minTimestampBump is the smallest amount a stored document's Timestamp is
// guaranteed to advance on each mutation, mirroring
// services/azuretable/store.go's identical constant and rationale: it
// guarantees ETag uniqueness across successive writes to the same document
// even when nowFunc returns the same instant twice in a row.
const minTimestampBump = 100 * time.Nanosecond

// etagFor derives an ETag from a document's Timestamp, in the quoted-string
// wire format real Cosmos DB uses (e.g. "0000d1a2-0000-0000-0000-...").
// Unlike Azure Table Storage's ETag (which encodes a human-readable
// datetime), Cosmos's ETag is an opaque quoted token; this emulator encodes
// the Timestamp's UnixNano value as hex for a stable, easy-to-reason-about
// value, since real SDKs never parse it -- they only echo back what the
// server returned via If-Match.
func etagFor(t time.Time) string {
	return fmt.Sprintf("%q", fmt.Sprintf("%016x", t.UnixNano()))
}

// DatabaseInfo is a read-only snapshot of a database, returned by the
// StorageBackend database accessors.
type DatabaseInfo struct {
	ID  string
	RID string
}

// ContainerSpec is the caller-supplied shape for creating a container:
// its ID and single partition key path (e.g. "/pk"). See
// ErrInvalidPartitionKeyPath for why exactly one path is required.
type ContainerSpec struct {
	ID               string
	PartitionKeyPath string
}

// ContainerInfo is a read-only snapshot of a container, returned by the
// StorageBackend container accessors.
type ContainerInfo struct {
	ID               string
	RID              string
	PartitionKeyPath string
}

// DocumentInfo is a read-only snapshot of a document, returned by the
// StorageBackend document accessors. Body holds the document's user-defined
// fields only (never the system properties, which callers -- handler.go's
// encodeDocument -- overlay from the other fields at response-encode time,
// mirroring services/azuretable's EntityInfo/encodeEntity split).
type DocumentInfo struct {
	Timestamp        time.Time
	Body             map[string]any
	ID               string
	RID              string
	Self             string
	ETag             string
	PartitionKeyJSON string
}

// storedDatabase is the backend's internal representation of a database.
type storedDatabase struct {
	Containers map[string]*storedContainer
	ID         string
}

// storedContainer is the backend's internal representation of a container.
type storedContainer struct {
	Documents        map[documentCompositeKey]*storedDocument
	ID               string
	PartitionKeyPath string
}

// storedDocument is the backend's internal representation of a document.
type storedDocument struct {
	Timestamp        time.Time
	Body             map[string]any
	ID               string
	PartitionKeyJSON string
}

// storedDocumentWire is storedDocument's on-disk (persistence snapshot)
// shape. Body is held as json.RawMessage here so
// storedDocument.UnmarshalJSON can re-decode it with json.Decoder.UseNumber
// (see decodeJSONObject's doc comment): encoding/json's default map[string]any
// decoding (what a derived UnmarshalJSON would do) always produces float64
// for JSON numbers, silently losing precision above 2^53 on every
// snapshot/restore cycle -- exactly the bug class AZURE.md's process rules
// call out.
type storedDocumentWire struct {
	Timestamp        time.Time       `json:"Timestamp"`
	ID               string          `json:"ID"`
	PartitionKeyJSON string          `json:"PartitionKeyJSON"`
	Body             json.RawMessage `json:"Body"`
}

// MarshalJSON implements json.Marshaler for persistence snapshots.
func (d *storedDocument) MarshalJSON() ([]byte, error) {
	bodyBytes, err := json.Marshal(d.Body)
	if err != nil {
		return nil, fmt.Errorf("cosmosdb: marshal stored document body: %w", err)
	}

	return json.Marshal(storedDocumentWire{
		Body: bodyBytes, Timestamp: d.Timestamp, ID: d.ID, PartitionKeyJSON: d.PartitionKeyJSON,
	})
}

// UnmarshalJSON implements json.Unmarshaler for persistence snapshots. See
// storedDocumentWire's doc comment for why Body needs UseNumber decoding.
func (d *storedDocument) UnmarshalJSON(data []byte) error {
	var wire storedDocumentWire

	if err := json.Unmarshal(data, &wire); err != nil {
		return fmt.Errorf("cosmosdb: unmarshal stored document: %w", err)
	}

	// A JSON null "Body" decodes to a nil map with NO error from
	// dec.Decode below (encoding/json treats null-into-a-map as "set it to
	// its zero value", not a type error) -- silently losing every
	// previously persisted field on the next restore, since a nil map
	// reads back as empty rather than failing loudly. Reject it explicitly
	// instead, mirroring services/azuretable's identical
	// ErrSnapshotEntityNull treatment of a null map/pointer entry: a
	// snapshot that can't be decoded exactly must not be decoded
	// approximately.
	if bytes.Equal(bytes.TrimSpace(wire.Body), []byte("null")) {
		return fmt.Errorf("%w: id %q", ErrSnapshotDocumentNullBody, wire.ID)
	}

	dec := json.NewDecoder(bytes.NewReader(wire.Body))
	dec.UseNumber()

	var body map[string]any
	if err := dec.Decode(&body); err != nil {
		return fmt.Errorf("cosmosdb: unmarshal stored document body: %w", err)
	}

	d.Body, d.Timestamp, d.ID, d.PartitionKeyJSON = body, wire.Timestamp, wire.ID, wire.PartitionKeyJSON

	return nil
}

// documentCompositeKey is a document's map key within
// storedContainer.Documents: a comparable struct of its two identifying
// fields, not a delimited string. Mirrors services/azuretable's
// entityCompositeKey exactly, and for the same reason: a delimited string
// (e.g. partitionKeyJSON+"\x00"+id) is unsafe because JSON permits any byte
// value -- including NUL -- inside a document "id" string, so two distinct
// (partitionKey, id) pairs could collide on the same delimited string. A
// struct key compares its two fields independently, so no such collision is
// possible.
type documentCompositeKey struct {
	PartitionKeyJSON string
	ID               string
}

// MarshalText implements encoding.TextMarshaler so documentCompositeKey can
// be used as a map key in backendSnapshot's persisted
// storedContainer.Documents (encoding/json only accepts string, integer, or
// TextMarshaler-implementing map key types). Encoded as a JSON string array
// rather than delimiter-joined, for the same collision-avoidance reason
// documented above and on services/azuretable's identical MarshalText.
func (k documentCompositeKey) MarshalText() ([]byte, error) {
	return json.Marshal([2]string{k.PartitionKeyJSON, k.ID})
}

// UnmarshalText implements encoding.TextUnmarshaler, the inverse of
// MarshalText.
func (k *documentCompositeKey) UnmarshalText(text []byte) error {
	var pair [2]string

	if err := json.Unmarshal(text, &pair); err != nil {
		return fmt.Errorf("cosmosdb: malformed document composite key %q: %w", text, err)
	}

	k.PartitionKeyJSON, k.ID = pair[0], pair[1]

	return nil
}

// canonicalPartitionKeyJSON returns the canonical JSON encoding of a
// partition key scalar value (string/number/bool/nil), used both as the
// documentCompositeKey's PartitionKeyJSON field and to compare a caller's
// x-ms-documentdb-partitionkey header value against a stored document's
// partition key. json.Marshal already disambiguates every scalar kind
// unambiguously (e.g. the string "123" marshals to `"123"`, the number 123
// marshals to `123` -- these can never collide).
//
// v MUST be one of the scalar kinds this doc comment claims: an object
// (map[string]any) or array ([]any) is rejected with ErrInvalidDocument
// rather than silently JSON-encoded -- real Cosmos partition keys are
// always scalar, and letting a nested object/array through here would
// let two documents whose partition-key-path field happens to be a
// same-shaped object collide as if they shared a partition key value, or
// (worse) make the composite key's identity depend on Go map key
// iteration-order-independent-but-still-fragile JSON encoding of nested
// data never intended to be a key at all.
func canonicalPartitionKeyJSON(v any) (string, error) {
	switch v.(type) {
	case nil, string, bool, json.Number, float64, float32, int, int32, int64:
	default:
		return "", fmt.Errorf(
			"%w: partition key value must be a scalar (string, number, bool, or null), got %T",
			ErrInvalidDocument, v,
		)
	}

	data, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("cosmosdb: encode partition key value: %w", err)
	}

	return string(data), nil
}

// decodeJSONObject decodes body into a map[string]any using
// json.Decoder.UseNumber, so JSON numbers decode as json.Number rather than
// float64. This is required for document fidelity: Cosmos documents can
// carry integers beyond float64's 53-bit mantissa (e.g. 9007199254740993),
// and a plain json.Unmarshal into map[string]any would silently corrupt
// them. json.Number round-trips back out through json.Marshal as the exact
// original digit sequence.
func decodeJSONObject(body []byte) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()

	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDocument, err)
	}

	if err := rejectTrailingJSON(dec); err != nil {
		return nil, fmt.Errorf("%w: trailing content after JSON object: %w", ErrInvalidDocument, err)
	}

	return m, nil
}

// rejectTrailingJSON reports an error unless dec has been fully consumed.
// json.Decoder.Decode only consumes one JSON value and silently ignores
// anything after it (e.g. a body of `{"id":"1"}{}` decodes as if it were
// just the first object), so callers that need "exactly one JSON value, and
// nothing else" must check this explicitly.
//
// dec.More() is not sufficient for this: it returns false for a decoded
// value followed only by an *unmatched* closing delimiter (e.g. `["a"]]` or
// `["a"]}`), since More's whitespace-skipping peek has no enclosing
// array/object to close against at the top level, and Go's own
// documentation only promises it detects a delimiter that would end the
// *current* enclosing structure -- there isn't one here. Decoding a second
// value and requiring io.EOF instead makes the JSON scanner itself validate
// what remains, catching those unmatched-delimiter cases too.
func rejectTrailingJSON(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return ErrUnexpectedTrailingJSON
		}

		return err
	}

	return nil
}

// deepCopyJSONValue returns a deep copy of v (typically a map[string]any
// document body) safe to store or return without aliasing the original: a
// caller mutating what it passed in, or what it got back, can never corrupt
// backend state or vice versa (see AZURE.md's non-negotiable process rule on
// aliasing). Implemented as a marshal/unmarshal round trip through
// json.Decoder.UseNumber (see decodeJSONObject) rather than manual
// recursion, since it must handle arbitrarily nested maps/slices/scalars
// uniformly and preserve json.Number fidelity throughout.
func deepCopyJSONValue(v any) (any, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("cosmosdb: deep copy: marshal: %w", err)
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	var out any
	if decodeErr := dec.Decode(&out); decodeErr != nil {
		return nil, fmt.Errorf("cosmosdb: deep copy: unmarshal: %w", decodeErr)
	}

	return out, nil
}

// deepCopyBody is deepCopyJSONValue specialized to a document body map,
// panicking never: a map[string]any that already round-tripped through
// decodeJSONObject once can always re-marshal/re-decode successfully, so an
// error here indicates a package bug, not bad input -- callers still handle
// it explicitly rather than ignoring it, per repo convention.
func deepCopyBody(body map[string]any) (map[string]any, error) {
	copied, err := deepCopyJSONValue(body)
	if err != nil {
		return nil, err
	}

	m, ok := copied.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: deep copy of body did not round-trip to an object", ErrInvalidDocument)
	}

	return m, nil
}

// documentAsMap builds a document's full JSON representation: its
// user-defined fields (already alias-safe, see DocumentInfo's doc comment)
// overlaid with the server-managed system properties. Shared by
// document_ops.go's response encoding and sql_exec.go's query row
// evaluation, so "SELECT c.id" / "WHERE c._ts > ..." resolve against the
// same field set a client actually receives.
func documentAsMap(info DocumentInfo) map[string]any {
	// Deliberately NOT `make(map[string]any, len(info.Body)+systemPropertyCount)`:
	// info.Body originates from an attacker-controlled request body, and
	// CodeQL's go/allocation-size-overflow rule flags arithmetic on a
	// tainted length feeding an allocation size (see PR review). The
	// arithmetic itself can never actually overflow -- len() of an
	// already-parsed in-memory map cannot approach MaxInt, and the
	// capacity argument is only a sizing hint, not a hard allocation --
	// but sizing off the untouched length sidesteps the tainted-arithmetic
	// pattern entirely. The five system properties then cost at most one
	// extra map growth, which is irrelevant here.
	m := make(map[string]any, len(info.Body))

	maps.Copy(m, info.Body)

	m["id"] = info.ID
	m[sysPropRID] = info.RID
	m[sysPropSelf] = info.Self
	m[sysPropETag] = info.ETag
	m[sysPropTS] = info.Timestamp.Unix()
	m[sysPropAttachments] = "attachments/"

	return m
}
