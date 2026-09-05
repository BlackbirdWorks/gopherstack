package cosmosdb

import (
	"crypto/rand"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// InMemoryBackend implements StorageBackend using an in-memory map guarded
// by a single RWMutex. Shaped after services/azuretable's InMemoryBackend.
type InMemoryBackend struct {
	mu        *lockmetrics.RWMutex
	databases map[string]*storedDatabase
	// nowFunc is the backend's time source, overridable in tests (see
	// export_test.go's SetNowFunc) for deterministic Timestamp/ETag
	// assertions.
	nowFunc func() time.Time
	// etagFunc derives a document's ETag from its Timestamp, overridable in
	// tests (see export_test.go's SetETagFunc).
	etagFunc func(time.Time) string
}

// NewInMemoryBackend creates a new empty InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		mu:        lockmetrics.New("cosmosdb"),
		databases: make(map[string]*storedDatabase),
		nowFunc:   time.Now,
		etagFunc:  etagFor,
	}
}

func (b *InMemoryBackend) now() time.Time { return b.nowFunc().UTC() }

// bumpTimestamp returns the Timestamp a mutated document should receive,
// identical in spirit to services/azuretable/store.go's bumpTimestamp:
// b.now() for a brand-new document, or b.now() advanced by at least
// minTimestampBump past the previous Timestamp for an existing one --
// guaranteeing a distinct ETag on every mutation.
func bumpTimestampFrom(now, prev time.Time, existedBefore bool) time.Time {
	if !existedBefore {
		return now
	}

	if !now.After(prev) {
		return prev.Add(minTimestampBump)
	}

	return now
}

// UUID v4 layout constants (RFC 4122 section 4.4): the version nibble
// (0100) is OR'd into byte 6's high nibble after masking it off, and the
// variant bits (10) are OR'd into byte 8's top two bits after masking them
// off.
const (
	uuidV4VersionMask = 0x0f
	uuidV4VersionBits = 0x40
	uuidV4VariantMask = 0x3f
	uuidV4VariantBits = 0x80
)

// newDocumentID generates a UUID-v4-shaped document ID for a document
// created without a client-supplied "id" field, mirroring real Cosmos's own
// server-side ID generation.
func newDocumentID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "00000000-0000-0000-0000-000000000000"
	}

	buf[6] = (buf[6] & uuidV4VersionMask) | uuidV4VersionBits
	buf[8] = (buf[8] & uuidV4VariantMask) | uuidV4VariantBits

	return fmt.Sprintf("%x-%x-%x-%x-%x", buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])
}

// --- Databases ---

// CreateDatabase creates a new, empty database. Returns
// ErrDatabaseAlreadyExists if a database with the same ID already exists.
func (b *InMemoryBackend) CreateDatabase(id string) (DatabaseInfo, error) {
	b.mu.Lock("CreateDatabase")
	defer b.mu.Unlock()

	if _, ok := b.databases[id]; ok {
		return DatabaseInfo{}, ErrDatabaseAlreadyExists
	}

	b.databases[id] = &storedDatabase{ID: id, Containers: make(map[string]*storedContainer)}

	return DatabaseInfo{ID: id, RID: fakeRID("dbs/" + id)}, nil
}

// GetDatabase retrieves a single database. Returns ErrDatabaseNotFound if
// absent.
func (b *InMemoryBackend) GetDatabase(id string) (DatabaseInfo, error) {
	b.mu.RLock("GetDatabase")
	defer b.mu.RUnlock()

	d, ok := b.databases[id]
	if !ok {
		return DatabaseInfo{}, ErrDatabaseNotFound
	}

	return DatabaseInfo{ID: d.ID, RID: fakeRID("dbs/" + d.ID)}, nil
}

// ListDatabases returns a snapshot of all databases, sorted by ID.
func (b *InMemoryBackend) ListDatabases() []DatabaseInfo {
	b.mu.RLock("ListDatabases")
	defer b.mu.RUnlock()

	out := make([]DatabaseInfo, 0, len(b.databases))
	for _, d := range b.databases {
		out = append(out, DatabaseInfo{ID: d.ID, RID: fakeRID("dbs/" + d.ID)})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

// DeleteDatabase removes a database and all of its containers/documents.
// Returns ErrDatabaseNotFound if absent.
func (b *InMemoryBackend) DeleteDatabase(id string) error {
	b.mu.Lock("DeleteDatabase")
	defer b.mu.Unlock()

	if _, ok := b.databases[id]; !ok {
		return ErrDatabaseNotFound
	}

	delete(b.databases, id)

	return nil
}

// --- Containers ---

// CreateContainer creates a new, empty container within dbID. Returns
// ErrDatabaseNotFound if dbID doesn't exist, or ErrContainerAlreadyExists if
// a container with the same ID already exists in it.
func (b *InMemoryBackend) CreateContainer(dbID string, spec ContainerSpec) (ContainerInfo, error) {
	if spec.PartitionKeyPath == "" {
		return ContainerInfo{}, ErrInvalidPartitionKeyPath
	}

	b.mu.Lock("CreateContainer")
	defer b.mu.Unlock()

	d, ok := b.databases[dbID]
	if !ok {
		return ContainerInfo{}, ErrDatabaseNotFound
	}

	if _, exists := d.Containers[spec.ID]; exists {
		return ContainerInfo{}, ErrContainerAlreadyExists
	}

	d.Containers[spec.ID] = &storedContainer{
		ID:               spec.ID,
		PartitionKeyPath: spec.PartitionKeyPath,
		Documents:        make(map[documentCompositeKey]*storedDocument),
	}

	return b.containerInfo(dbID, d.Containers[spec.ID]), nil
}

func (b *InMemoryBackend) containerInfo(dbID string, c *storedContainer) ContainerInfo {
	return ContainerInfo{
		ID:               c.ID,
		RID:              fakeRID("dbs/" + dbID + "/colls/" + c.ID),
		PartitionKeyPath: c.PartitionKeyPath,
	}
}

// findContainer looks up dbID/containerID, returning ErrDatabaseNotFound or
// ErrContainerNotFound as appropriate. Callers must hold b.mu.
func (b *InMemoryBackend) findContainer(dbID, containerID string) (*storedContainer, error) {
	d, ok := b.databases[dbID]
	if !ok {
		return nil, ErrDatabaseNotFound
	}

	c, ok := d.Containers[containerID]
	if !ok {
		return nil, ErrContainerNotFound
	}

	return c, nil
}

// GetContainer retrieves a single container.
func (b *InMemoryBackend) GetContainer(dbID, containerID string) (ContainerInfo, error) {
	b.mu.RLock("GetContainer")
	defer b.mu.RUnlock()

	c, err := b.findContainer(dbID, containerID)
	if err != nil {
		return ContainerInfo{}, err
	}

	return b.containerInfo(dbID, c), nil
}

// ListContainers returns a snapshot of all containers in dbID, sorted by ID.
func (b *InMemoryBackend) ListContainers(dbID string) ([]ContainerInfo, error) {
	b.mu.RLock("ListContainers")
	defer b.mu.RUnlock()

	d, ok := b.databases[dbID]
	if !ok {
		return nil, ErrDatabaseNotFound
	}

	out := make([]ContainerInfo, 0, len(d.Containers))
	for _, c := range d.Containers {
		out = append(out, b.containerInfo(dbID, c))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out, nil
}

// DeleteContainer removes a container and all of its documents.
func (b *InMemoryBackend) DeleteContainer(dbID, containerID string) error {
	b.mu.Lock("DeleteContainer")
	defer b.mu.Unlock()

	d, ok := b.databases[dbID]
	if !ok {
		return ErrDatabaseNotFound
	}

	if _, exists := d.Containers[containerID]; !exists {
		return ErrContainerNotFound
	}

	delete(d.Containers, containerID)

	return nil
}

// --- Documents ---

// extractPartitionKeyValue walks body along path's "/"-separated segments
// (e.g. "/a/b" -> body["a"].(map[string]any)["b"]), returning (value, true)
// if every segment resolves to a nested object field, or (nil, false) if the
// path (or any of its intermediate segments) is absent -- a document simply
// missing its partition key field uses a JSON null partition key value, real
// Cosmos's own behavior for an undefined partition key.
func extractPartitionKeyValue(body map[string]any, path string) (any, bool) {
	segments := strings.Split(strings.Trim(path, "/"), "/")

	var cur any = body

	for _, seg := range segments {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}

		v, ok := m[seg]
		if !ok {
			return nil, false
		}

		cur = v
	}

	return cur, true
}

// prepareDocumentBody validates and clones body for storage, extracting its
// ID (generating one if absent) and partition key JSON per c's declared
// path. System property keys are stripped -- they are always
// server-managed, never client-supplied (mirrors services/azuretable's
// treatment of "Timestamp").
func prepareDocumentBody(body map[string]any, path string) (string, string, map[string]any, error) {
	clone, err := deepCopyBody(body)
	if err != nil {
		return "", "", nil, err
	}

	stripSystemProperties(clone)

	id, err := resolveDocumentID(clone)
	if err != nil {
		return "", "", nil, err
	}

	clone["id"] = id

	pkValue, _ := extractPartitionKeyValue(clone, path)

	pkJSON, err := canonicalPartitionKeyJSON(pkValue)
	if err != nil {
		return "", "", nil, err
	}

	return id, pkJSON, clone, nil
}

// resolveDocumentID extracts body["id"] (must be a non-empty string if
// present) or generates a new one if absent.
func resolveDocumentID(body map[string]any) (string, error) {
	raw, ok := body["id"]
	if !ok {
		return newDocumentID(), nil
	}

	s, ok := raw.(string)
	if !ok || s == "" {
		return "", fmt.Errorf("%w: \"id\" must be a non-empty string", ErrInvalidDocument)
	}

	return s, nil
}

// CreateDocument creates (or, if upsert, inserts-or-replaces) a document.
// See StorageBackend's doc comment.
func (b *InMemoryBackend) CreateDocument(
	dbID, containerID string, body map[string]any, upsert bool,
) (DocumentInfo, error) {
	b.mu.Lock("CreateDocument")
	defer b.mu.Unlock()

	c, err := b.findContainer(dbID, containerID)
	if err != nil {
		return DocumentInfo{}, err
	}

	id, pkJSON, clean, err := prepareDocumentBody(body, c.PartitionKeyPath)
	if err != nil {
		return DocumentInfo{}, err
	}

	key := documentCompositeKey{PartitionKeyJSON: pkJSON, ID: id}

	existing, exists := c.Documents[key]
	if exists && !upsert {
		return DocumentInfo{}, ErrDocumentAlreadyExists
	}

	now := b.now()

	prevTimestamp := now
	if exists {
		prevTimestamp = existing.Timestamp
	}

	doc := &storedDocument{
		ID:               id,
		PartitionKeyJSON: pkJSON,
		Body:             clean,
		Timestamp:        bumpTimestampFrom(now, prevTimestamp, exists),
	}
	c.Documents[key] = doc

	return b.documentInfo(dbID, containerID, doc), nil
}

func (b *InMemoryBackend) documentInfo(dbID, containerID string, d *storedDocument) DocumentInfo {
	body, err := deepCopyBody(d.Body)
	if err != nil {
		// Unreachable in practice: d.Body was itself produced by a prior
		// successful deepCopyBody, so it always re-round-trips. Fall back to
		// an empty body rather than propagating an error type this method's
		// callers don't expect, matching the fail-safe (never a document
		// with the wrong ID/etag) posture over the fail-loud one here.
		body = map[string]any{}
	}

	rid := fakeRID("dbs/" + dbID + "/colls/" + containerID + "/docs/" + d.ID)

	return DocumentInfo{
		ID:               d.ID,
		RID:              rid,
		Self:             "dbs/" + dbID + "/colls/" + containerID + "/docs/" + d.ID + "/",
		ETag:             b.etagFunc(d.Timestamp),
		Timestamp:        d.Timestamp,
		PartitionKeyJSON: d.PartitionKeyJSON,
		Body:             body,
	}
}

// GetDocument retrieves a single document by partition key and ID.
func (b *InMemoryBackend) GetDocument(dbID, containerID, partitionKey, id string) (DocumentInfo, error) {
	b.mu.RLock("GetDocument")
	defer b.mu.RUnlock()

	c, err := b.findContainer(dbID, containerID)
	if err != nil {
		return DocumentInfo{}, err
	}

	d, ok := c.Documents[documentCompositeKey{PartitionKeyJSON: partitionKey, ID: id}]
	if !ok {
		return DocumentInfo{}, ErrDocumentNotFound
	}

	return b.documentInfo(dbID, containerID, d), nil
}

// checkIfMatch validates ifMatch against a document's current
// existence/ETag: "" means unconditional; anything else must match exactly.
func (b *InMemoryBackend) checkIfMatch(d *storedDocument, exists bool, ifMatch string) error {
	if ifMatch == "" {
		return nil
	}

	if !exists {
		return ErrDocumentNotFound
	}

	if b.etagFunc(d.Timestamp) != ifMatch {
		return ErrETagMismatch
	}

	return nil
}

// ReplaceDocument fully replaces an existing document's body (its "id" and
// partition key value are fixed by the path/header and cannot be changed by
// the replacement body's own "id"/partition key field -- consistent with
// real Cosmos DB, which rejects a partition-key-changing Replace outright;
// this emulator instead just always keys off the caller-supplied
// partitionKey/id).
func (b *InMemoryBackend) ReplaceDocument(
	dbID, containerID, partitionKey, id string, body map[string]any, ifMatch string,
) (DocumentInfo, error) {
	b.mu.Lock("ReplaceDocument")
	defer b.mu.Unlock()

	c, err := b.findContainer(dbID, containerID)
	if err != nil {
		return DocumentInfo{}, err
	}

	key := documentCompositeKey{PartitionKeyJSON: partitionKey, ID: id}
	existing, exists := c.Documents[key]

	if matchErr := b.checkIfMatch(existing, exists, ifMatch); matchErr != nil {
		return DocumentInfo{}, matchErr
	}

	clean, err := deepCopyBody(body)
	if err != nil {
		return DocumentInfo{}, err
	}

	stripSystemProperties(clean)

	clean["id"] = id

	// The replacement body's own partition-key-path field, if it declares
	// one at all, must agree with the caller-supplied partition key -- see
	// ErrPartitionKeyMismatch's doc comment for why silently allowing a
	// contradiction here would leave a document stored under one partition
	// while its own body claims another.
	if pkValue, ok := extractPartitionKeyValue(clean, c.PartitionKeyPath); ok {
		bodyPK, pkErr := canonicalPartitionKeyJSON(pkValue)
		if pkErr != nil {
			return DocumentInfo{}, pkErr
		}

		if bodyPK != partitionKey {
			return DocumentInfo{}, ErrPartitionKeyMismatch
		}
	}

	now := b.now()

	prevTimestamp := now
	if exists {
		prevTimestamp = existing.Timestamp
	}

	doc := &storedDocument{
		ID:               id,
		PartitionKeyJSON: partitionKey,
		Body:             clean,
		Timestamp:        bumpTimestampFrom(now, prevTimestamp, exists),
	}
	c.Documents[key] = doc

	return b.documentInfo(dbID, containerID, doc), nil
}

// DeleteDocument removes a document after verifying ifMatch.
func (b *InMemoryBackend) DeleteDocument(dbID, containerID, partitionKey, id, ifMatch string) error {
	b.mu.Lock("DeleteDocument")
	defer b.mu.Unlock()

	c, err := b.findContainer(dbID, containerID)
	if err != nil {
		return err
	}

	key := documentCompositeKey{PartitionKeyJSON: partitionKey, ID: id}

	existing, exists := c.Documents[key]
	if !exists {
		// checkIfMatch alone is not sufficient here: with ifMatch == "" it
		// returns nil unconditionally (that's the correct contract for
		// Replace's upsert semantics -- see StorageBackend's doc comment),
		// which would make deleting an already-absent document silently
		// "succeed" (204) instead of reporting 404. Delete, unlike
		// Replace, has no upsert concept -- there's nothing to "delete
		// into" -- so absence is always an error regardless of ifMatch.
		return ErrDocumentNotFound
	}

	if matchErr := b.checkIfMatch(existing, exists, ifMatch); matchErr != nil {
		return matchErr
	}

	delete(c.Documents, key)

	return nil
}

// ListDocuments returns every document in containerID, ordered by ID.
func (b *InMemoryBackend) ListDocuments(dbID, containerID string) ([]DocumentInfo, error) {
	b.mu.RLock("ListDocuments")
	defer b.mu.RUnlock()

	c, err := b.findContainer(dbID, containerID)
	if err != nil {
		return nil, err
	}

	docs := make([]*storedDocument, 0, len(c.Documents))
	for _, d := range c.Documents {
		docs = append(docs, d)
	}

	sort.Slice(docs, func(i, j int) bool { return docs[i].ID < docs[j].ID })

	out := make([]DocumentInfo, 0, len(docs))
	for _, d := range docs {
		out = append(out, b.documentInfo(dbID, containerID, d))
	}

	return out, nil
}

// Reset clears all in-memory state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.databases = make(map[string]*storedDatabase)
}
