package cosmosdb

import (
	"context"
	"crypto/rand"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/safemap"
)

// InMemoryBackend implements StorageBackend using an in-memory map guarded
// by a single RWMutex. Shaped after services/azuretable's InMemoryBackend.
type InMemoryBackend struct {
	nextSplitDue      time.Time
	tableAPI          tableSnapshotter
	regionsChanged    func()
	mu                *lockmetrics.RWMutex
	databases         map[string]*storedDatabase
	nowFunc           func() time.Time
	etagFunc          func(time.Time) string
	parsed            *safemap.Map[string, *sqlSelectStmt]
	cur               atomic.Pointer[regionCall]
	regionGate        *lockmetrics.RWMutex
	regions           AccountRegions
	budgetEpoch       int64
	partitionMaxBytes int64
	scriptTimeout     time.Duration
	splitDelay        time.Duration
	serverless        bool
}

// NewInMemoryBackend creates a new empty InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		mu:         lockmetrics.New("cosmosdb"),
		regionGate: lockmetrics.New("cosmosdb.region"),
		databases:  make(map[string]*storedDatabase),
		nowFunc:    time.Now,
		etagFunc:   etagFor,
		parsed:     safemap.New[string, *sqlSelectStmt]("cosmosdb-sql"),
	}
}

func (b *InMemoryBackend) now() time.Time { return b.nowFunc().UTC() }

func databaseInfo(d *storedDatabase) DatabaseInfo {
	return DatabaseInfo{ID: d.ID, RID: fakeRID("dbs/" + d.ID), Modified: d.Modified}
}

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

	d := &storedDatabase{ID: id, Containers: make(map[string]*storedContainer), Modified: b.now()}
	b.databases[id] = d

	return databaseInfo(d), nil
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

	return databaseInfo(d), nil
}

// ListDatabases returns a snapshot of all databases, sorted by ID.
func (b *InMemoryBackend) ListDatabases() []DatabaseInfo {
	b.mu.RLock("ListDatabases")
	defer b.mu.RUnlock()

	out := make([]DatabaseInfo, 0, len(b.databases))
	for _, d := range b.databases {
		out = append(out, databaseInfo(d))
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
	paths, err := normalizePartitionKeyPaths(spec)
	if err != nil {
		return ContainerInfo{}, err
	}

	if ttlErr := validateDefaultTTL(spec.DefaultTTL); ttlErr != nil {
		return ContainerInfo{}, ttlErr
	}

	if polErr := spec.ContainerPolicies.validate(); polErr != nil {
		return ContainerInfo{}, polErr
	}

	if crErr := validateConflictPolicy(spec.ConflictResolutionPolicy); crErr != nil {
		return ContainerInfo{}, crErr
	}

	if idxErr := validateSearchIndexes(spec.IndexingPolicy, spec.ContainerPolicies.stored()); idxErr != nil {
		return ContainerInfo{}, idxErr
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

	c := &storedContainer{
		ID:               spec.ID,
		PartitionKeyPath: paths[0],
		DefaultTTL:       spec.DefaultTTL,
		IndexingPolicy:   spec.IndexingPolicy,
		Documents:        make(map[documentCompositeKey]*storedDocument),

		ConflictResolutionPolicy: spec.ConflictResolutionPolicy,
		Modified:                 b.now(),
	}
	if len(paths) > 1 {
		c.PartitionKeyPaths = paths
	}

	_ = c.applyPolicies(spec.ContainerPolicies, false)

	d.Containers[spec.ID] = c

	return b.containerInfo(dbID, c), nil
}

// ReplaceContainer updates a container's defaultTtl and indexing policy. The partition key is immutable.
func (b *InMemoryBackend) ReplaceContainer(dbID string, spec ContainerSpec) (ContainerInfo, error) {
	paths, err := normalizePartitionKeyPaths(spec)
	if err != nil {
		return ContainerInfo{}, err
	}

	if ttlErr := validateDefaultTTL(spec.DefaultTTL); ttlErr != nil {
		return ContainerInfo{}, ttlErr
	}

	if polErr := spec.ContainerPolicies.validate(); polErr != nil {
		return ContainerInfo{}, polErr
	}

	b.mu.Lock("ReplaceContainer")
	defer b.mu.Unlock()

	c, err := b.findContainer(dbID, spec.ID)
	if err != nil {
		return ContainerInfo{}, err
	}

	if !slices.Equal(paths, c.paths()) {
		return ContainerInfo{}, ErrPartitionKeyImmutable
	}

	if idxErr := c.checkReplaceIndexes(spec); idxErr != nil {
		return ContainerInfo{}, idxErr
	}

	if crErr := validateConflictPolicy(spec.ConflictResolutionPolicy); crErr != nil {
		return ContainerInfo{}, crErr
	}

	if conflictPolicyChanged(c.ConflictResolutionPolicy, spec.ConflictResolutionPolicy) {
		return ContainerInfo{}, fmt.Errorf("%w: conflictResolutionPolicy can only be set when the container is created",
			ErrInvalidContainerPolicy)
	}

	if polErr := c.applyPolicies(spec.ContainerPolicies, true); polErr != nil {
		return ContainerInfo{}, polErr
	}

	c.DefaultTTL = spec.DefaultTTL
	if len(spec.IndexingPolicy) > 0 {
		c.IndexingPolicy = spec.IndexingPolicy
	}

	c.touch(b.now())

	return b.containerInfo(dbID, c), nil
}

func (b *InMemoryBackend) containerInfo(dbID string, c *storedContainer) ContainerInfo {
	return ContainerInfo{
		ID:                c.ID,
		RID:               fakeRID("dbs/" + dbID + "/colls/" + c.ID),
		PartitionKeyPath:  c.PartitionKeyPath,
		PartitionKeyPaths: slices.Clone(c.paths()),
		DefaultTTL:        c.DefaultTTL,
		IndexingPolicy:    c.IndexingPolicy,
		Modified:          c.Modified,

		ConflictResolutionPolicy: c.ConflictResolutionPolicy,
		ContainerPolicies:        c.policies(),
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
// ID (generating one if absent) and partition key JSON per the container's
// declared paths. System property keys are stripped -- they are always
// server-managed, never client-supplied.
func prepareDocumentBody(body map[string]any, paths []string) (string, string, map[string]any, error) {
	clone, err := deepCopyBody(body)
	if err != nil {
		return "", "", nil, err
	}

	stripSystemProperties(clone)

	if ttlErr := validateItemTTL(clone); ttlErr != nil {
		return "", "", nil, ttlErr
	}

	id, err := resolveDocumentID(clone)
	if err != nil {
		return "", "", nil, err
	}

	clone["id"] = id

	pkJSON, err := partitionKeyForBody(clone, paths)
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

// CreateDocument creates (or, if upsert, inserts-or-replaces) a document. See StorageBackend's doc comment.
func (b *InMemoryBackend) CreateDocument(
	dbID, containerID string, body map[string]any, upsert bool,
) (DocumentInfo, error) {
	b.mu.Lock("CreateDocument")
	defer b.mu.Unlock()

	tx, err := b.beginTx(dbID, containerID)
	if err != nil {
		return DocumentInfo{}, err
	}

	info, _, err := tx.createDoc("", body, upsert)

	return info, err
}

func (b *InMemoryBackend) documentInfo(dbID, containerID string, d *storedDocument) DocumentInfo {
	body, ok := copyStoredValue(d.Body).(map[string]any)
	if !ok {
		// Unreachable in practice: d.Body is always a map. Fail safe with an
		// empty body rather than a document with the wrong ID/etag.
		body = map[string]any{}
	}

	info := b.documentInfoShared(dbID, containerID, d)
	info.Body = body

	return info
}

// documentInfoShared is documentInfo without the body copy: Body aliases
// stored state and must not be mutated or retained.
func (b *InMemoryBackend) documentInfoShared(dbID, containerID string, d *storedDocument) DocumentInfo {
	path := "dbs/" + dbID + "/colls/" + containerID + "/docs/" + d.ID

	return DocumentInfo{
		ID:               d.ID,
		RID:              fakeRID(path),
		Self:             path + "/",
		ETag:             b.etagFunc(d.Timestamp),
		Timestamp:        d.Timestamp,
		PartitionKeyJSON: d.PartitionKeyJSON,
		Body:             d.Body,
		Seq:              d.Seq,
	}
}

// GetDocument retrieves a single document by partition key and ID.
func (b *InMemoryBackend) GetDocument(dbID, containerID, partitionKey, id string) (DocumentInfo, error) {
	b.mu.RLock("GetDocument")
	defer b.mu.RUnlock()

	tx, err := b.beginTxAt(dbID, containerID, true)
	if err != nil {
		return DocumentInfo{}, err
	}

	return tx.readDoc(partitionKey, id)
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

// ReplaceDocument fully replaces an existing document's body; id and partition key are fixed by the request.
func (b *InMemoryBackend) ReplaceDocument(
	dbID, containerID, partitionKey, id string, body map[string]any, ifMatch string,
) (DocumentInfo, error) {
	b.mu.Lock("ReplaceDocument")
	defer b.mu.Unlock()

	tx, err := b.beginTx(dbID, containerID)
	if err != nil {
		return DocumentInfo{}, err
	}

	return tx.replaceDoc(partitionKey, id, body, ifMatch)
}

// PatchDocument applies a partial update to an existing document.
func (b *InMemoryBackend) PatchDocument(
	dbID, containerID, partitionKey, id string, req patchRequest, ifMatch string,
) (DocumentInfo, error) {
	b.mu.Lock("PatchDocument")
	defer b.mu.Unlock()

	tx, err := b.beginTx(dbID, containerID)
	if err != nil {
		return DocumentInfo{}, err
	}

	return tx.patchDoc(partitionKey, id, req, ifMatch)
}

// DeleteDocument removes a document after verifying ifMatch.
func (b *InMemoryBackend) DeleteDocument(dbID, containerID, partitionKey, id, ifMatch string) error {
	b.mu.Lock("DeleteDocument")
	defer b.mu.Unlock()

	tx, err := b.beginTx(dbID, containerID)
	if err != nil {
		return err
	}

	return tx.deleteDoc(partitionKey, id, ifMatch)
}

// ListDocuments returns every live document in containerID, ordered by ID.
func (b *InMemoryBackend) ListDocuments(dbID, containerID string) ([]DocumentInfo, error) {
	b.mu.RLock("ListDocuments")
	defer b.mu.RUnlock()

	c, err := b.findDataContainer(dbID, containerID, true)
	if err != nil {
		return nil, err
	}

	docs := c.liveDocs(b.now())

	out := make([]DocumentInfo, 0, len(docs))
	for _, d := range docs {
		out = append(out, b.documentInfo(dbID, containerID, d))
	}

	return out, nil
}

// ListDocumentsPaged returns one page of live documents; token and limit follow pkgs/page (limit <= 0 is
// unbounded).
func (b *InMemoryBackend) ListDocumentsPaged(
	dbID, containerID, token string, limit int,
) (page.Page[DocumentInfo], error) {
	b.mu.RLock("ListDocumentsPaged")
	defer b.mu.RUnlock()

	c, err := b.findDataContainer(dbID, containerID, true)
	if err != nil {
		return page.Page[DocumentInfo]{}, err
	}

	docs := c.liveDocs(b.now())
	pg := page.New(docs, token, limit, max(len(docs), 1))

	out := make([]DocumentInfo, 0, len(pg.Data))
	for _, d := range pg.Data {
		out = append(out, b.documentInfo(dbID, containerID, d))
	}

	return page.Page[DocumentInfo]{Data: out, Next: pg.Next}, nil
}

// Reset clears all in-memory state.
func (b *InMemoryBackend) Reset() {
	defer b.notifyRegions()

	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.databases = make(map[string]*storedDatabase)
	b.regions = AccountRegions{}
	b.budgetEpoch++
}

type tableSnapshotter interface {
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
	Reset()
}

// AttachTableAPI includes t in this backend's Snapshot/Restore/Reset.
func (b *InMemoryBackend) AttachTableAPI(t tableSnapshotter) {
	b.mu.Lock("AttachTableAPI")
	defer b.mu.Unlock()

	b.tableAPI = t
}
