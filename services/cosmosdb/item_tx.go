package cosmosdb

import (
	"slices"
	"time"
)

type undoEntry struct {
	prev *storedDocument
	key  documentCompositeKey
}

// containerTx is a write transaction over one container. The caller holds the backend write lock for its whole
// life; rollback restores every document and the change-feed sequence touched since begin.
type containerTx struct {
	now    time.Time
	b      *InMemoryBackend
	c      *storedContainer
	dbID   string
	collID string
	undo   []undoEntry
	seq0   int64
}

func (b *InMemoryBackend) beginTx(dbID, collID string) (*containerTx, error) {
	c, err := b.findContainer(dbID, collID)
	if err != nil {
		return nil, err
	}

	return &containerTx{b: b, c: c, dbID: dbID, collID: collID, now: b.now(), seq0: c.ChangeSeq}, nil
}

func (tx *containerTx) put(key documentCompositeKey, doc *storedDocument) {
	prev := tx.c.Documents[key]
	tx.undo = append(tx.undo, undoEntry{key: key, prev: prev})

	rec := &changeRecord{
		Time:             doc.Timestamp,
		Current:          doc,
		Op:               opCreate,
		ID:               doc.ID,
		PartitionKeyJSON: doc.PartitionKeyJSON,
	}

	if prev != nil {
		if tx.c.alive(prev, tx.now) {
			rec.Op, rec.Previous, rec.PrevSeq = opReplace, prev, prev.Seq
		} else {
			tx.c.recordExpired(prev)
		}
	}

	tx.c.ChangeSeq++
	doc.Seq, rec.Seq = tx.c.ChangeSeq, tx.c.ChangeSeq
	tx.c.appendChange(rec)
	tx.c.Documents[key] = doc
	tx.c.sorted.Store(nil)
}

func (tx *containerTx) remove(key documentCompositeKey) {
	prev, ok := tx.c.Documents[key]
	if !ok {
		return
	}

	tx.undo = append(tx.undo, undoEntry{key: key, prev: prev})
	tx.c.ChangeSeq++
	tx.c.appendChange(&changeRecord{
		Time: tx.now, Previous: prev, Op: opDelete, ID: prev.ID, PartitionKeyJSON: prev.PartitionKeyJSON,
		Seq: tx.c.ChangeSeq, PrevSeq: prev.Seq,
	})
	delete(tx.c.Documents, key)
	tx.c.sorted.Store(nil)
}

func (tx *containerTx) rollback() {
	for _, e := range slices.Backward(tx.undo) {
		if e.prev == nil {
			delete(tx.c.Documents, e.key)
		} else {
			tx.c.Documents[e.key] = e.prev
		}
	}

	tx.undo = nil
	tx.c.ChangeSeq = tx.seq0
	tx.c.truncateChangeLog(tx.seq0)
	tx.c.sorted.Store(nil)
}

func (tx *containerTx) info(d *storedDocument) DocumentInfo {
	return tx.b.documentInfo(tx.dbID, tx.collID, d)
}

func (tx *containerTx) stamp(existing *storedDocument, exists bool) time.Time {
	prev := tx.now
	if exists {
		prev = existing.Timestamp
	}

	return bumpTimestampFrom(tx.now, prev, exists)
}

// createDoc creates (or upserts) a document; scope, when non-empty, is the only partition key it may carry.
// The bool reports whether a live document was replaced.
func (tx *containerTx) createDoc(scope string, body map[string]any, upsert bool) (DocumentInfo, bool, error) {
	id, pkJSON, clean, err := prepareDocumentBody(body, tx.c.paths())
	if err != nil {
		return DocumentInfo{}, false, err
	}

	if scope != "" && pkJSON != scope {
		return DocumentInfo{}, false, ErrPartitionKeyMismatch
	}

	key := documentCompositeKey{PartitionKeyJSON: pkJSON, ID: id}

	existing, exists := tx.c.lookup(key, tx.now)
	if exists && !upsert {
		return DocumentInfo{}, false, ErrDocumentAlreadyExists
	}

	doc := &storedDocument{ID: id, PartitionKeyJSON: pkJSON, Body: clean, Timestamp: tx.stamp(existing, exists)}
	if uniqueErr := tx.checkUnique(key, doc); uniqueErr != nil {
		return DocumentInfo{}, false, uniqueErr
	}

	tx.put(key, doc)

	return tx.info(doc), exists, nil
}

func (tx *containerTx) readDoc(partitionKey, id string) (DocumentInfo, error) {
	if err := tx.c.checkPartitionKey(partitionKey); err != nil {
		return DocumentInfo{}, err
	}

	d, ok := tx.c.lookup(documentCompositeKey{PartitionKeyJSON: partitionKey, ID: id}, tx.now)
	if !ok {
		return DocumentInfo{}, ErrDocumentNotFound
	}

	return tx.info(d), nil
}

func (tx *containerTx) replaceDoc(
	partitionKey, id string, body map[string]any, ifMatch string,
) (DocumentInfo, error) {
	if err := tx.c.checkPartitionKey(partitionKey); err != nil {
		return DocumentInfo{}, err
	}

	key := documentCompositeKey{PartitionKeyJSON: partitionKey, ID: id}
	existing, exists := tx.c.lookup(key, tx.now)

	if err := tx.b.checkIfMatch(existing, exists, ifMatch); err != nil {
		return DocumentInfo{}, err
	}

	clean, err := tx.replacementBody(partitionKey, id, body)
	if err != nil {
		return DocumentInfo{}, err
	}

	doc := &storedDocument{ID: id, PartitionKeyJSON: partitionKey, Body: clean, Timestamp: tx.stamp(existing, exists)}
	if uniqueErr := tx.checkUnique(key, doc); uniqueErr != nil {
		return DocumentInfo{}, uniqueErr
	}

	tx.put(key, doc)

	return tx.info(doc), nil
}

// replacementBody validates a replacement body: it must not declare a partition key different from the request's.
func (tx *containerTx) replacementBody(partitionKey, id string, body map[string]any) (map[string]any, error) {
	clean, err := deepCopyBody(body)
	if err != nil {
		return nil, err
	}

	stripSystemProperties(clean)

	if ttlErr := validateItemTTL(clean); ttlErr != nil {
		return nil, ttlErr
	}

	clean["id"] = id

	if bodyDeclaresPartitionKey(clean, tx.c.paths()) {
		bodyPK, pkErr := partitionKeyForBody(clean, tx.c.paths())
		if pkErr != nil {
			return nil, pkErr
		}

		if bodyPK != partitionKey {
			return nil, ErrPartitionKeyMismatch
		}
	}

	return clean, nil
}

func (tx *containerTx) deleteDoc(partitionKey, id, ifMatch string) error {
	if err := tx.c.checkPartitionKey(partitionKey); err != nil {
		return err
	}

	key := documentCompositeKey{PartitionKeyJSON: partitionKey, ID: id}

	existing, exists := tx.c.lookup(key, tx.now)
	if !exists {
		return ErrDocumentNotFound
	}

	if err := tx.b.checkIfMatch(existing, exists, ifMatch); err != nil {
		return err
	}

	tx.remove(key)

	return nil
}

func (tx *containerTx) patchDoc(partitionKey, id string, req patchRequest, ifMatch string) (DocumentInfo, error) {
	if err := tx.c.checkPartitionKey(partitionKey); err != nil {
		return DocumentInfo{}, err
	}

	key := documentCompositeKey{PartitionKeyJSON: partitionKey, ID: id}

	existing, exists := tx.c.lookup(key, tx.now)
	if !exists {
		return DocumentInfo{}, ErrDocumentNotFound
	}

	if err := tx.b.checkIfMatch(existing, exists, ifMatch); err != nil {
		return DocumentInfo{}, err
	}

	if err := tx.checkPatchCondition(req.Condition, existing); err != nil {
		return DocumentInfo{}, err
	}

	patched, err := applyPatch(existing.Body, req.Operations)
	if err != nil {
		return DocumentInfo{}, err
	}

	if err = tx.checkPatchedKeys(patched, partitionKey, id); err != nil {
		return DocumentInfo{}, err
	}

	doc := &storedDocument{ID: id, PartitionKeyJSON: partitionKey, Body: patched, Timestamp: tx.stamp(existing, true)}
	if uniqueErr := tx.checkUnique(key, doc); uniqueErr != nil {
		return DocumentInfo{}, uniqueErr
	}

	tx.put(key, doc)

	return tx.info(doc), nil
}

func (tx *containerTx) checkPatchCondition(condition string, existing *storedDocument) error {
	if condition == "" {
		return nil
	}

	ok, err := evalPatchCondition(condition, tx.info(existing))
	if err != nil {
		return err
	}

	if !ok {
		return ErrPatchConditionFailed
	}

	return nil
}

// checkPatchedKeys rejects a patch that changed the id, the partition key or the ttl validity.
func (tx *containerTx) checkPatchedKeys(patched map[string]any, partitionKey, id string) error {
	if got, _ := patched["id"].(string); got != id {
		return invalidPatchf("the id of a document cannot be patched")
	}

	pk, err := partitionKeyForBody(patched, tx.c.paths())
	if err != nil {
		return err
	}

	if pk != partitionKey {
		return invalidPatchf("the partition key of a document cannot be patched")
	}

	return validateItemTTL(patched)
}
