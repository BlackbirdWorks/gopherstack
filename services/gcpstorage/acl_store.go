package gcpstorage

import "time"

// aclRef locates the ACL list a request targets; the pointer is valid while the backend lock is held.
func (b *InMemoryBackend) aclRefLocked(t aclTarget) (*[]ACLEntry, *Bucket, *Object, error) {
	bk, err := b.bucketLocked(t.bucket)
	if err != nil {
		return nil, nil, nil, err
	}

	if bk.ubla() {
		return nil, nil, nil, errUBLA()
	}

	switch t.kind {
	case aclBucket:
		return &bk.ACL, bk, nil, nil
	case aclDefaultObject:
		return &bk.DefaultObjectACL, bk, nil, nil
	default:
		o := bk.version(t.object, t.gen)
		if o == nil {
			return nil, nil, nil, objectNotFound(t.bucket, t.object)
		}

		return &o.ACL, bk, o, nil
	}
}

func errACLNotFound(entity string) *APIError {
	return errNotFound("No such entity: " + entity)
}

// ListACL returns the entries of the targeted ACL and the resolved object generation.
func (b *InMemoryBackend) ListACL(t aclTarget) ([]ACLEntry, int64, error) {
	b.mu.RLock("ListACL")
	defer b.mu.RUnlock()

	list, _, o, err := b.aclRefLocked(t)
	if err != nil {
		return nil, 0, err
	}

	return append([]ACLEntry{}, *list...), objGen(o), nil
}

func objGen(o *Object) int64 {
	if o == nil {
		return 0
	}

	return o.Generation
}

// GetACL returns the entry for entity.
func (b *InMemoryBackend) GetACL(t aclTarget, entity string) (ACLEntry, int64, error) {
	b.mu.RLock("GetACL")
	defer b.mu.RUnlock()

	list, _, o, err := b.aclRefLocked(t)
	if err != nil {
		return ACLEntry{}, 0, err
	}

	i := findACL(*list, entity)
	if i < 0 {
		return ACLEntry{}, 0, errACLNotFound(entity)
	}

	return (*list)[i], objGen(o), nil
}

// SetACL inserts the entry or changes the role of the existing entry for the same entity.
func (b *InMemoryBackend) SetACL(t aclTarget, e ACLEntry) (ACLEntry, int64, error) {
	defer b.flushEvents()

	b.mu.Lock("SetACL")
	defer b.mu.Unlock()

	list, bk, o, err := b.aclRefLocked(t)
	if err != nil {
		return ACLEntry{}, 0, err
	}

	if apiErr := checkACLEntry(e, t.kind); apiErr != nil {
		return ACLEntry{}, 0, apiErr
	}

	next, apiErr := upsertACL(append([]ACLEntry(nil), *list...), e)
	if apiErr != nil {
		return ACLEntry{}, 0, apiErr
	}

	*list = next
	b.touchACLLocked(bk, o)

	return e, objGen(o), nil
}

// DeleteACL removes the entry for entity.
func (b *InMemoryBackend) DeleteACL(t aclTarget, entity string) error {
	defer b.flushEvents()

	b.mu.Lock("DeleteACL")
	defer b.mu.Unlock()

	list, bk, o, err := b.aclRefLocked(t)
	if err != nil {
		return err
	}

	i := findACL(*list, entity)
	if i < 0 {
		return errACLNotFound(entity)
	}

	next := append([]ACLEntry(nil), (*list)[:i]...)
	*list = append(next, (*list)[i+1:]...)
	b.touchACLLocked(bk, o)

	return nil
}

func (b *InMemoryBackend) touchACLLocked(bk *Bucket, o *Object) {
	now := time.Now().UTC()

	if o == nil {
		bk.Metageneration++
		bk.Updated = now

		return
	}

	o.Metageneration++
	o.Updated = now
	b.queueLocked(ObjectMetadataUpdated, o)
}
