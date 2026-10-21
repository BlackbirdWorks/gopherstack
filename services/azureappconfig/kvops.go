package azureappconfig

import (
	"fmt"
	"maps"
	"time"
)

// Precondition carries the If-Match / If-None-Match request headers (already unquoted).
type Precondition struct {
	IfMatch     string
	IfNoneMatch string
}

// KVInput is the writable part of a key-value.
type KVInput struct {
	Value       *string
	ContentType *string
	Tags        map[string]string
}

func validateKV(key, label string, in KVInput) error {
	switch {
	case key == "" || key == "." || key == "..":
		return fmt.Errorf("%w: invalid key %q", ErrInvalidArgument, key)
	case len(key) > maxKeyLen:
		return fmt.Errorf("%w: key is too long", ErrInvalidArgument)
	case len(label) > maxLabelLen:
		return fmt.Errorf("%w: label is too long", ErrInvalidArgument)
	}

	kv := KeyValue{Key: key, Label: label, Value: in.Value, ContentType: in.ContentType, Tags: in.Tags}
	if kvBytes(kv) > maxItemBytes {
		return fmt.Errorf("%w: key-value exceeds %d bytes", ErrInvalidArgument, maxItemBytes)
	}

	return nil
}

func checkWrite(cur *KeyValue, p Precondition) error {
	exists := cur != nil

	if p.IfMatch != "" && (!exists || (p.IfMatch != "*" && p.IfMatch != cur.ETag)) {
		return ErrPrecondition
	}

	if p.IfNoneMatch != "" && exists && (p.IfNoneMatch == "*" || p.IfNoneMatch == cur.ETag) {
		return ErrPrecondition
	}

	if exists && cur.Locked {
		return ErrLocked
	}

	return nil
}

func (s *store) record(kv KeyValue, at time.Time, deleted bool) {
	s.History = append(s.History, histEntry{At: at, KV: cloneKV(kv), Deleted: deleted})

	if over := len(s.History) - maxHistory; over > 0 {
		s.History = append([]histEntry(nil), s.History[over:]...)
	}
}

// SetKeyValue creates or replaces a key-value.
func (b *InMemoryBackend) SetKeyValue(storeName, key, label string, in KVInput, p Precondition) (KeyValue, error) {
	if err := validateKV(key, label, in); err != nil {
		return KeyValue{}, err
	}

	b.mu.Lock("SetKeyValue")
	defer b.mu.Unlock()

	s, err := b.storeLocked(storeName)
	if err != nil {
		return KeyValue{}, err
	}

	if err = checkWrite(s.KVs[kvID(key, label)], p); err != nil {
		return KeyValue{}, err
	}

	now := b.now().UTC()
	kv := KeyValue{
		Key: key, Label: label, Value: cloneStr(in.Value), ContentType: cloneStr(in.ContentType),
		Tags: maps.Clone(in.Tags), ETag: randomText(etagBytes), LastModified: now,
	}
	s.KVs[kvID(key, label)] = &kv
	s.record(kv, now, false)
	s.bump()

	return cloneKV(kv), nil
}

// GetKeyValue returns the key-value; a non-zero at reads its state at that time.
func (b *InMemoryBackend) GetKeyValue(storeName, key, label string, at time.Time) (KeyValue, error) {
	b.mu.RLock("GetKeyValue")
	defer b.mu.RUnlock()

	s, err := b.storeLocked(storeName)
	if err != nil {
		return KeyValue{}, err
	}

	if kv, ok := s.lookup(key, label, at, b.now()); ok {
		return cloneKV(kv), nil
	}

	return KeyValue{}, ErrKeyNotFound
}

func (s *store) lookup(key, label string, at, now time.Time) (KeyValue, bool) {
	if at.IsZero() || !at.Before(now) {
		cur, ok := s.KVs[kvID(key, label)]
		if !ok {
			return KeyValue{}, false
		}

		return *cur, true
	}

	for _, kv := range s.asOf(at) {
		if kv.Key == key && kv.Label == label {
			return kv, true
		}
	}

	return KeyValue{}, false
}

// DeleteKeyValue removes a key-value and returns its last state.
func (b *InMemoryBackend) DeleteKeyValue(storeName, key, label string, p Precondition) (KeyValue, error) {
	b.mu.Lock("DeleteKeyValue")
	defer b.mu.Unlock()

	s, err := b.storeLocked(storeName)
	if err != nil {
		return KeyValue{}, err
	}

	cur, ok := s.KVs[kvID(key, label)]
	if !ok {
		if p.IfMatch != "" {
			return KeyValue{}, ErrPrecondition
		}

		return KeyValue{}, ErrKeyNotFound
	}

	if err = checkWrite(cur, p); err != nil {
		return KeyValue{}, err
	}

	delete(s.KVs, kvID(key, label))
	s.record(*cur, b.now().UTC(), true)
	s.bump()

	return cloneKV(*cur), nil
}

// SetLock marks a key-value read-only or writable again.
func (b *InMemoryBackend) SetLock(storeName, key, label string, locked bool, p Precondition) (KeyValue, error) {
	b.mu.Lock("SetLock")
	defer b.mu.Unlock()

	s, err := b.storeLocked(storeName)
	if err != nil {
		return KeyValue{}, err
	}

	cur, ok := s.KVs[kvID(key, label)]
	if !ok {
		return KeyValue{}, ErrKeyNotFound
	}

	if p.IfMatch != "" && p.IfMatch != "*" && p.IfMatch != cur.ETag {
		return KeyValue{}, ErrPrecondition
	}

	if p.IfNoneMatch != "" && (p.IfNoneMatch == "*" || p.IfNoneMatch == cur.ETag) {
		return KeyValue{}, ErrPrecondition
	}

	if cur.Locked != locked {
		now := b.now().UTC()
		cur.Locked, cur.ETag, cur.LastModified = locked, randomText(etagBytes), now
		s.record(*cur, now, false)
		s.bump()
	}

	return cloneKV(*cur), nil
}
