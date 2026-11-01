package ocifilestorage

import (
	"encoding/json"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	maxNameLen = 255
	lockFull   = "FULL"
	lockDelete = "DELETE"
	maxLockMsg = 1024
)

func deref[T any](p *T, def T) T {
	if p == nil {
		return def
	}

	return *p
}

func checkName(v string) *APIError {
	if utf8.RuneCountInString(v) > maxNameLen {
		return errParam("displayName must be at most 255 characters long")
	}

	return nil
}

// project keeps only keys of v's JSON form, producing a summary or restricted view.
func project(v any, keys ...string) map[string]any {
	raw, err := json.Marshal(v)
	if err != nil {
		return map[string]any{}
	}

	var all map[string]any
	if json.Unmarshal(raw, &all) != nil {
		return map[string]any{}
	}

	out := make(map[string]any, len(keys))

	for _, k := range keys {
		if val, ok := all[k]; ok {
			out[k] = val
		}
	}

	return out
}

// guardChange rejects changes to a locked or deleting resource unless the caller overrides the lock.
func guardChange(bs *base, override bool) *APIError {
	if bs.State == stDeleting || bs.State == stDeleted {
		return errState("The resource " + bs.ID + " is in state " + bs.State)
	}

	if !override && slices.ContainsFunc(bs.Locks, func(l lock) bool { return l.Type == lockFull }) {
		return errLocked(bs.ID)
	}

	return nil
}

// guardDelete rejects deleting a locked resource unless the caller overrides the lock.
func guardDelete(bs *base, override bool) *APIError {
	if bs.State == stDeleting || bs.State == stDeleted {
		return errState("The resource " + bs.ID + " is in state " + bs.State)
	}

	if !override && len(bs.Locks) > 0 {
		return errLocked(bs.ID)
	}

	return nil
}

func errLocked(id string) *APIError {
	return errConflict("The resource " + id + " is locked; remove the lock or set isLockOverride to proceed")
}

func checkLock(l lock) *APIError {
	if l.Type != lockFull && l.Type != lockDelete {
		return errParam("lock type must be FULL or DELETE")
	}

	if len(l.Message) > maxLockMsg {
		return errParam("lock message is too long")
	}

	return nil
}

func checkLocks(locks []lock) *APIError {
	for _, l := range locks {
		if aerr := checkLock(l); aerr != nil {
			return aerr
		}
	}

	return nil
}

func sameLock(a, c lock) bool {
	return a.Type == c.Type && a.RelatedResourceID == c.RelatedResourceID
}

func (b *InMemoryBackend) addLock(bs *base, l lock) *APIError {
	if aerr := checkLock(l); aerr != nil {
		return aerr
	}

	if slices.ContainsFunc(bs.Locks, func(x lock) bool { return sameLock(x, l) }) {
		return errConflict("A lock of type " + l.Type + " with the same relatedResourceId already exists")
	}

	now := b.nowUTC()
	l.TimeCreated = &now
	bs.Locks = append(bs.Locks, l)

	return nil
}

func removeLock(bs *base, l lock) *APIError {
	if aerr := checkLock(l); aerr != nil {
		return aerr
	}

	i := slices.IndexFunc(bs.Locks, func(x lock) bool { return sameLock(x, l) })
	if i < 0 {
		return errNotFound("lock")
	}

	bs.Locks = slices.Delete(bs.Locks, i, i+1)

	return nil
}

// applyTags replaces the tag maps that the caller supplied.
func applyTags(bs *base, t tagsIn) {
	if t.Freeform != nil {
		bs.FreeformTags = maps.Clone(t.Freeform)
	}

	if t.Defined != nil {
		bs.DefinedTags = maps.Clone(t.Defined)
	}
}

func cloneBase(bs base) base {
	bs.Locks = slices.Clone(bs.Locks)
	if bs.Locks == nil {
		bs.Locks = []lock{}
	}

	bs.FreeformTags = maps.Clone(bs.FreeformTags)
	bs.DefinedTags = maps.Clone(bs.DefinedTags)

	return bs
}

func minutes(n int64) time.Duration { return time.Duration(n) * time.Minute }

func eqFold(a, c string) bool { return strings.EqualFold(a, c) }

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
