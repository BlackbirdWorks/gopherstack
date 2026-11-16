package gcpstorage

import (
	"slices"
	"time"
)

// Autoclass behaviour: docs.cloud.google.com/storage/docs/autoclass.
const (
	keyAutoclass          = "autoclass"
	autoclassMinSize      = 128 << 10
	autoclassNearlineDays = 30
	autoclassColdlineDays = 90
	autoclassArchiveDays  = 365
	classNearline         = "NEARLINE"
	classArchive          = "ARCHIVE"
	classColdline         = "COLDLINE"
	classStandard         = "STANDARD"
)

type autoclassState struct {
	terminal string
	enabled  bool
}

func (b *Bucket) autoclass() autoclassState {
	m, ok := configMap(b, keyAutoclass)
	if !ok {
		return autoclassState{}
	}

	enabled, _ := m[keyEnabled].(bool)
	terminal, _ := m["terminalStorageClass"].(string)

	if terminal == "" {
		terminal = classNearline
	}

	return autoclassState{enabled: enabled, terminal: terminal}
}

// finalizeAutoclass validates the autoclass configuration, stamps its output-only times and moves objects as
// the documentation describes when the configuration changes.
func (b *Bucket) finalizeAutoclass(prev autoclassState, now time.Time) *APIError {
	if apiErr := validateAbortRules(b); apiErr != nil {
		return apiErr
	}

	cur := b.autoclass()
	if !cur.enabled {
		return b.recordAutoclassOff(prev, now)
	}

	if cur.terminal != classNearline && cur.terminal != classArchive {
		return errInvalid("autoclass.terminalStorageClass must be NEARLINE or ARCHIVE.")
	}

	if lifecycleUsesStorageClass(b) {
		return errInvalid("Autoclass cannot be enabled on a bucket with a lifecycle rule that uses " +
			"SetStorageClass or matchesStorageClass.")
	}

	m, _ := configMap(b, keyAutoclass)
	m["terminalStorageClass"] = cur.terminal

	if !prev.enabled {
		m["toggleTime"] = fmtTime(now)
	}

	if !prev.enabled || prev.terminal != cur.terminal {
		m["terminalStorageClassUpdateTime"] = fmtTime(now)
	}

	b.setConfig(keyAutoclass, m)
	b.applyAutoclassTransition(prev, cur, now)

	return nil
}

func (b *Bucket) recordAutoclassOff(prev autoclassState, now time.Time) *APIError {
	if prev.enabled {
		m, ok := configMap(b, keyAutoclass)
		if !ok {
			m = map[string]any{keyEnabled: false}
		}

		m["toggleTime"] = fmtTime(now)
		b.setConfig(keyAutoclass, m)
	}

	return nil
}

func lifecycleUsesStorageClass(b *Bucket) bool {
	return slices.ContainsFunc(lifecycleRules(b), func(r lifecycleRule) bool {
		return r.Action.Type == actionSetClass || len(r.Condition.MatchesStorageClass) > 0
	})
}

func (b *Bucket) applyAutoclassTransition(prev, cur autoclassState, now time.Time) {
	for _, versions := range b.Objects {
		for _, o := range versions {
			switch {
			case !prev.enabled:
				o.setClass(classStandard, now)
				o.Accessed = now
			case prev.terminal == classNearline && cur.terminal == classArchive && o.StorageClass == classNearline:
				o.Accessed = now.Add(-autoclassNearlineDays * day)
			case prev.terminal == classArchive && cur.terminal == classNearline &&
				(o.StorageClass == classColdline || o.StorageClass == classArchive):
				o.setClass(classNearline, now)
			}
		}
	}
}

func (o *Object) setClass(class string, now time.Time) {
	if o.StorageClass != class {
		o.StorageClass, o.ClassUpdated = class, now
	}
}

// autoclassTarget returns the class an idle object has reached, "" while it stays where it is.
func autoclassTarget(o *Object, st autoclassState, now time.Time) string {
	if o.Size() < autoclassMinSize || !o.Deleted.IsZero() {
		return ""
	}

	ref := o.Accessed
	if ref.IsZero() {
		ref = o.Created
	}

	idle := now.Sub(ref)

	if st.terminal == classArchive {
		switch {
		case idle >= autoclassArchiveDays*day:
			return classArchive
		case idle >= autoclassColdlineDays*day:
			return classColdline
		}

		return ""
	}

	if idle >= autoclassNearlineDays*day {
		return classNearline
	}

	return ""
}

func (b *InMemoryBackend) sweepAutoclassLocked(bk *Bucket, now time.Time) {
	st := bk.autoclass()
	if !st.enabled {
		return
	}

	for _, versions := range bk.Objects {
		for _, o := range versions {
			target := autoclassTarget(o, st, now)
			if target != "" && rankOf(target) > rankOf(o.StorageClass) {
				o.setClass(target, now)
				o.Updated = now
			}
		}
	}
}

// NoteRead records a data read of an object; Autoclass returns the object to Standard storage on a read.
func (b *InMemoryBackend) NoteRead(bucket, name string, gen int64) {
	if !b.autoclassEnabled(bucket) {
		return
	}

	b.mu.Lock("NoteRead")
	defer b.mu.Unlock()

	bk, err := b.bucketLocked(bucket)
	if err != nil {
		return
	}

	if o := bk.version(name, gen); o != nil {
		now := b.now().UTC()
		o.Accessed = now
		o.setClass(classStandard, now)
	}
}

// checkAutoclassCompose requires every compose source to be in Standard storage on an Autoclass bucket.
func checkAutoclassCompose(bk *Bucket, src *Object) *APIError {
	if bk.autoclass().enabled && src.StorageClass != classStandard {
		return errInvalid("Objects in an Autoclass bucket can only be composed from Standard storage; " +
			src.Name + " is in " + src.StorageClass + ".")
	}

	return nil
}

func (b *InMemoryBackend) autoclassEnabled(bucket string) bool {
	b.mu.RLock("autoclassEnabled")
	defer b.mu.RUnlock()

	bk, err := b.bucketLocked(bucket)

	return err == nil && bk.autoclass().enabled
}
