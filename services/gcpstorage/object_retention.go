package gcpstorage

import (
	"net/http"
	"time"
)

// Object retention lock; field names and the overrideUnlockedRetention rule are from
// google.golang.org/api storage/v1 (ObjectRetention, ObjectsPatchCall.OverrideUnlockedRetention).
const (
	retentionModeLocked   = "Locked"
	retentionModeUnlocked = "Unlocked"
	retentionModeEnabled  = "Enabled"
	keyObjectRetention    = "objectRetention"
	keyRetention          = "retention"
	keyEnableRetention    = "enableObjectRetention"
	keyOverrideUnlocked   = "overrideUnlockedRetention"
)

// ObjectRetention is the retention lock of one object.
type ObjectRetention struct {
	RetainUntil time.Time `json:"retainUntil"`
	Mode        string    `json:"mode"`
}

type objectRetentionJSON struct {
	Mode            string `json:"mode"`
	RetainUntilTime string `json:"retainUntilTime"`
}

func (o *Object) retentionActive(now time.Time) bool {
	return o.ObjectRetention != nil && now.Before(o.ObjectRetention.RetainUntil)
}

func (o *Object) retentionJSON() *objectRetentionJSON {
	if o.ObjectRetention == nil {
		return nil
	}

	return &objectRetentionJSON{Mode: o.ObjectRetention.Mode, RetainUntilTime: fmtTime(o.ObjectRetention.RetainUntil)}
}

func (b *Bucket) objectRetentionEnabled() bool {
	m, ok := configMap(b, keyObjectRetention)

	return ok && m["mode"] == retentionModeEnabled
}

func errRetentionLocked(msg string) *APIError {
	return &APIError{
		Status:  http.StatusForbidden,
		Reason:  reasonRetentionNotMet,
		XMLCode: codeAccessDenied,
		Message: msg,
	}
}

// applyObjectRetention applies the retention field of p to o; prev is the stored object being updated, nil on create.
func (b *InMemoryBackend) applyObjectRetention(bk *Bucket, o, prev *Object, p Patch, now time.Time) *APIError {
	raw, set := p[keyRetention]
	if !set {
		return checkRetentionHold(o)
	}

	override := p.str(keyOverrideUnlocked) == "true"

	if isNull(raw) {
		if apiErr := checkRetentionRemoval(prev, override, now); apiErr != nil {
			return apiErr
		}

		o.ObjectRetention = nil

		return nil
	}

	in, apiErr := decodeField[objectRetentionJSON](raw, keyRetention)
	if apiErr != nil {
		return apiErr
	}

	next, apiErr := parseObjectRetention(bk, in, now)
	if apiErr != nil {
		return apiErr
	}

	if apiErr = checkRetentionChange(prev, next, override, now); apiErr != nil {
		return apiErr
	}

	o.ObjectRetention = next

	return checkRetentionHold(o)
}

func parseObjectRetention(bk *Bucket, in objectRetentionJSON, now time.Time) (*ObjectRetention, *APIError) {
	if !bk.objectRetentionEnabled() {
		return nil, errInvalid("Object retention is not enabled on bucket " + bk.Name + ".")
	}

	if in.Mode != retentionModeLocked && in.Mode != retentionModeUnlocked {
		return nil, errInvalid("Invalid object retention mode '" + in.Mode + "'; use Unlocked or Locked.")
	}

	until, err := time.Parse(time.RFC3339, in.RetainUntilTime)
	if err != nil {
		return nil, errInvalid("Invalid value for field 'retention.retainUntilTime'.")
	}

	until = until.UTC()
	if !until.After(now) || until.Sub(now) > time.Duration(maxRetentionSeconds)*time.Second {
		return nil, errInvalid("retention.retainUntilTime must be in the future and at most 3155760000 seconds ahead.")
	}

	return &ObjectRetention{Mode: in.Mode, RetainUntil: until}, nil
}

func checkRetentionHold(o *Object) *APIError {
	if o.ObjectRetention != nil && o.EventBasedHold {
		return errInvalid("An object cannot have both an event-based hold and an object retention configuration.")
	}

	return nil
}

func checkRetentionRemoval(prev *Object, override bool, now time.Time) *APIError {
	if prev == nil || !prev.retentionActive(now) {
		return nil
	}

	if prev.ObjectRetention.Mode == retentionModeLocked {
		return errRetentionLocked("A locked object retention configuration cannot be removed before it expires.")
	}

	if !override {
		return errInvalid("overrideUnlockedRetention must be true to remove an object retention configuration.")
	}

	return nil
}

func checkRetentionChange(prev *Object, next *ObjectRetention, override bool, now time.Time) *APIError {
	if prev == nil || !prev.retentionActive(now) {
		return nil
	}

	cur := prev.ObjectRetention
	shortened := next.RetainUntil.Before(cur.RetainUntil)

	if cur.Mode == retentionModeLocked {
		if next.Mode != retentionModeLocked || shortened {
			return errRetentionLocked("A locked object retention configuration can only be extended.")
		}

		return nil
	}

	if (shortened || next.Mode == retentionModeLocked) && !override {
		return errInvalid("overrideUnlockedRetention must be true to reduce an unlocked retention period " +
			"or to lock it.")
	}

	return nil
}

// guardObjectRetention rejects replacing or deleting o before its retain-until time. A live version of a
// versioned bucket may become noncurrent (docs.cloud.google.com/storage/docs/object-lock).
func (b *InMemoryBackend) guardObjectRetention(bk *Bucket, o *Object, permanent bool) *APIError {
	if !o.retentionActive(b.now()) || (bk.Versioning && o.Deleted.IsZero() && !permanent) {
		return nil
	}

	return errRetentionLocked("The object is under object retention until " + fmtTime(o.ObjectRetention.RetainUntil) +
		" and cannot be replaced or deleted.")
}
