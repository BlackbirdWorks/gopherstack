package gcpstorage

import (
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/labstack/echo/v5"
)

const (
	keyRetentionPolicy  = "retentionPolicy"
	keySoftDeletePolicy = "softDeletePolicy"

	maxRetentionSeconds    = 3155760000
	defaultSoftDeleteSecs  = 7 * 24 * 3600
	minSoftDeleteSecs      = 7 * 24 * 3600
	maxSoftDeleteSecs      = 90 * 24 * 3600
	reasonRetentionNotMet  = "retentionPolicyNotMet"
	reasonUnderActiveHold  = "objectUnderActiveHold"
	reasonBadRequestExcept = "badRequestException"
)

type retentionState struct {
	period time.Duration
	set    bool
	locked bool
}

type softState struct {
	effective string
	seconds   int64
	set       bool
}

func seconds(v any) (int64, bool) {
	switch n := v.(type) {
	case string:
		i, err := strconv.ParseInt(n, 10, 64)

		return i, err == nil
	case float64:
		return int64(n), n == float64(int64(n))
	default:
		return 0, false
	}
}

func configMap(b *Bucket, key string) (map[string]any, bool) {
	raw, ok := b.Config[key]
	if !ok {
		return nil, false
	}

	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil || m == nil {
		return nil, false
	}

	return m, true
}

func (b *Bucket) policyState() (retentionState, softState) {
	var rs retentionState

	if m, ok := configMap(b, keyRetentionPolicy); ok {
		if n, valid := seconds(m["retentionPeriod"]); valid {
			rs = retentionState{set: true, period: time.Duration(n) * time.Second}
			rs.locked, _ = m["isLocked"].(bool)
		}
	}

	var ss softState

	if m, ok := configMap(b, keySoftDeletePolicy); ok {
		if n, valid := seconds(m["retentionDurationSeconds"]); valid {
			ss = softState{set: true, seconds: n}
			ss.effective, _ = m["effectiveTime"].(string)
		}
	}

	return rs, ss
}

func (b *Bucket) setConfig(key string, v any) {
	if b.Config == nil {
		b.Config = map[string]json.RawMessage{}
	}

	b.Config[key] = marshalRaw(v)
}

func (b *Bucket) finalizePolicies(prevRet retentionState, prevSoft softState, now time.Time) *APIError {
	if apiErr := b.finalizeRetention(prevRet, now); apiErr != nil {
		return apiErr
	}

	return b.finalizeSoftDelete(prevSoft, now)
}

func (b *Bucket) finalizeRetention(prev retentionState, now time.Time) *APIError {
	cur, _ := b.policyState()

	if _, present := b.Config[keyRetentionPolicy]; present && !cur.set {
		return errInvalid("Invalid retentionPolicy: retentionPeriod must be an integer number of seconds.")
	}

	if !cur.set {
		if prev.locked {
			return newErr(http.StatusBadRequest, reasonBadRequestExcept,
				"The retention policy is locked and cannot be removed.")
		}

		return nil
	}

	secs := int64(cur.period / time.Second)
	if secs < 0 || secs > maxRetentionSeconds {
		return errInvalid("Invalid retentionPeriod: must be between 0 and 3155760000 seconds.")
	}

	if prev.locked && cur.period < prev.period {
		return newErr(http.StatusBadRequest, reasonBadRequestExcept,
			"The retention period of a locked retention policy cannot be reduced.")
	}

	m, _ := configMap(b, keyRetentionPolicy)
	eff, _ := m["effectiveTime"].(string)

	if !prev.set || prev.period != cur.period || eff == "" {
		eff = fmtTime(now)
	}

	out := map[string]any{"retentionPeriod": strconv.FormatInt(secs, 10), "effectiveTime": eff}
	if prev.locked {
		out["isLocked"] = true
	}

	b.setConfig(keyRetentionPolicy, out)

	return nil
}

func (b *Bucket) finalizeSoftDelete(prev softState, now time.Time) *APIError {
	_, cur := b.policyState()

	if _, present := b.Config[keySoftDeletePolicy]; present && !cur.set {
		return errInvalid("Invalid softDeletePolicy: retentionDurationSeconds must be an integer.")
	}

	if !cur.set {
		cur.seconds = defaultSoftDeleteSecs
	}

	if cur.seconds != 0 && (cur.seconds < minSoftDeleteSecs || cur.seconds > maxSoftDeleteSecs) {
		return errInvalid("Invalid retentionDurationSeconds: must be 0 or between 7 and 90 days.")
	}

	eff := now
	if prev.set && prev.seconds == cur.seconds && prev.effective != "" {
		if t, err := time.Parse(time.RFC3339, prev.effective); err == nil {
			eff = t
		}
	}

	b.setConfig(keySoftDeletePolicy, map[string]any{
		"retentionDurationSeconds": strconv.FormatInt(cur.seconds, 10), "effectiveTime": fmtTime(eff),
	})

	return nil
}

func (b *Bucket) softDeleteDuration() time.Duration {
	_, ss := b.policyState()
	if !ss.set {
		return 0
	}

	return time.Duration(ss.seconds) * time.Second
}

// retentionExpiry is the earliest time o may be deleted under the bucket's retention policy; zero if none.
func (b *Bucket) retentionExpiry(o *Object) time.Time {
	rs, _ := b.policyState()
	if !rs.set || rs.period == 0 {
		return time.Time{}
	}

	base := o.Created
	if !o.HoldReleased.IsZero() {
		base = o.HoldReleased
	}

	return base.Add(rs.period)
}

// guardMutation rejects replacing, archiving or deleting o while a hold or retention policy protects it.
func (b *InMemoryBackend) guardMutation(bk *Bucket, o *Object) *APIError {
	if o == nil {
		return nil
	}

	if o.TemporaryHold || o.EventBasedHold {
		return &APIError{
			Status: http.StatusForbidden, Reason: reasonUnderActiveHold, XMLCode: codeAccessDenied,
			Message: "Object replacement or deletion is not allowed due to an active hold on the object.",
		}
	}

	if exp := bk.retentionExpiry(o); b.now().Before(exp) {
		return &APIError{
			Status: http.StatusForbidden, Reason: reasonRetentionNotMet, XMLCode: codeAccessDenied,
			Message: "The object is under retention and cannot be replaced or deleted until " + fmtTime(exp) + ".",
		}
	}

	return nil
}

// LockRetentionPolicy irreversibly locks the bucket's retention policy.
func (b *InMemoryBackend) LockRetentionPolicy(name string, metageneration int64) (*Bucket, error) {
	b.mu.Lock("LockRetentionPolicy")
	defer b.mu.Unlock()

	bk, err := b.bucketLocked(name)
	if err != nil {
		return nil, err
	}

	if bk.Metageneration != metageneration {
		return nil, errPrecondition()
	}

	rs, _ := bk.policyState()
	if !rs.set {
		return nil, newErr(http.StatusBadRequest, "badRequest", "The bucket has no retention policy to lock.")
	}

	m, _ := configMap(bk, keyRetentionPolicy)
	m["isLocked"] = true
	bk.setConfig(keyRetentionPolicy, m)
	bk.Metageneration++
	bk.Updated = b.now().UTC()

	return bk.clone(), nil
}

func (h *Handler) lockRetentionPolicy(c *echo.Context, rt route) error {
	raw := c.QueryParam("ifMetagenerationMatch")
	if raw == "" {
		return errRequired("ifMetagenerationMatch")
	}

	mg, parseErr := strconv.ParseInt(raw, 10, 64)
	if parseErr != nil {
		return errInvalid("Invalid value for parameter 'ifMetagenerationMatch'.")
	}

	bk, err := h.Backend.LockRetentionPolicy(rt.bucket, mg)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, bucketResource(baseURL(c.Request()), bk))
}

// softDeleteLocked parks a permanently removed version for the bucket's soft-delete window.
func (b *InMemoryBackend) softDeleteLocked(bk *Bucket, o *Object) {
	d := bk.softDeleteDuration()
	if d <= 0 {
		return
	}

	now := b.now().UTC()
	if o.Deleted.IsZero() {
		o.Deleted = now
	}

	o.SoftDeleteTime, o.HardDeleteTime = now, now.Add(d)
	bk.SoftDeleted = append(bk.SoftDeleted, o)
}

// removeVersionLocked permanently removes one version, soft-deleting it when the bucket policy asks for it.
func (b *InMemoryBackend) removeVersionLocked(bk *Bucket, cur *Object) {
	b.queueLocked(ObjectDeleted, cur)

	name := cur.Name
	bk.Objects[name] = slices.DeleteFunc(bk.Objects[name], func(o *Object) bool { return o == cur })

	if len(bk.Objects[name]) == 0 {
		delete(bk.Objects, name)
	}

	b.softDeleteLocked(bk, cur)
}

func (b *Bucket) softDeletedVersion(name string, gen int64) (int, *Object) {
	for i, o := range b.SoftDeleted {
		if o.Name == name && o.Generation == gen {
			return i, o
		}
	}

	return -1, nil
}

// GetSoftDeletedObject returns a soft-deleted generation.
func (b *InMemoryBackend) GetSoftDeletedObject(bucket, name string, gen int64) (*Object, error) {
	if gen == 0 {
		return nil, errRequired("generation")
	}

	b.mu.RLock("GetSoftDeletedObject")
	defer b.mu.RUnlock()

	bk, err := b.bucketLocked(bucket)
	if err != nil {
		return nil, err
	}

	_, o := bk.softDeletedVersion(name, gen)
	if o == nil {
		return nil, objectNotFound(bucket, name)
	}

	return bk.out(o), nil
}

// RestoreObject makes a soft-deleted generation live again as a new generation.
func (b *InMemoryBackend) RestoreObject(
	bucket, name string, gen int64, pre Preconds, copySourceACL bool,
) (*Object, error) {
	if gen == 0 {
		return nil, errRequired("generation")
	}

	defer b.flushEvents()

	b.mu.Lock("RestoreObject")
	defer b.mu.Unlock()

	bk, err := b.bucketLocked(bucket)
	if err != nil {
		return nil, err
	}

	i, src := bk.softDeletedVersion(name, gen)
	if src == nil {
		return nil, objectNotFound(bucket, name)
	}

	if apiErr := pre.checkObject(bk.live(name), false); apiErr != nil {
		return nil, apiErr
	}

	if apiErr := b.guardMutation(bk, bk.live(name)); apiErr != nil {
		return nil, apiErr
	}

	now := b.now().UTC()
	o := src.clone()
	o.Generation, o.Metageneration = b.nextGen(), 1
	o.Created, o.Updated = now, now
	o.Deleted, o.SoftDeleteTime, o.HardDeleteTime, o.HoldReleased = time.Time{}, time.Time{}, time.Time{}, time.Time{}
	o.StorageClass, o.ClassUpdated = defaultStorageClass, time.Time{}

	if !copySourceACL {
		acl, aclErr := objectACL(bk, Patch{})
		if aclErr != nil {
			return nil, aclErr
		}

		o.ACL = acl
	}

	bk.SoftDeleted = slices.Delete(bk.SoftDeleted, i, i+1)
	b.storeLocked(bk, o)

	return bk.out(o), nil
}

func (h *Handler) restoreObject(c *echo.Context, rt route) error {
	q := c.QueryParams()

	gen, pre, err := objectParams(q)
	if err != nil {
		return err
	}

	o, err := h.Backend.RestoreObject(rt.bucket, rt.object, gen, pre, q.Get("copySourceAcl") == valTrue)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, objectResource(baseURL(c.Request()), o))
}
