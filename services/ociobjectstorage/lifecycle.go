package ociobjectstorage

import (
	"net/http"
	"path"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	targetObjects   = "objects"
	targetPrevious  = "previous-object-versions"
	targetMultipart = "multipart-uploads"

	actionDelete  = "DELETE"
	actionArchive = "ARCHIVE"
	actionIA      = "INFREQUENT_ACCESS"
	actionAbort   = "ABORT"

	unitDays  = "DAYS"
	unitYears = "YEARS"

	minLockLead = 14 * 24 * time.Hour
	percentDone = 100
)

func unitDuration(amount int64, unit string) time.Duration {
	d := time.Duration(amount) * hoursPerDay * time.Hour
	if unit == unitYears {
		d *= daysPerYear
	}

	return d
}

// LifecycleWire is the ObjectLifecyclePolicy body.
type LifecycleWire struct {
	TimeCreated time.Time       `json:"timeCreated"`
	Items       []LifecycleRule `json:"items"`
}

// PutLifecycle replaces the bucket's lifecycle policy and returns it with the policy etag.
func (b *InMemoryBackend) PutLifecycle(
	ns, bucket string,
	items []LifecycleRule,
	pre Preconds,
) (LifecycleWire, string, error) {
	seen := map[string]bool{}

	for i := range items {
		r := &items[i]
		if r.Name == "" || seen[r.Name] {
			return LifecycleWire{}, "", errParam("Lifecycle rule names are required and must be unique")
		}

		seen[r.Name] = true

		if r.TimeAmount < 1 || (r.TimeUnit != unitDays && r.TimeUnit != unitYears) {
			return LifecycleWire{}, "", errParam(
				"Rule '" + r.Name + "' needs timeAmount >= 1 and timeUnit DAYS or YEARS",
			)
		}

		if r.Target == nil {
			t := targetObjects
			r.Target = &t
		}

		if err := validateRule(r); err != nil {
			return LifecycleWire{}, "", err
		}
	}

	b.mu.Lock("PutLifecycle")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return LifecycleWire{}, "", err
	}

	if err = pre.check(bk.LifecycleEtag, bk.Lifecycle != nil, false); err != nil {
		return LifecycleWire{}, "", err
	}

	bk.Lifecycle = &Lifecycle{TimeCreated: b.nowUTC(), Items: items}
	bk.LifecycleEtag = newEtag()

	return LifecycleWire{TimeCreated: bk.Lifecycle.TimeCreated, Items: items}, bk.LifecycleEtag, nil
}

func validateRule(r *LifecycleRule) error {
	switch *r.Target {
	case targetObjects:
		if r.Action != actionDelete && r.Action != actionArchive && r.Action != actionIA {
			return errParam("Rule '" + r.Name + "': action must be DELETE, ARCHIVE or INFREQUENT_ACCESS")
		}
	case targetPrevious:
		if r.Action != actionDelete && r.Action != actionArchive && r.Action != actionIA {
			return errParam("Rule '" + r.Name + "': action must be DELETE, ARCHIVE or INFREQUENT_ACCESS")
		}
	case targetMultipart:
		if r.Action != actionAbort {
			return errParam("Rule '" + r.Name + "': multipart-uploads rules must use ABORT")
		}
	default:
		return errParam("Rule '" + r.Name + "': unknown target")
	}

	return nil
}

// GetLifecycle returns the lifecycle policy.
func (b *InMemoryBackend) GetLifecycle(ns, bucket string) (LifecycleWire, string, error) {
	b.mu.RLock("GetLifecycle")
	defer b.mu.RUnlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return LifecycleWire{}, "", err
	}

	if bk.Lifecycle == nil {
		return LifecycleWire{}, "", newErr(
			http.StatusNotFound,
			"LifecyclePolicyNotFound",
			"The bucket '"+bucket+"' has no lifecycle policy",
		)
	}

	return LifecycleWire{TimeCreated: bk.Lifecycle.TimeCreated, Items: bk.Lifecycle.Items}, bk.LifecycleEtag, nil
}

// DeleteLifecycle removes the lifecycle policy.
func (b *InMemoryBackend) DeleteLifecycle(ns, bucket string, pre Preconds) error {
	b.mu.Lock("DeleteLifecycle")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return err
	}

	if bk.Lifecycle == nil {
		return newErr(http.StatusNotFound, "LifecyclePolicyNotFound", "The bucket '"+bucket+"' has no lifecycle policy")
	}

	if err = pre.check(bk.LifecycleEtag, true, false); err != nil {
		return err
	}

	bk.Lifecycle, bk.LifecycleEtag = nil, ""

	return nil
}

func nameMatches(f *NameFilter, name string) bool {
	if f == nil {
		return true
	}

	for _, p := range f.ExclusionPatterns {
		if ok, _ := path.Match(p, name); ok {
			return false
		}
	}

	if len(f.InclusionPatterns) == 0 && len(f.InclusionPrefixes) == 0 {
		return true
	}

	for _, p := range f.InclusionPatterns {
		if ok, _ := path.Match(p, name); ok {
			return true
		}
	}

	return slices.ContainsFunc(f.InclusionPrefixes, func(p string) bool { return strings.HasPrefix(name, p) })
}

// SweepLifecycle applies every enabled lifecycle rule using the backend clock and returns the actions taken.
func (b *InMemoryBackend) SweepLifecycle() int {
	b.mu.Lock("SweepLifecycle")
	defer b.mu.Unlock()

	now := b.nowUTC()
	acted := 0

	for _, bk := range b.buckets {
		if bk.Lifecycle == nil {
			continue
		}

		for i := range bk.Lifecycle.Items {
			r := &bk.Lifecycle.Items[i]
			if !r.IsEnabled {
				continue
			}

			age := unitDuration(r.TimeAmount, r.TimeUnit)
			if *r.Target == targetMultipart {
				acted += abortStaleUploads(bk, r, age, now)
			} else {
				acted += b.sweepObjects(bk, r, age, now)
			}
		}
	}

	return acted
}

func abortStaleUploads(bk *Bucket, r *LifecycleRule, age time.Duration, now time.Time) int {
	n := 0

	for id, u := range bk.Uploads {
		if nameMatches(r.ObjectNameFilter, u.Object) && !now.Before(u.TimeCreated.Add(age)) {
			delete(bk.Uploads, id)

			n++
		}
	}

	return n
}

func (b *InMemoryBackend) sweepObjects(bk *Bucket, r *LifecycleRule, age time.Duration, now time.Time) int {
	acted := 0
	names := make([]string, 0, len(bk.Objects))

	for n := range bk.Objects {
		names = append(names, n)
	}

	sort.Strings(names)

	for _, n := range names {
		if !nameMatches(r.ObjectNameFilter, n) {
			continue
		}

		vs := append([]*Object(nil), bk.Objects[n]...)

		for i, v := range vs {
			isCurrent := i == len(vs)-1
			if isCurrent != (*r.Target == targetObjects) {
				continue
			}

			since := v.TimeCreated
			if !isCurrent {
				since = vs[i+1].TimeCreated
			}

			if now.Before(since.Add(age)) {
				continue
			}

			if b.applyAction(bk, n, v, r.Action, isCurrent) {
				acted++
			}
		}
	}

	return acted
}

func (b *InMemoryBackend) applyAction(bk *Bucket, name string, v *Object, action string, isCurrent bool) bool {
	switch action {
	case actionDelete:
		if isCurrent && v.IsDeleteMarker {
			return false
		}

		ver := ""
		if !isCurrent {
			ver = v.VersionID
		}

		_, err := b.deleteLocked(bk, name, ver, "")

		return err == nil
	case actionArchive:
		if v.IsDeleteMarker || v.Meta.StorageTier == tierArchive {
			return false
		}

		v.Meta.StorageTier = tierArchive
		v.RestoreReadyAt, v.RestoreExpiry = time.Time{}, time.Time{}

		return true
	case actionIA:
		if v.IsDeleteMarker || v.Meta.StorageTier != tierStandard {
			return false
		}

		v.Meta.StorageTier = tierIA

		return true
	default:
		return false
	}
}

// RetentionWire is the RetentionRule body.
type RetentionWire struct {
	TimeCreated    time.Time  `json:"timeCreated"`
	TimeModified   time.Time  `json:"timeModified"`
	TimeRuleLocked *time.Time `json:"timeRuleLocked,omitempty"`
	Duration       *Duration  `json:"duration,omitempty"`
	ID             string     `json:"id"`
	DisplayName    string     `json:"displayName"`
	Etag           string     `json:"etag"`
}

// Duration is the OCI Duration shape.
type Duration struct {
	TimeUnit   string `json:"timeUnit"`
	TimeAmount int64  `json:"timeAmount"`
}

// RetentionInput is Create/UpdateRetentionRuleDetails.
type RetentionInput struct {
	TimeRuleLocked *time.Time `json:"timeRuleLocked"`
	Duration       *Duration  `json:"duration"`
	DisplayName    string     `json:"displayName"`
}

func retentionWire(r *Retention) RetentionWire {
	w := RetentionWire{
		ID: r.ID, DisplayName: r.DisplayName, Etag: r.Etag, TimeCreated: r.TimeCreated,
		TimeModified: r.TimeModified, TimeRuleLocked: r.TimeRuleLocked,
	}
	if r.HasDuration {
		w.Duration = &Duration{TimeUnit: r.Unit, TimeAmount: r.Amount}
	}

	return w
}

func (b *InMemoryBackend) applyRetention(r *Retention, in RetentionInput) error {
	if in.Duration != nil {
		if in.Duration.TimeAmount < 1 || (in.Duration.TimeUnit != unitDays && in.Duration.TimeUnit != unitYears) {
			return errParam("duration needs timeAmount >= 1 and timeUnit DAYS or YEARS")
		}
	}

	if in.TimeRuleLocked != nil && in.TimeRuleLocked.Before(b.nowUTC().Add(minLockLead)) {
		return errParam("timeRuleLocked must be at least 14 days in the future")
	}

	if in.DisplayName != "" {
		r.DisplayName = in.DisplayName
	}

	if in.Duration != nil {
		r.HasDuration, r.Amount, r.Unit = true, in.Duration.TimeAmount, in.Duration.TimeUnit
	}

	if in.TimeRuleLocked != nil {
		t := in.TimeRuleLocked.UTC()
		r.TimeRuleLocked = &t
	}

	return nil
}

func (b *InMemoryBackend) isLocked(r *Retention) bool {
	return r.TimeRuleLocked != nil && !b.nowUTC().Before(*r.TimeRuleLocked)
}

// CreateRetention adds a retention rule.
func (b *InMemoryBackend) CreateRetention(ns, bucket string, in RetentionInput) (RetentionWire, error) {
	b.mu.Lock("CreateRetention")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return RetentionWire{}, err
	}

	now := b.nowUTC()
	r := &Retention{
		ID: awsid.NewN("", idHexLen), Etag: newEtag(), TimeCreated: now, TimeModified: now,
		DisplayName: "retention-rule-" + now.Format("20060102150405"),
	}

	if err = b.applyRetention(r, in); err != nil {
		return RetentionWire{}, err
	}

	bk.Retention[r.ID] = r

	return retentionWire(r), nil
}

func errRuleNotFound(id string) *APIError {
	return newErr(http.StatusNotFound, "RetentionRuleNotFound", "The retention rule '"+id+"' was not found")
}

// GetRetention returns one retention rule.
func (b *InMemoryBackend) GetRetention(ns, bucket, id string) (RetentionWire, error) {
	b.mu.RLock("GetRetention")
	defer b.mu.RUnlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return RetentionWire{}, err
	}

	r, ok := bk.Retention[id]
	if !ok {
		return RetentionWire{}, errRuleNotFound(id)
	}

	return retentionWire(r), nil
}

// ListRetention lists retention rules.
func (b *InMemoryBackend) ListRetention(ns, bucket, token string) (page.Page[RetentionWire], error) {
	b.mu.RLock("ListRetention")
	defer b.mu.RUnlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return page.Page[RetentionWire]{}, err
	}

	rs := make([]*Retention, 0, len(bk.Retention))
	for _, r := range bk.Retention {
		rs = append(rs, r)
	}

	sort.Slice(rs, func(i, j int) bool { return rs[i].ID < rs[j].ID })

	all := make([]RetentionWire, len(rs))
	for i, r := range rs {
		all[i] = retentionWire(r)
	}

	return page.New(all, token, 0, defaultListLimit), nil
}

// UpdateRetention modifies a rule; a locked rule can only be lengthened.
func (b *InMemoryBackend) UpdateRetention(
	ns, bucket, id string,
	in RetentionInput,
	pre Preconds,
) (RetentionWire, error) {
	b.mu.Lock("UpdateRetention")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return RetentionWire{}, err
	}

	r, ok := bk.Retention[id]
	if !ok {
		return RetentionWire{}, errRuleNotFound(id)
	}

	if err = pre.check(r.Etag, true, false); err != nil {
		return RetentionWire{}, err
	}

	if b.isLocked(r) {
		shorter := in.Duration == nil || in.Duration.TimeAmount < 1 ||
			unitDuration(in.Duration.TimeAmount, in.Duration.TimeUnit) < ruleDuration(r)
		if shorter || in.TimeRuleLocked != nil {
			return RetentionWire{}, errConflict("Conflict", "A locked retention rule can only be extended")
		}
	}

	cp := *r
	if err = b.applyRetention(&cp, in); err != nil {
		return RetentionWire{}, err
	}

	cp.Etag, cp.TimeModified = newEtag(), b.nowUTC()
	*r = cp

	return retentionWire(r), nil
}

// DeleteRetention removes a rule unless it is locked.
func (b *InMemoryBackend) DeleteRetention(ns, bucket, id string, pre Preconds) error {
	b.mu.Lock("DeleteRetention")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return err
	}

	r, ok := bk.Retention[id]
	if !ok {
		return errRuleNotFound(id)
	}

	if err = pre.check(r.Etag, true, false); err != nil {
		return err
	}

	if b.isLocked(r) {
		return errConflict("Conflict", "A locked retention rule cannot be deleted")
	}

	delete(bk.Retention, id)

	return nil
}

// WorkRequestWire is the WorkRequest / WorkRequestSummary body.
type WorkRequestWire struct {
	TimeAccepted    time.Time             `json:"timeAccepted"`
	TimeStarted     time.Time             `json:"timeStarted"`
	TimeFinished    time.Time             `json:"timeFinished"`
	OperationType   string                `json:"operationType"`
	Status          string                `json:"status"`
	ID              string                `json:"id"`
	CompartmentID   string                `json:"compartmentId"`
	Resources       []WorkRequestResource `json:"resources"`
	PercentComplete float32               `json:"percentComplete"`
}

// WorkRequestResource is an entity touched by a work request.
type WorkRequestResource struct {
	EntityType string `json:"entityType"`
	ActionType string `json:"actionType"`
	EntityURI  string `json:"entityUri"`
}

func wrWire(w *WorkRequest) WorkRequestWire {
	return WorkRequestWire{
		ID:              w.ID,
		OperationType:   w.OperationType,
		Status:          w.Status,
		CompartmentID:   w.CompartmentID,
		TimeAccepted:    w.TimeAccepted,
		TimeStarted:     w.TimeAccepted,
		TimeFinished:    w.TimeFinished,
		PercentComplete: percentDone,
		Resources:       []WorkRequestResource{{EntityType: "object", ActionType: "WRITTEN", EntityURI: w.EntityURI}},
	}
}

// GetWorkRequest returns a work request.
func (b *InMemoryBackend) GetWorkRequest(id string) (WorkRequestWire, error) {
	b.mu.RLock("GetWorkRequest")
	defer b.mu.RUnlock()

	w, ok := b.workRequests[id]
	if !ok {
		return WorkRequestWire{}, newErr(
			http.StatusNotFound,
			"NotAuthorizedOrNotFound",
			"Work request '"+id+"' was not found",
		)
	}

	return wrWire(w), nil
}

// ListWorkRequests lists work requests in a compartment.
func (b *InMemoryBackend) ListWorkRequests(compartment, token string, limit int) (page.Page[WorkRequestWire], error) {
	if compartment == "" {
		return page.Page[WorkRequestWire]{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListWorkRequests")
	defer b.mu.RUnlock()

	var ws []*WorkRequest

	for _, w := range b.workRequests {
		if w.CompartmentID == compartment {
			ws = append(ws, w)
		}
	}

	sort.Slice(ws, func(i, j int) bool { return ws[i].ID < ws[j].ID })

	all := make([]WorkRequestWire, len(ws))
	for i, w := range ws {
		all[i] = wrWire(w)
	}

	return page.New(all, token, limit, defaultListLimit), nil
}

// DeleteWorkRequest cancels a work request; finished ones cannot be canceled.
func (b *InMemoryBackend) DeleteWorkRequest(id string) error {
	b.mu.Lock("DeleteWorkRequest")
	defer b.mu.Unlock()

	if _, ok := b.workRequests[id]; !ok {
		return newErr(http.StatusNotFound, "NotAuthorizedOrNotFound", "Work request '"+id+"' was not found")
	}

	return errConflict("Conflict", "The work request has already finished and cannot be canceled")
}
