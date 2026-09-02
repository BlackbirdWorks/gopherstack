package resiliencehub

import (
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// resolveAppLocked resolves appArn to its stored App. Callers must hold
// b.mu. Every op in this service addresses an App exclusively by its full
// ARN (no bare-ID lookup path exists on the wire), so this extracts the
// "app/{id}" resource segment and looks the App up by ID.
func (b *InMemoryBackend) resolveAppLocked(appArn string) (*App, bool) {
	id, ok := resourceIDFromARN(appArn, ":app/")
	if !ok {
		return nil, false
	}

	return b.apps.Get(id)
}

// CreateApp creates a new Resilience Hub application, along with its
// implicit initial draft AppVersion (types.AppVersion is a bare *string with
// no enum trait -- "draft" is asserted from documented product behavior, not
// SDK-verified; see consts.go's draftVersion doc comment).
func (b *InMemoryBackend) CreateApp(req *createAppRequest) (*App, error) {
	if req.Name == "" {
		return nil, validationError("name is required")
	}

	b.mu.Lock("CreateApp")
	defer b.mu.Unlock()

	if req.PolicyArn != "" {
		if _, ok := b.resolvePolicyLocked(req.PolicyArn); !ok {
			return nil, notFoundError(resourcePolicy, req.PolicyArn)
		}
	}

	id := newAppID()
	now := time.Now().UTC()
	t := tags.New("resiliencehub.app." + id + ".tags")
	t.Merge(req.Tags)

	schedule := req.AssessmentSchedule
	if schedule == "" {
		schedule = AssessmentScheduleDisabled
	}

	a := &App{
		ID:                 id,
		ARN:                b.AppARN(id),
		Name:               req.Name,
		Description:        req.Description,
		CreationTime:       now,
		AssessmentSchedule: schedule,
		AwsApplicationArn:  req.AwsApplicationArn,
		ComplianceStatus:   AppComplianceStatusNotAssessed,
		DriftStatus:        DriftStatusNotChecked,
		EventSubscriptions: fromEventSubscriptionsWire(req.EventSubscriptions),
		PermissionModel:    fromPermissionModelWire(req.PermissionModel),
		PolicyArn:          req.PolicyArn,
		Status:             AppStatusActive,
		Tags:               t,
		Draft: &AppVersion{
			Number:       draftVersion,
			CreationTime: now,
		},
		NextVersionNumber: 1,
	}

	b.apps.Put(a)

	return a.clone(), nil
}

// GetApp returns a copy of the App identified by appArn.
func (b *InMemoryBackend) GetApp(appArn string) (*App, error) {
	b.mu.RLock("GetApp")
	defer b.mu.RUnlock()

	a, ok := b.resolveAppLocked(appArn)
	if !ok {
		return nil, notFoundError(resourceApp, appArn)
	}

	return a.clone(), nil
}

// UpdateApp applies a partial update to the App identified by appArn.
// ClearResiliencyPolicyArn is handled distinctly from an omitted PolicyArn
// (see PARITY.md's "App lifecycle" note): true clears the binding regardless
// of what PolicyArn carries, since a *bool -- unlike a *string -- can
// represent "explicitly clear" without colliding with "omitted".
func (b *InMemoryBackend) UpdateApp(appArn string, req *updateAppRequest) (*App, error) {
	b.mu.Lock("UpdateApp")
	defer b.mu.Unlock()

	a, ok := b.resolveAppLocked(appArn)
	if !ok {
		return nil, notFoundError(resourceApp, appArn)
	}

	if req.PolicyArn != "" {
		if _, polOK := b.resolvePolicyLocked(req.PolicyArn); !polOK {
			return nil, notFoundError(resourcePolicy, req.PolicyArn)
		}
	}

	if req.Description != "" {
		a.Description = req.Description
	}

	if req.AssessmentSchedule != "" {
		a.AssessmentSchedule = req.AssessmentSchedule
	}

	if len(req.EventSubscriptions) > 0 {
		a.EventSubscriptions = fromEventSubscriptionsWire(req.EventSubscriptions)
	}

	if req.PermissionModel != nil {
		a.PermissionModel = fromPermissionModelWire(req.PermissionModel)
	}

	switch {
	case req.ClearResiliencyPolicyArn != nil && *req.ClearResiliencyPolicyArn:
		a.PolicyArn = ""
	case req.PolicyArn != "":
		a.PolicyArn = req.PolicyArn
	}

	return a.clone(), nil
}

// DeleteApp deletes the App identified by appArn. Rejected (ConflictException)
// unless forceDelete is set when the app still has an in-flight (Pending/
// InProgress) assessment -- a real FK-integrity check, not a stub.
func (b *InMemoryBackend) DeleteApp(appArn string, forceDelete bool) error {
	b.mu.Lock("DeleteApp")
	defer b.mu.Unlock()

	a, ok := b.resolveAppLocked(appArn)
	if !ok {
		return notFoundError(resourceApp, appArn)
	}

	if !forceDelete {
		for _, asmt := range b.assessmentsByApp.Get(a.ID) {
			if asmt.Status == AssessmentStatusPending || asmt.Status == AssessmentStatusInProgress {
				return conflictErrorWithResource(resourceApp, a.ID, "app has an in-progress assessment: "+asmt.ARN)
			}
		}
	}

	for _, asmt := range b.assessmentsByApp.Get(a.ID) {
		if asmt.Tags != nil {
			asmt.Tags.Close()
		}

		b.assessments.Delete(assessmentKeyFn(asmt))
	}

	for _, gt := range b.groupingByApp.Get(a.ARN) {
		b.groupingTasks.Delete(gt.GroupingID)
	}

	if a.Tags != nil {
		a.Tags.Close()
	}

	b.apps.Delete(a.ID)

	return nil
}

// listAppsFilter holds ListApps' optional filters. Only one of
// appArn/awsApplicationArn/name may be set at a time, matching the real
// API's own documented rule ("Only one filter is supported for this
// operation"); fromLastAssessmentTime/toLastAssessmentTime is a separate
// time-window filter that combines with it.
type listAppsFilter struct {
	fromLastAssessmentTime time.Time
	toLastAssessmentTime   time.Time
	appArn                 string
	awsApplicationArn      string
	name                   string
	reverseOrder           bool
}

func matchesAppFilter(a *App, f listAppsFilter) bool {
	switch {
	case f.appArn != "":
		if a.ARN != f.appArn {
			return false
		}
	case f.awsApplicationArn != "":
		if a.AwsApplicationArn != f.awsApplicationArn {
			return false
		}
	case f.name != "":
		if a.Name != f.name {
			return false
		}
	}

	if !f.fromLastAssessmentTime.IsZero() && a.LastAppComplianceEvaluationTime.Before(f.fromLastAssessmentTime) {
		return false
	}

	if !f.toLastAssessmentTime.IsZero() && a.LastAppComplianceEvaluationTime.After(f.toLastAssessmentTime) {
		return false
	}

	return true
}

// ListApps returns a page of Apps matching f, sorted by
// LastAppComplianceEvaluationTime ascending (descending if f.reverseOrder),
// matching ListAppsInput's documented default sort
// (api_op_ListApps.go, resiliencehub@v1.38.3).
func (b *InMemoryBackend) ListApps(f listAppsFilter, token string, limit int) page.Page[*App] {
	b.mu.RLock("ListApps")
	defer b.mu.RUnlock()

	all := b.apps.Snapshot()
	filtered := make([]*App, 0, len(all))

	for _, a := range all {
		if matchesAppFilter(a, f) {
			filtered = append(filtered, a)
		}
	}

	sort.Slice(filtered, func(i, j int) bool {
		if f.reverseOrder {
			return filtered[i].LastAppComplianceEvaluationTime.After(filtered[j].LastAppComplianceEvaluationTime)
		}

		return filtered[i].LastAppComplianceEvaluationTime.Before(filtered[j].LastAppComplianceEvaluationTime)
	})

	return page.New(filtered, token, limit, defaultPageLimit)
}
