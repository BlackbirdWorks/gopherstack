package amp

import (
	"context"
	"slices"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
)

func (b *InMemoryBackend) idle(resType, id string, p Pending) error {
	if st := b.state(p); st != statusActive {
		return conflict(resType, id, "%s %s is %s", resType, id, st)
	}

	return nil
}

func (b *InMemoryBackend) mutableWorkspace(ctx context.Context, id string) (*Workspace, error) {
	b.purgeLocked()

	return b.activeWorkspaceLocked(b.scopeOf(ctx), id)
}

// CreateAlertManagerDefinition stores the workspace's alert manager configuration.
func (b *InMemoryBackend) CreateAlertManagerDefinition(
	ctx context.Context, wsID, token string, data []byte,
) (wStatus, error) {
	if err := validateData("alert manager definition", data); err != nil {
		return wStatus{}, err
	}

	if err := validateAlertManager(data); err != nil {
		return wStatus{}, err
	}

	b.mu.Lock("CreateAlertManagerDefinition")
	defer b.mu.Unlock()

	w, err := b.mutableWorkspace(ctx, wsID)
	if err != nil {
		return wStatus{}, err
	}

	st, err := idempotency.Replay(b.idem, "CreateAlertManagerDefinition", token, wsID+string(data),
		func() (*wStatus, error) {
			if w.AlertManager != nil {
				return nil, conflict(resAlertManager, wsID, "An alert manager definition already exists for %s", wsID)
			}

			now := b.now()
			w.AlertManager = &Doc{Data: data, CreatedAt: now, ModifiedAt: now, Pending: b.pend(statusCreating)}

			return &wStatus{StatusCode: statusCreating}, nil
		})
	if err != nil {
		return wStatus{}, mapIdem(err)
	}

	return *st, nil
}

// PutAlertManagerDefinition replaces the alert manager configuration.
func (b *InMemoryBackend) PutAlertManagerDefinition(ctx context.Context, wsID string, data []byte) (wStatus, error) {
	if err := validateData("alert manager definition", data); err != nil {
		return wStatus{}, err
	}

	if err := validateAlertManager(data); err != nil {
		return wStatus{}, err
	}

	b.mu.Lock("PutAlertManagerDefinition")
	defer b.mu.Unlock()

	w, err := b.mutableWorkspace(ctx, wsID)
	if err != nil {
		return wStatus{}, err
	}

	if w.AlertManager == nil || b.gone(w.AlertManager.Pending) {
		return wStatus{}, notFound(resAlertManager, wsID, "No alert manager definition exists for %s", wsID)
	}

	if ierr := b.idle(resAlertManager, wsID, w.AlertManager.Pending); ierr != nil {
		return wStatus{}, ierr
	}

	w.AlertManager.Data = data
	w.AlertManager.ModifiedAt = b.now()
	w.AlertManager.Pending = b.pend(statusUpdating)

	return wStatus{StatusCode: statusUpdating}, nil
}

// DescribeAlertManagerDefinition returns the alert manager configuration.
func (b *InMemoryBackend) DescribeAlertManagerDefinition(ctx context.Context, wsID string) (wAlertManager, error) {
	b.mu.RLock("DescribeAlertManagerDefinition")
	defer b.mu.RUnlock()

	w, err := b.workspaceLocked(b.scopeOf(ctx), wsID)
	if err != nil {
		return wAlertManager{}, err
	}

	d := w.AlertManager
	if d == nil || b.gone(d.Pending) {
		return wAlertManager{}, notFound(resAlertManager, wsID, "No alert manager definition exists for %s", wsID)
	}

	return wAlertManager{
		Data: d.Data, Status: b.wireStatus(d.Pending),
		CreatedAt: epoch(d.CreatedAt), ModifiedAt: epoch(d.ModifiedAt),
	}, nil
}

// DeleteAlertManagerDefinition starts deletion of the alert manager configuration.
func (b *InMemoryBackend) DeleteAlertManagerDefinition(ctx context.Context, wsID string) error {
	b.mu.Lock("DeleteAlertManagerDefinition")
	defer b.mu.Unlock()

	w, err := b.mutableWorkspace(ctx, wsID)
	if err != nil {
		return err
	}

	if w.AlertManager == nil || b.gone(w.AlertManager.Pending) {
		return notFound(resAlertManager, wsID, "No alert manager definition exists for %s", wsID)
	}

	if ierr := b.idle(resAlertManager, wsID, w.AlertManager.Pending); ierr != nil {
		return ierr
	}

	w.AlertManager.Pending = b.pend(statusDeleting)

	return nil
}

// CreateLoggingConfiguration enables rules and alerting log delivery.
func (b *InMemoryBackend) CreateLoggingConfiguration(
	ctx context.Context, wsID, token, logGroup string,
) (wStatus, error) {
	if err := validateLogGroup(logGroup); err != nil {
		return wStatus{}, err
	}

	b.mu.Lock("CreateLoggingConfiguration")
	defer b.mu.Unlock()

	w, err := b.mutableWorkspace(ctx, wsID)
	if err != nil {
		return wStatus{}, err
	}

	st, err := idempotency.Replay(b.idem, "CreateLoggingConfiguration", token, wsID+logGroup,
		func() (*wStatus, error) {
			if w.Logging != nil {
				return nil, conflict(resLogging, wsID, "A logging configuration already exists for %s", wsID)
			}

			now := b.now()
			w.Logging = &LoggingConfig{
				LogGroupARN: logGroup, CreatedAt: now, ModifiedAt: now, Pending: b.pend(statusCreating),
			}

			return &wStatus{StatusCode: statusCreating}, nil
		})
	if err != nil {
		return wStatus{}, mapIdem(err)
	}

	return *st, nil
}

// UpdateLoggingConfiguration changes the log group.
func (b *InMemoryBackend) UpdateLoggingConfiguration(ctx context.Context, wsID, logGroup string) (wStatus, error) {
	if err := validateLogGroup(logGroup); err != nil {
		return wStatus{}, err
	}

	b.mu.Lock("UpdateLoggingConfiguration")
	defer b.mu.Unlock()

	w, err := b.mutableWorkspace(ctx, wsID)
	if err != nil {
		return wStatus{}, err
	}

	if w.Logging == nil || b.gone(w.Logging.Pending) {
		return wStatus{}, notFound(resLogging, wsID, "No logging configuration exists for %s", wsID)
	}

	if ierr := b.idle(resLogging, wsID, w.Logging.Pending); ierr != nil {
		return wStatus{}, ierr
	}

	w.Logging.LogGroupARN = logGroup
	w.Logging.ModifiedAt = b.now()
	w.Logging.Pending = b.pend(statusUpdating)

	return wStatus{StatusCode: statusUpdating}, nil
}

// DescribeLoggingConfiguration returns the logging configuration.
func (b *InMemoryBackend) DescribeLoggingConfiguration(ctx context.Context, wsID string) (wLogging, error) {
	b.mu.RLock("DescribeLoggingConfiguration")
	defer b.mu.RUnlock()

	w, err := b.workspaceLocked(b.scopeOf(ctx), wsID)
	if err != nil {
		return wLogging{}, err
	}

	l := w.Logging
	if l == nil || b.gone(l.Pending) {
		return wLogging{}, notFound(resLogging, wsID, "No logging configuration exists for %s", wsID)
	}

	return wLogging{
		Workspace: wsID, LogGroupArn: l.LogGroupARN, Status: b.wireStatus(l.Pending),
		CreatedAt: epoch(l.CreatedAt), ModifiedAt: epoch(l.ModifiedAt),
	}, nil
}

// DeleteLoggingConfiguration starts deletion of the logging configuration.
func (b *InMemoryBackend) DeleteLoggingConfiguration(ctx context.Context, wsID string) error {
	b.mu.Lock("DeleteLoggingConfiguration")
	defer b.mu.Unlock()

	w, err := b.mutableWorkspace(ctx, wsID)
	if err != nil {
		return err
	}

	if w.Logging == nil || b.gone(w.Logging.Pending) {
		return notFound(resLogging, wsID, "No logging configuration exists for %s", wsID)
	}

	if ierr := b.idle(resLogging, wsID, w.Logging.Pending); ierr != nil {
		return ierr
	}

	w.Logging.Pending = b.pend(statusDeleting)

	return nil
}

func validateQueryDestinations(dests []LoggingDestination) error {
	if len(dests) != 1 {
		return invalid("destinations must contain exactly one element")
	}

	if err := validateLogGroup(dests[0].LogGroupARN); err != nil {
		return err
	}

	if dests[0].QspThreshold < 0 {
		return invalid("qspThreshold must not be negative")
	}

	return nil
}

// CreateQueryLoggingConfiguration enables query log delivery.
func (b *InMemoryBackend) CreateQueryLoggingConfiguration(
	ctx context.Context, wsID, token string, dests []LoggingDestination,
) (wStatus, error) {
	if err := validateQueryDestinations(dests); err != nil {
		return wStatus{}, err
	}

	b.mu.Lock("CreateQueryLoggingConfiguration")
	defer b.mu.Unlock()

	w, err := b.mutableWorkspace(ctx, wsID)
	if err != nil {
		return wStatus{}, err
	}

	st, err := idempotency.Replay(b.idem, "CreateQueryLoggingConfiguration", token, idempotency.Fingerprint(dests),
		func() (*wStatus, error) {
			if w.QueryLogging != nil {
				return nil, conflict(resQueryLogging, wsID, "A query logging configuration already exists for %s", wsID)
			}

			now := b.now()
			w.QueryLogging = &QueryLoggingConfig{
				Destinations: slices.Clone(dests), CreatedAt: now, ModifiedAt: now, Pending: b.pend(statusCreating),
			}

			return &wStatus{StatusCode: statusCreating}, nil
		})
	if err != nil {
		return wStatus{}, mapIdem(err)
	}

	return *st, nil
}

// UpdateQueryLoggingConfiguration replaces the query log destinations.
func (b *InMemoryBackend) UpdateQueryLoggingConfiguration(
	ctx context.Context, wsID string, dests []LoggingDestination,
) (wStatus, error) {
	if err := validateQueryDestinations(dests); err != nil {
		return wStatus{}, err
	}

	b.mu.Lock("UpdateQueryLoggingConfiguration")
	defer b.mu.Unlock()

	w, err := b.mutableWorkspace(ctx, wsID)
	if err != nil {
		return wStatus{}, err
	}

	if w.QueryLogging == nil || b.gone(w.QueryLogging.Pending) {
		return wStatus{}, notFound(resQueryLogging, wsID, "No query logging configuration exists for %s", wsID)
	}

	if ierr := b.idle(resQueryLogging, wsID, w.QueryLogging.Pending); ierr != nil {
		return wStatus{}, ierr
	}

	w.QueryLogging.Destinations = slices.Clone(dests)
	w.QueryLogging.ModifiedAt = b.now()
	w.QueryLogging.Pending = b.pend(statusUpdating)

	return wStatus{StatusCode: statusUpdating}, nil
}

// DescribeQueryLoggingConfiguration returns the query logging configuration.
func (b *InMemoryBackend) DescribeQueryLoggingConfiguration(ctx context.Context, wsID string) (wQueryLogging, error) {
	b.mu.RLock("DescribeQueryLoggingConfiguration")
	defer b.mu.RUnlock()

	w, err := b.workspaceLocked(b.scopeOf(ctx), wsID)
	if err != nil {
		return wQueryLogging{}, err
	}

	q := w.QueryLogging
	if q == nil || b.gone(q.Pending) {
		return wQueryLogging{}, notFound(resQueryLogging, wsID, "No query logging configuration exists for %s", wsID)
	}

	out := wQueryLogging{
		Workspace: wsID, Status: b.wireStatus(q.Pending),
		CreatedAt: epoch(q.CreatedAt), ModifiedAt: epoch(q.ModifiedAt),
	}
	for _, d := range q.Destinations {
		out.Destinations = append(out.Destinations, wLoggingDestination{
			CloudWatchLogs: wCloudWatch{LogGroupArn: d.LogGroupARN}, Filters: wQspFilter{QspThreshold: d.QspThreshold},
		})
	}

	return out, nil
}

// DeleteQueryLoggingConfiguration starts deletion of the query logging configuration.
func (b *InMemoryBackend) DeleteQueryLoggingConfiguration(ctx context.Context, wsID string) error {
	b.mu.Lock("DeleteQueryLoggingConfiguration")
	defer b.mu.Unlock()

	w, err := b.mutableWorkspace(ctx, wsID)
	if err != nil {
		return err
	}

	if w.QueryLogging == nil || b.gone(w.QueryLogging.Pending) {
		return notFound(resQueryLogging, wsID, "No query logging configuration exists for %s", wsID)
	}

	if ierr := b.idle(resQueryLogging, wsID, w.QueryLogging.Pending); ierr != nil {
		return ierr
	}

	w.QueryLogging.Pending = b.pend(statusDeleting)

	return nil
}

// ConfigInput holds UpdateWorkspaceConfiguration parameters; nil fields are unchanged.
type ConfigInput struct {
	RetentionPeriodInDays         *int32
	OutOfOrderTimeWindowInSeconds *int32
	RuleQueryOffsetInSeconds      *int32
	LimitsPerLabelSet             []LabelSetLimit
	ReplaceLimits                 bool
}

func validateConfig(in ConfigInput) error {
	if r := in.RetentionPeriodInDays; r != nil && (*r < 1 || *r > maxRetention) {
		return invalid("retentionPeriodInDays must be between 1 and %d", maxRetention)
	}

	if o := in.OutOfOrderTimeWindowInSeconds; o != nil && *o < 0 {
		return invalid("outOfOrderTimeWindowInSeconds must not be negative")
	}

	if q := in.RuleQueryOffsetInSeconds; q != nil && *q < 0 {
		return invalid("ruleQueryOffsetInSeconds must not be negative")
	}

	seen := map[string]bool{}

	for _, l := range in.LimitsPerLabelSet {
		if len(l.LabelSet) == 0 {
			return invalid("each limitsPerLabelSet entry needs a labelSet")
		}

		if l.MaxSeries != nil && *l.MaxSeries < 0 {
			return invalid("maxSeries must not be negative")
		}

		fp := idempotency.Fingerprint(l.LabelSet)
		if seen[fp] {
			return invalid("label sets in limitsPerLabelSet must be unique")
		}

		seen[fp] = true
	}

	return nil
}

// UpdateWorkspaceConfiguration changes retention, out-of-order window, rule offset and label-set limits.
func (b *InMemoryBackend) UpdateWorkspaceConfiguration(
	ctx context.Context,
	wsID string,
	in ConfigInput,
) (wStatus, error) {
	if err := validateConfig(in); err != nil {
		return wStatus{}, err
	}

	b.mu.Lock("UpdateWorkspaceConfiguration")
	defer b.mu.Unlock()

	w, err := b.mutableWorkspace(ctx, wsID)
	if err != nil {
		return wStatus{}, err
	}

	if ierr := b.idle(resWorkspace, wsID, w.Config.Pending); ierr != nil {
		return wStatus{}, ierr
	}

	if in.RetentionPeriodInDays != nil {
		w.Config.RetentionPeriodInDays = *in.RetentionPeriodInDays
	}

	if in.OutOfOrderTimeWindowInSeconds != nil {
		w.Config.OutOfOrderTimeWindowInSeconds = *in.OutOfOrderTimeWindowInSeconds
		w.Config.OutOfOrderSet = true
	}

	if in.RuleQueryOffsetInSeconds != nil {
		w.Config.RuleQueryOffsetInSeconds = *in.RuleQueryOffsetInSeconds
		w.Config.RuleQueryOffsetSet = true
	}

	if in.ReplaceLimits {
		w.Config.LimitsPerLabelSet = slices.Clone(in.LimitsPerLabelSet)
	}

	w.Config.Pending = b.pend(statusUpdating)

	return wStatus{StatusCode: statusUpdating}, nil
}

// DescribeWorkspaceConfiguration returns the workspace settings.
func (b *InMemoryBackend) DescribeWorkspaceConfiguration(ctx context.Context, wsID string) (wWorkspaceConfig, error) {
	b.mu.RLock("DescribeWorkspaceConfiguration")
	defer b.mu.RUnlock()

	w, err := b.workspaceLocked(b.scopeOf(ctx), wsID)
	if err != nil {
		return wWorkspaceConfig{}, err
	}

	c := w.Config
	out := wWorkspaceConfig{RetentionPeriodInDays: c.RetentionPeriodInDays, Status: b.wireStatus(c.Pending)}

	if c.OutOfOrderSet {
		out.OutOfOrderTimeWindowInSeconds = &c.OutOfOrderTimeWindowInSeconds
	}

	if c.RuleQueryOffsetSet {
		out.RuleQueryOffsetInSeconds = &c.RuleQueryOffsetInSeconds
	}

	for _, l := range c.LimitsPerLabelSet {
		out.LimitsPerLabelSet = append(out.LimitsPerLabelSet, wLimit{
			LabelSet: l.LabelSet, Limits: wLimitEntry{MaxSeries: l.MaxSeries},
		})
	}

	return out, nil
}

// PolicyResult is the outcome of a resource-policy operation.
type PolicyResult struct {
	Document   string
	RevisionID string
	Status     string
}

// PutResourcePolicy attaches or replaces the workspace resource policy.
func (b *InMemoryBackend) PutResourcePolicy(
	ctx context.Context, wsID, doc, revision string,
) (PolicyResult, error) {
	if err := validatePolicy(doc); err != nil {
		return PolicyResult{}, err
	}

	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	w, err := b.mutableWorkspace(ctx, wsID)
	if err != nil {
		return PolicyResult{}, err
	}

	kind := statusUpdating

	switch {
	case w.Policy == nil:
		kind = statusCreating

		if revision != "" && revision != policyNone {
			return PolicyResult{}, conflict(resPolicy, wsID, "No policy exists with revision %s", revision)
		}
	case revision != "" && revision != w.Policy.RevisionID:
		return PolicyResult{}, conflict(resPolicy, wsID, "Revision %s is not the current policy revision", revision)
	}

	w.Policy = &ResourcePolicy{Document: doc, RevisionID: uuid.NewString(), Pending: b.pend(kind)}

	return PolicyResult{Document: doc, RevisionID: w.Policy.RevisionID, Status: kind}, nil
}

// DescribeResourcePolicy returns the workspace resource policy.
func (b *InMemoryBackend) DescribeResourcePolicy(ctx context.Context, wsID string) (PolicyResult, error) {
	b.mu.RLock("DescribeResourcePolicy")
	defer b.mu.RUnlock()

	w, err := b.workspaceLocked(b.scopeOf(ctx), wsID)
	if err != nil {
		return PolicyResult{}, err
	}

	p := w.Policy
	if p == nil || b.gone(p.Pending) {
		return PolicyResult{}, notFound(resPolicy, wsID, "No resource policy exists for %s", wsID)
	}

	return PolicyResult{Document: p.Document, RevisionID: p.RevisionID, Status: b.state(p.Pending)}, nil
}

// DeleteResourcePolicy starts deletion of the workspace resource policy.
func (b *InMemoryBackend) DeleteResourcePolicy(ctx context.Context, wsID, revision string) error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	w, err := b.mutableWorkspace(ctx, wsID)
	if err != nil {
		return err
	}

	if w.Policy == nil || b.gone(w.Policy.Pending) {
		return notFound(resPolicy, wsID, "No resource policy exists for %s", wsID)
	}

	if revision != "" && revision != w.Policy.RevisionID {
		return conflict(resPolicy, wsID, "Revision %s is not the current policy revision", revision)
	}

	w.Policy.Pending = b.pend(statusDeleting)

	return nil
}
