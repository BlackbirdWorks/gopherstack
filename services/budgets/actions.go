package budgets

import (
	"cmp"
	"context"
	"slices"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	statusStandby           = "STANDBY"
	statusPending           = "PENDING"
	statusExecInProgress    = "EXECUTION_IN_PROGRESS"
	statusExecSuccess       = "EXECUTION_SUCCESS"
	statusExecFailure       = "EXECUTION_FAILURE"
	statusReverseInProgress = "REVERSE_IN_PROGRESS"
	statusReverseSuccess    = "REVERSE_SUCCESS"
	statusReverseFailure    = "REVERSE_FAILURE"
	statusResetInProgress   = "RESET_IN_PROGRESS"
	statusResetFailure      = "RESET_FAILURE"

	eventSystem  = "SYSTEM"
	eventCreate  = "CREATE_ACTION"
	eventUpdate  = "UPDATE_ACTION"
	eventExecute = "EXECUTE_ACTION"
)

func inProgress(status string) bool {
	return status == statusExecInProgress || status == statusReverseInProgress || status == statusResetInProgress
}

func validateAction(account string, a *Action) error {
	if t := a.ActionThreshold; t == nil || t.ActionThresholdValue < 0 ||
		(t.ActionThresholdType != thresholdPercentage && t.ActionThresholdType != thresholdAbsolute) {
		return fail(ErrInvalidParameter, "ActionThreshold is invalid")
	}

	if a.ApprovalModel != approvalAutomatic && a.ApprovalModel != approvalManual {
		return fail(ErrInvalidParameter, "ApprovalModel %q is not valid", a.ApprovalModel)
	}

	if a.NotificationType != notifActual && a.NotificationType != notifForecasted {
		return fail(ErrInvalidParameter, "NotificationType %q is not valid", a.NotificationType)
	}

	if m := roleRE.FindStringSubmatch(a.ExecutionRoleArn); m == nil || m[1] != account {
		return fail(ErrInvalidParameter, "ExecutionRoleArn must be an IAM role ARN in account %s", account)
	}

	if err := validateSubscribers(a.Subscribers); err != nil {
		return err
	}

	return validateDefinition(a)
}

func validateDefinition(a *Action) error {
	d := a.Definition
	if d == nil {
		return fail(ErrInvalidParameter, "Definition is required")
	}

	var ok bool

	switch a.ActionType {
	case actionIAM:
		ok = validIAMDefinition(d)
	case actionSCP:
		ok = validSCPDefinition(d)
	case actionSSM:
		ok = validSSMDefinition(d)
	default:
		return fail(ErrInvalidParameter, "ActionType %q is not valid", a.ActionType)
	}

	if !ok {
		return fail(ErrInvalidParameter, "Definition does not match ActionType %s", a.ActionType)
	}

	return nil
}

func validIAMDefinition(d *Definition) bool {
	i := d.IamActionDefinition

	return i != nil && d.ScpActionDefinition == nil && d.SsmActionDefinition == nil &&
		policyRE.MatchString(i.PolicyArn) && len(i.Roles)+len(i.Users)+len(i.Groups) > 0
}

func validSCPDefinition(d *Definition) bool {
	s := d.ScpActionDefinition

	return s != nil && d.IamActionDefinition == nil && d.SsmActionDefinition == nil &&
		s.PolicyID != "" && len(s.TargetIDs) > 0
}

func validSSMDefinition(d *Definition) bool {
	s := d.SsmActionDefinition

	return s != nil && d.IamActionDefinition == nil && d.ScpActionDefinition == nil && s.Region != "" &&
		len(s.InstanceIDs) > 0 && oneOf(s.ActionSubType, "STOP_EC2_INSTANCES", "STOP_RDS_INSTANCES")
}

func (b *InMemoryBackend) recordLocked(r *ActionRecord, event, status, msg string) {
	snap := *deepCopy(&r.Action)
	snap.Status = status
	r.History = append(r.History, ActionHistory{
		Timestamp: *ts(b.now()), EventType: event, Status: status,
		ActionHistoryDetails: ActionHistoryDetails{Action: snap, Message: msg},
	})
}

// CreateBudgetAction stores an action in STANDBY and returns its ID.
func (b *InMemoryBackend) CreateBudgetAction(
	account, name string, a Action, rtags []ResourceTag,
) (string, error) {
	if err := validateAccount(account); err != nil {
		return "", err
	}

	if err := validateAction(account, &a); err != nil {
		return "", err
	}

	tagMap, err := validateTags(rtags)
	if err != nil {
		return "", err
	}

	b.mu.Lock("CreateBudgetAction")
	defer b.mu.Unlock()

	if _, err = b.budgetLocked(account, name); err != nil {
		return "", err
	}

	if len(b.actionsOfLocked(account, name)) >= maxActionsPerBudget {
		return "", fail(ErrCreationLimitExceeded, "a budget can have at most %d actions", maxActionsPerBudget)
	}

	a.ActionID = uuid.NewString()
	a.BudgetName = name
	a.Status = statusStandby

	rec := &ActionRecord{Key: actionKeyOf(account, name, a.ActionID), AccountID: account, Action: a, Tags: tagMap}
	b.recordLocked(rec, eventCreate, statusStandby, "Action created")
	b.actions.Put(rec)

	return a.ActionID, nil
}

// GetAction returns one action.
func (b *InMemoryBackend) GetAction(ctx context.Context, account, name, id string) (*Action, error) {
	if err := validateAccount(account); err != nil {
		return nil, err
	}

	b.evaluate(ctx, budgetKeyOf(account, name))

	b.mu.RLock("GetAction")
	defer b.mu.RUnlock()

	r, err := b.actionLocked(account, name, id)
	if err != nil {
		return nil, err
	}

	return deepCopy(&r.Action), nil
}

// ActionPatch holds UpdateBudgetAction fields; zero values are unchanged.
type ActionPatch struct {
	ActionThreshold  *ActionThreshold
	Definition       *Definition
	ApprovalModel    string
	ExecutionRoleArn string
	NotificationType string
	Subscribers      []Subscriber
}

// UpdateBudgetAction patches an action and returns its old and new forms.
func (b *InMemoryBackend) UpdateBudgetAction(
	account, name, id string, p ActionPatch,
) (*Action, *Action, error) {
	if err := validateAccount(account); err != nil {
		return nil, nil, err
	}

	b.mu.Lock("UpdateBudgetAction")
	defer b.mu.Unlock()

	r, err := b.actionLocked(account, name, id)
	if err != nil {
		return nil, nil, err
	}

	if inProgress(r.Action.Status) {
		return nil, nil, fail(ErrResourceLocked, "action %s is in progress", id)
	}

	next := *deepCopy(&r.Action)
	if p.ActionThreshold != nil {
		next.ActionThreshold = p.ActionThreshold
	}

	if p.Definition != nil {
		next.Definition = p.Definition
	}

	if p.Subscribers != nil {
		next.Subscribers = p.Subscribers
	}

	next.ApprovalModel = cmp.Or(p.ApprovalModel, next.ApprovalModel)
	next.ExecutionRoleArn = cmp.Or(p.ExecutionRoleArn, next.ExecutionRoleArn)
	next.NotificationType = cmp.Or(p.NotificationType, next.NotificationType)

	if err = validateAction(account, &next); err != nil {
		return nil, nil, err
	}

	oldA := deepCopy(&r.Action)
	r.Action = next
	b.recordLocked(r, eventUpdate, next.Status, "Action updated")

	return oldA, deepCopy(&r.Action), nil
}

// DeleteBudgetAction removes an action and returns it.
func (b *InMemoryBackend) DeleteBudgetAction(account, name, id string) (*Action, error) {
	if err := validateAccount(account); err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteBudgetAction")
	defer b.mu.Unlock()

	r, err := b.actionLocked(account, name, id)
	if err != nil {
		return nil, err
	}

	if inProgress(r.Action.Status) {
		return nil, fail(ErrResourceLocked, "action %s is in progress", id)
	}

	b.actions.Delete(r.Key)

	return deepCopy(&r.Action), nil
}

// ListActionsForBudget pages a budget's actions.
func (b *InMemoryBackend) ListActionsForBudget(
	ctx context.Context, account, name, token string, maxResults *int32,
) (page.Page[Action], error) {
	if err := validateAccount(account); err != nil {
		return page.Page[Action]{}, err
	}

	b.evaluate(ctx, budgetKeyOf(account, name))

	b.mu.RLock("ListActionsForBudget")
	defer b.mu.RUnlock()

	if _, err := b.budgetLocked(account, name); err != nil {
		return page.Page[Action]{}, err
	}

	return paginate(actionsOf(b.actionsOfLocked(account, name)), token, maxResults, maxPageSize)
}

// ListActionsForAccount pages every action of an account.
func (b *InMemoryBackend) ListActionsForAccount(
	ctx context.Context, account, token string, maxResults *int32,
) (page.Page[Action], error) {
	if err := validateAccount(account); err != nil {
		return page.Page[Action]{}, err
	}

	b.evaluateAccount(ctx, account)

	b.mu.RLock("ListActionsForAccount")
	defer b.mu.RUnlock()

	return paginate(actionsOf(b.actionsOfLocked(account, "")), token, maxResults, maxPageSize)
}

func actionsOf(recs []*ActionRecord) []Action {
	out := make([]Action, len(recs))
	for i, r := range recs {
		out[i] = *deepCopy(&r.Action)
	}

	return out
}

// ListActionHistories pages an action's events, newest first, within an optional window.
func (b *InMemoryBackend) ListActionHistories(
	account, name, id string, tp *TimePeriod, token string, maxResults *int32,
) (page.Page[ActionHistory], error) {
	if err := validateAccount(account); err != nil {
		return page.Page[ActionHistory]{}, err
	}

	b.mu.RLock("ListActionHistories")
	defer b.mu.RUnlock()

	r, err := b.actionLocked(account, name, id)
	if err != nil {
		return page.Page[ActionHistory]{}, err
	}

	var all []ActionHistory

	for _, h := range r.History {
		if tp != nil && ((tp.Start != nil && h.Timestamp.Before(tp.Start.Time)) ||
			(tp.End != nil && h.Timestamp.After(tp.End.Time))) {
			continue
		}

		all = append(all, *deepCopy(&h))
	}

	slices.Reverse(all)

	return paginate(all, token, maxResults, maxPageSize)
}
