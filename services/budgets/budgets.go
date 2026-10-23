package budgets

import (
	"cmp"
	"context"
	"encoding/json"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func deepCopy[T any](v *T) *T {
	data, _ := json.Marshal(v)

	var out T

	_ = json.Unmarshal(data, &out)

	return &out
}

func applyCostTypeDefaults(ct *CostTypes) *CostTypes {
	if ct == nil {
		ct = &CostTypes{}
	}

	for _, p := range []**bool{
		&ct.IncludeCredit, &ct.IncludeDiscount, &ct.IncludeOtherSubscription, &ct.IncludeRecurring,
		&ct.IncludeRefund, &ct.IncludeSubscription, &ct.IncludeSupport, &ct.IncludeTax, &ct.IncludeUpfront,
	} {
		if *p == nil {
			*p = new(true)
		}
	}

	for _, p := range []**bool{&ct.UseAmortized, &ct.UseBlended} {
		if *p == nil {
			*p = new(false)
		}
	}

	return ct
}

// normalizeLocked fills defaults and server-owned fields on a validated budget.
func (b *InMemoryBackend) normalizeLocked(bg *Budget) {
	now := b.now()

	bg.CalculatedSpend = nil
	bg.LastUpdatedTime = ts(now)
	bg.CostTypes = applyCostTypeDefaults(bg.CostTypes)

	if bg.TimePeriod == nil {
		bg.TimePeriod = &TimePeriod{}
	}

	if bg.TimePeriod.Start == nil {
		bg.TimePeriod.Start = ts(periodStart(bg.TimeUnit, now))
	}

	if bg.TimePeriod.End == nil {
		bg.TimePeriod.End = ts(defaultEnd())
	}

	if bg.BillingViewArn != "" {
		bg.HealthStatus = &HealthStatus{Status: healthyStatus, LastUpdatedTime: ts(now)}
	} else {
		bg.HealthStatus = nil
	}

	if bg.AutoAdjustData != nil {
		b.autoAdjustLocked(bg)
	}
}

// rolloverLocked re-derives an auto-adjusted limit once the budget enters a period it has not adjusted for.
func (b *InMemoryBackend) rolloverLocked(bg *Budget) {
	a := bg.AutoAdjustData
	if a == nil || bg.TimeUnit == timeUnitCustom {
		return
	}

	now := b.now()
	if _, _, in := currentPeriod(bg, now); !in {
		return
	}

	if a.LastAutoAdjustTime != nil && !a.LastAutoAdjustTime.Time.Before(periodStart(bg.TimeUnit, now)) {
		return
	}

	b.autoAdjustLocked(bg)
}

// autoAdjustLocked derives the limit from past periods' spend and stamps the adjust time.
func (b *InMemoryBackend) autoAdjustLocked(bg *Budget) {
	now := b.now()
	a := bg.AutoAdjustData
	a.LastAutoAdjustTime = ts(now)

	periods := int32(1)
	if a.HistoricalOptions != nil {
		periods = a.HistoricalOptions.BudgetAdjustmentPeriod
	}

	var (
		sum   float64
		found int32
	)

	cur := periodStart(bg.TimeUnit, now)

	for range periods {
		prev := prevPeriod(bg.TimeUnit, cur)
		if v, ok := b.periodSpendLocked(bg, prev, cur); ok && v > 0 {
			sum += v
			found++
		}

		cur = prev
	}

	if a.HistoricalOptions != nil {
		a.HistoricalOptions.LookBackAvailablePeriods = found
	}

	avg := 0.0
	if found > 0 {
		avg = sum / float64(found)
	}

	bg.BudgetLimit = &Spend{Amount: formatAmount(avg), Unit: limitUnit(bg)}
}

// view returns a copy of the stored budget with computed fields for the response.
func (b *InMemoryBackend) viewLocked(r *BudgetRecord, showFilter bool) Budget {
	out := *deepCopy(&r.Budget)
	out.CalculatedSpend = b.calcSpendLocked(&r.Budget)

	if !showFilter {
		out.FilterExpression = nil
	}

	return out
}

// CreateBudget stores a budget with optional notifications and tags.
func (b *InMemoryBackend) CreateBudget(
	ctx context.Context, account string, bg Budget, notifs []NotificationWithSubscribers, rtags []ResourceTag,
) error {
	if err := validateAccount(account); err != nil {
		return err
	}

	if err := validateBudget(&bg); err != nil {
		return err
	}

	if len(notifs) > maxNotifications {
		return fail(ErrCreationLimitExceeded, "a budget can have at most %d notifications", maxNotifications)
	}

	records, err := buildNotifications(notifs)
	if err != nil {
		return err
	}

	tagMap, err := validateTags(rtags)
	if err != nil {
		return err
	}

	b.mu.Lock("CreateBudget")

	key := budgetKeyOf(account, bg.BudgetName)
	if b.budgets.Has(key) {
		b.mu.Unlock()

		return fail(ErrDuplicateRecord, "budget %s already exists", bg.BudgetName)
	}

	b.normalizeLocked(&bg)
	b.budgets.Put(&BudgetRecord{
		Key: key, AccountID: account, Budget: bg, Notifications: records, Tags: tagMap,
	})
	b.mu.Unlock()

	b.evaluate(ctx, key)

	return nil
}

func buildNotifications(in []NotificationWithSubscribers) ([]NotificationRecord, error) {
	out := make([]NotificationRecord, 0, len(in))

	for i := range in {
		n := in[i].Notification
		if err := validateNotification(&n); err != nil {
			return nil, err
		}

		if err := validateSubscribers(in[i].Subscribers); err != nil {
			return nil, err
		}

		if slices.ContainsFunc(out, func(r NotificationRecord) bool { return sameNotification(r.Notification, n) }) {
			return nil, fail(ErrDuplicateRecord, "duplicate notification")
		}

		n.NotificationState = stateOK
		out = append(out, NotificationRecord{Notification: n, Subscribers: slices.Clone(in[i].Subscribers)})
	}

	return out, nil
}

// UpdateBudget replaces a budget definition, keeping its notifications and tags.
func (b *InMemoryBackend) UpdateBudget(ctx context.Context, account string, bg Budget) error {
	if err := validateAccount(account); err != nil {
		return err
	}

	if err := validateBudget(&bg); err != nil {
		return err
	}

	b.mu.Lock("UpdateBudget")

	rec, err := b.budgetLocked(account, bg.BudgetName)
	if err != nil {
		b.mu.Unlock()

		return err
	}

	b.normalizeLocked(&bg)
	rec.Budget = bg
	key := rec.Key
	b.mu.Unlock()

	b.evaluate(ctx, key)

	return nil
}

// DeleteBudget removes a budget with its actions.
func (b *InMemoryBackend) DeleteBudget(account, name string) error {
	if err := validateAccount(account); err != nil {
		return err
	}

	b.mu.Lock("DeleteBudget")
	defer b.mu.Unlock()

	if _, err := b.budgetLocked(account, name); err != nil {
		return err
	}

	b.budgets.Delete(budgetKeyOf(account, name))

	for _, a := range b.actionsOfLocked(account, name) {
		b.actions.Delete(a.Key)
	}

	return nil
}

// GetBudget returns one budget.
func (b *InMemoryBackend) GetBudget(ctx context.Context, account, name string, showFilter bool) (*Budget, error) {
	if err := validateAccount(account); err != nil {
		return nil, err
	}

	b.evaluate(ctx, budgetKeyOf(account, name))

	b.mu.RLock("GetBudget")
	defer b.mu.RUnlock()

	r, err := b.budgetLocked(account, name)
	if err != nil {
		return nil, err
	}

	v := b.viewLocked(r, showFilter)

	return &v, nil
}

// ListBudgets pages an account's budgets by name.
func (b *InMemoryBackend) ListBudgets(
	ctx context.Context, account, token string, maxResults *int32, showFilter bool,
) (page.Page[Budget], error) {
	if err := validateAccount(account); err != nil {
		return page.Page[Budget]{}, err
	}

	b.evaluateAccount(ctx, account)

	b.mu.RLock("ListBudgets")
	defer b.mu.RUnlock()

	recs := b.accountBudgetsLocked(account)
	all := make([]Budget, 0, len(recs))

	for _, r := range recs {
		all = append(all, b.viewLocked(r, showFilter))
	}

	return paginate(all, token, maxResults, maxBudgetsPageSize)
}

func (b *InMemoryBackend) accountBudgetsLocked(account string) []*BudgetRecord {
	var out []*BudgetRecord

	b.budgets.Range(func(r *BudgetRecord) bool {
		if r.AccountID == account {
			out = append(out, r)
		}

		return true
	})

	slices.SortFunc(out, func(x, y *BudgetRecord) int { return cmp.Compare(x.Budget.BudgetName, y.Budget.BudgetName) })

	return out
}

func (b *InMemoryBackend) actionsOfLocked(account, name string) []*ActionRecord {
	var out []*ActionRecord

	b.actions.Range(func(r *ActionRecord) bool {
		if r.AccountID == account && (name == "" || r.Action.BudgetName == name) {
			out = append(out, r)
		}

		return true
	})

	slices.SortFunc(out, func(x, y *ActionRecord) int { return strings.Compare(x.Key, y.Key) })

	return out
}
