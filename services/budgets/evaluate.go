package budgets

import (
	"context"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const percent = 100

type queuedRun struct {
	exec    ActionExecutor
	key     string
	account string
	action  Action
}

// StartEvaluator re-evaluates every budget's notifications and actions each interval until ctx ends.
func (b *InMemoryBackend) StartEvaluator(ctx context.Context, interval time.Duration) {
	worker.NewGroup(ctx, serviceName).Ticker("evaluator", interval, 0, b.EvaluateAll)
}

// EvaluateAll evaluates every budget.
func (b *InMemoryBackend) EvaluateAll(ctx context.Context) {
	b.mu.RLock("EvaluateAll")

	keys := make([]string, 0, b.budgets.Len())
	b.budgets.Range(func(r *BudgetRecord) bool {
		keys = append(keys, r.Key)

		return true
	})
	b.mu.RUnlock()

	for _, k := range keys {
		b.evaluate(ctx, k)
	}
}

func (b *InMemoryBackend) evaluateAccount(ctx context.Context, account string) {
	b.mu.RLock("evaluateAccount")

	recs := b.accountBudgetsLocked(account)
	keys := make([]string, len(recs))

	for i, r := range recs {
		keys[i] = r.Key
	}

	b.mu.RUnlock()

	for _, k := range keys {
		b.evaluate(ctx, k)
	}
}

func compare(op string, value, threshold float64) bool {
	switch op {
	case opGreater:
		return value > threshold
	case opLess:
		return value < threshold
	default:
		return value == threshold
	}
}

func breached(kind, op, thresholdType string, threshold float64, calc *CalculatedSpend, limit float64) bool {
	value := parseAmount(calc.ActualSpend.Amount)

	if kind == notifForecasted {
		if calc.ForecastedSpend == nil {
			return false
		}

		value = parseAmount(calc.ForecastedSpend.Amount)
	}

	if thresholdType == thresholdAbsolute {
		return compare(op, value, threshold)
	}

	if limit <= 0 {
		return false
	}

	return compare(op, value/limit*percent, threshold)
}

// evaluate fires notifications and actions whose thresholds are crossed since the last pass.
func (b *InMemoryBackend) evaluate(ctx context.Context, key string) {
	b.mu.Lock("evaluate")

	rec, ok := b.budgets.Get(key)
	if !ok {
		b.mu.Unlock()

		return
	}

	bg := &rec.Budget
	b.rolloverLocked(bg)

	calc := b.calcSpendLocked(bg)

	if calc.ForecastedSpend == nil {
		b.mu.Unlock()

		return
	}

	limit := 0.0
	if start, _, inPeriod := currentPeriod(bg, b.now()); inPeriod {
		if l := limitForPeriod(bg, start); l != nil {
			limit = parseAmount(l.Amount)
		}
	}

	var deliveries []Delivery

	for i := range rec.Notifications {
		n := &rec.Notifications[i]
		nn := n.Notification

		state := stateOK
		if breached(nn.NotificationType, nn.ComparisonOperator, nn.ThresholdType, nn.Threshold, calc, limit) {
			state = stateAlarm
		}

		if state != nn.NotificationState {
			n.Notification.NotificationState = state

			if state == stateAlarm {
				deliveries = append(deliveries, alert(rec.AccountID, bg.BudgetName, nn, calc, n.Subscribers))
			}
		}
	}

	runs, actionAlerts := b.fireActionsLocked(rec, calc, limit)
	deliveries = append(deliveries, actionAlerts...)
	notifier := b.notifier
	b.mu.Unlock()

	for _, d := range deliveries {
		if notifier == nil {
			break
		}

		if err := notifier.Notify(ctx, d); err != nil {
			logger.Load(ctx).WarnContext(ctx, "budgets: alert delivery failed", "budget", d.BudgetName, "error", err)
		}
	}

	for _, r := range runs {
		b.finishRun(ctx, r.account, r.key, r.action, r.exec, false, eventSystem)
	}
}

func (b *InMemoryBackend) fireActionsLocked(
	rec *BudgetRecord, calc *CalculatedSpend, limit float64,
) ([]queuedRun, []Delivery) {
	var (
		runs   []queuedRun
		alerts []Delivery
	)

	for _, ar := range b.actionsOfLocked(rec.AccountID, rec.Budget.BudgetName) {
		a := &ar.Action
		if a.Status != statusStandby || a.ActionThreshold == nil ||
			!breached(a.NotificationType, opGreater, a.ActionThreshold.ActionThresholdType,
				a.ActionThreshold.ActionThresholdValue, calc, limit) {
			continue
		}

		n := Notification{
			NotificationType: a.NotificationType, ComparisonOperator: opGreater,
			Threshold: a.ActionThreshold.ActionThresholdValue, ThresholdType: a.ActionThreshold.ActionThresholdType,
		}
		alerts = append(alerts, alert(rec.AccountID, rec.Budget.BudgetName, n, calc, a.Subscribers))

		if a.ApprovalModel == approvalManual {
			a.Status = statusPending
			b.recordLocked(ar, eventSystem, statusPending, "Threshold crossed; awaiting approval")

			continue
		}

		act, exec := b.beginRunLocked(ar, false, eventSystem)
		runs = append(runs, queuedRun{key: ar.Key, account: rec.AccountID, action: act, exec: exec})
	}

	return runs, alerts
}

func alert(account, name string, n Notification, calc *CalculatedSpend, subs []Subscriber) Delivery {
	spend := calc.ActualSpend
	if n.NotificationType == notifForecasted && calc.ForecastedSpend != nil {
		spend = *calc.ForecastedSpend
	}

	subject := fmt.Sprintf("AWS Budgets: %s has exceeded your alert threshold", name)
	if n.NotificationType == notifForecasted {
		subject = fmt.Sprintf("AWS Budgets: %s is forecasted to exceed your alert threshold", name)
	}

	unit := "%"
	if n.ThresholdType == thresholdAbsolute {
		unit = " " + spend.Unit
	}

	msg := fmt.Sprintf("AWS Account %s\n\nYou requested that we alert you when the %s spend associated with your "+
		"%q budget is %s %s%s. The %s spend associated with this budget is %s %s.",
		account, n.NotificationType, name, operatorText(n.ComparisonOperator), formatAmount(n.Threshold), unit,
		n.NotificationType, spend.Amount, spend.Unit)

	return Delivery{
		AccountID: account, BudgetName: name, Subject: subject, Message: msg,
		Subscribers: append([]Subscriber(nil), subs...),
	}
}

func operatorText(op string) string {
	switch op {
	case opGreater:
		return "greater than"
	case opLess:
		return "less than"
	default:
		return "equal to"
	}
}
