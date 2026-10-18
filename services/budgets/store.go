package budgets

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	serviceName      = "budgets"
	homeRegion       = "us-east-1"
	evaluateInterval = time.Minute
)

// SpendSource supplies spend for a window [start, end). ok is false when no cost data exists.
type SpendSource interface {
	Spend(start, end time.Time, filters map[string][]string, metric string) (float64, bool)
}

// Delivery is one alert to send to subscribers.
type Delivery struct {
	AccountID   string
	BudgetName  string
	Subject     string
	Message     string
	Subscribers []Subscriber
}

// Notifier delivers alerts to SNS topics and email addresses.
type Notifier interface {
	Notify(ctx context.Context, d Delivery) error
}

// ActionExecutor applies (or reverses) a budget action against IAM, Organizations or SSM.
type ActionExecutor interface {
	Run(ctx context.Context, accountID string, a Action, reverse bool) error
}

// NotificationRecord is a notification and its subscribers.
type NotificationRecord struct {
	Notification Notification `json:"notification"`
	Subscribers  []Subscriber `json:"subscribers"`
}

// BudgetRecord is a stored budget with its notifications and tags.
type BudgetRecord struct {
	Tags          map[string]string    `json:"tags,omitempty"`
	Key           string               `json:"key"`
	AccountID     string               `json:"accountID"`
	Budget        Budget               `json:"budget"`
	Notifications []NotificationRecord `json:"notifications,omitempty"`
}

// ActionRecord is a stored budget action with its history and tags.
type ActionRecord struct {
	Tags      map[string]string `json:"tags,omitempty"`
	Key       string            `json:"key"`
	AccountID string            `json:"accountID"`
	Action    Action            `json:"action"`
	History   []ActionHistory   `json:"history,omitempty"`
}

// InMemoryBackend is the Budgets state; one coarse lock guards every table.
type InMemoryBackend struct {
	budgets   *store.Table[BudgetRecord]
	actions   *store.Table[ActionRecord]
	registry  *store.Registry
	mu        *lockmetrics.RWMutex
	now       func() time.Time
	spend     SpendSource
	notifier  Notifier
	executor  ActionExecutor
	accountID string
	region    string
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	if region == "" {
		region = homeRegion
	}

	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		mu:        lockmetrics.New(serviceName),
		now:       func() time.Time { return time.Now().UTC() },
		accountID: accountID,
		region:    region,
	}

	registerAllTables(b)

	return b
}

// Region returns the home region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the default account.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// SetSpendSource wires the cost data used for CalculatedSpend and alerting.
func (b *InMemoryBackend) SetSpendSource(s SpendSource) {
	b.mu.Lock("SetSpendSource")
	defer b.mu.Unlock()

	b.spend = s
}

// SetNotifier wires alert delivery.
func (b *InMemoryBackend) SetNotifier(n Notifier) {
	b.mu.Lock("SetNotifier")
	defer b.mu.Unlock()

	b.notifier = n
}

// SetActionExecutor wires action execution.
func (b *InMemoryBackend) SetActionExecutor(e ActionExecutor) {
	b.mu.Lock("SetActionExecutor")
	defer b.mu.Unlock()

	b.executor = e
}

// Reset clears every table.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
}

func budgetKeyOf(account, name string) string { return account + "/" + name }

func actionKeyOf(account, name, id string) string { return account + "/" + name + "/action/" + id }

func (b *InMemoryBackend) budgetLocked(account, name string) (*BudgetRecord, error) {
	r, ok := b.budgets.Get(budgetKeyOf(account, name))
	if !ok {
		return nil, fail(ErrNotFound, "budget %s not found", name)
	}

	return r, nil
}

func (b *InMemoryBackend) actionLocked(account, name, id string) (*ActionRecord, error) {
	r, ok := b.actions.Get(actionKeyOf(account, name, id))
	if !ok {
		return nil, fail(ErrNotFound, "action %s not found for budget %s", id, name)
	}

	return r, nil
}
