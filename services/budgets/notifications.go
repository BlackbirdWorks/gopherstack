package budgets

import (
	"context"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func sameNotification(a, b Notification) bool {
	at, bt := a.ThresholdType, b.ThresholdType
	if at == "" {
		at = thresholdPercentage
	}

	if bt == "" {
		bt = thresholdPercentage
	}

	return a.NotificationType == b.NotificationType && a.ComparisonOperator == b.ComparisonOperator &&
		a.Threshold == b.Threshold && at == bt
}

func notificationIndex(rec *BudgetRecord, n Notification) int {
	return slices.IndexFunc(rec.Notifications, func(r NotificationRecord) bool {
		return sameNotification(r.Notification, n)
	})
}

// notificationLocked validates account/budget/notification and returns the stored entry.
func (b *InMemoryBackend) notificationLocked(
	account, name string, n *Notification,
) (*BudgetRecord, int, error) {
	if err := validateAccount(account); err != nil {
		return nil, 0, err
	}

	if err := validateNotification(n); err != nil {
		return nil, 0, err
	}

	rec, err := b.budgetLocked(account, name)
	if err != nil {
		return nil, 0, err
	}

	i := notificationIndex(rec, *n)
	if i < 0 {
		return nil, 0, fail(ErrNotFound, "notification not found for budget %s", name)
	}

	return rec, i, nil
}

// CreateNotification adds a notification with its first subscribers.
func (b *InMemoryBackend) CreateNotification(
	ctx context.Context, account, name string, n Notification, subs []Subscriber,
) error {
	if err := validateAccount(account); err != nil {
		return err
	}

	if err := validateNotification(&n); err != nil {
		return err
	}

	if err := validateSubscribers(subs); err != nil {
		return err
	}

	b.mu.Lock("CreateNotification")

	rec, err := b.budgetLocked(account, name)
	if err != nil {
		b.mu.Unlock()

		return err
	}

	switch {
	case notificationIndex(rec, n) >= 0:
		err = fail(ErrDuplicateRecord, "notification already exists for budget %s", name)
	case len(rec.Notifications) >= maxNotifications:
		err = fail(ErrCreationLimitExceeded, "a budget can have at most %d notifications", maxNotifications)
	}

	if err != nil {
		b.mu.Unlock()

		return err
	}

	n.NotificationState = stateOK
	rec.Notifications = append(rec.Notifications, NotificationRecord{Notification: n, Subscribers: slices.Clone(subs)})
	key := rec.Key
	b.mu.Unlock()

	b.evaluate(ctx, key)

	return nil
}

// DeleteNotification removes a notification and its subscribers.
func (b *InMemoryBackend) DeleteNotification(account, name string, n Notification) error {
	b.mu.Lock("DeleteNotification")
	defer b.mu.Unlock()

	rec, i, err := b.notificationLocked(account, name, &n)
	if err != nil {
		return err
	}

	rec.Notifications = slices.Delete(rec.Notifications, i, i+1)

	return nil
}

// UpdateNotification replaces a notification's definition, keeping its subscribers.
func (b *InMemoryBackend) UpdateNotification(
	ctx context.Context, account, name string, oldN, newN Notification,
) error {
	if err := validateNotification(&newN); err != nil {
		return err
	}

	b.mu.Lock("UpdateNotification")

	rec, i, err := b.notificationLocked(account, name, &oldN)
	if err != nil {
		b.mu.Unlock()

		return err
	}

	if j := notificationIndex(rec, newN); j >= 0 && j != i {
		b.mu.Unlock()

		return fail(ErrDuplicateRecord, "notification already exists for budget %s", name)
	}

	newN.NotificationState = rec.Notifications[i].Notification.NotificationState
	rec.Notifications[i].Notification = newN
	key := rec.Key
	b.mu.Unlock()

	b.evaluate(ctx, key)

	return nil
}

// ListNotifications pages a budget's notifications.
func (b *InMemoryBackend) ListNotifications(
	ctx context.Context, account, name, token string, maxResults *int32,
) (page.Page[Notification], error) {
	if err := validateAccount(account); err != nil {
		return page.Page[Notification]{}, err
	}

	b.evaluate(ctx, budgetKeyOf(account, name))

	b.mu.RLock("ListNotifications")
	defer b.mu.RUnlock()

	rec, err := b.budgetLocked(account, name)
	if err != nil {
		return page.Page[Notification]{}, err
	}

	all := make([]Notification, len(rec.Notifications))
	for i, r := range rec.Notifications {
		all[i] = r.Notification
	}

	return paginate(all, token, maxResults, maxPageSize)
}

// ListBudgetNotificationsForAccount pages budgets that have notifications.
func (b *InMemoryBackend) ListBudgetNotificationsForAccount(
	ctx context.Context, account, token string, maxResults *int32,
) (page.Page[BudgetNotificationsForAccount], error) {
	if err := validateAccount(account); err != nil {
		return page.Page[BudgetNotificationsForAccount]{}, err
	}

	b.evaluateAccount(ctx, account)

	b.mu.RLock("ListBudgetNotificationsForAccount")
	defer b.mu.RUnlock()

	var all []BudgetNotificationsForAccount

	for _, r := range b.accountBudgetsLocked(account) {
		if len(r.Notifications) == 0 {
			continue
		}

		item := BudgetNotificationsForAccount{BudgetName: r.Budget.BudgetName}
		for _, n := range r.Notifications {
			item.Notifications = append(item.Notifications, n.Notification)
		}

		all = append(all, item)
	}

	return paginate(all, token, maxResults, maxPageSize)
}

// CreateSubscriber adds a recipient to a notification.
func (b *InMemoryBackend) CreateSubscriber(account, name string, n Notification, s Subscriber) error {
	if err := validateSubscriber(s); err != nil {
		return err
	}

	b.mu.Lock("CreateSubscriber")
	defer b.mu.Unlock()

	rec, i, err := b.notificationLocked(account, name, &n)
	if err != nil {
		return err
	}

	subs := rec.Notifications[i].Subscribers
	if slices.Contains(subs, s) {
		return fail(ErrDuplicateRecord, "subscriber %s already exists", s.Address)
	}

	if len(subs) >= maxSubscribers {
		return fail(ErrCreationLimitExceeded, "a notification can have at most %d subscribers", maxSubscribers)
	}

	rec.Notifications[i].Subscribers = append(rec.Notifications[i].Subscribers, s)

	return nil
}

// DeleteSubscriber removes a recipient; the last one cannot be removed.
func (b *InMemoryBackend) DeleteSubscriber(account, name string, n Notification, s Subscriber) error {
	b.mu.Lock("DeleteSubscriber")
	defer b.mu.Unlock()

	rec, i, err := b.notificationLocked(account, name, &n)
	if err != nil {
		return err
	}

	subs := rec.Notifications[i].Subscribers

	j := slices.Index(subs, s)
	if j < 0 {
		return fail(ErrNotFound, "subscriber %s not found", s.Address)
	}

	if len(subs) == 1 {
		return fail(ErrInvalidParameter, "a notification must keep at least one subscriber")
	}

	rec.Notifications[i].Subscribers = slices.Delete(subs, j, j+1)

	return nil
}

// UpdateSubscriber replaces a recipient.
func (b *InMemoryBackend) UpdateSubscriber(account, name string, n Notification, oldS, newS Subscriber) error {
	if err := validateSubscriber(newS); err != nil {
		return err
	}

	b.mu.Lock("UpdateSubscriber")
	defer b.mu.Unlock()

	rec, i, err := b.notificationLocked(account, name, &n)
	if err != nil {
		return err
	}

	subs := rec.Notifications[i].Subscribers

	j := slices.Index(subs, oldS)
	if j < 0 {
		return fail(ErrNotFound, "subscriber %s not found", oldS.Address)
	}

	if k := slices.Index(subs, newS); k >= 0 && k != j {
		return fail(ErrDuplicateRecord, "subscriber %s already exists", newS.Address)
	}

	subs[j] = newS

	return nil
}

// ListSubscribers pages a notification's recipients.
func (b *InMemoryBackend) ListSubscribers(
	account, name string, n Notification, token string, maxResults *int32,
) (page.Page[Subscriber], error) {
	b.mu.RLock("ListSubscribers")
	defer b.mu.RUnlock()

	rec, i, err := b.notificationLocked(account, name, &n)
	if err != nil {
		return page.Page[Subscriber]{}, err
	}

	return paginate(slices.Clone(rec.Notifications[i].Subscribers), token, maxResults, maxPageSize)
}
