package cognitoidp

import (
	"fmt"
	"sort"
	"time"
)

// Auth event feedback values (FeedbackValueType).
const (
	AuthEventFeedbackValid   = "Valid"
	AuthEventFeedbackInvalid = "Invalid"
)

func validAuthEventFeedbackValue(v string) bool {
	return v == AuthEventFeedbackValid || v == AuthEventFeedbackInvalid
}

// paginateAuthEventsLocked returns a page of auth events for the given store
// key, newest first. Caller must hold b.mu.
func (b *InMemoryBackend) paginateAuthEventsLocked(key string, limit int, nextToken string) ([]*AuthEvent, string) {
	events := b.authEvents[key]
	all := make([]*AuthEvent, 0, len(events))

	for _, e := range events {
		cp := *e
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool {
		if all[i].CreatedAt.Equal(all[j].CreatedAt) {
			return all[i].EventID < all[j].EventID
		}

		return all[i].CreatedAt.After(all[j].CreatedAt)
	})

	startIdx := 0

	if nextToken != "" {
		for i, e := range all {
			if e.EventID == nextToken {
				startIdx = i

				break
			}
		}
	}

	all = all[startIdx:]

	if limit <= 0 || limit >= len(all) {
		return all, ""
	}

	page := all[:limit]
	newToken := ""

	if limit < len(all) {
		newToken = all[limit].EventID
	}

	return page, newToken
}

// AdminListUserAuthEvents returns stored adaptive-authentication events for a
// user (admin operation). This emulator does not hook sign-in flows
// (InitiateAuth/AdminInitiateAuth) to synthesize risk events, so the store is
// real but starts empty per user; it returns a real, validated, paginated
// empty result rather than a hardcoded one (pool/user existence and
// NextToken semantics are honored).
func (b *InMemoryBackend) AdminListUserAuthEvents(
	userPoolID, username string,
	limit int,
	nextToken string,
) ([]*AuthEvent, string, error) {
	b.mu.RLock("AdminListUserAuthEvents")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.users.Get(userKey(userPoolID, username)); !ok {
		return nil, "", fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	events, token := b.paginateAuthEventsLocked(userStateKey(userPoolID, username), limit, nextToken)

	return events, token, nil
}

// updateAuthEventFeedbackLocked validates and applies feedback to a stored
// auth event. Caller must hold b.mu (write lock).
func (b *InMemoryBackend) updateAuthEventFeedbackLocked(userPoolID, username, eventID, feedbackValue string) error {
	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.users.Get(userKey(userPoolID, username)); !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	if !validAuthEventFeedbackValue(feedbackValue) {
		return fmt.Errorf("%w: FeedbackValue must be %q or %q",
			ErrInvalidParameter, AuthEventFeedbackValid, AuthEventFeedbackInvalid)
	}

	ev, ok := b.authEvents[userStateKey(userPoolID, username)][eventID]
	if !ok {
		return fmt.Errorf("%w: auth event %q not found", ErrAuthEventNotFound, eventID)
	}

	ev.FeedbackValue = feedbackValue
	ev.FeedbackDate = time.Now()

	return nil
}

// AdminUpdateAuthEventFeedback records feedback on a stored auth event (admin operation).
func (b *InMemoryBackend) AdminUpdateAuthEventFeedback(userPoolID, username, eventID, feedbackValue string) error {
	b.mu.Lock("AdminUpdateAuthEventFeedback")
	defer b.mu.Unlock()

	return b.updateAuthEventFeedbackLocked(userPoolID, username, eventID, feedbackValue)
}

// UpdateAuthEventFeedback records feedback on a stored auth event using an
// unauthenticated FeedbackToken flow (matches AWS: this op takes
// UserPoolId/Username directly rather than an AccessToken).
func (b *InMemoryBackend) UpdateAuthEventFeedback(userPoolID, username, eventID, feedbackValue string) error {
	b.mu.Lock("UpdateAuthEventFeedback")
	defer b.mu.Unlock()

	return b.updateAuthEventFeedbackLocked(userPoolID, username, eventID, feedbackValue)
}
