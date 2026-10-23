package azureservicebus

import (
	"context"
	"errors"
	"time"
)

const (
	sessionRecheck = time.Second

	// MaxSessionStateBytes is Service Bus's documented session state limit.
	MaxSessionStateBytes = 256 * 1024
)

// sessionRecord is the persisted state of one session of a session entity.
type sessionRecord struct {
	LockedUntil time.Time
	LockOwner   string
	State       []byte
}

func (r *sessionRecord) lockedBy(owner string, now time.Time) bool {
	return r.LockOwner == owner && r.LockedUntil.After(now)
}

func (r *sessionRecord) lockedByOther(owner string, now time.Time) bool {
	return r.LockOwner != "" && r.LockOwner != owner && r.LockedUntil.After(now)
}

// SessionGrant describes an acquired session lock.
type SessionGrant struct {
	LockedUntil time.Time
	SessionID   string
}

// SessionBackend is the session surface the AMQP plane needs.
type SessionBackend interface {
	RequiresSession(ref EntityRef) (bool, error)
	AcceptSessionWait(
		ctx context.Context, ref EntityRef, sessionID, owner string, timeout time.Duration,
	) (SessionGrant, error)
	PeekLockSessionWait(
		ctx context.Context, ref EntityRef, sessionID, owner string, timeout time.Duration,
	) (MessageInfo, error)
	CheckSessionLock(ref EntityRef, sessionID, owner string) error
	RenewSessionLock(ref EntityRef, sessionID, owner string) (time.Time, error)
	GetSessionState(ref EntityRef, sessionID, owner string) ([]byte, error)
	SetSessionState(ref EntityRef, sessionID, owner string, state []byte) error
	ReleaseSession(ref EntityRef, sessionID, owner string)
	SessionOf(ref EntityRef, owner string) (string, bool)
	PeekSession(ref EntityRef, sessionID string, fromSeq int64, maxCount int) ([]MessageInfo, error)
}

var _ SessionBackend = (*InMemoryBackend)(nil)

func (cfg EntityConfig) checkSend(msg NewMessage) error {
	if cfg.RequiresSession && msg.SessionID == "" {
		return ErrSessionIDRequired
	}

	return nil
}

func (b *InMemoryBackend) checkSendLocked(ref EntityRef, msg NewMessage) error {
	if ref.Topic != "" && ref.Subscription == "" {
		if t := b.topics[ref.Topic]; t != nil {
			for _, sub := range t.Subscriptions {
				if err := sub.Config.checkSend(msg); err != nil {
					return err
				}
			}
		}

		return nil
	}

	if _, cfg, err := b.resolveEntityLocked(ref); err == nil {
		return cfg.checkSend(msg)
	}

	return nil
}

// RequiresSession reports whether the addressed queue or subscription is
// session-enabled.
func (b *InMemoryBackend) RequiresSession(ref EntityRef) (bool, error) {
	b.mu.RLock("RequiresSession")
	defer b.mu.RUnlock()

	if ref.Topic != "" && ref.Subscription == "" {
		return false, nil
	}

	_, cfg, err := b.resolveEntityLocked(ref)

	return cfg.RequiresSession, err
}

func (b *InMemoryBackend) sessionEntityLocked(ref EntityRef) (*messageQueue, EntityConfig, error) {
	mq, cfg, err := b.resolveEntityLocked(ref)
	if err != nil {
		return nil, cfg, err
	}

	if !cfg.RequiresSession {
		return nil, cfg, ErrNotSessionEntity
	}

	return mq, cfg, nil
}

func (mq *messageQueue) grantLocked(id, owner string, until time.Time) SessionGrant {
	if mq.Sessions == nil {
		mq.Sessions = make(map[string]*sessionRecord)
	}

	rec := mq.Sessions[id]
	if rec == nil {
		rec = &sessionRecord{}
		mq.Sessions[id] = rec
	}

	rec.LockOwner, rec.LockedUntil = owner, until

	return SessionGrant{SessionID: id, LockedUntil: until}
}

// nextSessionLocked picks the session of the oldest deliverable message whose
// session is not locked by anyone else.
func (mq *messageQueue) nextSessionLocked(now time.Time) (string, bool) {
	for _, m := range mq.Messages {
		if m.SessionID == "" || m.Deferred || m.isExpired(now) || m.isLocked(now) {
			continue
		}

		if rec := mq.Sessions[m.SessionID]; rec != nil && rec.LockOwner != "" && rec.LockedUntil.After(now) {
			continue
		}

		return m.SessionID, true
	}

	return "", false
}

func (b *InMemoryBackend) acceptSessionOnce(
	ref EntityRef, sessionID, owner string,
) (SessionGrant, chan struct{}, error) {
	b.mu.Lock("AcceptSession")
	defer b.mu.Unlock()

	mq, cfg, err := b.sessionEntityLocked(ref)
	if err != nil {
		return SessionGrant{}, nil, err
	}

	now := b.now()
	until := now.Add(cfg.lockDuration())

	if sessionID != "" {
		if rec := mq.Sessions[sessionID]; rec != nil && rec.lockedByOther(owner, now) {
			return SessionGrant{}, nil, ErrSessionCannotBeLocked
		}

		return mq.grantLocked(sessionID, owner, until), nil, nil
	}

	if id, ok := mq.nextSessionLocked(now); ok {
		return mq.grantLocked(id, owner, until), nil, nil
	}

	mq.ensureNotifyLocked()

	return SessionGrant{}, mq.notify, ErrNoSessionAvailable
}

// AcceptSessionWait locks sessionID for owner. An empty sessionID selects the
// next session holding deliverable messages, waiting up to timeout for one;
// ErrNoSessionAvailable is returned when none appears. A specific session held
// by another owner fails at once with ErrSessionCannotBeLocked.
func (b *InMemoryBackend) AcceptSessionWait(
	ctx context.Context, ref EntityRef, sessionID, owner string, timeout time.Duration,
) (SessionGrant, error) {
	return pollWait(ctx, timeout, ErrNoSessionAvailable, func() (SessionGrant, chan struct{}, error) {
		return b.acceptSessionOnce(ref, sessionID, owner)
	})
}

func (b *InMemoryBackend) peekLockSessionOnce(
	ref EntityRef, sessionID, owner string,
) (MessageInfo, chan struct{}, error) {
	b.mu.Lock("PeekLockSession")
	defer b.mu.Unlock()

	mq, cfg, err := b.sessionEntityLocked(ref)
	if err != nil {
		return MessageInfo{}, nil, err
	}

	now := b.now()

	if rec := mq.Sessions[sessionID]; rec == nil || !rec.lockedBy(owner, now) {
		return MessageInfo{}, nil, ErrSessionLockLost
	}

	for _, msg := range mq.Messages {
		if msg.SessionID != sessionID || msg.Deferred || msg.isExpired(now) || msg.isLocked(now) {
			continue
		}

		msg.DeliveryCount++
		msg.LockToken = b.idFunc()
		msg.LockedUntil = now.Add(cfg.lockDuration())

		return msg.info(), nil, nil
	}

	mq.ensureNotifyLocked()

	return MessageInfo{}, mq.notify, ErrMessageNotFound
}

// PeekLockSessionWait locks the oldest deliverable message of the session
// owner holds, waiting up to timeout. ErrSessionLockLost means the session
// lock expired or moved.
func (b *InMemoryBackend) PeekLockSessionWait(
	ctx context.Context, ref EntityRef, sessionID, owner string, timeout time.Duration,
) (MessageInfo, error) {
	return pollWait(ctx, timeout, ErrMessageNotFound, func() (MessageInfo, chan struct{}, error) {
		return b.peekLockSessionOnce(ref, sessionID, owner)
	})
}

// pollWait retries once until it stops returning retry, waiting on the
// returned notify channel with a one second recheck backstop.
func pollWait[T any](
	ctx context.Context, timeout time.Duration, retry error, once func() (T, chan struct{}, error),
) (T, error) {
	var zero T

	val, notifyCh, err := once()
	if !errors.Is(err, retry) || timeout <= 0 {
		return val, err
	}

	deadline := time.Now().Add(timeout)
	timer := time.NewTimer(min(timeout, sessionRecheck))

	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return zero, retry
		case <-notifyCh:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}

		if !time.Now().Before(deadline) {
			return zero, retry
		}

		val, notifyCh, err = once()
		if !errors.Is(err, retry) {
			return val, err
		}

		timer.Reset(min(time.Until(deadline), sessionRecheck))
	}
}

func (b *InMemoryBackend) heldSessionLocked(
	ref EntityRef, sessionID, owner string,
) (*messageQueue, *sessionRecord, error) {
	mq, _, err := b.sessionEntityLocked(ref)
	if err != nil {
		return nil, nil, err
	}

	rec := mq.Sessions[sessionID]
	if rec == nil || !rec.lockedBy(owner, b.now()) {
		return nil, nil, ErrSessionLockLost
	}

	return mq, rec, nil
}

// CheckSessionLock returns ErrSessionLockLost unless owner holds sessionID.
func (b *InMemoryBackend) CheckSessionLock(ref EntityRef, sessionID, owner string) error {
	b.mu.RLock("CheckSessionLock")
	defer b.mu.RUnlock()

	_, _, err := b.heldSessionLocked(ref, sessionID, owner)

	return err
}

// RenewSessionLock extends owner's lock on sessionID and returns its expiry.
func (b *InMemoryBackend) RenewSessionLock(ref EntityRef, sessionID, owner string) (time.Time, error) {
	b.mu.Lock("RenewSessionLock")
	defer b.mu.Unlock()

	mq, rec, err := b.heldSessionLocked(ref, sessionID, owner)
	if err != nil {
		return time.Time{}, err
	}

	_, cfg, _ := b.resolveEntityLocked(ref)
	rec.LockedUntil = b.now().Add(cfg.lockDuration())
	mq.broadcastLocked()

	return rec.LockedUntil, nil
}

// GetSessionState returns the session's state, nil when unset.
func (b *InMemoryBackend) GetSessionState(ref EntityRef, sessionID, owner string) ([]byte, error) {
	b.mu.RLock("GetSessionState")
	defer b.mu.RUnlock()

	_, rec, err := b.heldSessionLocked(ref, sessionID, owner)
	if err != nil {
		return nil, err
	}

	if rec.State == nil {
		return nil, nil
	}

	return append([]byte(nil), rec.State...), nil
}

// SetSessionState stores the session's state; nil clears it.
func (b *InMemoryBackend) SetSessionState(ref EntityRef, sessionID, owner string, state []byte) error {
	b.mu.Lock("SetSessionState")
	defer b.mu.Unlock()

	_, rec, err := b.heldSessionLocked(ref, sessionID, owner)
	if err != nil {
		return err
	}

	if state == nil {
		rec.State = nil
	} else {
		rec.State = append([]byte{}, state...)
	}

	return nil
}

// ReleaseSession drops owner's lock on sessionID, if it still holds it.
func (b *InMemoryBackend) ReleaseSession(ref EntityRef, sessionID, owner string) {
	b.mu.Lock("ReleaseSession")
	defer b.mu.Unlock()

	mq, _, err := b.sessionEntityLocked(ref)
	if err != nil {
		return
	}

	rec := mq.Sessions[sessionID]
	if rec == nil || rec.LockOwner != owner {
		return
	}

	rec.LockOwner, rec.LockedUntil = "", time.Time{}
	mq.dropIdleSessionLocked(sessionID, rec)
	mq.broadcastLocked()
}

func (mq *messageQueue) dropIdleSessionLocked(id string, rec *sessionRecord) {
	if rec.LockOwner == "" && rec.State == nil {
		delete(mq.Sessions, id)
	}
}

// SessionOf returns the session owner currently holds on ref.
func (b *InMemoryBackend) SessionOf(ref EntityRef, owner string) (string, bool) {
	b.mu.RLock("SessionOf")
	defer b.mu.RUnlock()

	mq, _, err := b.sessionEntityLocked(ref)
	if err != nil || owner == "" {
		return "", false
	}

	now := b.now()

	for id, rec := range mq.Sessions {
		if rec.lockedBy(owner, now) {
			return id, true
		}
	}

	return "", false
}

// PeekSession is Peek restricted to one session.
func (b *InMemoryBackend) PeekSession(
	ref EntityRef, sessionID string, fromSeq int64, maxCount int,
) ([]MessageInfo, error) {
	all, err := b.Peek(ref, false, fromSeq, maxPeekCount)
	if err != nil {
		return nil, err
	}

	out := make([]MessageInfo, 0, min(len(all), max(maxCount, 0)))

	for _, m := range all {
		if len(out) >= maxCount {
			break
		}

		if m.SessionID == sessionID {
			out = append(out, m)
		}
	}

	return out, nil
}

// sweepSessionsLocked clears expired session locks and idle records.
func (mq *messageQueue) sweepSessionsLocked(now time.Time) bool {
	changed := false

	for id, rec := range mq.Sessions {
		if rec.LockOwner != "" && !rec.LockedUntil.After(now) {
			rec.LockOwner, rec.LockedUntil = "", time.Time{}
			changed = true
		}

		mq.dropIdleSessionLocked(id, rec)
	}

	return changed
}
