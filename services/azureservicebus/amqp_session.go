package azureservicebus

import (
	"context"
	"errors"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/amqp1"
)

const (
	sessionFilterKey    = "com.microsoft:session-filter"
	lockedUntilProp     = "com.microsoft:locked-until-utc"
	acceptTimeoutProp   = "com.microsoft:timeout"
	condSessionLockLost = amqp1.Symbol("com.microsoft:session-lock-lost")
	condSessionNoLock   = amqp1.Symbol("com.microsoft:session-cannot-be-locked")
	condTimeout         = amqp1.Symbol("com.microsoft:timeout")

	defaultAcceptWait = time.Minute
	dotNetEpochTicks  = int64(621355968000000000)
	ticksPerMilli     = int64(10000)
)

const sessionFilterDescriptor = uint64(0x00000137000000C)

// sessionFilter is a parsed com.microsoft:session-filter source filter.
type sessionFilter struct {
	descriptor any
	id         string
}

func sessionFilterOf(src *amqp1.Source) (sessionFilter, bool) {
	if src == nil {
		return sessionFilter{}, false
	}

	for k, v := range src.Filter {
		if amqpKey(k) != sessionFilterKey {
			continue
		}

		sf := sessionFilter{descriptor: sessionFilterDescriptor}
		if d, ok := v.(amqp1.Described); ok {
			sf.descriptor = d.Descriptor
			sf.id, _ = d.Value.(string)
		}

		return sf, true
	}

	return sessionFilter{}, false
}

func acceptWait(a *amqp1.Attach, sf sessionFilter) time.Duration {
	if sf.id != "" {
		return 0
	}

	ms := int64Arg(a.Properties[amqp1.Symbol(acceptTimeoutProp)])
	if ms <= 0 {
		return defaultAcceptWait
	}

	return min(time.Duration(ms)*time.Millisecond, defaultAcceptWait)
}

func (c *amqpConn) attachSessionSender(
	a *amqp1.Attach, addr amqpAddr, sf sessionFilter, peekLock bool,
) (amqp1.SenderEndpoint, *amqp1.Error) {
	req, err := c.b.RequiresSession(addr.ref)
	if err != nil || addr.dlq || !req {
		return nil, &amqp1.Error{Condition: amqp1.CondNotAllowed, Description: ErrNotSessionEntity.Error()}
	}

	return &sessionSender{
		entitySender: &entitySender{b: c.b, ref: addr.ref, peekLock: peekLock},
		owner:        a.Name,
		want:         sf,
		wait:         acceptWait(a, sf),
	}, nil
}

// sessionSender feeds a session receiver from the one session it has locked.
type sessionSender struct {
	*entitySender
	owner string
	want  sessionFilter
	grant SessionGrant
	wait  time.Duration
	held  bool
}

func sessionErr(err error) *amqp1.Error {
	switch {
	case errors.Is(err, ErrSessionCannotBeLocked):
		return &amqp1.Error{Condition: condSessionNoLock, Description: err.Error()}
	case errors.Is(err, ErrNoSessionAvailable):
		return &amqp1.Error{Condition: condTimeout, Description: "no session is available"}
	case errors.Is(err, ErrSessionLockLost):
		return &amqp1.Error{Condition: condSessionLockLost, Description: err.Error()}
	case errors.Is(err, ErrNotSessionEntity):
		return &amqp1.Error{Condition: amqp1.CondNotAllowed, Description: err.Error()}
	}

	return &amqp1.Error{Condition: amqp1.CondNotFound, Description: err.Error()}
}

// Ready acquires the session lock without blocking the connection loop.
func (s *sessionSender) Ready(ctx context.Context) *amqp1.Error {
	grant, err := s.b.AcceptSessionWait(ctx, s.ref, s.want.id, s.owner, s.wait)
	if err != nil {
		return sessionErr(err)
	}

	s.grant, s.held = grant, true

	return nil
}

// ReplyAttach reports the locked session and its lock expiry.
func (s *sessionSender) ReplyAttach(reply *amqp1.Attach) {
	src := *reply.Source
	src.Filter = amqp1.Map{
		amqp1.Symbol(sessionFilterKey): amqp1.Described{Descriptor: s.want.descriptor, Value: s.grant.SessionID},
	}
	reply.Source = &src
	reply.Properties = amqp1.Map{amqp1.Symbol(lockedUntilProp): ticksOf(s.grant.LockedUntil)}
}

func ticksOf(t time.Time) int64 { return t.UnixMilli()*ticksPerMilli + dotNetEpochTicks }

func (s *sessionSender) Next(ctx context.Context) (*amqp1.Outgoing, error) {
	for {
		info, err := s.b.PeekLockSessionWait(ctx, s.ref, s.grant.SessionID, s.owner, amqpPollWait)

		switch {
		case err == nil:
			if out, ok := s.outgoing(info); ok {
				return out, nil
			}
		case ctx.Err() != nil:
			return nil, ctx.Err()
		case !errors.Is(err, ErrMessageNotFound):
			return nil, sessionErr(err)
		}
	}
}

// SettleError detaches the link when the session lock is gone.
func (s *sessionSender) SettleError(*amqp1.Outgoing) *amqp1.Error {
	if err := s.b.CheckSessionLock(s.ref, s.grant.SessionID, s.owner); err != nil {
		return sessionErr(err)
	}

	return nil
}

// Close releases the session lock.
func (s *sessionSender) Close() {
	if s.held {
		s.b.ReleaseSession(s.ref, s.grant.SessionID, s.owner)
	}
}
