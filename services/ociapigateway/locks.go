package ociapigateway

import (
	"net/http"
	"time"
)

// Resource lock types and the lockable resource kinds.
const (
	lockFull   = "FULL"
	lockDelete = "DELETE"

	lockOpsPerKind = 2

	kindGateway     = "Gateway"
	kindDeployment  = "Deployment"
	kindCertificate = "Certificate"
	kindAPI         = "Api"
	kindSdk         = "Sdk"
	kindSubscriber  = "Subscriber"
	kindUsagePlan   = "UsagePlan"
)

// Lock is a resource lock as the SDK's ResourceLock model reports it.
type Lock struct {
	TimeCreated       time.Time `json:"timeCreated"`
	Type              string    `json:"type"`
	Message           string    `json:"message,omitempty"`
	RelatedResourceID string    `json:"relatedResourceId,omitempty"`
}

type lockIn struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

func validLockType(t string) bool { return t == lockFull || t == lockDelete }

func newLocks(in []lockIn, now time.Time) ([]Lock, *APIError) {
	var out []Lock

	for _, l := range in {
		if !validLockType(l.Type) {
			return nil, errParam("lock type must be FULL or DELETE")
		}

		out = append(out, Lock{Type: l.Type, Message: l.Message, TimeCreated: now})
	}

	return out, nil
}

func lockedErr(l Lock) *APIError {
	msg := "the resource is locked (" + l.Type + ")"
	if l.Message != "" {
		msg += ": " + l.Message
	}

	return newErr(http.StatusConflict, "Conflict", msg)
}

// checkLocked refuses a modification (FULL locks) or a deletion (FULL and DELETE locks).
func checkLocked(locks []Lock, deleting bool) *APIError {
	for _, l := range locks {
		if l.Type == lockFull || (deleting && l.Type == lockDelete) {
			return lockedErr(l)
		}
	}

	return nil
}

// lockTarget points at the lock-relevant fields of one resource; view renders its wire form.
type lockTarget struct {
	locks   *[]Lock
	etag    *string
	updated *time.Time
	view    func() any
}

func (b *InMemoryBackend) lockTargetFor(kind, id string) (lockTarget, *APIError) {
	switch kind {
	case kindGateway:
		if g, ok := b.gateways[id]; ok {
			return lockTarget{&g.Locks, &g.Etag, &g.TimeUpdated, func() any { return gatewayView(g) }}, nil
		}
	case kindDeployment:
		if d, ok := b.deployments[id]; ok {
			return lockTarget{&d.Locks, &d.Etag, &d.TimeUpdated, func() any { return b.deploymentView(d, true) }}, nil
		}
	case kindCertificate:
		if c, ok := b.certificates[id]; ok {
			return lockTarget{&c.Locks, &c.Etag, &c.TimeUpdated, func() any { return certificateView(c, true) }}, nil
		}
	case kindAPI:
		if a, ok := b.apis[id]; ok {
			return lockTarget{&a.Locks, &a.Etag, &a.TimeUpdated, func() any { return apiView(a) }}, nil
		}
	case kindSdk:
		if s, ok := b.sdks[id]; ok {
			return lockTarget{&s.Locks, &s.Etag, &s.TimeUpdated, func() any { return sdkView(s) }}, nil
		}
	case kindSubscriber:
		if s, ok := b.subscribers[id]; ok {
			return lockTarget{&s.Locks, &s.Etag, &s.TimeUpdated, func() any { return subscriberView(s, true) }}, nil
		}
	case kindUsagePlan:
		if p, ok := b.usagePlans[id]; ok {
			return lockTarget{&p.Locks, &p.Etag, &p.TimeUpdated, func() any { return usagePlanView(p) }}, nil
		}
	}

	return lockTarget{}, errNotFound(id)
}

// AddLock appends a lock to a resource and returns the resource with its new etag.
func (b *InMemoryBackend) AddLock(kind, id, ifMatch string, in lockIn) (any, string, error) {
	b.mu.Lock("AddLock")
	defer b.mu.Unlock()

	t, err := b.lockTargetFor(kind, id)
	if err != nil {
		return nil, "", err
	}

	if err = checkEtag(*t.etag, ifMatch); err != nil {
		return nil, "", err
	}

	locks, lerr := newLocks([]lockIn{in}, b.nowUTC())
	if lerr != nil {
		return nil, "", lerr
	}

	*t.locks = append(*t.locks, locks...)
	*t.updated, *t.etag = b.nowUTC(), newEtag()

	return t.view(), *t.etag, nil
}

// RemoveLock removes every lock of the given type and returns the resource with its new etag.
func (b *InMemoryBackend) RemoveLock(kind, id, ifMatch, lockType string) (any, string, error) {
	b.mu.Lock("RemoveLock")
	defer b.mu.Unlock()

	t, err := b.lockTargetFor(kind, id)
	if err != nil {
		return nil, "", err
	}

	if err = checkEtag(*t.etag, ifMatch); err != nil {
		return nil, "", err
	}

	if !validLockType(lockType) {
		return nil, "", errParam("lock type must be FULL or DELETE")
	}

	kept := (*t.locks)[:0:0]
	for _, l := range *t.locks {
		if l.Type != lockType {
			kept = append(kept, l)
		}
	}

	if len(kept) == len(*t.locks) {
		return nil, "", errNotFound(id + " has no " + lockType + " lock")
	}

	*t.locks = kept
	*t.updated, *t.etag = b.nowUTC(), newEtag()

	return t.view(), *t.etag, nil
}

func lockHandler(kind, param string, add bool) opFunc {
	return func(h *Handler, q *req) error {
		var in lockIn
		if err := q.decode(&in); err != nil {
			return err
		}

		var (
			out  any
			etag string
			err  error
		)

		if add {
			out, etag, err = h.Backend.AddLock(kind, q.param(param), q.ifMatch(), in)
		} else {
			out, etag, err = h.Backend.RemoveLock(kind, q.param(param), q.ifMatch(), in.Type)
		}

		return reply(q, out, etag, err)
	}
}
