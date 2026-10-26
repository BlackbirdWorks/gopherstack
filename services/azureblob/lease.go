package azureblob

import (
	"net/http"
	"time"

	"github.com/google/uuid"
)

// Lease states and statuses as reported in x-ms-lease-state / x-ms-lease-status.
const (
	LeaseStateAvailable = "available"
	LeaseStateLeased    = "leased"
	LeaseStateExpired   = "expired"
	LeaseStateBreaking  = "breaking"
	LeaseStateBroken    = "broken"

	LeaseStatusLocked   = "locked"
	LeaseStatusUnlocked = "unlocked"

	LeaseDurationInfinite = "infinite"
	LeaseDurationFixed    = "fixed"
)

// Values of the x-ms-lease-action header.
const (
	LeaseActionAcquire = "acquire"
	LeaseActionRenew   = "renew"
	LeaseActionChange  = "change"
	LeaseActionRelease = "release"
	LeaseActionBreak   = "break"
)

// LeaseInfinite is the x-ms-lease-duration value of an infinite lease.
const LeaseInfinite = -1

// LeaseProps are the lease fields reported in blob and container properties.
type LeaseProps struct {
	State    string
	Status   string
	Duration string
}

// LeaseRequest is one Lease Blob / Lease Container call.
type LeaseRequest struct {
	Action     string
	ID         string
	ProposedID string
	Cond       Precond
	// Duration is seconds, or LeaseInfinite; used by acquire.
	Duration int
	// BreakPeriod is seconds; used by break when HasBreakPeriod.
	BreakPeriod    int
	HasBreakPeriod bool
}

// LeaseResult is the outcome of a lease call.
type LeaseResult struct {
	LastModified time.Time
	ID           string
	ETag         string
	// BreakRemaining is x-ms-lease-time, whole seconds left in the break period.
	BreakRemaining int
}

// leaseRecord is the persisted lease of a blob or container. Its state is
// derived from the clock, never stored.
type leaseRecord struct {
	ExpiresAt time.Time
	BreakAt   time.Time
	ID        string
	Duration  int
	Infinite  bool
}

func (l *leaseRecord) state(now time.Time) string {
	switch {
	case l == nil:
		return LeaseStateAvailable
	case !l.BreakAt.IsZero():
		if now.Before(l.BreakAt) {
			return LeaseStateBreaking
		}

		return LeaseStateBroken
	case !l.Infinite && !now.Before(l.ExpiresAt):
		return LeaseStateExpired
	default:
		return LeaseStateLeased
	}
}

func (l *leaseRecord) props(now time.Time) LeaseProps {
	st := l.state(now)
	p := LeaseProps{State: st, Status: LeaseStatusUnlocked}

	if st == LeaseStateLeased || st == LeaseStateBreaking {
		p.Status = LeaseStatusLocked
		p.Duration = LeaseDurationFixed

		if l.Infinite {
			p.Duration = LeaseDurationInfinite
		}
	}

	return p
}

func (l *leaseRecord) active(now time.Time) bool {
	st := l.state(now)

	return st == LeaseStateLeased || st == LeaseStateBreaking
}

func leaseErr(status int, code, msg string) *StorageError { return storageErr(status, code, msg) }

// gate checks a request's x-ms-lease-id against the lease. kind is "Blob" or
// "Container". With require, an active lease demands a matching id.
func (l *leaseRecord) gate(now time.Time, id string, require bool, kind string) error {
	if l.active(now) {
		switch {
		case id == "" && require:
			return leaseErr(http.StatusPreconditionFailed, "LeaseIdMissing",
				"There is currently a lease on the "+kind+" and no lease ID was specified in the request.")
		case id != "" && id != l.ID:
			return leaseErr(http.StatusPreconditionFailed, "LeaseIdMismatchWith"+kind+"Operation",
				"The lease ID specified did not match the lease ID for the "+kind+".")
		}

		return nil
	}

	switch {
	case id == "":
		return nil
	case l == nil:
		return leaseErr(http.StatusPreconditionFailed, "LeaseNotPresentWith"+kind+"Operation",
			"There is currently no lease on the "+kind+".")
	case id == l.ID:
		return leaseErr(http.StatusPreconditionFailed, "LeaseLost",
			"A lease ID was specified, but the lease for the "+kind+" has expired.")
	default:
		return leaseErr(http.StatusPreconditionFailed, "LeaseIdMismatchWith"+kind+"Operation",
			"The lease ID specified did not match the lease ID for the "+kind+".")
	}
}

func errLeaseNotPresent() *StorageError {
	return leaseErr(http.StatusConflict, "LeaseNotPresentWithLeaseOperation",
		"There is currently no lease on the resource.")
}

func errLeaseMismatch() *StorageError {
	return leaseErr(http.StatusConflict, "LeaseIdMismatchWithLeaseOperation",
		"The lease ID specified did not match the lease ID for the resource.")
}

// applyLease runs one lease action against slot (a blob's or container's
// lease pointer).
func applyLease(slot **leaseRecord, now time.Time, req LeaseRequest) (LeaseResult, error) {
	l := *slot
	st := l.state(now)

	switch req.Action {
	case LeaseActionAcquire:
		return acquireLease(slot, now, req, st)
	case LeaseActionBreak:
		return breakLease(slot, now, req, st)
	}

	if st == LeaseStateAvailable {
		return LeaseResult{}, errLeaseNotPresent()
	}

	switch req.Action {
	case LeaseActionRenew:
		return renewLease(l, now, req, st)
	case LeaseActionChange:
		return changeLease(l, req, st)
	default:
		if req.ID != l.ID {
			return LeaseResult{}, errLeaseMismatch()
		}

		*slot = nil

		return LeaseResult{ID: req.ID}, nil
	}
}

func acquireLease(slot **leaseRecord, now time.Time, req LeaseRequest, st string) (LeaseResult, error) {
	l := *slot

	if st == LeaseStateLeased || st == LeaseStateBreaking {
		if req.ProposedID != l.ID {
			return LeaseResult{}, leaseErr(http.StatusConflict, "LeaseAlreadyPresent",
				"There is already a lease present.")
		}

		if st == LeaseStateBreaking {
			return LeaseResult{}, leaseErr(
				http.StatusConflict,
				"LeaseIsBreakingAndCannotBeAcquired",
				"The lease ID matched, but the lease is currently in breaking state and cannot be acquired until it is broken.",
			)
		}
	}

	id := req.ProposedID
	if id == "" {
		id = uuid.NewString()
	}

	rec := &leaseRecord{ID: id, Infinite: req.Duration == LeaseInfinite, Duration: req.Duration}
	if !rec.Infinite {
		rec.ExpiresAt = now.Add(time.Duration(req.Duration) * time.Second)
	}

	*slot = rec

	return LeaseResult{ID: id}, nil
}

func renewLease(l *leaseRecord, now time.Time, req LeaseRequest, st string) (LeaseResult, error) {
	if req.ID != l.ID {
		return LeaseResult{}, errLeaseMismatch()
	}

	if st == LeaseStateBreaking || st == LeaseStateBroken {
		return LeaseResult{}, leaseErr(http.StatusConflict, "LeaseIsBrokenAndCannotBeRenewed",
			"The lease ID matched, but the lease has been broken explicitly and cannot be renewed.")
	}

	if !l.Infinite {
		l.ExpiresAt = now.Add(time.Duration(l.Duration) * time.Second)
	}

	return LeaseResult{ID: l.ID}, nil
}

func changeLease(l *leaseRecord, req LeaseRequest, st string) (LeaseResult, error) {
	if req.ID != l.ID && req.ProposedID != l.ID {
		return LeaseResult{}, errLeaseMismatch()
	}

	switch st {
	case LeaseStateBreaking:
		return LeaseResult{}, leaseErr(http.StatusConflict, "LeaseIsBreakingAndCannotBeChanged",
			"The lease ID matched, but the lease is currently in breaking state and cannot be changed.")
	case LeaseStateLeased:
		l.ID = req.ProposedID

		return LeaseResult{ID: l.ID}, nil
	default:
		return LeaseResult{}, errLeaseNotPresent()
	}
}

func breakLease(slot **leaseRecord, now time.Time, req LeaseRequest, st string) (LeaseResult, error) {
	l := *slot
	period := time.Duration(req.BreakPeriod) * time.Second

	switch st {
	case LeaseStateAvailable:
		return LeaseResult{}, errLeaseNotPresent()
	case LeaseStateExpired:
		l.BreakAt = now
	case LeaseStateLeased:
		d := time.Duration(0)
		if req.HasBreakPeriod {
			d = period
		} else if !l.Infinite {
			d = l.ExpiresAt.Sub(now)
		}

		if !l.Infinite {
			d = min(d, l.ExpiresAt.Sub(now))
		}

		l.BreakAt = now.Add(d)
	case LeaseStateBreaking:
		if req.HasBreakPeriod && now.Add(period).Before(l.BreakAt) {
			l.BreakAt = now.Add(period)
		}
	}

	remaining := l.BreakAt.Sub(now)
	if remaining <= 0 {
		return LeaseResult{ID: l.ID}, nil
	}

	return LeaseResult{ID: l.ID, BreakRemaining: int((remaining + time.Second - 1) / time.Second)}, nil
}

// LeaseBlob runs a lease action on a blob. Conditional headers apply.
func (b *InMemoryBackend) LeaseBlob(container, blob string, req LeaseRequest) (LeaseResult, error) {
	b.mu.Lock("LeaseBlob")
	defer b.mu.Unlock()

	s, err := b.lookupBlobLocked(container, blob, "")
	if err != nil {
		return LeaseResult{}, err
	}

	if err = req.Cond.check(condWrite, true, s.ETag, s.LastModified); err != nil {
		return LeaseResult{}, err
	}

	res, err := applyLease(&s.Lease, b.now(), req)
	res.ETag, res.LastModified = s.ETag, s.LastModified

	return res, err
}

// LeaseContainer runs a lease action on a container.
func (b *InMemoryBackend) LeaseContainer(container string, req LeaseRequest) (LeaseResult, error) {
	b.mu.Lock("LeaseContainer")
	defer b.mu.Unlock()

	c, err := b.containerLocked(container)
	if err != nil {
		return LeaseResult{}, err
	}

	if err = req.Cond.checkContainer(c.info(b.now()).LastModified); err != nil {
		return LeaseResult{}, err
	}

	res, err := applyLease(&c.Lease, b.now(), req)
	ci := c.info(b.now())
	res.ETag, res.LastModified = ci.ETag, ci.LastModified

	return res, err
}
