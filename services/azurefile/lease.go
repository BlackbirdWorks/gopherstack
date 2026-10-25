package azurefile

import (
	"net/http"
	"time"

	"github.com/google/uuid"
)

const (
	leaseLeased   = "leased"
	leaseBreaking = "breaking"
	leaseBroken   = "broken"
	leaseExpired  = "expired"
	leaseAvail    = "available"

	minLeaseSecs = 15
	maxLeaseSecs = 60
)

// effective reports the lease state as of now.
func (l *leaseInfo) effective(now time.Time) string {
	switch l.State {
	case "":
		return leaseAvail
	case leaseLeased:
		if !l.Infinite && !now.Before(l.Until) {
			return leaseExpired
		}
	case leaseBreaking:
		if !now.Before(l.Until) {
			return leaseBroken
		}
	}

	return l.State
}

func (l *leaseInfo) active(now time.Time) bool {
	s := l.effective(now)

	return s == leaseLeased || s == leaseBreaking
}

// setHeaders writes x-ms-lease-status/state/duration.
func (l *leaseInfo) setHeaders(h http.Header, now time.Time) {
	state := l.effective(now)

	status := "unlocked"
	if state == leaseLeased || state == leaseBreaking {
		status = "locked"

		if l.Infinite {
			h.Set("X-Ms-Lease-Duration", "infinite")
		} else {
			h.Set("X-Ms-Lease-Duration", "fixed")
		}
	}

	h.Set("X-Ms-Lease-Status", status)
	h.Set("X-Ms-Lease-State", state)
}

// check validates an operation's supplied lease id against the lease.
func (l *leaseInfo) check(supplied string, now time.Time, file bool) *apiError {
	idMismatch, notPresent := "LeaseIdMismatchWithLeaseOperation", "LeaseNotPresentWithLeaseOperation"
	if file {
		idMismatch, notPresent = "LeaseIdMismatchWithFileOperation", "LeaseNotPresentWithFileOperation"
	}

	if l.active(now) {
		switch {
		case supplied == "":
			return newErr(http.StatusPreconditionFailed, "LeaseIdMissing",
				"There is currently a lease on the resource and no lease ID was specified in the request.")
		case supplied != l.ID:
			return newErr(http.StatusPreconditionFailed, idMismatch,
				"The lease ID specified did not match the lease ID for the resource.")
		}

		return nil
	}

	if supplied != "" {
		return newErr(http.StatusPreconditionFailed, notPresent,
			"There is currently no lease on the resource.")
	}

	return nil
}

// leaseRequest carries the headers of a lease action.
type leaseRequest struct {
	Break    *int
	Action   string
	LeaseID  string
	Proposed string
	Duration int
	HasDur   bool
}

// leaseResult is what a lease action returns to the client.
type leaseResult struct {
	ID        string
	BreakTime int
	Status    int
}

func newLeaseID(proposed string) string {
	if proposed != "" {
		return proposed
	}

	return uuid.NewString()
}

func errLeaseMismatch() *apiError {
	return newErr(http.StatusConflict, "LeaseIdMismatchWithLeaseOperation",
		"The lease ID specified did not match the lease ID for the resource.")
}

func errLeaseNotPresent() *apiError {
	return newErr(http.StatusConflict, "LeaseNotPresentWithLeaseOperation",
		"There is currently no lease on the resource.")
}

// apply runs a lease action. fileLease restricts acquire to infinite leases.
func (l *leaseInfo) apply(req leaseRequest, now time.Time, fileLease bool) (leaseResult, *apiError) {
	switch req.Action {
	case "acquire":
		return l.acquire(req, now, fileLease)
	case "renew":
		return l.renew(req, now)
	case "release":
		return l.release(req, now)
	case "change":
		return l.change(req, now)
	case "break":
		return l.breakLease(req, now, fileLease)
	}

	return leaseResult{}, errInvalidHeader("x-ms-lease-action")
}

func (l *leaseInfo) acquire(req leaseRequest, now time.Time, fileLease bool) (leaseResult, *apiError) {
	if !req.HasDur {
		return leaseResult{}, errMissingHeader("x-ms-lease-duration")
	}

	infinite := req.Duration == -1
	if !infinite && (fileLease || req.Duration < minLeaseSecs || req.Duration > maxLeaseSecs) {
		return leaseResult{}, errInvalidHeader("x-ms-lease-duration")
	}

	switch l.effective(now) {
	case leaseLeased:
		if req.Proposed == "" || req.Proposed != l.ID {
			return leaseResult{}, newErr(http.StatusConflict, "LeaseAlreadyPresent",
				"There is already a lease present.")
		}
	case leaseBreaking:
		return leaseResult{}, newErr(
			http.StatusConflict,
			"LeaseIsBreakingAndCannotBeAcquired",
			"The lease ID matched, but the lease is currently in breaking state and cannot be acquired until it is broken.",
		)
	}

	id := newLeaseID(req.Proposed)
	*l = leaseInfo{ID: id, State: leaseLeased, Infinite: infinite, Dur: req.Duration}

	if !infinite {
		l.Until = now.Add(time.Duration(req.Duration) * time.Second)
	}

	return leaseResult{ID: id, Status: http.StatusCreated}, nil
}

func (l *leaseInfo) renew(req leaseRequest, now time.Time) (leaseResult, *apiError) {
	state := l.effective(now)
	if state == leaseAvail {
		return leaseResult{}, errLeaseNotPresent()
	}

	if req.LeaseID != l.ID {
		return leaseResult{}, errLeaseMismatch()
	}

	if state == leaseBreaking || state == leaseBroken {
		return leaseResult{}, newErr(http.StatusConflict, "LeaseIsBrokenAndCannotBeRenewed",
			"The lease ID matched, but the lease has been broken explicitly and cannot be renewed.")
	}

	if !l.Infinite {
		l.Until = now.Add(time.Duration(l.Dur) * time.Second)
	}

	l.State = leaseLeased

	return leaseResult{ID: l.ID, Status: http.StatusOK}, nil
}

func (l *leaseInfo) release(req leaseRequest, now time.Time) (leaseResult, *apiError) {
	if l.effective(now) == leaseAvail {
		return leaseResult{}, errLeaseNotPresent()
	}

	if req.LeaseID != l.ID {
		return leaseResult{}, errLeaseMismatch()
	}

	*l = leaseInfo{}

	return leaseResult{Status: http.StatusOK}, nil
}

func (l *leaseInfo) change(req leaseRequest, now time.Time) (leaseResult, *apiError) {
	state := l.effective(now)
	if state != leaseLeased && state != leaseBreaking {
		return leaseResult{}, errLeaseNotPresent()
	}

	if req.Proposed == "" {
		return leaseResult{}, errMissingHeader("x-ms-proposed-lease-id")
	}

	if req.LeaseID != l.ID && req.LeaseID != req.Proposed {
		return leaseResult{}, errLeaseMismatch()
	}

	if state == leaseBreaking {
		return leaseResult{}, newErr(http.StatusConflict, "LeaseIsBreakingAndCannotBeChanged",
			"The lease ID matched, but the lease is currently in breaking state and cannot be changed.")
	}

	l.ID = req.Proposed

	return leaseResult{ID: l.ID, Status: http.StatusOK}, nil
}

func (l *leaseInfo) breakLease(req leaseRequest, now time.Time, fileLease bool) (leaseResult, *apiError) {
	state := l.effective(now)
	if state == leaseAvail {
		return leaseResult{}, errLeaseNotPresent()
	}

	if req.LeaseID != "" && req.LeaseID != l.ID {
		return leaseResult{}, errLeaseMismatch()
	}

	if state == leaseExpired || state == leaseBroken {
		l.State = leaseBroken

		return leaseResult{Status: http.StatusAccepted}, nil
	}

	period := 0

	switch {
	case fileLease || (req.Break == nil && l.Infinite):
	case req.Break != nil:
		period = *req.Break
	default:
		period = int(l.Until.Sub(now).Seconds())
	}

	if period > maxLeaseSecs {
		return leaseResult{}, errInvalidHeader("x-ms-lease-break-period")
	}

	if state == leaseBreaking {
		period = min(period, int(l.Until.Sub(now).Seconds()))
	}

	if period <= 0 {
		l.State = leaseBroken

		return leaseResult{Status: http.StatusAccepted}, nil
	}

	l.State = leaseBreaking
	l.Until = now.Add(time.Duration(period) * time.Second)

	return leaseResult{BreakTime: period, Status: http.StatusAccepted}, nil
}
