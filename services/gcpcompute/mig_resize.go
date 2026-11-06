package gcpcompute

import (
	"slices"
	"strconv"
	"time"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	collMIGResize   = "migResizeRequests"
	resizeAccepted  = "ACCEPTED"
	resizeSucceeded = "SUCCEEDED"
	resizeCancelled = "CANCELLED"
	resizeFailed    = "FAILED"
	resizeKind      = "compute#instanceGroupManagerResizeRequest"
)

type resizeRec struct {
	Created time.Time                                     `json:"created"`
	Req     *computeapi.InstanceGroupManagerResizeRequest `json:"req"`
}

func resizeTarget(project string, rec *migRec, r *computeapi.InstanceGroupManagerResizeRequest) target {
	scope, loc := migScopeOf(rec.Mgr)

	return target{
		project: project, scope: scope, loc: loc, collection: collMIG,
		name: rec.Mgr.Name + "/resizeRequests/" + r.Name, id: r.Id,
	}
}

func (rec *migRec) resize(name string) *resizeRec {
	for _, r := range rec.Resizes {
		if r.Req.Name == name {
			return r
		}
	}

	return nil
}

func validResizeRequest(in *computeapi.InstanceGroupManagerResizeRequest) *APIError {
	if err := validName("name", in.Name); err != nil {
		return err
	}

	if (in.ResizeBy > 0) == (len(in.Instances) > 0) {
		return errInvalid(
			"Invalid value for field 'resource': exactly one of resizeBy and instances must be specified.",
		)
	}

	if in.ResizeBy < 0 {
		return errField("resource.resizeBy", "", "Must be positive.")
	}

	if d := in.RequestedRunDuration; d != nil && (d.Seconds < 0 || d.Nanos < 0) {
		return errField("resource.requestedRunDuration", "", "Must not be negative.")
	}

	return nil
}

func (b *InMemoryBackend) insertResizeRequest(
	project, scope, loc, mig string, in *computeapi.InstanceGroupManagerResizeRequest,
) (*computeapi.Operation, error) {
	b.mu.Lock("InsertResizeRequest")
	defer b.mu.Unlock()

	b.sweepGroupsLocked()

	rec, err := b.getMIGLocked(project, scope, loc, mig)
	if err != nil {
		return nil, err
	}

	if err = validResizeRequest(in); err != nil {
		return nil, err
	}

	if rec.resize(in.Name) != nil {
		return nil, errExists(trimURL(rec.Mgr.SelfLink) + "/resizeRequests/" + in.Name)
	}

	req := &computeapi.InstanceGroupManagerResizeRequest{
		Kind: resizeKind, Id: newID(), CreationTimestamp: b.stamp(), Name: in.Name, Description: in.Description,
		ResizeBy: in.ResizeBy, Instances: in.Instances, RequestedRunDuration: in.RequestedRunDuration,
		State: resizeAccepted, SelfLink: rec.Mgr.SelfLink + "/resizeRequests/" + in.Name,
		Status: &computeapi.InstanceGroupManagerResizeRequestStatus{},
	}
	req.SelfLinkWithId = rec.Mgr.SelfLink + "/resizeRequests/" + strconv.FormatUint(req.Id, 10)

	if scope == scopeRegion {
		req.Region = rec.Mgr.Region
	} else {
		req.Zone = rec.Mgr.Zone
	}

	rec.Resizes = append(rec.Resizes, &resizeRec{Created: b.now().UTC(), Req: req})

	return b.newOpLocked(opResize+"insert", resizeTarget(project, rec, req), b.timing.Done), nil
}

const opResize = "compute.instanceGroupManagerResizeRequests."

func (b *InMemoryBackend) getResizeLocked(project, scope, loc, mig, name string) (*migRec, *resizeRec, *APIError) {
	rec, err := b.getMIGLocked(project, scope, loc, mig)
	if err != nil {
		return nil, nil, err
	}

	r := rec.resize(name)
	if r == nil {
		return nil, nil, errNotFound(trimURL(rec.Mgr.SelfLink) + "/resizeRequests/" + name)
	}

	return rec, r, nil
}

func (b *InMemoryBackend) getResizeRequest(
	project, scope, loc, mig, name string,
) (*computeapi.InstanceGroupManagerResizeRequest, error) {
	b.mu.Lock("GetResizeRequest")
	defer b.mu.Unlock()

	b.sweepGroupsLocked()

	_, r, err := b.getResizeLocked(project, scope, loc, mig, name)
	if err != nil {
		return nil, err
	}

	return clone(r.Req), nil
}

func (b *InMemoryBackend) listResizeRequests(project, scope, loc, mig string) ([]entry, error) {
	b.mu.Lock("ListResizeRequests")
	defer b.mu.Unlock()

	b.sweepGroupsLocked()

	rec, err := b.getMIGLocked(project, scope, loc, mig)
	if err != nil {
		return nil, err
	}

	out := make([]entry, 0, len(rec.Resizes))
	for _, r := range rec.Resizes {
		out = append(out, entry{scope: scopeGlobal, item: clone(r.Req)})
	}

	return out, nil
}

func (b *InMemoryBackend) cancelResizeRequest(project, scope, loc, mig, name string) (*computeapi.Operation, error) {
	b.mu.Lock("CancelResizeRequest")
	defer b.mu.Unlock()

	b.sweepGroupsLocked()

	rec, r, err := b.getResizeLocked(project, scope, loc, mig, name)
	if err != nil {
		return nil, err
	}

	if r.Req.State != resizeAccepted {
		return nil, errInvalid("Invalid value for field 'resizeRequest': '%s'. Only ACCEPTED resize requests can be "+
			"cancelled; the state is %s.", name, r.Req.State)
	}

	r.Req.State = resizeCancelled

	return b.newOpLocked(opResize+"cancel", resizeTarget(project, rec, r.Req), b.timing.Done), nil
}

func (b *InMemoryBackend) deleteResizeRequest(project, scope, loc, mig, name string) (*computeapi.Operation, error) {
	b.mu.Lock("DeleteResizeRequest")
	defer b.mu.Unlock()

	b.sweepGroupsLocked()

	rec, r, err := b.getResizeLocked(project, scope, loc, mig, name)
	if err != nil {
		return nil, err
	}

	if r.Req.State == resizeAccepted {
		return nil, errInvalid("Invalid value for field 'resizeRequest': '%s'. Active resize requests cannot be "+
			"deleted; cancel the request first.", name)
	}

	rec.Resizes = slices.DeleteFunc(rec.Resizes, func(o *resizeRec) bool { return o == r })

	return b.newOpLocked(opResize+"delete", resizeTarget(project, rec, r.Req), b.timing.Done), nil
}

// advanceResizesLocked provisions ACCEPTED requests whose capacity wait elapsed and expires run durations.
func (b *InMemoryBackend) advanceResizesLocked(rec *migRec) {
	now := b.now().UTC()

	for _, r := range rec.Resizes {
		if r.Req.State == resizeAccepted && !now.Before(r.Created.Add(b.timing.Done)) {
			b.provisionResizeLocked(rec, r, now)
		}
	}

	for _, m := range rec.Members {
		if m.RunUntil.IsZero() || m.Deleting || now.Before(m.RunUntil) {
			continue
		}

		m.RunUntil = time.Time{}
		b.markDeletingLocked(rec.Project, rec, m)
		rec.Mgr.TargetSize = max(rec.Mgr.TargetSize-1, 0)
	}
}

func (b *InMemoryBackend) provisionResizeLocked(rec *migRec, r *resizeRec, now time.Time) {
	before := len(rec.Members)

	var err *APIError

	if n := r.Req.ResizeBy; n > 0 {
		rec.Mgr.TargetSize += n

		if err = b.growLocked(rec.Project, rec, int(n)); err != nil {
			rec.Mgr.TargetSize -= n
		}
	} else {
		err = b.createNamedLocked(rec, r.Req.Instances, r.Req.Name)
	}

	if err != nil {
		r.Req.State = resizeFailed
		r.Req.Status.Error = &computeapi.InstanceGroupManagerResizeRequestStatusError{
			Errors: []*computeapi.InstanceGroupManagerResizeRequestStatusErrorErrors{
				{Code: "RESOURCE_OPERATION_ERROR", Message: err.Message},
			},
		}

		return
	}

	r.Req.State = resizeSucceeded
	rec.Mgr.Fingerprint = newFingerprint()

	for _, m := range rec.Members[min(before, len(rec.Members)):] {
		m.Request = r.Req.Name
	}

	for _, m := range rec.Members {
		if m.Request == r.Req.Name {
			m.RunUntil = runUntil(now, r.Req.RequestedRunDuration)
		}
	}
}

func runUntil(now time.Time, d *computeapi.Duration) time.Time {
	if d == nil || d.Seconds == 0 && d.Nanos == 0 {
		return time.Time{}
	}

	return now.Add(time.Duration(d.Seconds)*time.Second + time.Duration(d.Nanos))
}
