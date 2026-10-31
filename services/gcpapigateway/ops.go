package gcpapigateway

import (
	"context"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/apigateway/apiv1/apigatewaypb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	apiVersion      = "v1"
	defaultListPage = 100
	verbCreate      = "create"
	verbUpdate      = "update"
	verbDelete      = "delete"
)

type diagnostic struct {
	Location string `json:"location"`
	Message  string `json:"message"`
}

type opRec struct {
	Created         time.Time    `json:"created"`
	EndAt           time.Time    `json:"endAt"`
	CancelledAt     time.Time    `json:"cancelledAt"`
	Name            string       `json:"name"`
	Target          string       `json:"target"`
	Verb            string       `json:"verb"`
	FailMsg         string       `json:"failMsg,omitempty"`
	Resp            []byte       `json:"resp,omitempty"`
	Diags           []diagnostic `json:"diagnostics,omitempty"`
	FailCode        int32        `json:"failCode,omitempty"`
	CancelRequested bool         `json:"cancelRequested,omitempty"`
}

func (o *opRec) cancelled() bool { return !o.CancelledAt.IsZero() }

func (o *opRec) done(now time.Time) bool { return o.cancelled() || !now.Before(o.EndAt) }

type opSpec struct {
	resp     proto.Message
	failMsg  string
	target   string
	verb     string
	project  string
	location string
	diags    []diagnostic
	dur      time.Duration
	failCode codes.Code
}

// newOpLocked records an operation that finishes dur from now with resp, or with a failure when failMsg is set.
func (b *InMemoryBackend) newOpLocked(s opSpec) (*opRec, error) {
	now := b.now()
	o := &opRec{
		Name:    childName(s.project, s.location, kindOperations, "operation-"+uuid.NewString()),
		Target:  s.target,
		Verb:    s.verb,
		Created: now,
		EndAt:   now.Add(s.dur),
		Diags:   s.diags,
	}

	if s.failMsg != "" {
		o.FailMsg, o.FailCode = s.failMsg, int32(s.failCode) //nolint:gosec // codes are small
	} else if s.resp != nil {
		a, err := anypb.New(s.resp)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "record operation response: %v", err)
		}

		raw, err := proto.Marshal(a)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "record operation response: %v", err)
		}

		o.Resp = raw
	}

	b.ops[o.Name] = o

	return o, nil
}

func (b *InMemoryBackend) opViewLocked(o *opRec) *longrunningpb.Operation {
	now := b.now()
	done := o.done(now)

	meta := &apigatewaypb.OperationMetadata{
		CreateTime: timestamppb.New(o.Created), Target: o.Target, Verb: o.Verb, ApiVersion: apiVersion,
		RequestedCancellation: o.CancelRequested,
	}

	for _, d := range o.Diags {
		meta.Diagnostics = append(meta.Diagnostics, &apigatewaypb.OperationMetadata_Diagnostic{
			Location: d.Location, Message: d.Message,
		})
	}

	op := &longrunningpb.Operation{Name: o.Name}

	if done {
		end := o.EndAt
		if o.cancelled() {
			end = o.CancelledAt
		}

		meta.EndTime = timestamppb.New(end)
		op.Done = true

		switch {
		case o.cancelled():
			meta.StatusMessage = "Operation cancelled."
			op.Result = &longrunningpb.Operation_Error{
				Error: status.New(codes.Canceled, "Operation cancelled.").Proto(),
			}
		case o.FailMsg != "":
			meta.StatusMessage = o.FailMsg
			op.Result = &longrunningpb.Operation_Error{
				Error: status.New(codes.Code(o.FailCode), o.FailMsg).Proto(), //nolint:gosec // stored from a codes.Code
			}
		default:
			resp := &anypb.Any{}
			if len(o.Resp) > 0 && proto.Unmarshal(o.Resp, resp) == nil {
				op.Result = &longrunningpb.Operation_Response{Response: resp}
			}
		}
	}

	if a, err := anypb.New(meta); err == nil {
		op.Metadata = a
	}

	return op
}

// GetOperation returns one operation.
func (b *InMemoryBackend) GetOperation(name string) (*longrunningpb.Operation, error) {
	b.mu.RLock("GetOperation")
	defer b.mu.RUnlock()

	o, ok := b.ops[name]
	if !ok {
		return nil, errNotFound("Operation %q not found.", name)
	}

	return b.opViewLocked(o), nil
}

// WaitOperation returns once the operation is done or the timeout passes, whichever comes first.
func (b *InMemoryBackend) WaitOperation(ctx context.Context, in *longrunningpb.WaitOperationRequest) (
	*longrunningpb.Operation, error,
) {
	b.mu.RLock("WaitOperation")

	o, ok := b.ops[in.GetName()]
	if !ok {
		b.mu.RUnlock()

		return nil, errNotFound("Operation %q not found.", in.GetName())
	}

	wait := o.EndAt.Sub(b.now())
	if o.done(b.now()) {
		wait = 0
	}

	b.mu.RUnlock()

	if in.GetTimeout() != nil {
		wait = min(wait, in.GetTimeout().AsDuration())
	}

	if wait > 0 {
		t := time.NewTimer(wait)
		defer t.Stop()

		select {
		case <-t.C:
		case <-ctx.Done():
		}
	}

	return b.GetOperation(in.GetName())
}

// ListOperations lists the operations of one location, ordered by name.
func (b *InMemoryBackend) ListOperations(in *longrunningpb.ListOperationsRequest) (
	*longrunningpb.ListOperationsResponse, error,
) {
	if in.GetFilter() != "" {
		return nil, errInvalid("ListOperations filter is not supported.")
	}

	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	if _, _, err := parseParent(strings.TrimSuffix(in.GetName(), "/")); err != nil {
		return nil, err
	}

	prefix := strings.TrimSuffix(in.GetName(), "/") + "/" + kindOperations + "/"

	b.mu.RLock("ListOperations")

	names := make([]string, 0, len(b.ops))

	for n := range b.ops {
		if strings.HasPrefix(n, prefix) {
			names = append(names, n)
		}
	}

	slices.Sort(names)

	all := make([]*longrunningpb.Operation, 0, len(names))
	for _, n := range names {
		all = append(all, b.opViewLocked(b.ops[n]))
	}

	b.mu.RUnlock()

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultListPage)

	return &longrunningpb.ListOperationsResponse{Operations: pg.Data, NextPageToken: pg.Next}, nil
}

// DeleteOperation removes a finished operation.
func (b *InMemoryBackend) DeleteOperation(name string) (*emptypb.Empty, error) {
	b.mu.Lock("DeleteOperation")
	defer b.mu.Unlock()

	o, ok := b.ops[name]
	if !ok {
		return nil, errNotFound("Operation %q not found.", name)
	}

	if !o.done(b.now()) {
		return nil, errPrecondition("Operation %q is still running.", name)
	}

	delete(b.ops, name)

	return &emptypb.Empty{}, nil
}

// CancelOperation cancels a running operation: a create is rolled back, a delete is undone and an update keeps
// its change. Cancelling a finished operation has no effect.
func (b *InMemoryBackend) CancelOperation(name string) (*emptypb.Empty, error) {
	b.mu.Lock("CancelOperation")
	defer b.mu.Unlock()

	o, ok := b.ops[name]
	if !ok {
		return nil, errNotFound("Operation %q not found.", name)
	}

	now := b.now()
	if o.done(now) {
		return &emptypb.Empty{}, nil
	}

	o.CancelRequested, o.CancelledAt = true, now
	b.rollbackLocked(o, now)

	return &emptypb.Empty{}, nil
}

func (b *InMemoryBackend) rollbackLocked(o *opRec, now time.Time) {
	l := b.lifeOf(o.Target)
	if l == nil {
		return
	}

	switch o.Verb {
	case verbCreate:
		l.DeleteAt = now
	case verbDelete:
		l.DeleteAt, l.DeleteOp = time.Time{}, ""
	case verbUpdate:
		l.Until = now
	}
}

// lifeOf returns the lifecycle of the resource a target names.
func (b *InMemoryBackend) lifeOf(target string) *life {
	if r, ok := b.apis[target]; ok {
		return &r.life
	}

	if r, ok := b.configs[target]; ok {
		return &r.life
	}

	if r, ok := b.gateways[target]; ok {
		return &r.life
	}

	return nil
}

func emptyResp() proto.Message { return &emptypb.Empty{} }
