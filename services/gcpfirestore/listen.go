package gcpfirestore

import (
	"errors"
	"io"
	"strconv"
	"time"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// listenTarget tracks what has been sent to the client for one watch target.
type listenTarget struct {
	t    *firestorepb.Target
	sent map[string]time.Time
	id   int32
}

func (b *InMemoryBackend) targetState(
	t *firestorepb.Target, sent map[string]time.Time, at time.Time,
) (map[string]*firestorepb.Document, map[string]bool, time.Time, error) {
	b.mu.RLock("Listen")
	defer b.mu.RUnlock()

	rv := &readView{readTime: b.readNowLocked()}
	if !at.IsZero() {
		rv.readTime, rv.at = at, at
	}
	cur := map[string]*firestorepb.Document{}

	switch tt := t.GetTargetType().(type) {
	case *firestorepb.Target_Documents:
		for _, n := range tt.Documents.GetDocuments() {
			if err := validateDocName(n); err != nil {
				return nil, nil, time.Time{}, err
			}

			if d := b.docAt(n, rv); d != nil {
				cur[n] = d
			}
		}
	case *firestorepb.Target_Query:
		docs, _, err := b.runQuery(tt.Query.GetParent(), tt.Query.GetStructuredQuery(), rv)
		if err != nil {
			return nil, nil, time.Time{}, err
		}

		for _, d := range docs {
			cur[d.GetName()] = d
		}
	default:
		return nil, nil, time.Time{}, errInvalid("Target has no type.")
	}

	deleted := map[string]bool{}

	for n := range sent {
		if _, ok := cur[n]; !ok {
			rec := b.docs[n]
			deleted[n] = rec == nil || rec.cur() == nil
		}
	}

	return cur, deleted, rv.readTime, nil
}

// resumePoint is the time a target asks to resume from: its resume token or its read_time.
func resumePoint(t *firestorepb.Target) (time.Time, bool) {
	if tok := t.GetResumeToken(); len(tok) > 0 {
		n, err := strconv.ParseInt(string(tok), 10, 64)

		return time.Unix(0, n).UTC(), err == nil
	}

	if rt := t.GetReadTime(); rt != nil {
		return rt.AsTime(), true
	}

	return time.Time{}, false
}

// seedResume records the target's state at its resume point as already delivered, so the following diff replays
// only the changes since then. It reports false when the point is unusable (malformed, future or older than the
// retained history) and the client must be reset.
func (b *InMemoryBackend) seedResume(lt *listenTarget) (bool, error) {
	at, ok := resumePoint(lt.t)
	if !ok {
		return false, nil
	}

	b.mu.RLock("ListenResume")
	now := b.readNowLocked()
	b.mu.RUnlock()

	if at.After(now) || now.Sub(at) > historyWindow {
		return false, nil
	}

	past, _, _, err := b.targetState(lt.t, nil, at)
	if err != nil {
		return false, err
	}

	for n, d := range past {
		lt.sent[n] = d.GetUpdateTime().AsTime()
	}

	return true, nil
}

func resumeToken(t time.Time) []byte { return []byte(strconv.FormatInt(t.UnixNano(), 10)) }

// diff sends the changes needed to bring the client's view of a target up to date; it reports whether any were sent.
func (b *InMemoryBackend) diff(
	lt *listenTarget, stream firestorepb.Firestore_ListenServer,
) (time.Time, bool, error) {
	cur, deleted, rt, err := b.targetState(lt.t, lt.sent, time.Time{})
	if err != nil {
		return rt, false, err
	}

	ids := []int32{lt.id}
	ts := timestamppb.New(rt)
	changed := false

	for n, d := range cur {
		if prev, ok := lt.sent[n]; ok && prev.Equal(d.GetUpdateTime().AsTime()) {
			continue
		}

		lt.sent[n] = d.GetUpdateTime().AsTime()
		changed = true

		if serr := stream.Send(&firestorepb.ListenResponse{ResponseType: &firestorepb.ListenResponse_DocumentChange{
			DocumentChange: &firestorepb.DocumentChange{Document: d, TargetIds: ids},
		}}); serr != nil {
			return rt, changed, serr
		}
	}

	for n, gone := range deleted {
		delete(lt.sent, n)

		changed = true

		var resp *firestorepb.ListenResponse
		if gone {
			resp = &firestorepb.ListenResponse{ResponseType: &firestorepb.ListenResponse_DocumentDelete{
				DocumentDelete: &firestorepb.DocumentDelete{Document: n, RemovedTargetIds: ids, ReadTime: ts},
			}}
		} else {
			resp = &firestorepb.ListenResponse{ResponseType: &firestorepb.ListenResponse_DocumentRemove{
				DocumentRemove: &firestorepb.DocumentRemove{Document: n, RemovedTargetIds: ids, ReadTime: ts},
			}}
		}

		if serr := stream.Send(resp); serr != nil {
			return rt, changed, serr
		}
	}

	return rt, changed, nil
}

func sendTargetChange(
	stream firestorepb.Firestore_ListenServer, typ firestorepb.TargetChange_TargetChangeType,
	ids []int32, rt time.Time, withToken bool,
) error {
	tc := &firestorepb.TargetChange{TargetChangeType: typ, TargetIds: ids}
	if !rt.IsZero() {
		tc.ReadTime = timestamppb.New(rt)
	}

	if withToken {
		tc.ResumeToken = resumeToken(rt)
	}

	return stream.Send(
		&firestorepb.ListenResponse{ResponseType: &firestorepb.ListenResponse_TargetChange{TargetChange: tc}},
	)
}

// addTarget sends the initial state of a target followed by CURRENT and a consistent global snapshot.
func (b *InMemoryBackend) addTarget(
	stream firestorepb.Firestore_ListenServer,
	t *firestorepb.Target,
) (*listenTarget, error) {
	if t.GetTargetId() == 0 {
		return nil, errInvalid("Target id must be non-zero.")
	}

	lt := &listenTarget{t: t, id: t.GetTargetId(), sent: map[string]time.Time{}}
	ids := []int32{lt.id}

	if err := sendTargetChange(stream, firestorepb.TargetChange_ADD, ids, time.Time{}, false); err != nil {
		return nil, err
	}

	resumed, err := b.seedResume(lt)
	if err != nil {
		return nil, err
	}

	if (len(t.GetResumeToken()) > 0 || t.GetReadTime() != nil) && !resumed {
		if serr := sendTargetChange(stream, firestorepb.TargetChange_RESET, ids, time.Time{}, false); serr != nil {
			return nil, serr
		}
	}

	rt, _, err := b.diff(lt, stream)
	if err != nil {
		return nil, err
	}

	if resumed {
		filter := &firestorepb.ExistenceFilter{TargetId: lt.id, Count: count32(len(lt.sent))}
		if serr := stream.Send(&firestorepb.ListenResponse{
			ResponseType: &firestorepb.ListenResponse_Filter{Filter: filter},
		}); serr != nil {
			return nil, serr
		}
	}

	if serr := sendTargetChange(stream, firestorepb.TargetChange_CURRENT, ids, rt, true); serr != nil {
		return nil, serr
	}

	if serr := sendTargetChange(stream, firestorepb.TargetChange_NO_CHANGE, nil, rt, true); serr != nil {
		return nil, serr
	}

	if t.GetOnce() {
		return nil, sendTargetChange(stream, firestorepb.TargetChange_REMOVE, ids, time.Time{}, false)
	}

	return lt, nil
}

// Listen serves watch streams: document and query targets with change events and resume tokens.
func (s *server) Listen(stream firestorepb.Firestore_ListenServer) error {
	ctx := stream.Context()
	reqs := make(chan *firestorepb.ListenRequest)
	errc := make(chan error, 1)

	go func() {
		for {
			r, err := stream.Recv()
			if err != nil {
				errc <- err

				return
			}

			select {
			case reqs <- r:
			case <-ctx.Done():
				return
			}
		}
	}()

	notify, cancel := s.b.Subscribe()
	defer cancel()

	targets := map[int32]*listenTarget{}

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errc:
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		case r := <-reqs:
			if err := s.handleListenRequest(stream, targets, r); err != nil {
				return err
			}
		case <-notify:
			if err := s.resync(stream, targets); err != nil {
				return err
			}
		}
	}
}

func (s *server) handleListenRequest(
	stream firestorepb.Firestore_ListenServer, targets map[int32]*listenTarget, r *firestorepb.ListenRequest,
) error {
	if err := validateDatabase(r.GetDatabase()); err != nil {
		return err
	}

	switch tc := r.GetTargetChange().(type) {
	case *firestorepb.ListenRequest_AddTarget:
		lt, err := s.b.addTarget(stream, tc.AddTarget)
		if err != nil {
			return err
		}

		if lt != nil {
			targets[lt.id] = lt
		}
	case *firestorepb.ListenRequest_RemoveTarget:
		delete(targets, tc.RemoveTarget)

		return sendTargetChange(stream, firestorepb.TargetChange_REMOVE, []int32{tc.RemoveTarget}, time.Time{}, false)
	default:
		return status.Error(codes.InvalidArgument, "ListenRequest has no target change.")
	}

	return nil
}

func (s *server) resync(stream firestorepb.Firestore_ListenServer, targets map[int32]*listenTarget) error {
	var (
		dirty bool
		rt    time.Time
	)

	for _, lt := range targets {
		t, changed, err := s.b.diff(lt, stream)
		if err != nil {
			return err
		}

		dirty, rt = dirty || changed, t
	}

	if !dirty {
		return nil
	}

	return sendTargetChange(stream, firestorepb.TargetChange_NO_CHANGE, nil, rt, true)
}
