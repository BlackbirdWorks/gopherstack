package gcpfirestore_test

import (
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type stepClock struct{ ns atomic.Int64 }

func newStepClock() *stepClock {
	c := &stepClock{}
	c.ns.Store(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())

	return c
}

func (c *stepClock) Now() time.Time { return time.Unix(0, c.ns.Load()).UTC() }

func (c *stepClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

func collectionTarget(coll string, resume []byte) *firestorepb.Target {
	return &firestorepb.Target{
		TargetId: 1,
		TargetType: &firestorepb.Target_Query{Query: &firestorepb.Target_QueryTarget{
			Parent: docRoot,
			QueryType: &firestorepb.Target_QueryTarget_StructuredQuery{StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: coll}},
			}},
		}},
		ResumeType: &firestorepb.Target_ResumeToken{ResumeToken: resume},
	}
}

type listened struct {
	changed []string
	removed []string
	filter  *firestorepb.ExistenceFilter
	token   []byte
	reset   bool
}

func listenOnce(t *testing.T, e *env, target *firestorepb.Target) listened {
	t.Helper()

	ctx, cancel := t.Context(), func() {}
	defer cancel()

	stream, err := e.raw.Listen(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&firestorepb.ListenRequest{
		Database: dbRoot, TargetChange: &firestorepb.ListenRequest_AddTarget{AddTarget: target},
	}))

	var out listened

	for {
		resp, rerr := stream.Recv()
		require.NoError(t, rerr)

		switch v := resp.GetResponseType().(type) {
		case *firestorepb.ListenResponse_DocumentChange:
			out.changed = append(out.changed, v.DocumentChange.GetDocument().GetName())
		case *firestorepb.ListenResponse_DocumentDelete:
			out.removed = append(out.removed, v.DocumentDelete.GetDocument())
		case *firestorepb.ListenResponse_DocumentRemove:
			out.removed = append(out.removed, v.DocumentRemove.GetDocument())
		case *firestorepb.ListenResponse_Filter:
			out.filter = v.Filter
		case *firestorepb.ListenResponse_TargetChange:
			tc := v.TargetChange
			if tc.GetTargetChangeType() == firestorepb.TargetChange_RESET {
				out.reset = true
			}

			if tc.GetTargetChangeType() == firestorepb.TargetChange_NO_CHANGE && len(tc.GetResumeToken()) > 0 {
				out.token = tc.GetResumeToken()

				return out
			}
		}
	}
}

func TestListenResumeReplaysMissedChanges(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	clock := newStepClock()
	e.handler.Backend.SetClock(clock.Now)

	e.seed(t, "ls", map[string]map[string]any{"keep": {"v": 1}, "gone": {"v": 1}, "edit": {"v": 1}})
	clock.Advance(time.Second)

	first := listenOnce(t, e, collectionTarget("ls", nil))
	require.Len(t, first.changed, 3)
	require.NotEmpty(t, first.token)
	assert.Nil(t, first.filter, "no filter without a resume")

	clock.Advance(time.Second)

	_, err := e.client.Collection("ls").Doc("gone").Delete(t.Context())
	require.NoError(t, err)

	_, err = e.client.Collection("ls").Doc("edit").Set(t.Context(), map[string]any{"v": 2})
	require.NoError(t, err)

	_, err = e.client.Collection("ls").Doc("new").Set(t.Context(), map[string]any{"v": 1})
	require.NoError(t, err)

	second := listenOnce(t, e, collectionTarget("ls", first.token))
	assert.False(t, second.reset, "a resume inside the retained history is not reset")
	assert.ElementsMatch(
		t,
		[]string{docRoot + "/ls/edit", docRoot + "/ls/new"},
		second.changed,
		"only changed documents are replayed",
	)
	assert.Equal(t, []string{docRoot + "/ls/gone"}, second.removed)
	require.NotNil(t, second.filter)
	assert.Equal(t, int32(3), second.filter.GetCount(), "keep, edit and new match now")
	assert.Equal(t, int32(1), second.filter.GetTargetId())
}

func TestListenResumeTooOldResets(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	clock := newStepClock()
	e.handler.Backend.SetClock(clock.Now)

	e.seed(t, "old", map[string]map[string]any{"a": {"v": 1}})
	clock.Advance(time.Second)

	first := listenOnce(t, e, collectionTarget("old", nil))

	clock.Advance(2 * time.Hour)

	second := listenOnce(t, e, collectionTarget("old", first.token))
	assert.True(t, second.reset)
	assert.Len(t, second.changed, 1, "a full snapshot follows the reset")
	assert.Nil(t, second.filter)
}

func TestListenResumeFromReadTime(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	clock := newStepClock()
	e.handler.Backend.SetClock(clock.Now)

	e.seed(t, "rt", map[string]map[string]any{"a": {"v": 1}})
	clock.Advance(time.Second)

	at := clock.Now()

	clock.Advance(time.Second)
	e.seed(t, "rt", map[string]map[string]any{"b": {"v": 1}})

	target := collectionTarget("rt", nil)
	target.ResumeType = &firestorepb.Target_ReadTime{ReadTime: timestamppbOf(at)}

	got := listenOnce(t, e, target)
	assert.Equal(t, []string{docRoot + "/rt/b"}, got.changed)
	require.NotNil(t, got.filter)
	assert.Equal(t, int32(2), got.filter.GetCount())
}

func timestamppbOf(t time.Time) *timestamppb.Timestamp { return timestamppb.New(t) }
