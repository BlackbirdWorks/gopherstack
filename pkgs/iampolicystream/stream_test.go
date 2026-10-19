package iampolicystream_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/iampolicystream"
)

func ev(action, res, decision, reason string) iampolicystream.Event {
	return iampolicystream.Event{Action: action, Resource: res, Decision: decision, Reason: reason, Principal: "p"}
}

func TestRingWraps(t *testing.T) {
	t.Parallel()

	r := iampolicystream.New(3)
	for _, a := range []string{"a:1", "a:2", "a:3", "a:4", "a:5"} {
		r.Record(ev(a, "*", iampolicystream.DecisionAllow, ""))
	}

	got := r.Snapshot(nil)
	require.Len(t, got, 3)
	assert.Equal(t, "a:3", got[0].Action)
	assert.Equal(t, "a:5", got[2].Action)
	assert.Equal(t, uint64(5), got[2].Seq)

	r.Clear()
	assert.Empty(t, r.Snapshot(nil))
}

func TestFilterMatch(t *testing.T) {
	t.Parallel()

	now := time.Now()
	e := iampolicystream.Event{Decision: "deny", Service: "s3", Principal: "p", Time: now, Seq: 5}

	tests := []struct {
		name string
		f    iampolicystream.Filter
		want bool
	}{
		{"empty", iampolicystream.Filter{}, true},
		{"decision_fold", iampolicystream.Filter{Decision: "DENY"}, true},
		{"decision_miss", iampolicystream.Filter{Decision: "allow"}, false},
		{"service_miss", iampolicystream.Filter{Service: "sqs"}, false},
		{"principal_miss", iampolicystream.Filter{Principal: "q"}, false},
		{"since_future", iampolicystream.Filter{Since: now.Add(time.Hour)}, false},
		{"after_seq", iampolicystream.Filter{AfterSeq: 5}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.f.Match(e))
		})
	}
}

func TestSuggest(t *testing.T) {
	t.Parallel()

	events := []iampolicystream.Event{
		ev("s3:GetObject", "arn:aws:s3:::b/k", "deny", iampolicystream.ReasonImplicitDeny),
		ev("s3:PutObject", "arn:aws:s3:::b/k", "deny", iampolicystream.ReasonImplicitDeny),
		ev("s3:GetObject", "arn:aws:s3:::b/k", "deny", iampolicystream.ReasonImplicitDeny),
		ev("sqs:SendMessage", "", "deny", iampolicystream.ReasonImplicitDeny),
		ev("kms:Decrypt", "*", "deny", iampolicystream.ReasonIdentityPolicy),
		ev("iam:GetUser", "*", "allow", iampolicystream.ReasonIdentityPolicy),
	}

	s := iampolicystream.Suggest(events, false)
	assert.Equal(t, 4, s.EventsConsumed)
	require.Len(t, s.Policy.Statement, 2)
	assert.Equal(t, []string{"*"}, s.Policy.Statement[0].Resource)
	assert.Equal(t, []string{"sqs:SendMessage"}, s.Policy.Statement[0].Action)
	assert.Equal(t, []string{"s3:GetObject", "s3:PutObject"}, s.Policy.Statement[1].Action)
	assert.Equal(t, []string{"kms:Decrypt on *"}, s.BlockedByDeny)

	all := iampolicystream.Suggest(events, true)
	assert.Len(t, all.Policy.Statement, 2)
	assert.Contains(t, all.Policy.Statement[0].Action, "iam:GetUser")
}

func TestSubscribeReleasedOnCancel(t *testing.T) {
	t.Parallel()

	r := iampolicystream.New(5)
	ctx, cancel := context.WithCancel(t.Context())
	ch := r.Subscribe(ctx)
	require.Equal(t, 1, r.Subscribers())

	r.Record(ev("a:b", "*", "allow", ""))
	assert.Equal(t, "a:b", (<-ch).Action)

	cancel()
	require.Eventually(t, func() bool { return r.Subscribers() == 0 }, 5*time.Second, time.Millisecond)

	_, open := <-ch
	assert.False(t, open)
}

func TestSlowSubscriberDoesNotBlock(t *testing.T) {
	t.Parallel()

	r := iampolicystream.New(5)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	_ = r.Subscribe(ctx)

	for range 1000 {
		r.Record(ev("a:b", "*", "allow", ""))
	}

	assert.Positive(t, r.Dropped())
}
