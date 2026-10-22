package bedrockagentcore_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcore"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcore/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func putEvent(
	t *testing.T, f *fixture, actor, session, text string, at time.Time, mut func(*sdk.CreateEventInput),
) *types.Event {
	t.Helper()

	in := &sdk.CreateEventInput{
		MemoryId: new(testMemory), ActorId: &actor, SessionId: &session, EventTimestamp: &at,
		Payload: []types.PayloadType{&types.PayloadTypeMemberConversational{
			Value: types.Conversational{Role: types.RoleUser, Content: &types.ContentMemberText{Value: text}},
		}},
	}
	if mut != nil {
		mut(in)
	}

	out, err := f.client.CreateEvent(t.Context(), in)
	require.NoError(t, err)

	return out.Event
}

//go:fix inline
func TestMemoryEvents_Lifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t, defaultControl())
	base := f.clock.now()

	e1 := putEvent(t, f, "alice", "s1", "hello", base, nil)
	e2 := putEvent(t, f, "alice", "s1", "world", base.Add(time.Minute), nil)
	putEvent(t, f, "alice", "s2", "other", base.Add(2*time.Minute), nil)
	putEvent(t, f, "bob", "s3", "bob", base, nil)

	got, err := f.client.GetEvent(t.Context(), &sdk.GetEventInput{
		MemoryId: new(testMemory), ActorId: new("alice"), SessionId: new("s1"), EventId: e1.EventId,
	})
	require.NoError(t, err)
	assert.Equal(t, *e1.EventId, *got.Event.EventId)
	assert.True(t, got.Event.EventTimestamp.Equal(base))

	list, err := f.client.ListEvents(t.Context(), &sdk.ListEventsInput{
		MemoryId: new(testMemory), ActorId: new("alice"), SessionId: new("s1"),
	})
	require.NoError(t, err)
	require.Len(t, list.Events, 2)
	assert.Equal(t, *e1.EventId, *list.Events[0].EventId)
	assert.Equal(t, *e2.EventId, *list.Events[1].EventId)

	actors, err := f.client.ListActors(t.Context(), &sdk.ListActorsInput{MemoryId: new(testMemory)})
	require.NoError(t, err)
	require.Len(t, actors.ActorSummaries, 2)
	assert.Equal(t, "alice", *actors.ActorSummaries[0].ActorId)

	sessions, err := f.client.ListSessions(t.Context(), &sdk.ListSessionsInput{
		MemoryId: new(testMemory), ActorId: new("alice"),
	})
	require.NoError(t, err)
	require.Len(t, sessions.SessionSummaries, 2)
	assert.True(t, sessions.SessionSummaries[0].CreatedAt.Equal(base))

	del, err := f.client.DeleteEvent(t.Context(), &sdk.DeleteEventInput{
		MemoryId: new(testMemory), ActorId: new("alice"), SessionId: new("s1"), EventId: e1.EventId,
	})
	require.NoError(t, err)
	assert.Equal(t, *e1.EventId, *del.EventId)

	_, err = f.client.GetEvent(t.Context(), &sdk.GetEventInput{
		MemoryId: new(testMemory), ActorId: new("alice"), SessionId: new("s1"), EventId: e1.EventId,
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestMemoryEvents_Pagination(t *testing.T) {
	t.Parallel()

	f := newFixture(t, defaultControl())
	base := f.clock.now()

	for i := range 5 {
		putEvent(t, f, "a", "s", "m", base.Add(time.Duration(i)*time.Second), nil)
	}

	var total int

	in := &sdk.ListEventsInput{
		MemoryId: new(testMemory), ActorId: new("a"), SessionId: new("s"), MaxResults: new(int32(2)),
	}

	for range 5 {
		out, err := f.client.ListEvents(t.Context(), in)
		require.NoError(t, err)

		total += len(out.Events)

		if out.NextToken == nil {
			break
		}

		in.NextToken = out.NextToken
	}

	assert.Equal(t, 5, total)
}

//go:fix inline
func TestMemoryEvents_Validation(t *testing.T) {
	t.Parallel()

	f := newFixture(t, defaultControl())
	now := f.clock.now()

	tests := []struct {
		name string
		mut  func(*sdk.CreateEventInput)
		mem  string
		code string
	}{
		{name: "unknown memory", mem: "nope", code: "ResourceNotFoundException"},
		{
			name: "bad role", mem: testMemory, code: "ValidationException",
			mut: func(in *sdk.CreateEventInput) {
				in.Payload = []types.PayloadType{&types.PayloadTypeMemberConversational{
					Value: types.Conversational{Role: "BOGUS", Content: &types.ContentMemberText{Value: "x"}},
				}}
			},
		},
		{
			name: "unknown branch root", mem: testMemory, code: "ResourceNotFoundException",
			mut: func(in *sdk.CreateEventInput) {
				in.Branch = &types.Branch{Name: new("b"), RootEventId: new("missing")}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := &sdk.CreateEventInput{
				MemoryId: &tt.mem, ActorId: new("a"), SessionId: new("s"), EventTimestamp: &now,
				Payload: []types.PayloadType{&types.PayloadTypeMemberConversational{
					Value: types.Conversational{Role: types.RoleUser, Content: &types.ContentMemberText{Value: "x"}},
				}},
			}
			if tt.mut != nil {
				tt.mut(in)
			}

			_, err := f.client.CreateEvent(t.Context(), in)
			assert.Equal(t, tt.code, apiCode(err))
		})
	}
}

func TestMemoryEvents_BranchesAndMetadata(t *testing.T) {
	t.Parallel()

	f := newFixture(t, defaultControl())
	base := f.clock.now()

	root := putEvent(t, f, "a", "s", "root", base, func(in *sdk.CreateEventInput) {
		in.Metadata = map[string]types.MetadataValue{"topic": &types.MetadataValueMemberStringValue{Value: "x"}}
	})
	putEvent(t, f, "a", "s", "main-later", base.Add(time.Minute), nil)
	putEvent(t, f, "a", "s", "fork", base.Add(2*time.Minute), func(in *sdk.CreateEventInput) {
		in.Branch = &types.Branch{Name: new("alt"), RootEventId: root.EventId}
	})

	count := func(filter *types.FilterInput) int {
		out, err := f.client.ListEvents(t.Context(), &sdk.ListEventsInput{
			MemoryId: new(testMemory), ActorId: new("a"), SessionId: new("s"), Filter: filter,
		})
		require.NoError(t, err)

		return len(out.Events)
	}

	assert.Equal(t, 3, count(nil))
	assert.Equal(t, 2, count(&types.FilterInput{Branch: &types.BranchFilter{Name: new("main")}}))
	assert.Equal(t, 1, count(&types.FilterInput{Branch: &types.BranchFilter{Name: new("alt")}}))
	assert.Equal(t, 2, count(&types.FilterInput{
		Branch: &types.BranchFilter{Name: new("alt"), IncludeParentBranches: aws.Bool(true)},
	}))
	assert.Equal(t, 1, count(&types.FilterInput{EventMetadata: []types.EventMetadataFilterExpression{{
		Left:     &types.LeftExpressionMemberMetadataKey{Value: "topic"},
		Operator: types.OperatorTypeEqualsTo,
		Right:    &types.RightExpressionMemberMetadataValue{Value: &types.MetadataValueMemberStringValue{Value: "x"}},
	}}}))
	assert.Equal(t, 2, count(&types.FilterInput{EventMetadata: []types.EventMetadataFilterExpression{{
		Left: &types.LeftExpressionMemberMetadataKey{Value: "topic"}, Operator: types.OperatorTypeNotExists,
	}}}))
}

func TestMemoryEvents_ClientTokenAndExpiry(t *testing.T) {
	t.Parallel()

	f := newFixture(t, defaultControl())
	base := f.clock.now()

	mut := func(in *sdk.CreateEventInput) { in.ClientToken = new("tok-1") }
	a := putEvent(t, f, "a", "s", "once", base, mut)
	b := putEvent(t, f, "a", "s", "once", base, mut)
	assert.Equal(t, *a.EventId, *b.EventId)

	f.clock.advance(31 * 24 * time.Hour)

	out, err := f.client.ListEvents(t.Context(), &sdk.ListEventsInput{
		MemoryId: new(testMemory), ActorId: new("a"), SessionId: new("s"),
	})
	require.NoError(t, err)
	assert.Empty(t, out.Events, "events older than eventExpiryDuration are gone")
}
