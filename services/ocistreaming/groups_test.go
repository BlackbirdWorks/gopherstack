package ocistreaming_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/streaming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func groupCursor(
	t *testing.T, c streaming.StreamClient, s streaming.Stream, group, instance string,
	mod func(*streaming.CreateGroupCursorDetails),
) string {
	t.Helper()

	d := streaming.CreateGroupCursorDetails{
		Type: streaming.CreateGroupCursorDetailsTypeTrimHorizon, GroupName: new(group), InstanceName: new(instance),
	}
	if mod != nil {
		mod(&d)
	}

	resp, err := c.CreateGroupCursor(t.Context(), streaming.CreateGroupCursorRequest{StreamId: s.Id, CreateGroupCursorDetails: d})
	require.NoError(t, err)

	return *resp.Value
}

func TestGroupCommitOnGet(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	s := e.mkStream(t, "g1", 1)
	c := e.msgs(t, s)

	for _, v := range []string{"a", "b", "c"} {
		putOne(t, c, s, "", v)
	}

	cur := groupCursor(t, c, s, "workers", "i1", nil)
	first := getMsgs(t, c, s, cur, 2)
	assert.Equal(t, []string{"a", "b"}, values(first.Items))

	rest := getMsgs(t, c, s, *first.OpcNextCursor, 2)
	assert.Equal(t, []string{"c"}, values(rest.Items))

	restarted := groupCursor(t, c, s, "workers", "i1", func(d *streaming.CreateGroupCursorDetails) {
		d.Type = streaming.CreateGroupCursorDetailsTypeLatest
	})
	assert.Empty(t, getMsgs(t, c, s, restarted, 0).Items, "committed offsets survive; type only applies at group creation")

	grp, err := c.GetGroup(t.Context(), streaming.GetGroupRequest{StreamId: s.Id, GroupName: new("workers")})
	require.NoError(t, err)
	require.Len(t, grp.Reservations, 1)
	assert.Equal(t, int64(2), *grp.Reservations[0].CommittedOffset)
	assert.Equal(t, "i1", *grp.Reservations[0].ReservedInstance)
	assert.Equal(t, "0", *grp.Reservations[0].Partition)
	assert.True(t, grp.Reservations[0].TimeReservedUntil.After(e.clock.Now()))
}

func TestGroupManualCommit(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	s := e.mkStream(t, "g2", 1)
	c := e.msgs(t, s)

	for _, v := range []string{"a", "b"} {
		putOne(t, c, s, "", v)
	}

	manual := func(d *streaming.CreateGroupCursorDetails) { d.CommitOnGet = new(false) }
	cur := groupCursor(t, c, s, "g", "i1", manual)

	got := getMsgs(t, c, s, cur, 0)
	assert.Equal(t, []string{"a", "b"}, values(got.Items))

	fresh := groupCursor(t, c, s, "g", "i1", manual)
	assert.Equal(t, []string{"a", "b"}, values(getMsgs(t, c, s, fresh, 0).Items), "uncommitted reads are redelivered")

	committed, err := c.ConsumerCommit(t.Context(), streaming.ConsumerCommitRequest{StreamId: s.Id, Cursor: got.OpcNextCursor})
	require.NoError(t, err)

	after := getMsgs(t, c, s, *committed.Value, 0)
	assert.Empty(t, after.Items)

	again := groupCursor(t, c, s, "g", "i1", manual)
	assert.Empty(t, getMsgs(t, c, s, again, 0).Items)
}

func TestGroupPartitionAssignment(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	s := e.mkStream(t, "g3", 4)
	c := e.msgs(t, s)

	for _, v := range []string{"a", "b", "c", "d"} {
		putOne(t, c, s, "", v)
	}

	c1 := groupCursor(t, c, s, "g", "one", nil)
	c2 := groupCursor(t, c, s, "g", "two", nil)

	grp, err := c.GetGroup(t.Context(), streaming.GetGroupRequest{StreamId: s.Id, GroupName: new("g")})
	require.NoError(t, err)

	owners := map[string]int{}
	for _, r := range grp.Reservations {
		if r.ReservedInstance != nil {
			owners[*r.ReservedInstance]++
		}
	}

	assert.Equal(t, map[string]int{"one": 4}, owners, "the first instance holds everything until it rebalances")

	_, err = c.ConsumerHeartbeat(t.Context(), streaming.ConsumerHeartbeatRequest{StreamId: s.Id, Cursor: &c1})
	require.NoError(t, err)
	_, err = c.ConsumerHeartbeat(t.Context(), streaming.ConsumerHeartbeatRequest{StreamId: s.Id, Cursor: &c2})
	require.NoError(t, err)

	grp, err = c.GetGroup(t.Context(), streaming.GetGroupRequest{StreamId: s.Id, GroupName: new("g")})
	require.NoError(t, err)

	owners = map[string]int{}
	for _, r := range grp.Reservations {
		owners[*r.ReservedInstance]++
	}

	assert.Equal(t, map[string]int{"one": 2, "two": 2}, owners)

	var got []string

	for _, cur := range []string{c1, c2} {
		for range 2 {
			got = append(got, values(getMsgs(t, c, s, cur, 0).Items)...)
		}
	}

	assert.ElementsMatch(t, []string{"a", "b", "c", "d"}, got)
}

func TestGroupInstanceTimeout(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	s := e.mkStream(t, "g4", 2)
	c := e.msgs(t, s)

	short := func(d *streaming.CreateGroupCursorDetails) { d.TimeoutInMs = new(5000) }
	groupCursor(t, c, s, "g", "dead", short)

	e.clock.Advance(10 * time.Second)

	live := groupCursor(t, c, s, "g", "alive", short)

	grp, err := c.GetGroup(t.Context(), streaming.GetGroupRequest{StreamId: s.Id, GroupName: new("g")})
	require.NoError(t, err)

	for _, r := range grp.Reservations {
		require.NotNil(t, r.ReservedInstance)
		assert.Equal(t, "alive", *r.ReservedInstance)
	}

	_, err = c.ConsumerHeartbeat(t.Context(), streaming.ConsumerHeartbeatRequest{StreamId: s.Id, Cursor: &live})
	require.NoError(t, err)
}

func TestUpdateGroupResetsOffsets(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	s := e.mkStream(t, "g5", 1)
	c := e.msgs(t, s)

	for _, v := range []string{"a", "b"} {
		putOne(t, c, s, "", v)
	}

	cur := groupCursor(t, c, s, "g", "i", nil)
	assert.Len(t, getMsgs(t, c, s, cur, 0).Items, 2)
	assert.Empty(t, getMsgs(t, c, s, cur, 0).Items)

	_, err := c.UpdateGroup(t.Context(), streaming.UpdateGroupRequest{
		StreamId: s.Id, GroupName: new("g"),
		UpdateGroupDetails: streaming.UpdateGroupDetails{Type: streaming.UpdateGroupDetailsTypeTrimHorizon},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, values(getMsgs(t, c, s, cur, 0).Items))

	_, err = c.UpdateGroup(t.Context(), streaming.UpdateGroupRequest{
		StreamId: s.Id, GroupName: new("g"),
		UpdateGroupDetails: streaming.UpdateGroupDetails{Type: streaming.UpdateGroupDetailsTypeLatest},
	})
	require.NoError(t, err)
	assert.Empty(t, getMsgs(t, c, s, cur, 0).Items)
}

func TestGroupErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	s := e.mkStream(t, "g6", 1)
	c := e.msgs(t, s)
	plain := plainCursor(t, c, s, "0", streaming.CreateCursorDetailsTypeLatest, nil)

	tests := []struct {
		call     func() error
		name     string
		wantCode string
		wantHTTP int
	}{
		{func() error {
			_, err := c.GetGroup(t.Context(), streaming.GetGroupRequest{StreamId: s.Id, GroupName: new("nope")})
			return err
		}, "unknown group", "NotAuthorizedOrNotFound", http.StatusNotFound},
		{func() error {
			_, err := c.CreateGroupCursor(t.Context(), streaming.CreateGroupCursorRequest{StreamId: s.Id, CreateGroupCursorDetails: streaming.CreateGroupCursorDetails{Type: streaming.CreateGroupCursorDetailsTypeLatest}})
			return err
		}, "missing group name", "MissingParameter", http.StatusBadRequest},
		{func() error {
			_, err := c.CreateGroupCursor(t.Context(), streaming.CreateGroupCursorRequest{StreamId: s.Id, CreateGroupCursorDetails: streaming.CreateGroupCursorDetails{GroupName: new("g"), Type: streaming.CreateGroupCursorDetailsTypeAtTime}})
			return err
		}, "at time without time", "MissingParameter", http.StatusBadRequest},
		{func() error {
			_, err := c.ConsumerCommit(t.Context(), streaming.ConsumerCommitRequest{StreamId: s.Id, Cursor: &plain})
			return err
		}, "commit plain cursor", "InvalidParameter", http.StatusBadRequest},
		{func() error {
			_, err := c.ConsumerHeartbeat(t.Context(), streaming.ConsumerHeartbeatRequest{StreamId: s.Id, Cursor: &plain})
			return err
		}, "heartbeat plain cursor", "InvalidParameter", http.StatusBadRequest},
		{func() error {
			_, err := c.UpdateGroup(t.Context(), streaming.UpdateGroupRequest{StreamId: s.Id, GroupName: new("nope"), UpdateGroupDetails: streaming.UpdateGroupDetails{Type: streaming.UpdateGroupDetailsTypeLatest}})
			return err
		}, "update unknown group", "NotAuthorizedOrNotFound", http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			se := svcErr(t, tt.call())
			assert.Equal(t, tt.wantHTTP, se.GetHTTPStatusCode())
			assert.Equal(t, tt.wantCode, se.GetCode())
		})
	}
}
