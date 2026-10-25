package ocistreaming_test

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/streaming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getMsgs(t *testing.T, c streaming.StreamClient, s streaming.Stream, cursor string, limit int) streaming.GetMessagesResponse {
	t.Helper()

	r := streaming.GetMessagesRequest{StreamId: s.Id, Cursor: &cursor}
	if limit > 0 {
		r.Limit = new(limit)
	}

	resp, err := c.GetMessages(t.Context(), r)
	require.NoError(t, err)

	return resp
}

func values(items []streaming.Message) []string {
	out := make([]string, 0, len(items))
	for _, m := range items {
		out = append(out, string(m.Value))
	}

	return out
}

func TestPutGetRoundTrip(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	s := e.mkStream(t, "rt", 4)
	c := e.msgs(t, s)

	first := putOne(t, c, s, "user-1", "v0")
	require.Nil(t, first.Error)
	require.NotNil(t, first.Partition)
	assert.Equal(t, int64(0), *first.Offset)
	assert.WithinDuration(t, e.clock.Now(), first.Timestamp.Time, time.Second)

	for i, v := range []string{"v1", "v2"} {
		next := putOne(t, c, s, "user-1", v)
		assert.Equal(t, *first.Partition, *next.Partition, "same key keeps its partition")
		assert.Equal(t, int64(i+1), *next.Offset)
	}

	cur := plainCursor(t, c, s, *first.Partition, streaming.CreateCursorDetailsTypeTrimHorizon, nil)
	resp := getMsgs(t, c, s, cur, 0)
	assert.Equal(t, []string{"v0", "v1", "v2"}, values(resp.Items))
	assert.Equal(t, "user-1", string(resp.Items[0].Key))
	assert.Equal(t, "rt", *resp.Items[0].Stream)
	assert.Equal(t, *first.Partition, *resp.Items[0].Partition)

	again := getMsgs(t, c, s, *resp.OpcNextCursor, 0)
	assert.Empty(t, again.Items)
	require.NotNil(t, again.OpcNextCursor)

	putOne(t, c, s, "user-1", "v3")
	tail := getMsgs(t, c, s, *again.OpcNextCursor, 0)
	assert.Equal(t, []string{"v3"}, values(tail.Items))
}

func TestKeylessSpreadsAcrossPartitions(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	s := e.mkStream(t, "spread", 3)
	c := e.msgs(t, s)
	seen := map[string]int{}

	for i := range 6 {
		seen[*putOne(t, c, s, "", "m"+strconv.Itoa(i)).Partition]++
	}

	assert.Equal(t, map[string]int{"0": 2, "1": 2, "2": 2}, seen)
}

func TestKeyPartitionIsStableMurmur2(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	s := e.mkStream(t, "stable", 8)
	c := e.msgs(t, s)

	a := putOne(t, c, s, "order-42", "x")
	b := putOne(t, c, s, "order-42", "y")
	other := e.mkStream(t, "stable2", 8)
	o := putOne(t, e.msgs(t, other), other, "order-42", "z")

	assert.Equal(t, *a.Partition, *b.Partition)
	assert.Equal(t, *a.Partition, *o.Partition, "partition depends only on key and partition count")
}

func TestCursorTypes(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	s := e.mkStream(t, "cursors", 1)
	c := e.msgs(t, s)

	var stamps []time.Time

	for _, v := range []string{"m0", "m1", "m2", "m3"} {
		r := putOne(t, c, s, "", v)
		stamps = append(stamps, r.Timestamp.Time)
		e.clock.Advance(10 * time.Second)
	}

	tests := []struct {
		mod  func(*streaming.CreateCursorDetails)
		name string
		typ  streaming.CreateCursorDetailsTypeEnum
		want []string
	}{
		{nil, "trim horizon", streaming.CreateCursorDetailsTypeTrimHorizon, []string{"m0", "m1", "m2", "m3"}},
		{nil, "latest", streaming.CreateCursorDetailsTypeLatest, []string{}},
		{func(d *streaming.CreateCursorDetails) { d.Offset = new(int64(2)) }, "at offset", streaming.CreateCursorDetailsTypeAtOffset, []string{"m2", "m3"}},
		{func(d *streaming.CreateCursorDetails) { d.Offset = new(int64(2)) }, "after offset", streaming.CreateCursorDetailsTypeAfterOffset, []string{"m3"}},
		{func(d *streaming.CreateCursorDetails) { d.Time = &common.SDKTime{Time: stamps[1].Add(time.Second)} }, "at time", streaming.CreateCursorDetailsTypeAtTime, []string{"m2", "m3"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cur := plainCursor(t, c, s, "0", tt.typ, tt.mod)
			assert.Equal(t, tt.want, values(getMsgs(t, c, s, cur, 0).Items))
		})
	}

	cur := plainCursor(t, c, s, "0", streaming.CreateCursorDetailsTypeTrimHorizon, nil)
	limited := getMsgs(t, c, s, cur, 2)
	assert.Equal(t, []string{"m0", "m1"}, values(limited.Items))
	assert.Equal(t, []string{"m2", "m3"}, values(getMsgs(t, c, s, *limited.OpcNextCursor, 2).Items))
}

func TestCursorAndGetErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	s := e.mkStream(t, "errs", 2)
	other := e.mkStream(t, "other", 2)
	c := e.msgs(t, s)
	putOne(t, c, s, "k", "v")
	foreign := plainCursor(t, c, other, "0", streaming.CreateCursorDetailsTypeTrimHorizon, nil)

	tests := []struct {
		call     func() error
		name     string
		wantCode string
		wantHTTP int
	}{
		{func() error {
			_, err := c.CreateCursor(t.Context(), streaming.CreateCursorRequest{StreamId: s.Id, CreateCursorDetails: streaming.CreateCursorDetails{Partition: new("9"), Type: streaming.CreateCursorDetailsTypeLatest}})
			return err
		}, "bad partition", "InvalidParameter", 400},
		{func() error {
			_, err := c.CreateCursor(t.Context(), streaming.CreateCursorRequest{StreamId: s.Id, CreateCursorDetails: streaming.CreateCursorDetails{Partition: new("0"), Type: streaming.CreateCursorDetailsTypeAtOffset}})
			return err
		}, "missing offset", "MissingParameter", 400},
		{func() error {
			_, err := c.CreateCursor(t.Context(), streaming.CreateCursorRequest{StreamId: s.Id, CreateCursorDetails: streaming.CreateCursorDetails{Partition: new("0"), Type: streaming.CreateCursorDetailsTypeAtOffset, Offset: new(int64(99))}})
			return err
		}, "offset out of range", "InvalidParameter", 400},
		{func() error {
			_, err := c.CreateCursor(t.Context(), streaming.CreateCursorRequest{StreamId: s.Id, CreateCursorDetails: streaming.CreateCursorDetails{Partition: new("0"), Type: streaming.CreateCursorDetailsTypeAtTime}})
			return err
		}, "missing time", "MissingParameter", 400},
		{func() error {
			_, err := c.GetMessages(t.Context(), streaming.GetMessagesRequest{StreamId: s.Id, Cursor: new("garbage")})
			return err
		}, "garbage cursor", "InvalidParameter", 400},
		{func() error {
			_, err := c.GetMessages(t.Context(), streaming.GetMessagesRequest{StreamId: s.Id, Cursor: &foreign})
			return err
		}, "foreign cursor", "InvalidParameter", 400},
		{func() error {
			_, err := c.GetMessages(t.Context(), streaming.GetMessagesRequest{StreamId: s.Id, Cursor: &foreign, Limit: new(10001)})
			return err
		}, "limit too high", "InvalidParameter", 400},
		{func() error {
			_, err := c.GetMessages(t.Context(), streaming.GetMessagesRequest{StreamId: new("ocid1.stream.oc1..none"), Cursor: &foreign})
			return err
		}, "unknown stream", "NotAuthorizedOrNotFound", 404},
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

func TestPutMessagesLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		entries    []streaming.PutMessagesDetailsEntry
		name       string
		wantCode   string
		wantHTTP   int
		wantFailed int
	}{
		{[]streaming.PutMessagesDetailsEntry{{Value: make([]byte, 1<<20+1)}}, "request over 1 MiB", "RequestEntityTooLarge", 413, 0},
		{[]streaming.PutMessagesDetailsEntry{{Value: []byte("ok")}, {Key: make([]byte, 257), Value: []byte("x")}}, "key over 256 bytes fails one entry", "", 200, 1},
		{[]streaming.PutMessagesDetailsEntry{{Value: make([]byte, 600<<10)}, {Value: make([]byte, 400<<10)}}, "within request limit", "", 200, 0},
		{[]streaming.PutMessagesDetailsEntry{}, "no messages", "MissingParameter", 400, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			s := e.mkStream(t, "limits", 4)
			c := e.msgs(t, s)

			resp, err := c.PutMessages(t.Context(), streaming.PutMessagesRequest{
				StreamId: s.Id, PutMessagesDetails: streaming.PutMessagesDetails{Messages: tt.entries},
			})
			if tt.wantHTTP != http.StatusOK {
				se := svcErr(t, err)
				assert.Equal(t, tt.wantHTTP, se.GetHTTPStatusCode())
				assert.Equal(t, tt.wantCode, se.GetCode())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantFailed, *resp.Failures)

			for _, en := range resp.Entries {
				if en.Error != nil {
					assert.NotEmpty(t, *en.ErrorMessage)
				} else {
					assert.NotNil(t, en.Offset)
				}
			}
		})
	}
}

func TestPartitionIngestThrottle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	s := e.mkStream(t, "hot", 1)
	c := e.msgs(t, s)
	big := streaming.PutMessagesDetailsEntry{Value: make([]byte, 700<<10)}
	put := func() error {
		_, err := c.PutMessages(t.Context(), streaming.PutMessagesRequest{
			StreamId: s.Id, PutMessagesDetails: streaming.PutMessagesDetails{Messages: []streaming.PutMessagesDetailsEntry{big}},
		})

		return err
	}

	require.NoError(t, put())
	se := svcErr(t, put())
	assert.Equal(t, http.StatusTooManyRequests, se.GetHTTPStatusCode())
	assert.Equal(t, "TooManyRequests", se.GetCode())

	e.clock.Advance(time.Second)
	require.NoError(t, put())

	cur := plainCursor(t, c, s, "0", streaming.CreateCursorDetailsTypeTrimHorizon, nil)
	assert.Len(t, getMsgs(t, c, s, cur, 0).Items, 2)
}

func TestRetentionDropsOldMessages(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	s := e.mkStream(t, "ret", 1)
	c := e.msgs(t, s)

	putOne(t, c, s, "", "old0")
	putOne(t, c, s, "", "old1")
	e.clock.Advance(20 * time.Hour)
	putOne(t, c, s, "", "mid")
	e.clock.Advance(5 * time.Hour)
	putOne(t, c, s, "", "new")

	cur := plainCursor(t, c, s, "0", streaming.CreateCursorDetailsTypeTrimHorizon, nil)
	assert.Equal(t, []string{"mid", "new"}, values(getMsgs(t, c, s, cur, 0).Items))

	_, err := c.CreateCursor(t.Context(), streaming.CreateCursorRequest{
		StreamId: s.Id, CreateCursorDetails: streaming.CreateCursorDetails{
			Partition: new("0"), Type: streaming.CreateCursorDetailsTypeAtOffset, Offset: new(int64(0)),
		},
	})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

	pre := plainCursor(t, c, s, "0", streaming.CreateCursorDetailsTypeAtOffset, func(d *streaming.CreateCursorDetails) { d.Offset = new(int64(2)) })
	e.clock.Advance(20 * time.Hour)
	assert.Equal(t, []string{"new"}, values(getMsgs(t, c, s, pre, 0).Items), "an existing cursor skips expired offsets")

	e.clock.Advance(30 * time.Hour)
	assert.Empty(t, getMsgs(t, c, s, pre, 0).Items)
	assert.Equal(t, int64(4), *putOne(t, c, s, "", "after").Offset, "offsets keep counting after expiry")
}

func TestPutToInactiveStream(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	created, err := e.admin.CreateStream(t.Context(), streaming.CreateStreamRequest{CreateStreamDetails: streaming.CreateStreamDetails{
		Name: new("early"), Partitions: new(1), CompartmentId: new(testCompartment),
	}})
	require.NoError(t, err)

	c := e.msgs(t, created.Stream)
	_, err = c.PutMessages(t.Context(), streaming.PutMessagesRequest{
		StreamId: created.Id, PutMessagesDetails: streaming.PutMessagesDetails{
			Messages: []streaming.PutMessagesDetailsEntry{{Value: []byte("x")}},
		},
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	e.settle()
	_, err = e.admin.DeleteStream(t.Context(), streaming.DeleteStreamRequest{StreamId: created.Id})
	require.NoError(t, err)
	e.settle()

	_, err = c.PutMessages(t.Context(), streaming.PutMessagesRequest{
		StreamId: created.Id, PutMessagesDetails: streaming.PutMessagesDetails{
			Messages: []streaming.PutMessagesDetailsEntry{{Value: []byte("x")}},
		},
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}
