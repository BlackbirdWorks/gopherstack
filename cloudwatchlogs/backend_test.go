package cloudwatchlogs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/cloudwatchlogs"
)

func newBackend() *cloudwatchlogs.InMemoryBackend {
	return cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
}

func TestCreateAndDescribeLogGroup(t *testing.T) {
	t.Parallel()
	b := newBackend()

	g, err := b.CreateLogGroup("/my/group")
	require.NoError(t, err)
	assert.Equal(t, "/my/group", g.LogGroupName)
	assert.Contains(t, g.Arn, "/my/group")
}

func TestCreateLogGroup_AlreadyExists(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateLogGroup("dup-group")
	require.NoError(t, err)

	_, err = b.CreateLogGroup("dup-group")
	require.ErrorIs(t, err, cloudwatchlogs.ErrLogGroupAlreadyExists)
}

func TestDeleteLogGroup(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateLogGroup("to-delete")
	require.NoError(t, err)

	err = b.DeleteLogGroup("to-delete")
	require.NoError(t, err)

	groups, _, err := b.DescribeLogGroups("", "", 0)
	require.NoError(t, err)
	assert.Empty(t, groups)
}

func TestDeleteLogGroup_NotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()

	err := b.DeleteLogGroup("nonexistent")
	require.ErrorIs(t, err, cloudwatchlogs.ErrLogGroupNotFound)
}

func TestDescribeLogGroups_Prefix(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _ = b.CreateLogGroup("/prod/app")
	_, _ = b.CreateLogGroup("/dev/app")

	groups, next, err := b.DescribeLogGroups("/prod", "", 0)
	require.NoError(t, err)
	assert.Empty(t, next)
	assert.Len(t, groups, 1)
	assert.Equal(t, "/prod/app", groups[0].LogGroupName)
}

func TestDescribeLogGroups_Pagination(t *testing.T) {
	t.Parallel()
	b := newBackend()

	for i := range 5 {
		_, _ = b.CreateLogGroup("/group/" + string(rune('a'+i)))
	}

	page1, token, err := b.DescribeLogGroups("", "", 2)
	require.NoError(t, err)
	assert.Len(t, page1, 2)
	assert.NotEmpty(t, token)

	page2, token2, err := b.DescribeLogGroups("", token, 2)
	require.NoError(t, err)
	assert.Len(t, page2, 2)
	assert.NotEmpty(t, token2)

	page3, token3, err := b.DescribeLogGroups("", token2, 2)
	require.NoError(t, err)
	assert.Len(t, page3, 1)
	assert.Empty(t, token3)
}

func TestDescribeLogGroups_BeyondEnd(t *testing.T) {
	t.Parallel()
	b := newBackend()
	_, _ = b.CreateLogGroup("/group/a")

	groups, token, err := b.DescribeLogGroups("", "999", 10)
	require.NoError(t, err)
	assert.Empty(t, groups)
	assert.Empty(t, token)
}

func TestCreateLogStream(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _ = b.CreateLogGroup("my-group")
	s, err := b.CreateLogStream("my-group", "my-stream")
	require.NoError(t, err)
	assert.Equal(t, "my-stream", s.LogStreamName)
	assert.Contains(t, s.Arn, "my-group")
	assert.Contains(t, s.Arn, "my-stream")
}

func TestCreateLogStream_GroupNotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateLogStream("nonexistent", "stream")
	require.ErrorIs(t, err, cloudwatchlogs.ErrLogGroupNotFound)
}

func TestCreateLogStream_AlreadyExists(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _ = b.CreateLogGroup("grp")
	_, _ = b.CreateLogStream("grp", "dup")
	_, err := b.CreateLogStream("grp", "dup")
	require.ErrorIs(t, err, cloudwatchlogs.ErrLogStreamAlreadyExist)
}

func TestDescribeLogStreams(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _ = b.CreateLogGroup("grp")
	_, _ = b.CreateLogStream("grp", "stream-a")
	_, _ = b.CreateLogStream("grp", "stream-b")

	streams, next, err := b.DescribeLogStreams("grp", "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, next)
	assert.Len(t, streams, 2)
}

func TestDescribeLogStreams_Prefix(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _ = b.CreateLogGroup("grp")
	_, _ = b.CreateLogStream("grp", "prod-stream")
	_, _ = b.CreateLogStream("grp", "dev-stream")

	streams, _, err := b.DescribeLogStreams("grp", "prod", "", 0)
	require.NoError(t, err)
	assert.Len(t, streams, 1)
	assert.Equal(t, "prod-stream", streams[0].LogStreamName)
}

func TestDescribeLogStreams_GroupNotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _, err := b.DescribeLogStreams("nonexistent", "", "", 0)
	require.ErrorIs(t, err, cloudwatchlogs.ErrLogGroupNotFound)
}

func TestPutLogEvents(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _ = b.CreateLogGroup("grp")
	_, _ = b.CreateLogStream("grp", "stream")

	events := []cloudwatchlogs.InputLogEvent{
		{Message: "first", Timestamp: 1000},
		{Message: "second", Timestamp: 2000},
	}
	token, err := b.PutLogEvents("grp", "stream", events)
	require.NoError(t, err)
	assert.NotEmpty(t, token)
}

func TestPutLogEvents_GroupNotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutLogEvents("nonexistent", "stream", nil)
	require.ErrorIs(t, err, cloudwatchlogs.ErrLogGroupNotFound)
}

func TestPutLogEvents_StreamNotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _ = b.CreateLogGroup("grp")
	_, err := b.PutLogEvents("grp", "nonexistent", nil)
	require.ErrorIs(t, err, cloudwatchlogs.ErrLogStreamNotFound)
}

func TestGetLogEvents(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _ = b.CreateLogGroup("grp")
	_, _ = b.CreateLogStream("grp", "stream")
	_, _ = b.PutLogEvents("grp", "stream", []cloudwatchlogs.InputLogEvent{
		{Message: "msg1", Timestamp: 1000},
		{Message: "msg2", Timestamp: 2000},
		{Message: "msg3", Timestamp: 3000},
	})

	evts, fwd, bwd, err := b.GetLogEvents("grp", "stream", nil, nil, 0, "")
	require.NoError(t, err)
	assert.Len(t, evts, 3)
	assert.NotEmpty(t, fwd)
	assert.NotEmpty(t, bwd)
}

func TestGetLogEvents_TimeFilter(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _ = b.CreateLogGroup("grp")
	_, _ = b.CreateLogStream("grp", "stream")
	_, _ = b.PutLogEvents("grp", "stream", []cloudwatchlogs.InputLogEvent{
		{Message: "old", Timestamp: 100},
		{Message: "new", Timestamp: 5000},
	})

	start := int64(1000)
	evts, _, _, err := b.GetLogEvents("grp", "stream", &start, nil, 0, "")
	require.NoError(t, err)
	assert.Len(t, evts, 1)
	assert.Equal(t, "new", evts[0].Message)
}

func TestGetLogEvents_Pagination(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _ = b.CreateLogGroup("grp")
	_, _ = b.CreateLogStream("grp", "stream")
	_, _ = b.PutLogEvents("grp", "stream", []cloudwatchlogs.InputLogEvent{
		{Message: "a", Timestamp: 1},
		{Message: "b", Timestamp: 2},
		{Message: "c", Timestamp: 3},
	})

	evts, fwd, _, err := b.GetLogEvents("grp", "stream", nil, nil, 2, "")
	require.NoError(t, err)
	assert.Len(t, evts, 2)

	evts2, _, _, err := b.GetLogEvents("grp", "stream", nil, nil, 2, fwd)
	require.NoError(t, err)
	assert.Len(t, evts2, 1)
}

func TestGetLogEvents_GroupNotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _, _, err := b.GetLogEvents("nonexistent", "stream", nil, nil, 0, "")
	require.ErrorIs(t, err, cloudwatchlogs.ErrLogGroupNotFound)
}

func TestGetLogEvents_StreamNotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _ = b.CreateLogGroup("grp")
	_, _, _, err := b.GetLogEvents("grp", "nonexistent", nil, nil, 0, "")
	require.ErrorIs(t, err, cloudwatchlogs.ErrLogStreamNotFound)
}

func TestFilterLogEvents(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _ = b.CreateLogGroup("grp")
	_, _ = b.CreateLogStream("grp", "s1")
	_, _ = b.CreateLogStream("grp", "s2")
	_, _ = b.PutLogEvents("grp", "s1", []cloudwatchlogs.InputLogEvent{
		{Message: "ERROR: something bad", Timestamp: 1000},
	})
	_, _ = b.PutLogEvents("grp", "s2", []cloudwatchlogs.InputLogEvent{
		{Message: "INFO: all good", Timestamp: 2000},
	})

	evts, _, err := b.FilterLogEvents("grp", nil, "ERROR", nil, nil, 0, "")
	require.NoError(t, err)
	assert.Len(t, evts, 1)
	assert.Equal(t, "ERROR: something bad", evts[0].Message)
}

func TestFilterLogEvents_StreamFilter(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _ = b.CreateLogGroup("grp")
	_, _ = b.CreateLogStream("grp", "s1")
	_, _ = b.CreateLogStream("grp", "s2")
	_, _ = b.PutLogEvents("grp", "s1", []cloudwatchlogs.InputLogEvent{
		{Message: "from s1", Timestamp: 1000},
	})
	_, _ = b.PutLogEvents("grp", "s2", []cloudwatchlogs.InputLogEvent{
		{Message: "from s2", Timestamp: 2000},
	})

	evts, _, err := b.FilterLogEvents("grp", []string{"s1"}, "", nil, nil, 0, "")
	require.NoError(t, err)
	assert.Len(t, evts, 1)
	assert.Equal(t, "from s1", evts[0].Message)
}

func TestFilterLogEvents_Pagination(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _ = b.CreateLogGroup("grp")
	_, _ = b.CreateLogStream("grp", "s")
	for i := range 5 {
		_, _ = b.PutLogEvents("grp", "s", []cloudwatchlogs.InputLogEvent{
			{Message: "msg", Timestamp: int64(i * 100)},
		})
	}

	evts, token, err := b.FilterLogEvents("grp", nil, "", nil, nil, 2, "")
	require.NoError(t, err)
	assert.Len(t, evts, 2)
	assert.NotEmpty(t, token)

	evts2, _, err := b.FilterLogEvents("grp", nil, "", nil, nil, 10, token)
	require.NoError(t, err)
	assert.Len(t, evts2, 3)
}

func TestFilterLogEvents_GroupNotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _, err := b.FilterLogEvents("nonexistent", nil, "", nil, nil, 0, "")
	require.ErrorIs(t, err, cloudwatchlogs.ErrLogGroupNotFound)
}

func TestPutLogEvents_UpdatesTimestamps(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _ = b.CreateLogGroup("grp")
	_, _ = b.CreateLogStream("grp", "s")

	_, _ = b.PutLogEvents("grp", "s", []cloudwatchlogs.InputLogEvent{
		{Message: "a", Timestamp: 500},
		{Message: "b", Timestamp: 1500},
	})

	streams, _, err := b.DescribeLogStreams("grp", "", "", 0)
	require.NoError(t, err)
	require.Len(t, streams, 1)
	require.NotNil(t, streams[0].FirstEventTimestamp)
	require.NotNil(t, streams[0].LastEventTimestamp)
	assert.Equal(t, int64(500), *streams[0].FirstEventTimestamp)
	assert.Equal(t, int64(1500), *streams[0].LastEventTimestamp)
}

func TestFilterLogEvents_TimeFilter(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _ = b.CreateLogGroup("grp")
	_, _ = b.CreateLogStream("grp", "s")
	_, _ = b.PutLogEvents("grp", "s", []cloudwatchlogs.InputLogEvent{
		{Message: "old", Timestamp: 100},
		{Message: "new", Timestamp: 9000},
	})

	start := int64(1000)
	end := int64(10000)
	evts, _, err := b.FilterLogEvents("grp", nil, "", &start, &end, 0, "")
	require.NoError(t, err)
	assert.Len(t, evts, 1)
	assert.Equal(t, "new", evts[0].Message)
}
