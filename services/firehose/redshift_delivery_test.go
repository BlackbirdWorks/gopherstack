package firehose_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

// mockRedshiftDataExecutor captures ExecuteStatement calls for assertions.
type mockRedshiftDataExecutor struct {
	err   error
	calls []redshiftExecCall
	mu    sync.Mutex
}

type redshiftExecCall struct {
	sql, clusterIdentifier, database, dbUser string
}

func (m *mockRedshiftDataExecutor) ExecuteStatement(
	_ context.Context, sql, clusterIdentifier, database, dbUser string,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(
		m.calls,
		redshiftExecCall{
			sql:               sql,
			clusterIdentifier: clusterIdentifier,
			database:          database,
			dbUser:            dbUser,
		},
	)

	return m.err
}

func (m *mockRedshiftDataExecutor) captured() []redshiftExecCall {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]redshiftExecCall, len(m.calls))
	copy(out, m.calls)

	return out
}

func newRedshiftTestStream(t *testing.T, b *firehose.InMemoryBackend) *firehose.DeliveryStream {
	t.Helper()

	stream, err := b.CreateDeliveryStream(context.Background(), firehose.CreateDeliveryStreamInput{
		Name: "redshift-test-stream",
		RedshiftDestination: &firehose.RedshiftDestinationDescription{
			ClusterJDBCURL: "jdbc:redshift://mycluster.abc123.us-east-1.redshift.amazonaws.com:5439/mydb",
			Username:       "myuser",
			RoleARN:        "arn:aws:iam::000000000000:role/firehose-redshift",
			CopyCommand: &firehose.RedshiftCopyCommand{
				DataTableName:    "events",
				DataTableColumns: "col1,col2",
				CopyOptions:      "JSON 'auto'",
			},
			S3Destination: &firehose.S3DestinationDescription{
				BucketARN: "arn:aws:s3:::redshift-staging-bucket",
				Prefix:    "staging/",
			},
			RetryOptions: &firehose.RetryOptions{DurationInSeconds: 1},
		},
	})
	require.NoError(t, err)

	return stream
}

// TestFirehose_Redshift_Delivery verifies the two-hop S3-staging + COPY delivery model:
// records land in the destination's S3Configuration bucket first, then a COPY command
// referencing that staged object and the CopyCommand config is issued via the wired
// Redshift Data executor.
func TestFirehose_Redshift_Delivery(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	s3mock := &mockS3Storer{}
	rdMock := &mockRedshiftDataExecutor{}
	b.SetS3Backend(s3mock)
	b.SetRedshiftDataBackend(rdMock)

	stream := newRedshiftTestStream(t, b)

	require.NoError(t, b.PutRecord(context.Background(), stream.Name, []byte(`{"a":1}`)))
	b.FlushAll(context.Background())

	assert.Eventually(t, func() bool {
		return len(rdMock.captured()) > 0
	}, 3*time.Second, 20*time.Millisecond, "expected a COPY statement to be executed")

	require.NotEmpty(t, s3mock.calls, "expected records staged to S3 before COPY")
	stagedCall := s3mock.calls[0]
	assert.Equal(t, "redshift-staging-bucket", stagedCall.bucket)
	assert.True(t, strings.HasPrefix(stagedCall.key, "staging/"), "key=%q", stagedCall.key)
	assert.Contains(t, string(stagedCall.body), `{"a":1}`)

	calls := rdMock.captured()
	require.Len(t, calls, 1)
	assert.Equal(t, "mycluster", calls[0].clusterIdentifier)
	assert.Equal(t, "mydb", calls[0].database)
	assert.Equal(t, "myuser", calls[0].dbUser)
	assert.Contains(
		t,
		calls[0].sql,
		"COPY events (col1,col2) FROM 's3://redshift-staging-bucket/"+stagedCall.key+"'",
	)
	assert.Contains(
		t,
		calls[0].sql,
		"CREDENTIALS 'aws_iam_role=arn:aws:iam::000000000000:role/firehose-redshift'",
	)
	assert.Contains(t, calls[0].sql, "JSON 'auto'")
}

// TestFirehose_Redshift_Delivery_NoExecutorWired verifies that when no Redshift Data
// backend is wired (the cli.go wiring step this pass could not touch), staging to S3 still
// genuinely happens -- only the COPY step is a documented no-op, not a silent failure that
// also drops the staged data.
func TestFirehose_Redshift_Delivery_NoExecutorWired(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	s3mock := &mockS3Storer{}
	b.SetS3Backend(s3mock)

	stream := newRedshiftTestStream(t, b)

	require.NoError(t, b.PutRecord(context.Background(), stream.Name, []byte("row")))
	b.FlushAll(context.Background())

	assert.Eventually(t, func() bool {
		return len(s3mock.calls) > 0
	}, 3*time.Second, 20*time.Millisecond, "expected S3 staging even without a Redshift Data executor")
}

// TestFirehose_Redshift_Delivery_MissingS3Configuration verifies that a Redshift
// destination without the required S3 staging configuration never attempts delivery
// (matches real AWS, which requires S3Configuration on every RedshiftDestinationConfiguration).
func TestFirehose_Redshift_Delivery_MissingS3Configuration(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	rdMock := &mockRedshiftDataExecutor{}
	b.SetRedshiftDataBackend(rdMock)

	stream, err := b.CreateDeliveryStream(context.Background(), firehose.CreateDeliveryStreamInput{
		Name: "redshift-no-s3-stream",
		RedshiftDestination: &firehose.RedshiftDestinationDescription{
			ClusterJDBCURL: "jdbc:redshift://mycluster.abc123.us-east-1.redshift.amazonaws.com:5439/mydb",
			Username:       "myuser",
			CopyCommand: &firehose.RedshiftCopyCommand{
				DataTableName: "events",
			},
		},
	})
	require.NoError(t, err)

	require.NoError(t, b.PutRecord(context.Background(), stream.Name, []byte("row")))
	b.FlushAll(context.Background())

	require.Never(t, func() bool {
		return len(rdMock.captured()) > 0
	}, 200*time.Millisecond, 20*time.Millisecond, "must not attempt COPY without a staging location")
}
