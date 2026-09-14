package rdsdata_test

import (
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdsdatasdk "github.com/aws/aws-sdk-go-v2/service/rdsdata"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rdsdata"
)

// fakeClock is a goroutine-safe time source for rdsdata.InMemoryBackend.
// WithClock: a BeginTransaction/ExecuteStatement call and the test's own
// Advance both run on different goroutines (the httptest.Server handles the
// request on its own goroutine), so a bare closure over a plain time.Time
// would race under go test -race.
type fakeClock struct {
	cur time.Time
	mu  sync.Mutex
}

func newFakeClock() *fakeClock {
	return &fakeClock{cur: time.Now()}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cur
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cur = c.cur.Add(d)
}

// TestTransactionContext_RealClient covers gopherstack-wh8gv:
// sqlEngine.beginTx (engine.go) used to open its engine-side *sql.Tx against
// BeginTransaction's own per-request context. database/sql.DB.BeginTx
// documents "[t]he provided context is used until the transaction is
// committed or rolled back. If the context is canceled, the sql package will
// roll back the transaction" (database/sql/sql.go:1866-1868, go1.27 stdlib),
// and a real net/http.Server cancels that context the instant the
// BeginTransaction response finishes -- so a *separate*, later
// ExecuteStatement request against that transactionId silently hit
// sql.ErrTxDone, which the historical lenient fallback swallowed into a
// fabricated empty-success envelope instead of an error.
//
// Every case here drives a real aws-sdk-go-v2 client against a real
// httptest.Server, with each call as its own separate HTTP request, so the
// bug (had it still been present) would actually reproduce -- unlike this
// package's doRDSDataRequest-based transaction tests, whose
// httptest.NewRequest carries a never-canceled context.Background() and so
// could not have caught it.
func TestTransactionContext_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *rdsdatasdk.Client, backend *rdsdata.InMemoryBackend)
		name string
	}{
		{name: "commit makes writes visible", run: testTxCommitMakesWritesVisible},
		{name: "rollback discards writes", run: testTxRollbackDiscardsWrites},
		{name: "unknown transaction id", run: testTxUnknownID},
		{name: "execute after commit", run: testTxExecuteAfterCommit},
		{name: "idle timeout", run: testTxIdleTimeout},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
			client := newRoundTripClient(t, rdsdata.NewHandler(backend))

			tt.run(t, client, backend)
		})
	}
}

func testTxCommitMakesWritesVisible(t *testing.T, client *rdsdatasdk.Client, _ *rdsdata.InMemoryBackend) {
	t.Helper()

	ctx := t.Context()

	_, err := client.ExecuteStatement(ctx, &rdsdatasdk.ExecuteStatementInput{
		ResourceArn: aws.String(txnResourceARN),
		SecretArn:   aws.String(txnSecretARN),
		Sql:         aws.String("CREATE TABLE widgets (id INTEGER PRIMARY KEY, val TEXT)"),
	})
	require.NoError(t, err)

	begin, err := client.BeginTransaction(ctx, &rdsdatasdk.BeginTransactionInput{
		ResourceArn: aws.String(txnResourceARN),
		SecretArn:   aws.String(txnSecretARN),
	})
	require.NoError(t, err)

	txID := aws.ToString(begin.TransactionId)

	insert, err := client.ExecuteStatement(ctx, &rdsdatasdk.ExecuteStatementInput{
		ResourceArn:   aws.String(txnResourceARN),
		SecretArn:     aws.String(txnSecretARN),
		Sql:           aws.String("INSERT INTO widgets (id, val) VALUES (1, 'a')"),
		TransactionId: aws.String(txID),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), insert.NumberOfRecordsUpdated,
		"the insert must actually run against a live engine transaction, not a silently-dead one")

	_, err = client.CommitTransaction(ctx, &rdsdatasdk.CommitTransactionInput{
		ResourceArn:   aws.String(txnResourceARN),
		SecretArn:     aws.String(txnSecretARN),
		TransactionId: aws.String(txID),
	})
	require.NoError(t, err)

	sel, err := client.ExecuteStatement(ctx, &rdsdatasdk.ExecuteStatementInput{
		ResourceArn: aws.String(txnResourceARN),
		SecretArn:   aws.String(txnSecretARN),
		Sql:         aws.String("SELECT val FROM widgets WHERE id = 1"),
	})
	require.NoError(t, err)
	require.Len(t, sel.Records, 1, "committed insert must be visible to a later, separate autocommit SELECT")

	val, ok := sel.Records[0][0].(*types.FieldMemberStringValue)
	require.True(t, ok)
	assert.Equal(t, "a", val.Value)
}

func testTxRollbackDiscardsWrites(t *testing.T, client *rdsdatasdk.Client, _ *rdsdata.InMemoryBackend) {
	t.Helper()

	ctx := t.Context()

	_, err := client.ExecuteStatement(ctx, &rdsdatasdk.ExecuteStatementInput{
		ResourceArn: aws.String(txnResourceARN),
		SecretArn:   aws.String(txnSecretARN),
		Sql:         aws.String("CREATE TABLE gadgets (id INTEGER PRIMARY KEY, val TEXT)"),
	})
	require.NoError(t, err)

	begin, err := client.BeginTransaction(ctx, &rdsdatasdk.BeginTransactionInput{
		ResourceArn: aws.String(txnResourceARN),
		SecretArn:   aws.String(txnSecretARN),
	})
	require.NoError(t, err)

	txID := aws.ToString(begin.TransactionId)

	insert, err := client.ExecuteStatement(ctx, &rdsdatasdk.ExecuteStatementInput{
		ResourceArn:   aws.String(txnResourceARN),
		SecretArn:     aws.String(txnSecretARN),
		Sql:           aws.String("INSERT INTO gadgets (id, val) VALUES (1, 'a')"),
		TransactionId: aws.String(txID),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), insert.NumberOfRecordsUpdated,
		"the insert must actually run against a live engine transaction before it's rolled back")

	_, err = client.RollbackTransaction(ctx, &rdsdatasdk.RollbackTransactionInput{
		ResourceArn:   aws.String(txnResourceARN),
		SecretArn:     aws.String(txnSecretARN),
		TransactionId: aws.String(txID),
	})
	require.NoError(t, err)

	sel, err := client.ExecuteStatement(ctx, &rdsdatasdk.ExecuteStatementInput{
		ResourceArn: aws.String(txnResourceARN),
		SecretArn:   aws.String(txnSecretARN),
		Sql:         aws.String("SELECT val FROM gadgets WHERE id = 1"),
	})
	require.NoError(t, err)
	assert.Empty(t, sel.Records, "rolled-back insert must not be visible to a later autocommit SELECT")
}

func testTxUnknownID(t *testing.T, client *rdsdatasdk.Client, _ *rdsdata.InMemoryBackend) {
	t.Helper()

	_, err := client.ExecuteStatement(t.Context(), &rdsdatasdk.ExecuteStatementInput{
		ResourceArn:   aws.String(txnResourceARN),
		SecretArn:     aws.String(txnSecretARN),
		Sql:           aws.String("SELECT 1"),
		TransactionId: aws.String("txn-999999"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "TransactionNotFoundException", apiErr.ErrorCode())
}

func testTxExecuteAfterCommit(t *testing.T, client *rdsdatasdk.Client, _ *rdsdata.InMemoryBackend) {
	t.Helper()

	ctx := t.Context()

	begin, err := client.BeginTransaction(ctx, &rdsdatasdk.BeginTransactionInput{
		ResourceArn: aws.String(txnResourceARN),
		SecretArn:   aws.String(txnSecretARN),
	})
	require.NoError(t, err)

	txID := aws.ToString(begin.TransactionId)

	_, err = client.CommitTransaction(ctx, &rdsdatasdk.CommitTransactionInput{
		ResourceArn:   aws.String(txnResourceARN),
		SecretArn:     aws.String(txnSecretARN),
		TransactionId: aws.String(txID),
	})
	require.NoError(t, err)

	_, err = client.ExecuteStatement(ctx, &rdsdatasdk.ExecuteStatementInput{
		ResourceArn:   aws.String(txnResourceARN),
		SecretArn:     aws.String(txnSecretARN),
		Sql:           aws.String("SELECT 1"),
		TransactionId: aws.String(txID),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "TransactionNotFoundException", apiErr.ErrorCode())
}

// testTxIdleTimeout drives BeginTransaction's documented idle timeout
// (rdsdata@v1.35.4 api_op_BeginTransaction.go: "[a] transaction times out if
// no calls use its transaction ID in three minutes"; also
// https://docs.aws.amazon.com/rdsdataservice/latest/APIReference/API_BeginTransaction.html)
// via backend.WithClock plus a direct Janitor.SweepOnce, instead of a real
// three-minute wall-clock wait or a background goroutine.
func testTxIdleTimeout(t *testing.T, client *rdsdatasdk.Client, backend *rdsdata.InMemoryBackend) {
	t.Helper()

	ctx := t.Context()

	clock := newFakeClock()
	backend.WithClock(clock.Now)

	begin, err := client.BeginTransaction(ctx, &rdsdatasdk.BeginTransactionInput{
		ResourceArn: aws.String(txnResourceARN),
		SecretArn:   aws.String(txnSecretARN),
	})
	require.NoError(t, err)

	txID := aws.ToString(begin.TransactionId)

	clock.Advance(4 * time.Minute)

	janitor := rdsdata.NewJanitor(backend, time.Minute, 3*time.Minute, 24*time.Hour)
	janitor.SweepOnce(ctx)

	_, err = client.ExecuteStatement(ctx, &rdsdatasdk.ExecuteStatementInput{
		ResourceArn:   aws.String(txnResourceARN),
		SecretArn:     aws.String(txnSecretARN),
		Sql:           aws.String("SELECT 1"),
		TransactionId: aws.String(txID),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "TransactionNotFoundException", apiErr.ErrorCode())
}
