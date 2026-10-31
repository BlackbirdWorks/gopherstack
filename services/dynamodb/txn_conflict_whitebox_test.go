package dynamodb

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

type conflictMetrics struct {
	counts map[string]int
	mu     sync.Mutex
}

func (r *conflictMetrics) EmitMetric(p cwmetric.Point) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.counts == nil {
		r.counts = map[string]int{}
	}

	table := ""
	for _, d := range p.Dimensions {
		if d.Name == "TableName" {
			table = d.Value
		}
	}

	r.counts[p.Name+"/"+table]++

	return nil
}

func (r *conflictMetrics) conflicts() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.counts["TransactionConflict/tbl"]
}

type countingTransport struct {
	next  http.RoundTripper
	count *atomic.Int64
}

func (c countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.count.Add(1)

	return c.next.RoundTrip(r)
}

type conflictFixture struct {
	db       *InMemoryDB
	client   *sdk.Client
	metrics  *conflictMetrics
	requests *atomic.Int64
	tb       testing.TB
}

func s(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }

func key(pk string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"pk": s(pk)}
}

func item(pk, v string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"pk": s(pk), "v": s(v)}
}

func newConflictFixture(t *testing.T, retryer func() aws.Retryer) *conflictFixture {
	t.Helper()

	db := NewInMemoryDB()
	db.SetDefaultRegion("us-east-1")

	f := &conflictFixture{tb: t, db: db, metrics: &conflictMetrics{}, requests: &atomic.Int64{}}
	db.SetMetricEmitter(f.metrics)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(NewHandler(db)))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	hc := testclient.New(t)
	hc.Transport = countingTransport{next: hc.Transport, count: f.requests}

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awscfg.WithHTTPClient(hc),
	)
	require.NoError(t, err)

	f.client = sdk.NewFromConfig(cfg, func(o *sdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.Retryer = retryer()
	})

	for _, name := range []string{"tbl", "other"} {
		_, err = f.client.CreateTable(t.Context(), &sdk.CreateTableInput{
			TableName: aws.String(name),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			},
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			},
			BillingMode: types.BillingModePayPerRequest,
		})
		require.NoError(t, err)
	}

	for _, pk := range []string{"a", "b"} {
		_, err = f.client.PutItem(t.Context(), &sdk.PutItemInput{TableName: aws.String("tbl"), Item: item(pk, "old")})
		require.NoError(t, err)
	}

	return f
}

func noRetry() aws.Retryer { return aws.NopRetryer{} }

// gate parks the first matching in-flight transaction until release is closed.
type gate struct {
	entered chan struct{}
	release chan struct{}
	taken   atomic.Bool
}

func (f *conflictFixture) hold(op string, step int) *gate {
	g := &gate{entered: make(chan struct{}), release: make(chan struct{})}
	f.db.txnHook = func(ev txnHookEvent) {
		if ev.Op == op && ev.Step == step && g.taken.CompareAndSwap(false, true) {
			close(g.entered)
			<-g.release
		}
	}

	return g
}

func (f *conflictFixture) twi(items ...types.TransactWriteItem) *sdk.TransactWriteItemsInput {
	return &sdk.TransactWriteItemsInput{TransactItems: items}
}

func putAction(pk, v string) types.TransactWriteItem {
	return types.TransactWriteItem{Put: &types.Put{TableName: aws.String("tbl"), Item: item(pk, v)}}
}

func tgi(pks ...string) *sdk.TransactGetItemsInput {
	in := &sdk.TransactGetItemsInput{}
	for _, pk := range pks {
		in.TransactItems = append(in.TransactItems, types.TransactGetItem{
			Get: &types.Get{TableName: aws.String("tbl"), Key: key(pk)},
		})
	}

	return in
}

func reasonCodes(t *testing.T, err error) []string {
	t.Helper()

	var canceled *types.TransactionCanceledException
	require.ErrorAs(t, err, &canceled)

	codes := make([]string, len(canceled.CancellationReasons))
	for i, r := range canceled.CancellationReasons {
		codes[i] = aws.ToString(r.Code)
	}

	return codes
}

func TestTransactionConflict_InFlightWrite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		probe func(t *testing.T, f *conflictFixture)
		name  string
	}{
		{
			name: "put on held item is rejected",
			probe: func(t *testing.T, f *conflictFixture) {
				t.Helper()
				_, err := f.client.PutItem(
					t.Context(),
					&sdk.PutItemInput{TableName: aws.String("tbl"), Item: item("a", "x")},
				)
				var conflict *types.TransactionConflictException
				require.ErrorAs(t, err, &conflict)
				assert.Equal(t, 1, f.metrics.conflicts())
			},
		},
		{
			name: "update on held item is rejected",
			probe: func(t *testing.T, f *conflictFixture) {
				t.Helper()
				_, err := f.client.UpdateItem(t.Context(), &sdk.UpdateItemInput{
					TableName:                 aws.String("tbl"),
					Key:                       key("a"),
					UpdateExpression:          aws.String("SET v = :v"),
					ExpressionAttributeValues: map[string]types.AttributeValue{":v": s("x")},
				})
				var conflict *types.TransactionConflictException
				require.ErrorAs(t, err, &conflict)
			},
		},
		{
			name: "delete on held item is rejected",
			probe: func(t *testing.T, f *conflictFixture) {
				t.Helper()
				_, err := f.client.DeleteItem(
					t.Context(),
					&sdk.DeleteItemInput{TableName: aws.String("tbl"), Key: key("a")},
				)
				var conflict *types.TransactionConflictException
				require.ErrorAs(t, err, &conflict)
			},
		},
		{
			name: "put on another item succeeds",
			probe: func(t *testing.T, f *conflictFixture) {
				t.Helper()
				_, err := f.client.PutItem(
					t.Context(),
					&sdk.PutItemInput{TableName: aws.String("tbl"), Item: item("b", "x")},
				)
				require.NoError(t, err)
				assert.Equal(t, 0, f.metrics.conflicts())
			},
		},
		{
			name: "same key in another table succeeds",
			probe: func(t *testing.T, f *conflictFixture) {
				t.Helper()
				_, err := f.client.PutItem(
					t.Context(),
					&sdk.PutItemInput{TableName: aws.String("other"), Item: item("a", "x")},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "overlapping write transaction is canceled per item",
			probe: func(t *testing.T, f *conflictFixture) {
				t.Helper()
				_, err := f.client.TransactWriteItems(
					t.Context(),
					f.twi(putAction("b", "x"), putAction("a", "x")),
				)
				assert.Equal(t, []string{"None", "TransactionConflict"}, reasonCodes(t, err))
				assert.Contains(t, err.Error(), "[None, TransactionConflict]")
				assert.Equal(t, 1, f.metrics.conflicts())

				got, getErr := f.client.GetItem(
					t.Context(),
					&sdk.GetItemInput{TableName: aws.String("tbl"), Key: key("b")},
				)
				require.NoError(t, getErr)
				assert.Equal(
					t,
					"old",
					got.Item["v"].(*types.AttributeValueMemberS).Value,
					"canceled transaction must not write",
				)
			},
		},
		{
			name: "disjoint write transaction succeeds",
			probe: func(t *testing.T, f *conflictFixture) {
				t.Helper()
				_, err := f.client.TransactWriteItems(t.Context(), f.twi(putAction("b", "x")))
				require.NoError(t, err)
			},
		},
		{
			name: "transact get is canceled",
			probe: func(t *testing.T, f *conflictFixture) {
				t.Helper()
				_, err := f.client.TransactGetItems(t.Context(), tgi("a", "b"))
				assert.Equal(t, []string{"TransactionConflict", "None"}, reasonCodes(t, err))
			},
		},
		{
			name: "batch write returns the conflicting write as unprocessed",
			probe: func(t *testing.T, f *conflictFixture) {
				t.Helper()
				out, err := f.client.BatchWriteItem(t.Context(), &sdk.BatchWriteItemInput{
					RequestItems: map[string][]types.WriteRequest{"tbl": {
						{PutRequest: &types.PutRequest{Item: item("a", "x")}},
						{PutRequest: &types.PutRequest{Item: item("b", "x")}},
					}},
				})
				require.NoError(t, err)
				require.Len(t, out.UnprocessedItems["tbl"], 1)
				assert.Equal(
					t,
					"a",
					out.UnprocessedItems["tbl"][0].PutRequest.Item["pk"].(*types.AttributeValueMemberS).Value,
				)

				got, getErr := f.client.GetItem(
					t.Context(),
					&sdk.GetItemInput{TableName: aws.String("tbl"), Key: key("b")},
				)
				require.NoError(t, getErr)
				assert.Equal(t, "x", got.Item["v"].(*types.AttributeValueMemberS).Value)
				assert.Equal(t, 1, f.metrics.conflicts())
			},
		},
		{
			name: "get item reads committed state and is not blocked",
			probe: func(t *testing.T, f *conflictFixture) {
				t.Helper()
				got, err := f.client.GetItem(
					t.Context(),
					&sdk.GetItemInput{TableName: aws.String("tbl"), Key: key("a"), ConsistentRead: aws.Bool(true)},
				)
				require.NoError(t, err)
				assert.Equal(t, "old", got.Item["v"].(*types.AttributeValueMemberS).Value)
			},
		},
		{
			name: "execute transaction statement is canceled",
			probe: func(t *testing.T, f *conflictFixture) {
				t.Helper()
				_, err := f.client.ExecuteTransaction(t.Context(), &sdk.ExecuteTransactionInput{
					TransactStatements: []types.ParameterizedStatement{{
						Statement: aws.String(`UPDATE "tbl" SET v = 'x' WHERE pk = 'a'`),
					}},
				})
				assert.Equal(t, []string{"TransactionConflict"}, reasonCodes(t, err))
			},
		},
		{
			name: "execute statement is rejected like update item",
			probe: func(t *testing.T, f *conflictFixture) {
				t.Helper()
				_, err := f.client.ExecuteStatement(t.Context(), &sdk.ExecuteStatementInput{
					Statement: aws.String(`UPDATE "tbl" SET v = 'x' WHERE pk = 'a'`),
				})
				var conflict *types.TransactionConflictException
				require.ErrorAs(t, err, &conflict)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newConflictFixture(t, noRetry)
			g := f.hold(opTransactWriteItems, 0)

			done := make(chan error, 1)

			go func() {
				_, err := f.client.TransactWriteItems(
					t.Context(),
					f.twi(putAction("a", "held"), putAction("c", "held")),
				)
				done <- err
			}()

			<-g.entered
			tt.probe(t, f)
			close(g.release)
			require.NoError(t, <-done)

			got, err := f.client.GetItem(
				t.Context(),
				&sdk.GetItemInput{TableName: aws.String("tbl"), Key: key("a"), ConsistentRead: aws.Bool(true)},
			)
			require.NoError(t, err)
			assert.Equal(t, "held", got.Item["v"].(*types.AttributeValueMemberS).Value)

			_, err = f.client.PutItem(
				t.Context(),
				&sdk.PutItemInput{TableName: aws.String("tbl"), Item: item("a", "after")},
			)
			require.NoError(t, err, "item must be free once the transaction finishes")
			assert.Zero(t, f.db.txn.live.Load())
		})
	}
}

func TestTransactionConflict_InFlightRead(t *testing.T) {
	t.Parallel()

	tests := []struct {
		probe func(t *testing.T, f *conflictFixture)
		name  string
	}{
		{
			name: "write transaction on the same item proceeds",
			probe: func(t *testing.T, f *conflictFixture) {
				t.Helper()
				_, err := f.client.TransactWriteItems(t.Context(), f.twi(putAction("a", "x")))
				require.NoError(t, err)
			},
		},
		{
			name: "single put is not blocked",
			probe: func(t *testing.T, f *conflictFixture) {
				t.Helper()
				_, err := f.client.PutItem(
					t.Context(),
					&sdk.PutItemInput{TableName: aws.String("tbl"), Item: item("a", "x")},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "another read transaction is not blocked",
			probe: func(t *testing.T, f *conflictFixture) {
				t.Helper()
				_, err := f.client.TransactGetItems(t.Context(), tgi("a"))
				require.NoError(t, err)
			},
		},
		{
			name: "write transaction on another item succeeds",
			probe: func(t *testing.T, f *conflictFixture) {
				t.Helper()
				_, err := f.client.TransactWriteItems(t.Context(), f.twi(putAction("c", "x")))
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newConflictFixture(t, noRetry)
			g := f.hold(opTransactGetItems, 0)

			done := make(chan error, 1)

			go func() {
				_, err := f.client.TransactGetItems(t.Context(), tgi("a", "b"))
				done <- err
			}()

			<-g.entered
			tt.probe(t, f)
			close(g.release)
			require.NoError(t, <-done)
			assert.Zero(t, f.db.txn.live.Load())
		})
	}
}

func TestTransactionConflict_ExecuteTransactionHoldsWrittenItems(t *testing.T) {
	t.Parallel()

	f := newConflictFixture(t, noRetry)
	g := f.hold("ExecuteTransaction", 1)

	done := make(chan error, 1)

	go func() {
		_, err := f.client.ExecuteTransaction(t.Context(), &sdk.ExecuteTransactionInput{
			TransactStatements: []types.ParameterizedStatement{
				{Statement: aws.String(`UPDATE "tbl" SET v = 'x' WHERE pk = 'a'`)},
				{Statement: aws.String(`UPDATE "tbl" SET v = 'x' WHERE pk = 'b'`)},
			},
		})
		done <- err
	}()

	<-g.entered

	_, err := f.client.PutItem(t.Context(), &sdk.PutItemInput{TableName: aws.String("tbl"), Item: item("a", "y")})
	var conflict *types.TransactionConflictException
	require.ErrorAs(t, err, &conflict)

	_, err = f.client.PutItem(t.Context(), &sdk.PutItemInput{TableName: aws.String("tbl"), Item: item("b", "y")})
	require.NoError(t, err, "second statement has not run yet")

	close(g.release)
	require.NoError(t, <-done, "a completed single put is not in flight, so statement 2 proceeds")
	assert.Zero(t, f.db.txn.live.Load())
}

func TestTransactionConflict_TokenInProgress(t *testing.T) {
	t.Parallel()

	f := newConflictFixture(t, noRetry)
	g := f.hold(opTransactWriteItems, 0)
	in := f.twi(putAction("a", "held"))
	in.ClientRequestToken = aws.String("token-1")

	done := make(chan error, 1)

	go func() {
		_, err := f.client.TransactWriteItems(t.Context(), in)
		done <- err
	}()

	<-g.entered

	_, err := f.client.TransactWriteItems(t.Context(), in)
	var inProgress *types.TransactionInProgressException
	require.ErrorAs(t, err, &inProgress)

	close(g.release)
	require.NoError(t, <-done)

	_, err = f.client.TransactWriteItems(t.Context(), in)
	require.NoError(t, err, "same token and request replays successfully once the first call finished")
}

func TestTransactionConflict_SDKRetryBehaviour(t *testing.T) {
	t.Parallel()

	retrying := func() aws.Retryer {
		return retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxAttempts = 3
			o.RateLimiter = ratelimit.None
			o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
		})
	}

	tests := []struct {
		probe        func(f *conflictFixture) error
		name         string
		wantRequests int64
	}{
		{
			name: "TransactionConflictException is not retried",
			probe: func(f *conflictFixture) error {
				_, err := f.client.PutItem(
					f.tb.Context(),
					&sdk.PutItemInput{TableName: aws.String("tbl"), Item: item("a", "x")},
				)

				return err
			},
			wantRequests: 1,
		},
		{
			name: "TransactionCanceledException is not retried",
			probe: func(f *conflictFixture) error {
				_, err := f.client.TransactGetItems(f.tb.Context(), tgi("a"))

				return err
			},
			wantRequests: 1,
		},
		{
			name: "TransactionInProgressException is retried",
			probe: func(f *conflictFixture) error {
				in := f.twi(putAction("a", "x"))
				in.ClientRequestToken = aws.String("tok")
				_, err := f.client.TransactWriteItems(f.tb.Context(), in)

				return err
			},
			wantRequests: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newConflictFixture(t, retrying)
			g := f.hold(opTransactWriteItems, 0)

			in := f.twi(putAction("a", "held"))
			in.ClientRequestToken = aws.String("tok")

			done := make(chan error, 1)

			go func() {
				_, err := f.client.TransactWriteItems(t.Context(), in)
				done <- err
			}()

			<-g.entered

			before := f.requests.Load()
			require.Error(t, tt.probe(f))
			assert.Equal(t, tt.wantRequests, f.requests.Load()-before)

			close(g.release)
			require.NoError(t, <-done)
		})
	}
}

func TestTransactionConflict_DefaultIsUnchanged(t *testing.T) {
	t.Parallel()

	f := newConflictFixture(t, noRetry)

	_, err := f.client.TransactWriteItems(t.Context(), f.twi(putAction("a", "1"), putAction("b", "1")))
	require.NoError(t, err)

	_, err = f.client.PutItem(t.Context(), &sdk.PutItemInput{TableName: aws.String("tbl"), Item: item("a", "2")})
	require.NoError(t, err)

	_, err = f.client.TransactGetItems(t.Context(), tgi("a", "b"))
	require.NoError(t, err)

	assert.Zero(t, f.metrics.conflicts())
}

func effectRule(op string, target *chaos.EffectTarget, p float64, holdOps, holdMs int) chaos.FaultRule {
	return chaos.FaultRule{
		Effect:      chaos.EffectDynamoDBTransactionConflict,
		Operation:   op,
		Target:      target,
		Probability: &p,
		HoldOps:     holdOps,
		HoldMs:      holdMs,
	}
}

func TestTransactionConflict_ChaosEffect(t *testing.T) {
	t.Parallel()

	put := func(f *conflictFixture, table, pk string) error {
		_, err := f.client.PutItem(f.tb.Context(), &sdk.PutItemInput{TableName: aws.String(table), Item: item(pk, "x")})

		return err
	}

	tests := []struct {
		run  func(t *testing.T, f *conflictFixture, fs *chaos.FaultStore)
		name string
	}{
		{
			name: "table target conflicts every write to the table only",
			run: func(t *testing.T, f *conflictFixture, fs *chaos.FaultStore) {
				t.Helper()
				fs.SetRules([]chaos.FaultRule{effectRule("", &chaos.EffectTarget{Table: "tbl"}, 1, 0, 0)})

				var conflict *types.TransactionConflictException
				require.ErrorAs(t, put(f, "tbl", "a"), &conflict)
				require.ErrorAs(t, put(f, "tbl", "b"), &conflict)
				require.NoError(t, put(f, "other", "a"))

				_, err := f.client.GetItem(t.Context(), &sdk.GetItemInput{TableName: aws.String("tbl"), Key: key("a")})
				require.NoError(t, err, "GetItem is never blocked")
			},
		},
		{
			name: "key target conflicts only that item",
			run: func(t *testing.T, f *conflictFixture, fs *chaos.FaultStore) {
				t.Helper()
				fs.SetRules(
					[]chaos.FaultRule{
						effectRule(
							"PutItem",
							&chaos.EffectTarget{Table: "tbl", Key: map[string]string{"pk": "a"}},
							1,
							0,
							0,
						),
					},
				)

				var conflict *types.TransactionConflictException
				require.ErrorAs(t, put(f, "tbl", "a"), &conflict)
				require.NoError(t, put(f, "tbl", "b"))
			},
		},
		{
			name: "operation scoping leaves other operations alone",
			run: func(t *testing.T, f *conflictFixture, fs *chaos.FaultStore) {
				t.Helper()
				fs.SetRules([]chaos.FaultRule{effectRule("DeleteItem", &chaos.EffectTarget{Table: "tbl"}, 1, 0, 0)})

				require.NoError(t, put(f, "tbl", "a"))

				_, err := f.client.DeleteItem(
					t.Context(),
					&sdk.DeleteItemInput{TableName: aws.String("tbl"), Key: key("a")},
				)
				var conflict *types.TransactionConflictException
				require.ErrorAs(t, err, &conflict)
			},
		},
		{
			name: "probability zero never conflicts",
			run: func(t *testing.T, f *conflictFixture, fs *chaos.FaultStore) {
				t.Helper()
				fs.SetRules([]chaos.FaultRule{effectRule("", &chaos.EffectTarget{Table: "tbl"}, 0, 0, 0)})

				require.NoError(t, put(f, "tbl", "a"))
			},
		},
		{
			name: "write transaction is canceled for targeted entries",
			run: func(t *testing.T, f *conflictFixture, fs *chaos.FaultStore) {
				t.Helper()
				fs.SetRules(
					[]chaos.FaultRule{
						effectRule(
							"TransactWriteItems",
							&chaos.EffectTarget{Table: "tbl", Key: map[string]string{"pk": "b"}},
							1,
							0,
							0,
						),
					},
				)

				_, err := f.client.TransactWriteItems(
					t.Context(),
					f.twi(putAction("a", "x"), putAction("b", "x")),
				)
				assert.Equal(t, []string{"None", "TransactionConflict"}, reasonCodes(t, err))
			},
		},
		{
			name: "transact get is canceled",
			run: func(t *testing.T, f *conflictFixture, fs *chaos.FaultStore) {
				t.Helper()
				fs.SetRules(
					[]chaos.FaultRule{effectRule("TransactGetItems", &chaos.EffectTarget{Table: "tbl"}, 1, 0, 0)},
				)

				_, err := f.client.TransactGetItems(t.Context(), tgi("a"))
				assert.Equal(t, []string{"TransactionConflict"}, reasonCodes(t, err))
			},
		},
		{
			name: "batch write returns targeted writes as unprocessed",
			run: func(t *testing.T, f *conflictFixture, fs *chaos.FaultStore) {
				t.Helper()
				fs.SetRules(
					[]chaos.FaultRule{
						effectRule(
							"BatchWriteItem",
							&chaos.EffectTarget{Table: "tbl", Key: map[string]string{"pk": "a"}},
							1,
							0,
							0,
						),
					},
				)

				out, err := f.client.BatchWriteItem(t.Context(), &sdk.BatchWriteItemInput{
					RequestItems: map[string][]types.WriteRequest{"tbl": {
						{PutRequest: &types.PutRequest{Item: item("a", "x")}},
						{PutRequest: &types.PutRequest{Item: item("b", "x")}},
					}},
				})
				require.NoError(t, err)
				assert.Len(t, out.UnprocessedItems["tbl"], 1)
			},
		},
		{
			name: "holdOps keeps the item in flight after the rule stops firing",
			run: func(t *testing.T, f *conflictFixture, fs *chaos.FaultStore) {
				t.Helper()
				target := &chaos.EffectTarget{Table: "tbl"}
				fs.SetRules([]chaos.FaultRule{effectRule("", target, 1, 2, 0)})

				var conflict *types.TransactionConflictException
				require.ErrorAs(t, put(f, "tbl", "a"), &conflict)

				fs.SetRules([]chaos.FaultRule{effectRule("", target, 0, 2, 0)})
				require.ErrorAs(t, put(f, "tbl", "a"), &conflict)
				require.ErrorAs(t, put(f, "tbl", "a"), &conflict)
				require.NoError(t, put(f, "tbl", "a"))
			},
		},
		{
			name: "holdMs keeps the item in flight until the injected clock passes the window",
			run: func(t *testing.T, f *conflictFixture, fs *chaos.FaultStore) {
				t.Helper()
				now := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
				f.db.clock = func() time.Time { return now }

				target := &chaos.EffectTarget{Table: "tbl"}
				fs.SetRules([]chaos.FaultRule{effectRule("", target, 1, 0, 500)})

				var conflict *types.TransactionConflictException
				require.ErrorAs(t, put(f, "tbl", "a"), &conflict)

				fs.SetRules([]chaos.FaultRule{effectRule("", target, 0, 0, 500)})
				now = now.Add(499 * time.Millisecond)
				require.ErrorAs(t, put(f, "tbl", "a"), &conflict)

				now = now.Add(2 * time.Millisecond)
				require.NoError(t, put(f, "tbl", "a"))
			},
		},
		{
			name: "clearing the rules restores default behaviour",
			run: func(t *testing.T, f *conflictFixture, fs *chaos.FaultStore) {
				t.Helper()
				fs.SetRules([]chaos.FaultRule{effectRule("", &chaos.EffectTarget{Table: "tbl"}, 1, 0, 0)})

				var conflict *types.TransactionConflictException
				require.ErrorAs(t, put(f, "tbl", "a"), &conflict)

				fs.SetRules(nil)
				require.NoError(t, put(f, "tbl", "a"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newConflictFixture(t, noRetry)

			fs := chaos.NewFaultStore()
			f.db.SetFaultStore(fs)

			tt.run(t, f, fs)
		})
	}
}

func TestTransactionConflict_ConcurrentWritersStayAtomic(t *testing.T) {
	t.Parallel()

	f := newConflictFixture(t, noRetry)

	const writers = 16

	var (
		wg        sync.WaitGroup
		succeeded atomic.Int64
		canceled  atomic.Int64
	)

	for i := range writers {
		wg.Go(func() {
			v := string(rune('A' + i))
			_, err := f.client.TransactWriteItems(
				t.Context(),
				f.twi(putAction("a", v), putAction("b", v)),
			)

			if err == nil {
				succeeded.Add(1)

				return
			}

			if _, ok := errors.AsType[*types.TransactionCanceledException](err); ok {
				canceled.Add(1)
			}
		})
	}

	wg.Wait()

	assert.Equal(t, int64(writers), succeeded.Load()+canceled.Load())
	assert.Positive(t, succeeded.Load())

	a, err := f.client.GetItem(
		t.Context(),
		&sdk.GetItemInput{TableName: aws.String("tbl"), Key: key("a"), ConsistentRead: aws.Bool(true)},
	)
	require.NoError(t, err)

	b, err := f.client.GetItem(
		t.Context(),
		&sdk.GetItemInput{TableName: aws.String("tbl"), Key: key("b"), ConsistentRead: aws.Bool(true)},
	)
	require.NoError(t, err)

	assert.Equal(t, a.Item["v"], b.Item["v"], "transactions are all-or-nothing and serializable")
	assert.Zero(t, f.db.txn.live.Load())
}

func TestTransactionConflict_ChaosFaultAPI(t *testing.T) {
	t.Parallel()

	f := newConflictFixture(t, noRetry)
	fs := chaos.NewFaultStore()
	f.db.SetFaultStore(fs)

	e := echo.New()
	registry := service.NewRegistry()
	chaos.RegisterRoutes(e.Group("/_gopherstack/chaos"), fs, registry)
	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	hc := testclient.New(t)

	post := func(body string) int {
		req, err := http.NewRequestWithContext(
			t.Context(),
			http.MethodPost,
			srv.URL+"/_gopherstack/chaos/faults",
			strings.NewReader(body),
		)
		require.NoError(t, err)

		resp, err := hc.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		return resp.StatusCode
	}

	assert.Equal(t, http.StatusBadRequest, post(`[{"effect":"dynamodb:no-such-effect"}]`))
	assert.Equal(
		t,
		http.StatusBadRequest,
		post(`[{"effect":"dynamodb:transaction-conflict","error":{"code":"X","statusCode":500}}]`),
	)
	assert.Equal(t, http.StatusOK, post(
		`[{"effect":"dynamodb:transaction-conflict","service":"dynamodb","operation":"PutItem",`+
			`"target":{"table":"tbl","key":{"pk":"a"}},"probability":1,"holdOps":1}]`))

	_, err := f.client.PutItem(t.Context(), &sdk.PutItemInput{TableName: aws.String("tbl"), Item: item("a", "x")})
	var conflict *types.TransactionConflictException
	require.ErrorAs(t, err, &conflict)

	assert.NotEmpty(t, fs.GetActivity())
}
