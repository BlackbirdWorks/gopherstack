package dynamodb_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// concurrent_table_lifecycle_test.go is a regression suite for two real
// db.mu / table.mu lock-discipline defects found in the v15.0.0 -> HEAD
// store.go/table_ops.go rewrite (the map[string]map[string]*Table ->
// *store.Table[Table] conversion). Both were reachable only through code
// paths that (like the real HTTP handler) run the background janitor and/or
// serve concurrent requests against shared tables -- calling the backend
// directly, single-threaded, never exercised them.
//
//  1. TestConcurrentTableLifecycle_NoDataRace reproduces a genuine `-race`
//     failure: CreateTable/PutItem/BatchWriteItem/DeleteTable/
//     UpdateTimeToLive driven concurrently, with the REAL background janitor
//     running (both of its tickers via Janitor.Run, not the single-goroutine
//     SweepOnce helper most other tests use) and a live CREATING->ACTIVE
//     window. Two unsynchronized reads used to race here:
//       - DeleteTable (table_ops.go) read table.Items and
//         table.GlobalSecondaryIndexes without table.mu, while
//         PutItem/BatchWriteItem write those same fields under table.mu --
//         a PutItem that already resolved the *Table before a concurrent
//         DeleteTable moved it to db.deletingTables still mutates it.
//       - buildCreateTableOutput (table_ops.go) read t.Status (and other
//         fields) without table.mu immediately after CreateTable makes the
//         table visible, racing the activation timer that flips t.Status
//         under table.mu on another goroutine.
//
//  2. TestConcurrentTableLifecycle_NoABBADeadlock reproduces a genuine
//     mutex ABBA deadlock (not a data race): TransactWriteItems'
//     executeTransactWrite (transact_ops.go) held every target table's
//     table.mu (via lockTablesWrite) and only then acquired db.mu.Lock to
//     commit the idempotency token -- backwards relative to this backend's
//     documented db.mu -> table.mu order. TaggedTables (store.go) and the
//     handler-reachable ListContributorInsights (extra_ops.go) both hold
//     db.mu.RLock for their entire body while nested-RLocking each table's
//     table.mu, i.e. the correct order. One goroutine holding table.mu and
//     wanting db.mu, while another holds db.mu and wants that same
//     table.mu, is a textbook two-goroutine deadlock that never resolves on
//     its own. A goroutine-dump captured from this exact test before the fix
//     showed the cycle directly:
//       - 4 goroutines blocked in sync.(*RWMutex).Lock inside
//         executeTransactWrite (transact_ops.go, the tokenCommit db.mu.Lock
//         call), each already holding a table's table.mu.
//       - 3 goroutines blocked in sync.(*RWMutex).RLock inside TaggedTables
//         (store.go:652, the per-table `table.mu.RLock("TaggedTables.tag")`
//         call), each already holding db.mu.RLock.
//     Fixed by releasing every table lock before ever touching db.mu.

// TestConcurrentTableLifecycle_NoDataRace exercises the full table lifecycle
// (Create -> wait-for-ACTIVE -> enable TTL -> Put -> BatchWrite -> Delete ->
// wait-for-gone) from many concurrent workers against a live background
// janitor, streams, and a GSI. Run with `-race`: this must be clean.
func TestConcurrentTableLifecycle_NoDataRace(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	db.SetCreateDelay(3 * time.Millisecond) // live CREATING->ACTIVE window
	ctx := context.Background()

	// The real janitor, with both tickers (TableCleaner + TTLSweeper)
	// running as independent goroutines exactly as NewHandler().
	// WithJanitor().StartWorker() wires it up -- unlike the single-goroutine
	// SweepOnce helper most other tests in this package use, which never lets
	// runTableCleaner and sweepTTL run truly concurrently with each other or
	// with the requests below.
	jan := dynamodb.NewJanitor(db, dynamodb.Settings{JanitorInterval: 2 * time.Millisecond})
	janCtx, janCancel := context.WithCancel(ctx)
	janDone := make(chan struct{})
	go func() {
		defer close(janDone)
		jan.Run(janCtx)
	}()
	defer func() {
		janCancel()
		<-janDone
	}()

	keySchema := []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}}
	attrDefs := []types.AttributeDefinition{
		{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		{AttributeName: aws.String("gsi"), AttributeType: types.ScalarAttributeTypeS},
	}

	mkTable := func(name string) {
		_, _ = db.CreateTable(ctx, &dynamodb_sdk.CreateTableInput{
			TableName:            aws.String(name),
			KeySchema:            keySchema,
			AttributeDefinitions: attrDefs,
			BillingMode:          types.BillingModePayPerRequest,
			StreamSpecification: &types.StreamSpecification{
				StreamEnabled:  aws.Bool(true),
				StreamViewType: types.StreamViewTypeNewAndOldImages,
			},
			GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{{
				IndexName:  aws.String("gsi-idx"),
				KeySchema:  []types.KeySchemaElement{{AttributeName: aws.String("gsi"), KeyType: types.KeyTypeHash}},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			}},
		})
	}

	item := func(k string) map[string]types.AttributeValue {
		return map[string]types.AttributeValue{
			"pk":  &types.AttributeValueMemberS{Value: k},
			"gsi": &types.AttributeValueMemberS{Value: k},
			// Already-expired TTL so the concurrent TTLSweeper ticker evicts
			// it almost immediately, forcing table.mu contention with PutItem.
			"ttl": &types.AttributeValueMemberN{
				Value: strconv.FormatInt(time.Now().Add(-time.Hour).Unix(), 10),
			},
		}
	}

	// waitActive/waitGone poll with a bounded deadline, exactly like the
	// wait-for-active pattern real client test suites use, but capped so a
	// regression fails fast instead of hanging the whole test binary.
	waitActive := func(name string) bool {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			out, err := db.DescribeTable(ctx, &dynamodb_sdk.DescribeTableInput{TableName: aws.String(name)})
			if err == nil && out.Table != nil && out.Table.TableStatus == types.TableStatusActive {
				return true
			}
			time.Sleep(50 * time.Microsecond)
		}

		return false
	}
	waitGone := func(name string) bool {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			_, err := db.DescribeTable(ctx, &dynamodb_sdk.DescribeTableInput{TableName: aws.String(name)})
			if err != nil {
				return true
			}
			time.Sleep(50 * time.Microsecond)
		}

		return false
	}

	const workers = 8
	const iters = 60
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range iters {
				// Each worker/iteration owns a uniquely-named table: this
				// keeps the wait-for-active/wait-for-gone polling below
				// deterministic (a name shared across independent worker
				// lifecycles can legitimately be deleted by someone else
				// mid-poll -- a test-harness livelock unrelated to the
				// backend, not the defect under test here) while still
				// running Create/Put/BatchWrite/Delete concurrently against
				// the same backend and the same live janitor.
				name := fmt.Sprintf("t-%d-%d", w, i)
				mkTable(name)
				if !waitActive(name) {
					t.Errorf("worker %d: table %s never became ACTIVE", w, name)

					return
				}

				_, _ = db.UpdateTimeToLive(ctx, &dynamodb_sdk.UpdateTimeToLiveInput{
					TableName: aws.String(name),
					TimeToLiveSpecification: &types.TimeToLiveSpecification{
						AttributeName: aws.String("ttl"),
						Enabled:       aws.Bool(true),
					},
				})

				_, _ = db.PutItem(ctx, &dynamodb_sdk.PutItemInput{
					TableName: aws.String(name),
					Item:      item(fmt.Sprintf("k-%d-%d", w, i)),
				})
				_, _ = db.BatchWriteItem(ctx, &dynamodb_sdk.BatchWriteItemInput{
					RequestItems: map[string][]types.WriteRequest{
						name: {{PutRequest: &types.PutRequest{Item: item(fmt.Sprintf("b-%d-%d", w, i))}}},
					},
				})
				_, _ = db.DeleteTable(ctx, &dynamodb_sdk.DeleteTableInput{TableName: aws.String(name)})
				if !waitGone(name) {
					t.Errorf("worker %d: table %s never disappeared", w, name)

					return
				}
			}
		}(w)
	}
	wg.Wait()
}

// TestConcurrentTableLifecycle_NoABBADeadlock reproduces the table.mu -> db.mu
// reverse-order deadlock between TransactWriteItems and TaggedTables. It uses
// an internal bounded deadline (rather than relying on `go test -timeout` to
// kill the whole binary) so a regression fails this one test cleanly.
func TestConcurrentTableLifecycle_NoABBADeadlock(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	ctx := context.Background()

	keySchema := []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}}
	attrDefs := []types.AttributeDefinition{
		{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
	}

	const numTables = 8
	for i := range numTables {
		name := fmt.Sprintf("tbl-%d", i)

		_, createErr := db.CreateTable(ctx, &dynamodb_sdk.CreateTableInput{
			TableName:            aws.String(name),
			KeySchema:            keySchema,
			AttributeDefinitions: attrDefs,
			BillingMode:          types.BillingModePayPerRequest,
		})
		if createErr != nil {
			t.Fatalf("CreateTable: %v", createErr)
		}

		// TaggedTables only takes table.mu when table.Tags != nil (lazily
		// initialised by TagResource), so seed it here or the contended
		// branch is never exercised. tableNameFromARN falls back to treating
		// a non-ARN input as a plain table name.
		if _, tagErr := db.TagResource(ctx, &dynamodb_sdk.TagResourceInput{
			ResourceArn: aws.String(name),
			Tags:        []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
		}); tagErr != nil {
			t.Fatalf("TagResource: %v", tagErr)
		}
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Goroutines hammering TransactWriteItems with a fresh idempotency token
	// on every call: table.mu (write, via lockTablesWrite) held, then
	// (pre-fix) db.mu.Lock sought for the token commit.
	for w := range 4 {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				tableName := fmt.Sprintf("tbl-%d", i%numTables)
				i++
				_, _ = db.TransactWriteItems(ctx, &dynamodb_sdk.TransactWriteItemsInput{
					ClientRequestToken: aws.String(fmt.Sprintf("tok-%d-%d", w, i)),
					TransactItems: []types.TransactWriteItem{
						{
							Put: &types.Put{
								TableName: aws.String(tableName),
								Item: map[string]types.AttributeValue{
									"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("k-%d-%d", w, i)},
								},
							},
						},
					},
				})
			}
		}(w)
	}

	// Goroutines hammering TaggedTables: db.mu.RLock held (via defer) for the
	// whole call while nested-RLocking each table.mu -- the correct order,
	// and the other half of the ABBA.
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = db.TaggedTables()
			}
		})
	}

	allDone := make(chan struct{})
	go func() {
		defer close(allDone)
		wg.Wait()
	}()

	select {
	case <-time.After(3 * time.Second):
	case <-allDone:
		t.Fatal("workers exited before being asked to stop")
	}
	close(stop)

	select {
	case <-allDone:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: TransactWriteItems and TaggedTables workers did not " +
			"exit after stop was signalled -- table.mu/db.mu ABBA cycle")
	}
}
