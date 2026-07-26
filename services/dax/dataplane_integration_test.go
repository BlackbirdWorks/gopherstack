package dax_test

// The aws-sdk-go v1 imports below are deliberate and not migratable: aws-dax-go
// v1.2.15 is built on aws-sdk-go v1 (it depends on v1.55.5 directly and its
// client methods take v1 types such as *dynamodb.PutItemInput). AWS ships no v2
// DAX client, so driving the real DAX wire protocol requires them. The dep bump
// in eb437919a marked aws-sdk-go v1 deprecated, hence the staticcheck
// suppressions.

import (
	"context"
	"testing"
	"time"

	daxgo "github.com/aws/aws-dax-go/dax"
	"github.com/aws/aws-sdk-go-v2/aws"
	v2dynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	v2types "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	v1creds "github.com/aws/aws-sdk-go/aws/credentials"     //nolint:staticcheck // SA1019: aws-dax-go is v1-only; see note above the import block.
	v1dynamodb "github.com/aws/aws-sdk-go/service/dynamodb" //nolint:staticcheck // SA1019: aws-dax-go is v1-only; see note above the import block.
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

const integrationTable = "dax-integration"

// Tests that drive the amazon-dax-go client through an expression
// (Update/Query/Scan/Transact carrying a *Expression) intentionally omit
// t.Parallel(): the client parses those expressions with the old ANTLR Go
// runtime, whose shared ATN/DFA caches are not goroutine-safe, so running the
// parses concurrently would race inside that third-party, test-only dependency.
// The gopherstack server itself is concurrency-safe; serializing only the
// expression-bearing tests keeps the parse single-threaded while letting the
// purely key-based tests (Put/Get/Delete) still run in parallel.

// newDataPlaneFixture starts a DAX data-plane listener on an ephemeral port with
// a single table created in its backing DynamoDB store. It returns the bound
// "dax://host:port" endpoint.
func newDataPlaneFixture(t *testing.T) string {
	t.Helper()

	handler := dax.NewHandler(dax.NewInMemoryBackend("000000000000", "us-east-1"))
	dp := handler.EnableDataPlane(context.TODO(), "127.0.0.1:0")

	if err := handler.StartWorker(context.Background()); err != nil {
		t.Fatalf("start data plane: %v", err)
	}

	t.Cleanup(func() { handler.Shutdown(context.Background()) })

	createIntegrationTable(t, handler.DataPlaneBackend())

	addr := dp.Addr()
	if addr == nil {
		t.Fatal("data plane has no bound address")
	}

	return "dax://" + addr.String()
}

func createIntegrationTable(t *testing.T, backend interface {
	CreateTable(context.Context, *v2dynamodb.CreateTableInput) (*v2dynamodb.CreateTableOutput, error)
},
) {
	t.Helper()

	_, err := backend.CreateTable(context.Background(), &v2dynamodb.CreateTableInput{
		TableName: aws.String(integrationTable),
		KeySchema: []v2types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: v2types.KeyTypeHash},
		},
		AttributeDefinitions: []v2types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: v2types.ScalarAttributeTypeS},
		},
		BillingMode: v2types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
}

// newDaxClient builds a real amazon-dax-go client pointed at endpoint.
func newDaxClient(t *testing.T, endpoint string) *daxgo.Dax {
	t.Helper()

	cfg := daxgo.DefaultConfig()
	cfg.HostPorts = []string{endpoint}
	cfg.Region = "us-east-1"
	cfg.Credentials = v1creds.NewStaticCredentials("AKID", "SECRET", "")
	cfg.RequestTimeout = 5 * time.Second
	cfg.WriteRetries = 0
	cfg.ReadRetries = 0

	client, err := daxgo.New(cfg)
	if err != nil {
		t.Fatalf("new dax client: %v", err)
	}

	t.Cleanup(func() { _ = client.Close() })

	return client
}

// TestDataPlanePutGetRoundTrip exercises the full DAX binary protocol with the
// real amazon-dax-go client: PutItem then GetItem against the gopherstack
// listener, which delegates to the embedded DynamoDB backend.
func TestDataPlanePutGetRoundTrip(t *testing.T) {
	t.Parallel()

	endpoint := newDataPlaneFixture(t)
	client := newDaxClient(t, endpoint)

	_, err := client.PutItem(&v1dynamodb.PutItemInput{
		TableName: new(integrationTable),
		Item: map[string]*v1dynamodb.AttributeValue{
			"pk":    {S: new("user#1")},
			"name":  {S: new("Ada")},
			"score": {N: new("42")},
			"admin": {BOOL: new(true)},
		},
	})
	if err != nil {
		t.Fatalf("PutItem via DAX: %v", err)
	}

	out, err := client.GetItem(&v1dynamodb.GetItemInput{
		TableName: new(integrationTable),
		Key: map[string]*v1dynamodb.AttributeValue{
			"pk": {S: new("user#1")},
		},
	})
	if err != nil {
		t.Fatalf("GetItem via DAX: %v", err)
	}

	assertString(t, out.Item, "pk", "user#1")
	assertString(t, out.Item, "name", "Ada")
	assertNumber(t, out.Item, "score", "42")

	if admin := out.Item["admin"]; admin == nil || admin.BOOL == nil || !*admin.BOOL {
		t.Fatalf("admin attribute mismatch: %#v", out.Item["admin"])
	}
}

// TestDataPlaneGetMissingItem verifies a GetItem for an absent key returns an
// empty item rather than an error.
func TestDataPlaneGetMissingItem(t *testing.T) {
	t.Parallel()

	endpoint := newDataPlaneFixture(t)
	client := newDaxClient(t, endpoint)

	out, err := client.GetItem(&v1dynamodb.GetItemInput{
		TableName: new(integrationTable),
		Key: map[string]*v1dynamodb.AttributeValue{
			"pk": {S: new("missing")},
		},
	})
	if err != nil {
		t.Fatalf("GetItem via DAX: %v", err)
	}

	if len(out.Item) != 0 {
		t.Fatalf("expected empty item, got %#v", out.Item)
	}
}

// TestDataPlaneDeleteItem verifies DeleteItem removes an item that a later
// GetItem then cannot find.
func TestDataPlaneDeleteItem(t *testing.T) {
	t.Parallel()

	endpoint := newDataPlaneFixture(t)
	client := newDaxClient(t, endpoint)

	_, err := client.PutItem(&v1dynamodb.PutItemInput{
		TableName: new(integrationTable),
		Item: map[string]*v1dynamodb.AttributeValue{
			"pk": {S: new("doomed")},
		},
	})
	if err != nil {
		t.Fatalf("PutItem via DAX: %v", err)
	}

	if _, err = client.DeleteItem(&v1dynamodb.DeleteItemInput{
		TableName: new(integrationTable),
		Key: map[string]*v1dynamodb.AttributeValue{
			"pk": {S: new("doomed")},
		},
	}); err != nil {
		t.Fatalf("DeleteItem via DAX: %v", err)
	}

	out, err := client.GetItem(&v1dynamodb.GetItemInput{
		TableName: new(integrationTable),
		Key: map[string]*v1dynamodb.AttributeValue{
			"pk": {S: new("doomed")},
		},
	})
	if err != nil {
		t.Fatalf("GetItem via DAX: %v", err)
	}

	if len(out.Item) != 0 {
		t.Fatalf("expected deleted item to be absent, got %#v", out.Item)
	}
}

const rangeTable = "dax-integration-range"

// newRangeFixture starts a listener whose backing store has both the hash-only
// integration table and a hash+numeric-range table, exercising the lexdecimal
// range-key codec end to end.
func newRangeFixture(t *testing.T) string {
	t.Helper()

	handler := dax.NewHandler(dax.NewInMemoryBackend("000000000000", "us-east-1"))
	dp := handler.EnableDataPlane(context.TODO(), "127.0.0.1:0")

	if err := handler.StartWorker(context.Background()); err != nil {
		t.Fatalf("start data plane: %v", err)
	}

	t.Cleanup(func() { handler.Shutdown(context.Background()) })

	createIntegrationTable(t, handler.DataPlaneBackend())
	createRangeTable(t, handler.DataPlaneBackend())

	addr := dp.Addr()
	if addr == nil {
		t.Fatal("data plane has no bound address")
	}

	return "dax://" + addr.String()
}

func createRangeTable(t *testing.T, backend interface {
	CreateTable(context.Context, *v2dynamodb.CreateTableInput) (*v2dynamodb.CreateTableOutput, error)
},
) {
	t.Helper()

	_, err := backend.CreateTable(context.Background(), &v2dynamodb.CreateTableInput{
		TableName: aws.String(rangeTable),
		KeySchema: []v2types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: v2types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: v2types.KeyTypeRange},
		},
		AttributeDefinitions: []v2types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: v2types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: v2types.ScalarAttributeTypeN},
		},
		BillingMode: v2types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("create range table: %v", err)
	}
}

// TestDataPlaneUpdateItem exercises an UpdateItem with a SET expression and an
// expression attribute value, then verifies the stored item.
func TestDataPlaneUpdateItem(t *testing.T) {
	endpoint := newDataPlaneFixture(t)
	client := newDaxClient(t, endpoint)

	if _, err := client.PutItem(&v1dynamodb.PutItemInput{
		TableName: new(integrationTable),
		Item: map[string]*v1dynamodb.AttributeValue{
			"pk":    {S: new("user#u")},
			"score": {N: new("1")},
		},
	}); err != nil {
		t.Fatalf("PutItem via DAX: %v", err)
	}

	_, err := client.UpdateItem(&v1dynamodb.UpdateItemInput{
		TableName: new(integrationTable),
		Key: map[string]*v1dynamodb.AttributeValue{
			"pk": {S: new("user#u")},
		},
		UpdateExpression: new("SET score = :s, nickname = :n"),
		ExpressionAttributeValues: map[string]*v1dynamodb.AttributeValue{
			":s": {N: new("99")},
			":n": {S: new("ace")},
		},
	})
	if err != nil {
		t.Fatalf("UpdateItem via DAX: %v", err)
	}

	out, err := client.GetItem(&v1dynamodb.GetItemInput{
		TableName: new(integrationTable),
		Key: map[string]*v1dynamodb.AttributeValue{
			"pk": {S: new("user#u")},
		},
	})
	if err != nil {
		t.Fatalf("GetItem via DAX: %v", err)
	}

	assertNumber(t, out.Item, "score", "99")
	assertString(t, out.Item, "nickname", "ace")
}

// TestDataPlaneUpdateItemReturnAllNew verifies UpdateItem with ReturnValues
// ALL_NEW returns the updated attributes.
func TestDataPlaneUpdateItemReturnAllNew(t *testing.T) {
	endpoint := newDataPlaneFixture(t)
	client := newDaxClient(t, endpoint)

	if _, err := client.PutItem(&v1dynamodb.PutItemInput{
		TableName: new(integrationTable),
		Item: map[string]*v1dynamodb.AttributeValue{
			"pk":    {S: new("user#rv")},
			"count": {N: new("5")},
		},
	}); err != nil {
		t.Fatalf("PutItem via DAX: %v", err)
	}

	out, err := client.UpdateItem(&v1dynamodb.UpdateItemInput{
		TableName: new(integrationTable),
		Key: map[string]*v1dynamodb.AttributeValue{
			"pk": {S: new("user#rv")},
		},
		UpdateExpression: new("SET #c = :c"),
		ExpressionAttributeNames: map[string]*string{
			"#c": new("count"),
		},
		ExpressionAttributeValues: map[string]*v1dynamodb.AttributeValue{
			":c": {N: new("7")},
		},
		ReturnValues: new(v1dynamodb.ReturnValueAllNew),
	})
	if err != nil {
		t.Fatalf("UpdateItem via DAX: %v", err)
	}

	assertNumber(t, out.Attributes, "count", "7")
}

// TestDataPlaneUpdateItemReturnUpdated verifies UpdateItem with ReturnValues
// UPDATED_NEW and UPDATED_OLD returns the touched attributes via the
// ordinal-keyed attribute-projection response sub-protocol.
func TestDataPlaneUpdateItemReturnUpdated(t *testing.T) {
	tests := []struct {
		name string
		rv   string
		want string
	}{
		{name: "updated_new", rv: v1dynamodb.ReturnValueUpdatedNew, want: "2"},
		{name: "updated_old", rv: v1dynamodb.ReturnValueUpdatedOld, want: "1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			endpoint := newDataPlaneFixture(t)
			client := newDaxClient(t, endpoint)

			pk := "user#" + tc.name
			if _, err := client.PutItem(&v1dynamodb.PutItemInput{
				TableName: new(integrationTable),
				Item: map[string]*v1dynamodb.AttributeValue{
					"pk":    {S: new(pk)},
					"count": {N: new("1")},
				},
			}); err != nil {
				t.Fatalf("PutItem via DAX: %v", err)
			}

			out, err := client.UpdateItem(&v1dynamodb.UpdateItemInput{
				TableName: new(integrationTable),
				Key: map[string]*v1dynamodb.AttributeValue{
					"pk": {S: new(pk)},
				},
				UpdateExpression: new("SET #c = :c"),
				ExpressionAttributeNames: map[string]*string{
					"#c": new("count"),
				},
				ExpressionAttributeValues: map[string]*v1dynamodb.AttributeValue{
					":c": {N: new("2")},
				},
				ReturnValues: new(tc.rv),
			})
			if err != nil {
				t.Fatalf("UpdateItem via DAX: %v", err)
			}

			assertNumber(t, out.Attributes, "count", tc.want)

			// UPDATED_* must not echo the (unchanged) primary key.
			if _, ok := out.Attributes["pk"]; ok {
				t.Fatalf("UPDATED_* response unexpectedly included key: %#v", out.Attributes)
			}
		})
	}
}

// TestDataPlaneQueryProjection exercises a Query carrying a
// KeyConditionExpression and a ProjectionExpression, asserting that only the
// projected attributes are returned via the projection response sub-protocol.
func TestDataPlaneQueryProjection(t *testing.T) {
	endpoint := newRangeFixture(t)
	client := newDaxClient(t, endpoint)

	seedRangeItems(t, client)

	out, err := client.Query(&v1dynamodb.QueryInput{
		TableName:              new(rangeTable),
		KeyConditionExpression: new("pk = :p"),
		ProjectionExpression:   new("sk, label"),
		ExpressionAttributeValues: map[string]*v1dynamodb.AttributeValue{
			":p": {S: new("g1")},
		},
	})
	if err != nil {
		t.Fatalf("Query projection via DAX: %v", err)
	}

	if got := len(out.Items); got != 3 {
		t.Fatalf("projected Query count: got %d want 3", got)
	}

	for _, item := range out.Items {
		if _, ok := item["sk"]; !ok {
			t.Fatalf("projected item missing sk: %#v", item)
		}

		if _, ok := item["label"]; !ok {
			t.Fatalf("projected item missing label: %#v", item)
		}

		// pk was not projected, so it must be absent.
		if _, ok := item["pk"]; ok {
			t.Fatalf("projected item unexpectedly included pk: %#v", item)
		}
	}
}

// TestDataPlaneScanProjection exercises a Scan with a ProjectionExpression that
// uses an expression-attribute-name substitution.
func TestDataPlaneScanProjection(t *testing.T) {
	endpoint := newRangeFixture(t)
	client := newDaxClient(t, endpoint)

	seedRangeItems(t, client)

	out, err := client.Scan(&v1dynamodb.ScanInput{
		TableName:            new(rangeTable),
		ProjectionExpression: new("#l"),
		ExpressionAttributeNames: map[string]*string{
			"#l": new("label"),
		},
	})
	if err != nil {
		t.Fatalf("Scan projection via DAX: %v", err)
	}

	if got := len(out.Items); got != 3 {
		t.Fatalf("projected Scan count: got %d want 3", got)
	}

	for _, item := range out.Items {
		if len(item) != 1 {
			t.Fatalf("projected Scan item should have exactly 1 attribute: %#v", item)
		}

		if _, ok := item["label"]; !ok {
			t.Fatalf("projected Scan item missing label: %#v", item)
		}
	}
}

// TestDataPlaneQuery exercises a Query with a KeyConditionExpression over a
// numeric range key and asserts the matching items come back in order.
func TestDataPlaneQuery(t *testing.T) {
	endpoint := newRangeFixture(t)
	client := newDaxClient(t, endpoint)

	seedRangeItems(t, client)

	out, err := client.Query(&v1dynamodb.QueryInput{
		TableName:              new(rangeTable),
		KeyConditionExpression: new("pk = :p AND sk > :s"),
		ExpressionAttributeValues: map[string]*v1dynamodb.AttributeValue{
			":p": {S: new("g1")},
			":s": {N: new("10")},
		},
	})
	if err != nil {
		t.Fatalf("Query via DAX: %v", err)
	}

	if got := len(out.Items); got != 2 {
		t.Fatalf("Query item count: got %d want 2", got)
	}

	assertNumber(t, out.Items[0], "sk", "20")
	assertNumber(t, out.Items[1], "sk", "30")
}

// TestDataPlaneQueryWithFilter exercises a Query carrying both a
// KeyConditionExpression and a FilterExpression.
func TestDataPlaneQueryWithFilter(t *testing.T) {
	endpoint := newRangeFixture(t)
	client := newDaxClient(t, endpoint)

	seedRangeItems(t, client)

	out, err := client.Query(&v1dynamodb.QueryInput{
		TableName:              new(rangeTable),
		KeyConditionExpression: new("pk = :p"),
		FilterExpression:       new("label = :l"),
		ExpressionAttributeValues: map[string]*v1dynamodb.AttributeValue{
			":p": {S: new("g1")},
			":l": {S: new("keep")},
		},
	})
	if err != nil {
		t.Fatalf("Query via DAX: %v", err)
	}

	if got := len(out.Items); got != 1 {
		t.Fatalf("filtered Query count: got %d want 1", got)
	}

	assertNumber(t, out.Items[0], "sk", "30")
}

// TestDataPlaneScan exercises a Scan with a FilterExpression.
func TestDataPlaneScan(t *testing.T) {
	endpoint := newRangeFixture(t)
	client := newDaxClient(t, endpoint)

	seedRangeItems(t, client)

	out, err := client.Scan(&v1dynamodb.ScanInput{
		TableName:        new(rangeTable),
		FilterExpression: new("sk >= :min"),
		ExpressionAttributeValues: map[string]*v1dynamodb.AttributeValue{
			":min": {N: new("20")},
		},
	})
	if err != nil {
		t.Fatalf("Scan via DAX: %v", err)
	}

	if got := len(out.Items); got != 2 {
		t.Fatalf("Scan filtered count: got %d want 2", got)
	}
}

// TestDataPlaneNumericRangeKeyRoundTrip puts and gets an item on a hash+numeric
// range-key table, asserting the lexdecimal range-key codec round-trips.
func TestDataPlaneNumericRangeKeyRoundTrip(t *testing.T) {
	t.Parallel()

	endpoint := newRangeFixture(t)
	client := newDaxClient(t, endpoint)

	for _, sk := range []string{"-12.5", "0", "42", "1000000"} {
		if _, err := client.PutItem(&v1dynamodb.PutItemInput{
			TableName: new(rangeTable),
			Item: map[string]*v1dynamodb.AttributeValue{
				"pk":   {S: new("rt")},
				"sk":   {N: new(sk)},
				"note": {S: new("v" + sk)},
			},
		}); err != nil {
			t.Fatalf("PutItem sk=%s: %v", sk, err)
		}

		out, err := client.GetItem(&v1dynamodb.GetItemInput{
			TableName: new(rangeTable),
			Key: map[string]*v1dynamodb.AttributeValue{
				"pk": {S: new("rt")},
				"sk": {N: new(sk)},
			},
		})
		if err != nil {
			t.Fatalf("GetItem sk=%s: %v", sk, err)
		}

		assertNumber(t, out.Item, "sk", sk)
		assertString(t, out.Item, "note", "v"+sk)
	}
}

// TestDataPlaneTransactWriteAndGet exercises TransactWriteItems (Put + Update)
// followed by TransactGetItems against the real amazon-dax-go client, verifying
// the multi-section transact request/response framing round-trips.
func TestDataPlaneTransactWriteAndGet(t *testing.T) {
	endpoint := newDataPlaneFixture(t)
	client := newDaxClient(t, endpoint)

	// Seed an item that the transaction will update.
	if _, err := client.PutItem(&v1dynamodb.PutItemInput{
		TableName: new(integrationTable),
		Item: map[string]*v1dynamodb.AttributeValue{
			"pk":    {S: new("tx#b")},
			"count": {N: new("1")},
		},
	}); err != nil {
		t.Fatalf("seed PutItem via DAX: %v", err)
	}

	_, err := client.TransactWriteItems(&v1dynamodb.TransactWriteItemsInput{
		TransactItems: []*v1dynamodb.TransactWriteItem{
			{Put: &v1dynamodb.Put{
				TableName: new(integrationTable),
				Item: map[string]*v1dynamodb.AttributeValue{
					"pk":   {S: new("tx#a")},
					"name": {S: new("alpha")},
				},
			}},
			{Update: &v1dynamodb.Update{
				TableName: new(integrationTable),
				Key: map[string]*v1dynamodb.AttributeValue{
					"pk": {S: new("tx#b")},
				},
				UpdateExpression: new("SET #c = :c"),
				ExpressionAttributeNames: map[string]*string{
					"#c": new("count"),
				},
				ExpressionAttributeValues: map[string]*v1dynamodb.AttributeValue{
					":c": {N: new("9")},
				},
			}},
		},
	})
	if err != nil {
		t.Fatalf("TransactWriteItems via DAX: %v", err)
	}

	out, err := client.TransactGetItems(&v1dynamodb.TransactGetItemsInput{
		TransactItems: []*v1dynamodb.TransactGetItem{
			{Get: &v1dynamodb.Get{
				TableName: new(integrationTable),
				Key: map[string]*v1dynamodb.AttributeValue{
					"pk": {S: new("tx#a")},
				},
			}},
			{Get: &v1dynamodb.Get{
				TableName: new(integrationTable),
				Key: map[string]*v1dynamodb.AttributeValue{
					"pk": {S: new("tx#b")},
				},
			}},
		},
	})
	if err != nil {
		t.Fatalf("TransactGetItems via DAX: %v", err)
	}

	if got := len(out.Responses); got != 2 {
		t.Fatalf("TransactGetItems response count: got %d want 2", got)
	}

	assertString(t, out.Responses[0].Item, "pk", "tx#a")
	assertString(t, out.Responses[0].Item, "name", "alpha")
	assertNumber(t, out.Responses[1].Item, "count", "9")
}

// TestDataPlaneTransactGetProjection verifies TransactGetItems honors a
// per-item ProjectionExpression via the projection response sub-protocol.
func TestDataPlaneTransactGetProjection(t *testing.T) {
	endpoint := newDataPlaneFixture(t)
	client := newDaxClient(t, endpoint)

	if _, err := client.PutItem(&v1dynamodb.PutItemInput{
		TableName: new(integrationTable),
		Item: map[string]*v1dynamodb.AttributeValue{
			"pk":    {S: new("txp#1")},
			"name":  {S: new("zed")},
			"score": {N: new("3")},
		},
	}); err != nil {
		t.Fatalf("seed PutItem via DAX: %v", err)
	}

	out, err := client.TransactGetItems(&v1dynamodb.TransactGetItemsInput{
		TransactItems: []*v1dynamodb.TransactGetItem{
			{Get: &v1dynamodb.Get{
				TableName: new(integrationTable),
				Key: map[string]*v1dynamodb.AttributeValue{
					"pk": {S: new("txp#1")},
				},
				ProjectionExpression: new("#n"),
				ExpressionAttributeNames: map[string]*string{
					"#n": new("name"),
				},
			}},
		},
	})
	if err != nil {
		t.Fatalf("TransactGetItems projection via DAX: %v", err)
	}

	if got := len(out.Responses); got != 1 {
		t.Fatalf("response count: got %d want 1", got)
	}

	item := out.Responses[0].Item
	assertString(t, item, "name", "zed")

	if len(item) != 1 {
		t.Fatalf("projected transact-get item should have exactly 1 attribute: %#v", item)
	}
}

// TestDataPlaneTransactWriteConditionFails verifies a TransactWriteItems whose
// ConditionCheck fails is surfaced as an error (atomic rollback), exercising the
// condition-expression section of the request framing.
func TestDataPlaneTransactWriteConditionFails(t *testing.T) {
	endpoint := newDataPlaneFixture(t)
	client := newDaxClient(t, endpoint)

	if _, err := client.PutItem(&v1dynamodb.PutItemInput{
		TableName: new(integrationTable),
		Item: map[string]*v1dynamodb.AttributeValue{
			"pk":    {S: new("txc#guard")},
			"state": {S: new("locked")},
		},
	}); err != nil {
		t.Fatalf("seed PutItem via DAX: %v", err)
	}

	_, err := client.TransactWriteItems(&v1dynamodb.TransactWriteItemsInput{
		TransactItems: []*v1dynamodb.TransactWriteItem{
			{ConditionCheck: &v1dynamodb.ConditionCheck{
				TableName: new(integrationTable),
				Key: map[string]*v1dynamodb.AttributeValue{
					"pk": {S: new("txc#guard")},
				},
				ConditionExpression: new("#s = :open"),
				ExpressionAttributeNames: map[string]*string{
					"#s": new("state"),
				},
				ExpressionAttributeValues: map[string]*v1dynamodb.AttributeValue{
					":open": {S: new("open")},
				},
			}},
			{Put: &v1dynamodb.Put{
				TableName: new(integrationTable),
				Item: map[string]*v1dynamodb.AttributeValue{
					"pk": {S: new("txc#new")},
				},
			}},
		},
	})
	if err == nil {
		t.Fatal("expected TransactWriteItems with failing ConditionCheck to error")
	}

	// The aborted transaction must not have written the second item.
	got, err := client.GetItem(&v1dynamodb.GetItemInput{
		TableName: new(integrationTable),
		Key: map[string]*v1dynamodb.AttributeValue{
			"pk": {S: new("txc#new")},
		},
	})
	if err != nil {
		t.Fatalf("GetItem via DAX: %v", err)
	}

	if len(got.Item) != 0 {
		t.Fatalf("aborted transaction leaked a write: %#v", got.Item)
	}
}

func seedRangeItems(t *testing.T, client *daxgo.Dax) {
	t.Helper()

	rows := []struct {
		sk    string
		label string
	}{
		{"10", "drop"},
		{"20", "drop"},
		{"30", "keep"},
	}

	for _, row := range rows {
		if _, err := client.PutItem(&v1dynamodb.PutItemInput{
			TableName: new(rangeTable),
			Item: map[string]*v1dynamodb.AttributeValue{
				"pk":    {S: new("g1")},
				"sk":    {N: new(row.sk)},
				"label": {S: new(row.label)},
			},
		}); err != nil {
			t.Fatalf("seed PutItem sk=%s: %v", row.sk, err)
		}
	}
}

func assertString(t *testing.T, item map[string]*v1dynamodb.AttributeValue, key, want string) {
	t.Helper()

	av := item[key]
	if av == nil || av.S == nil {
		t.Fatalf("attribute %q missing or not a string: %#v", key, av)
	}

	if *av.S != want {
		t.Fatalf("attribute %q: got %q want %q", key, *av.S, want)
	}
}

func assertNumber(t *testing.T, item map[string]*v1dynamodb.AttributeValue, key, want string) {
	t.Helper()

	av := item[key]
	if av == nil || av.N == nil {
		t.Fatalf("attribute %q missing or not a number: %#v", key, av)
	}

	if *av.N != want {
		t.Fatalf("attribute %q: got %q want %q", key, *av.N, want)
	}
}

func TestDataPlaneCaching(t *testing.T) {
	t.Parallel()

	handler := dax.NewHandler(dax.NewInMemoryBackend("000000000000", "us-east-1"))
	dp := handler.EnableDataPlane(context.TODO(), "127.0.0.1:0")
	if err := handler.StartWorker(context.Background()); err != nil {
		t.Fatalf("start data plane: %v", err)
	}
	t.Cleanup(func() { handler.Shutdown(context.Background()) })
	createIntegrationTable(t, handler.DataPlaneBackend())
	endpoint := "dax://" + dp.Addr().String()

	client := newDaxClient(t, endpoint)

	// 1. Put via DAX
	_, err := client.PutItem(&v1dynamodb.PutItemInput{
		TableName: new(integrationTable),
		Item: map[string]*v1dynamodb.AttributeValue{
			"pk":   {S: new("cache#1")},
			"name": {S: new("Original")},
		},
	})
	require.NoError(t, err)

	// 2. Get via DAX (loads cache)
	out1, err := client.GetItem(&v1dynamodb.GetItemInput{
		TableName: new(integrationTable),
		Key: map[string]*v1dynamodb.AttributeValue{
			"pk": {S: new("cache#1")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "Original", *out1.Item["name"].S)

	// 3. Mutate backend directly (bypassing DAX cache)
	backend := handler.DataPlaneBackend()
	_, err = backend.PutItem(context.Background(), &v2dynamodb.PutItemInput{
		TableName: new(integrationTable),
		Item: map[string]v2types.AttributeValue{
			"pk":   &v2types.AttributeValueMemberS{Value: "cache#1"},
			"name": &v2types.AttributeValueMemberS{Value: "Bypassed"},
		},
	})
	require.NoError(t, err)

	// 4. Get via DAX again - should return cached "Original"
	out2, err := client.GetItem(&v1dynamodb.GetItemInput{
		TableName: new(integrationTable),
		Key: map[string]*v1dynamodb.AttributeValue{
			"pk": {S: new("cache#1")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "Original", *out2.Item["name"].S, "Expected cached Original value, but got Bypassed")

	// 5. Update via DAX (invalidates cache)
	_, err = client.UpdateItem(&v1dynamodb.UpdateItemInput{
		TableName: new(integrationTable),
		Key: map[string]*v1dynamodb.AttributeValue{
			"pk": {S: new("cache#1")},
		},
		UpdateExpression: new("SET #n = :v"),
		ExpressionAttributeNames: map[string]*string{
			"#n": new("name"),
		},
		ExpressionAttributeValues: map[string]*v1dynamodb.AttributeValue{
			":v": {S: new("UpdatedViaDax")},
		},
	})
	require.NoError(t, err)

	// 6. Get via DAX - should return "UpdatedViaDax"
	out3, err := client.GetItem(&v1dynamodb.GetItemInput{
		TableName: new(integrationTable),
		Key: map[string]*v1dynamodb.AttributeValue{
			"pk": {S: new("cache#1")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "UpdatedViaDax", *out3.Item["name"].S)
}

func TestDataPlaneProjectionExpression(t *testing.T) {
	// Must run serially due to ANTLR lexer issue in the client SDK
	// t.Parallel()

	endpoint := newDataPlaneFixture(t)
	client := newDaxClient(t, endpoint)

	_, err := client.PutItem(&v1dynamodb.PutItemInput{
		TableName: new(integrationTable),
		Item: map[string]*v1dynamodb.AttributeValue{
			"pk":     {S: new("proj#1")},
			"name":   {S: new("Ada")},
			"age":    {N: new("25")},
			"hidden": {S: new("secret")},
		},
	})
	require.NoError(t, err)

	out, err := client.GetItem(&v1dynamodb.GetItemInput{
		TableName: new(integrationTable),
		Key: map[string]*v1dynamodb.AttributeValue{
			"pk": {S: new("proj#1")},
		},
		ProjectionExpression: new("#n, age"),
		ExpressionAttributeNames: map[string]*string{
			"#n": new("name"),
		},
	})
	require.NoError(t, err)

	require.Len(t, out.Item, 2)
	assert.Equal(t, "Ada", *out.Item["name"].S)
	assert.Equal(t, "25", *out.Item["age"].N)
	assert.NotContains(t, out.Item, "hidden")
	assert.NotContains(t, out.Item, "pk") // unless pk is requested, it's not returned
}
