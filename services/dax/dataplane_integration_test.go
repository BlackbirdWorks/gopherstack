package dax_test

import (
	"context"
	"testing"
	"time"

	daxgo "github.com/aws/aws-dax-go/dax"
	"github.com/aws/aws-sdk-go-v2/aws"
	v2dynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	v2types "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	v1creds "github.com/aws/aws-sdk-go/aws/credentials"
	v1dynamodb "github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

const integrationTable = "dax-integration"

// newDataPlaneFixture starts a DAX data-plane listener on an ephemeral port with
// a single table created in its backing DynamoDB store. It returns the bound
// "dax://host:port" endpoint.
func newDataPlaneFixture(t *testing.T) string {
	t.Helper()

	handler := dax.NewHandler(dax.NewInMemoryBackend("000000000000", "us-east-1"))
	dp := handler.EnableDataPlane(nil, "127.0.0.1:0")

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
	dp := handler.EnableDataPlane(nil, "127.0.0.1:0")

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
	t.Parallel()

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
	t.Parallel()

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

// TestDataPlaneUpdateItemReturnUpdatedNewRejected verifies UpdateItem with
// ReturnValues UPDATED_NEW is surfaced as a request failure (the projected
// response sub-protocol is not served) rather than corrupting the response.
func TestDataPlaneUpdateItemReturnUpdatedNewRejected(t *testing.T) {
	t.Parallel()

	endpoint := newDataPlaneFixture(t)
	client := newDaxClient(t, endpoint)

	if _, err := client.PutItem(&v1dynamodb.PutItemInput{
		TableName: new(integrationTable),
		Item: map[string]*v1dynamodb.AttributeValue{
			"pk":    {S: new("user#un")},
			"count": {N: new("1")},
		},
	}); err != nil {
		t.Fatalf("PutItem via DAX: %v", err)
	}

	_, err := client.UpdateItem(&v1dynamodb.UpdateItemInput{
		TableName: new(integrationTable),
		Key: map[string]*v1dynamodb.AttributeValue{
			"pk": {S: new("user#un")},
		},
		UpdateExpression: new("SET #c = :c"),
		ExpressionAttributeNames: map[string]*string{
			"#c": new("count"),
		},
		ExpressionAttributeValues: map[string]*v1dynamodb.AttributeValue{
			":c": {N: new("2")},
		},
		ReturnValues: new(v1dynamodb.ReturnValueUpdatedNew),
	})
	if err == nil {
		t.Fatal("expected UPDATED_NEW UpdateItem to be rejected")
	}
}

// TestDataPlaneQuery exercises a Query with a KeyConditionExpression over a
// numeric range key and asserts the matching items come back in order.
func TestDataPlaneQuery(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
