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
