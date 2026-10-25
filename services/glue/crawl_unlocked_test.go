package glue_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/glue"
)

const apiCallBound = 10 * time.Second

// gatedDDB blocks DescribeTable until release is closed.
type gatedDDB struct {
	*ddbbackend.InMemoryDB
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *gatedDDB) DescribeTable(
	ctx context.Context, in *dynamodb.DescribeTableInput,
) (*dynamodb.DescribeTableOutput, error) {
	g.once.Do(func() { close(g.entered) })

	select {
	case <-g.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return g.InMemoryDB.DescribeTable(ctx, in)
}

type gatedCrawl struct {
	b       *glue.InMemoryBackend
	release func()
	advance chan struct{}
}

func newGatedCrawl(t *testing.T) *gatedCrawl {
	t.Helper()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	g := &gatedDDB{InMemoryDB: ddbbackend.NewInMemoryDB(), entered: make(chan struct{}), release: make(chan struct{})}
	b.SetDynamoDBBackend(g)

	var releaseOnce sync.Once

	release := func() { releaseOnce.Do(func() { close(g.release) }) }
	t.Cleanup(release)

	_, err := g.CreateTable(t.Context(), &dynamodb.CreateTableInput{
		TableName:   aws.String("orders"),
		BillingMode: ddbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeN},
		},
		KeySchema: []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}},
	})
	require.NoError(t, err)

	_, err = b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
	require.NoError(t, err)

	_, err = b.CreateCrawler("cr", crawlRole, "db", glue.CrawlerTarget{
		DynamoDBTargets: []glue.DynamoDBTarget{{Path: "orders"}},
	}, nil)
	require.NoError(t, err)
	require.NoError(t, b.StartCrawler("cr"))

	gc := &gatedCrawl{b: b, release: release, advance: make(chan struct{})}

	go func() {
		defer close(gc.advance)
		glue.AdvanceStatesForTest(b, time.Now().Add(time.Hour))
	}()

	select {
	case <-g.entered:
	case <-time.After(apiCallBound):
		release()
		t.Fatal("crawl never reached its DynamoDB I/O")
	}

	return gc
}

func (gc *gatedCrawl) unblock(t *testing.T) {
	t.Helper()

	gc.release()

	select {
	case <-gc.advance:
	case <-time.After(apiCallBound):
		t.Fatal("crawl did not finish after its I/O was released")
	}
}

func TestCrawl_APICallsNotBlockedByTargetIO(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(b *glue.InMemoryBackend) error
		name string
	}{
		{name: "get_database", call: func(b *glue.InMemoryBackend) error { _, err := b.GetDatabase("db"); return err }},
		{name: "get_crawler", call: func(b *glue.InMemoryBackend) error { _, err := b.GetCrawler("cr"); return err }},
		{name: "get_tables", call: func(b *glue.InMemoryBackend) error { _, err := b.GetTables("db"); return err }},
		{name: "create_database", call: func(b *glue.InMemoryBackend) error {
			_, err := b.CreateDatabase(glue.DatabaseInput{Name: "other"}, nil)

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gc := newGatedCrawl(t)
			done := make(chan error, 1)

			go func() { done <- tt.call(gc.b) }()

			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(apiCallBound):
				t.Fatal("API call blocked while a crawl's target I/O was in flight")
			}

			c, err := gc.b.GetCrawler("cr")
			require.NoError(t, err)
			assert.Equal(t, "RUNNING", c.State)

			gc.unblock(t)

			c, err = gc.b.GetCrawler("cr")
			require.NoError(t, err)
			assert.Equal(t, "READY", c.State)

			tables, err := gc.b.GetTables("db")
			require.NoError(t, err)
			assert.Len(t, tables, 1)
		})
	}
}

func TestCrawl_StopDuringIODiscardsResults(t *testing.T) {
	t.Parallel()

	gc := newGatedCrawl(t)

	require.NoError(t, gc.b.StopCrawler("cr"))

	c, err := gc.b.GetCrawler("cr")
	require.NoError(t, err)
	assert.Equal(t, "STOPPING", c.State)

	gc.unblock(t)

	glue.AdvanceStatesForTest(gc.b, time.Now().Add(2*time.Hour))

	c, err = gc.b.GetCrawler("cr")
	require.NoError(t, err)
	assert.Equal(t, "READY", c.State)
	require.NotNil(t, c.LastCrawl)
	assert.Equal(t, "CANCELLED", c.LastCrawl.Status)

	tables, err := gc.b.GetTables("db")
	require.NoError(t, err)
	assert.Empty(t, tables, "a stopped crawl must not write to the catalog")

	require.NoError(t, gc.b.StartCrawler("cr"), "crawler is restartable after a stop")
}
