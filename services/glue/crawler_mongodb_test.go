package glue_test

import (
	"maps"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func (e *crawlEnv) mongoConnection(name, connType, url string, extra map[string]string) {
	e.t.Helper()

	props := map[string]string{"CONNECTION_URL": url}
	maps.Copy(props, extra)

	_, err := e.client.CreateConnection(e.t.Context(), &gluesdk.CreateConnectionInput{
		ConnectionInput: &gluetypes.ConnectionInput{
			Name: aws.String(name), ConnectionType: gluetypes.ConnectionType(connType), ConnectionProperties: props,
		},
	})
	require.NoError(e.t, err)
}

func TestCrawler_MongoDBTargetFailures(t *testing.T) {
	t.Parallel()

	const dead = "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=300&connectTimeoutMS=300"

	tests := []struct {
		extra                           map[string]string
		name, connType, url, path, want string
		noConn                          bool
	}{
		{name: "no connection", noConn: true, path: "db/%", want: "connection not found"},
		{
			name:     "wrong connection type",
			connType: "NETWORK",
			url:      dead,
			path:     "db/%",
			want:     "needs a connection of type MONGODB",
		},
		{name: "path without collection", connType: "MONGODB", url: dead, path: "db", want: "database/collection"},
		{name: "unreachable", connType: "MONGODB", url: dead, path: "db/%", want: "MongoDB data store"},
		{
			name: "secret cannot be resolved", connType: "MONGODB", url: dead, path: "db/%",
			extra: map[string]string{"SECRET_ID": "creds"}, want: "Secrets Manager is not available",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCrawlEnv(t)
			if !tt.noConn {
				env.mongoConnection("mongo", tt.connType, tt.url, tt.extra)
			}

			env.create(&gluetypes.CrawlerTargets{MongoDBTargets: []gluetypes.MongoDBTarget{{
				ConnectionName: aws.String("mongo"), Path: aws.String(tt.path),
			}}}, nil)

			c := env.crawl()
			assert.Equal(t, gluetypes.LastCrawlStatusFailed, c.LastCrawl.Status)
			assert.Contains(t, aws.ToString(c.LastCrawl.ErrorMessage), tt.want)
			assert.Equal(t, gluetypes.CrawlerHistoryStateFailed, env.lastCrawl().State)
		})
	}
}

func TestCrawler_MongoDBTarget_Docker(t *testing.T) {
	t.Parallel()

	addr := startContainer(t, jdbcEngine{image: "mongo:5.0", port: "27017"})
	uri := "mongodb://" + addr

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Disconnect(t.Context()) })

	require.Eventually(t, func() bool {
		return client.Ping(t.Context(), nil) == nil
	}, 2*time.Minute, time.Second)

	db := client.Database("appdb")
	_, err = db.Collection("orders").InsertMany(t.Context(), []any{
		bson.D{{Key: "sku", Value: "a"}, {Key: "qty", Value: int32(2)}, {Key: "price", Value: 1.5},
			{Key: "tags", Value: bson.A{"x", "y"}}, {Key: "ship", Value: bson.D{{Key: "city", Value: "sf"}}}},
		bson.D{{Key: "sku", Value: "b"}, {Key: "qty", Value: int64(3)}, {Key: "ok", Value: true},
			{Key: "at", Value: time.Unix(1700000000, 0)}},
	})
	require.NoError(t, err)
	_, err = db.Collection("scratch").InsertOne(t.Context(), bson.D{{Key: "z", Value: "1"}})
	require.NoError(t, err)

	env := newCrawlEnv(t)
	env.mongoConnection("mongo", "MONGODB", uri, nil)
	env.create(&gluetypes.CrawlerTargets{MongoDBTargets: []gluetypes.MongoDBTarget{{
		ConnectionName: aws.String("mongo"), Path: aws.String("appdb/ord%"), ScanAll: aws.Bool(true),
	}}}, nil)

	c := env.crawl()
	require.Equal(t, gluetypes.LastCrawlStatusSucceeded, c.LastCrawl.Status, aws.ToString(c.LastCrawl.ErrorMessage))

	tbl := env.table("appdb_orders")
	assert.Equal(t, []string{
		"_id:struct<oid:string>", "sku:string", "qty:bigint", "price:double",
		"tags:array<string>", "ship:struct<city:string>", "ok:boolean", "at:timestamp",
	}, colSpec(tbl.StorageDescriptor.Columns))
	assert.Equal(t, "appdb.orders", aws.ToString(tbl.StorageDescriptor.Location))
	assert.Equal(t, "mongodb", tbl.Parameters["classification"])
	assert.Equal(t, "mongo", tbl.Parameters["connectionName"])
	assert.Equal(t, "2", tbl.Parameters["recordCount"])
	assert.False(t, env.hasTable("appdb_scratch"), "collection outside the % pattern")
	assert.Equal(t, int32(1), env.metrics().TablesCreated)
}
