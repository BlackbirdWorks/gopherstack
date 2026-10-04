package dynamodb

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// wireReadBackend is implemented by backends that can return read results in wire form,
// letting the handler serialise items without an SDK round trip.
type wireReadBackend interface {
	getItemWire(ctx context.Context, input *dynamodb.GetItemInput) (map[string]any, *types.ConsumedCapacity, error)
	queryWire(ctx context.Context, input *dynamodb.QueryInput) (*pageResult, error)
	scanWire(ctx context.Context, input *dynamodb.ScanInput) (*pageResult, error)
}

func (db *InMemoryDB) getItemWire(
	ctx context.Context,
	input *dynamodb.GetItemInput,
) (map[string]any, *types.ConsumedCapacity, error) {
	if !db.metrics.Enabled() {
		return db.getItemCore(ctx, input)
	}

	start := time.Now()
	item, cc, err := db.getItemCore(ctx, input)
	db.observeOp(ctx, opGetItem, aws.ToString(input.TableName), start, err, -1)

	return item, cc, err
}

func (db *InMemoryDB) queryWire(ctx context.Context, input *dynamodb.QueryInput) (*pageResult, error) {
	if !db.metrics.Enabled() {
		return db.queryCore(ctx, input)
	}

	start := time.Now()
	res, err := db.queryCore(ctx, input)

	items := -1
	if res != nil {
		items = int(res.count)
	}

	db.observeOp(ctx, opQuery, aws.ToString(input.TableName), start, err, items)

	return res, err
}

func (db *InMemoryDB) scanWire(ctx context.Context, input *dynamodb.ScanInput) (*pageResult, error) {
	if !db.metrics.Enabled() {
		return db.scanCore(ctx, input)
	}

	start := time.Now()
	res, err := db.scanCore(ctx, input)

	items := -1
	if res != nil {
		items = int(res.count)
	}

	db.observeOp(ctx, opScan, aws.ToString(input.TableName), start, err, items)

	return res, err
}
