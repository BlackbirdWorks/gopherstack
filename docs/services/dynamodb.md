# DynamoDB

Full in-memory DynamoDB implementation with rich expression support, secondary indexes, transactions, streams, and TTL.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `CreateTable` | Create a table with key schema, LSIs, and GSIs |
| `DeleteTable` | Delete a table and all its items |
| `DescribeTable` | Get table metadata, status, and index descriptions |
| `ListTables` | List all tables with pagination |
| `UpdateTable` | Update table throughput or index configuration |
| `PutItem` | Write or replace an item |
| `GetItem` | Read a single item by primary key |
| `UpdateItem` | Perform a conditional or expression-based update |
| `DeleteItem` | Delete an item by primary key |
| `Query` | Query items by partition key with sort key conditions |
| `Scan` | Scan an entire table or index with filter expressions |
| `BatchGetItem` | Read up to 100 items in one request |
| `BatchWriteItem` | Write or delete up to 25 items in one request |
| `TransactGetItems` | Read up to 25 items atomically |
| `TransactWriteItems` | Write up to 25 items atomically with conditions |
| `DescribeTimeToLive` | Get TTL configuration |
| `UpdateTimeToLive` | Enable or disable TTL on an attribute |
| `DescribeContinuousBackups` | Get PITR status |
| `ExportTableToPointInTime` | Initiate an export job (stub) |
| `DescribeExport` | Describe an export job |
| `ListExports` | List export jobs |
| `DescribeStream` | Describe a DynamoDB stream |
| `GetShardIterator` | Get a shard iterator for stream processing |
| `GetRecords` | Read stream records |
| `ListStreams` | List streams for a table |
| `ListTagsOfResource` | List tags on a table |
| `TagResource` | Add tags to a table |
| `UntagResource` | Remove tags from a table |

## AWS CLI Examples

```bash
# Create a table
aws --endpoint-url http://localhost:8000 dynamodb create-table \
    --table-name Users \
    --attribute-definitions AttributeName=UserId,AttributeType=S \
    --key-schema AttributeName=UserId,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST

# Put an item
aws --endpoint-url http://localhost:8000 dynamodb put-item \
    --table-name Users \
    --item '{"UserId":{"S":"u123"},"Name":{"S":"Alice"},"Age":{"N":"30"}}'

# Get an item
aws --endpoint-url http://localhost:8000 dynamodb get-item \
    --table-name Users \
    --key '{"UserId":{"S":"u123"}}'

# Query with condition
aws --endpoint-url http://localhost:8000 dynamodb query \
    --table-name Orders \
    --key-condition-expression "UserId = :uid AND OrderDate > :d" \
    --expression-attribute-values '{":uid":{"S":"u123"},":d":{"S":"2024-01-01"}}'

# Update an item
aws --endpoint-url http://localhost:8000 dynamodb update-item \
    --table-name Users \
    --key '{"UserId":{"S":"u123"}}' \
    --update-expression "SET Age = :newage" \
    --expression-attribute-values '{":newage":{"N":"31"}}'

# Scan with filter
aws --endpoint-url http://localhost:8000 dynamodb scan \
    --table-name Users \
    --filter-expression "Age > :min" \
    --expression-attribute-values '{":min":{"N":"25"}}'

# Transact write
aws --endpoint-url http://localhost:8000 dynamodb transact-write-items \
    --transact-items '[
        {"Put":{"TableName":"Users","Item":{"UserId":{"S":"u456"},"Name":{"S":"Bob"}}}},
        {"Delete":{"TableName":"Users","Key":{"UserId":{"S":"u789"}}}}
    ]'
```

## Known Limitations

- TTL expiration is tracked but items are not automatically deleted on expiry unless the background janitor is running.
- `ExportTableToPointInTime` creates an export job record but does not write actual data files.
- Streams deliver records in-memory; integration with Lambda event source mappings requires the Lambda service to be running.
- Conditional expressions support the core operators (`=`, `<>`, `<`, `>`, `<=`, `>=`, `attribute_exists`, `attribute_not_exists`, `begins_with`, `contains`).
