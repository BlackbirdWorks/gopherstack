package events

// TableCreatedEvent is emitted when a DynamoDB table is created.
type TableCreatedEvent struct {
	Table string
}

func (e *TableCreatedEvent) EventType() string {
	return "dynamodb.table.created"
}

// TableDeletedEvent is emitted when a DynamoDB table is deleted.
type TableDeletedEvent struct {
	Table string
}

func (e *TableDeletedEvent) EventType() string {
	return "dynamodb.table.deleted"
}

// ItemCreatedEvent is emitted when an item is added to a DynamoDB table.
type ItemCreatedEvent struct {
	Key   map[string]any
	Table string
}

func (e *ItemCreatedEvent) EventType() string {
	return "dynamodb.item.created"
}

// ItemUpdatedEvent is emitted when an item in a DynamoDB table is updated.
type ItemUpdatedEvent struct {
	Key   map[string]any
	Table string
}

func (e *ItemUpdatedEvent) EventType() string {
	return "dynamodb.item.updated"
}

// ItemDeletedEvent is emitted when an item is deleted from a DynamoDB table.
type ItemDeletedEvent struct {
	Key   map[string]any
	Table string
}

func (e *ItemDeletedEvent) EventType() string {
	return "dynamodb.item.deleted"
}

// BucketCreatedEvent is emitted when an S3 bucket is created.
type BucketCreatedEvent struct {
	BucketName string
}

func (e *BucketCreatedEvent) EventType() string {
	return "s3.bucket.created"
}

// BucketDeletedEvent is emitted when an S3 bucket is deleted.
type BucketDeletedEvent struct {
	BucketName string
}

func (e *BucketDeletedEvent) EventType() string {
	return "s3.bucket.deleted"
}

// ObjectCreatedEvent is emitted when an object is added to an S3 bucket.
type ObjectCreatedEvent struct {
	BucketName string
	Key        string
	Size       int64
}

func (e *ObjectCreatedEvent) EventType() string {
	return "s3.object.created"
}

// ObjectDeletedEvent is emitted when an object is deleted from an S3 bucket.
type ObjectDeletedEvent struct {
	BucketName string
	Key        string
}

func (e *ObjectDeletedEvent) EventType() string {
	return "s3.object.deleted"
}
