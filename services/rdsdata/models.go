package rdsdata

// Field represents a single field value in an RDS Data API record.
type Field struct {
	IsNull       *bool    `json:"isNull,omitempty"`
	BooleanValue *bool    `json:"booleanValue,omitempty"`
	LongValue    *int64   `json:"longValue,omitempty"`
	DoubleValue  *float64 `json:"doubleValue,omitempty"`
	StringValue  *string  `json:"stringValue,omitempty"`
	BlobValue    []byte   `json:"blobValue,omitempty"`
}

// ColumnMetadata describes a single column returned by a SQL statement.
// Field set mirrors the real RDS Data API shape (types.ColumnMetadata in
// aws-sdk-go-v2/service/rdsdata); see engine.go's columnMetadataFor for how
// each field is derived from the pure-Go SQLite driver's limited column
// introspection.
type ColumnMetadata struct {
	Name                string `json:"name"`
	Label               string `json:"label"`
	TypeName            string `json:"typeName"`
	SchemaName          string `json:"schemaName"`
	TableName           string `json:"tableName"`
	Type                int32  `json:"type"`
	ArrayBaseColumnType int32  `json:"arrayBaseColumnType"`
	Nullable            int32  `json:"nullable"`
	Precision           int32  `json:"precision"`
	Scale               int32  `json:"scale"`
	IsAutoIncrement     bool   `json:"isAutoIncrement"`
	IsCaseSensitive     bool   `json:"isCaseSensitive"`
	IsCurrency          bool   `json:"isCurrency"`
	IsSigned            bool   `json:"isSigned"`
}

// Transaction represents an in-progress database transaction.
type Transaction struct {
	TransactionID string `json:"transactionId"`
	Status        string `json:"status"`
}

// ExecutedStatement represents a record of an executed SQL statement.
type ExecutedStatement struct {
	SQL           string `json:"sql"`
	ResourceARN   string `json:"resourceArn"`
	TransactionID string `json:"transactionId,omitempty"`
}

// SQLParameter represents a named parameter for a SQL statement.
// TypeHint mirrors the real API's DATE/DECIMAL/JSON/TIME/TIMESTAMP/UUID hint
// values; it is accepted on the wire but does not change bind behavior since
// the mock SQLite engine has no distinct DATE/TIMESTAMP/UUID column types to
// convert to (see PARITY.md).
type SQLParameter struct {
	Name     string `json:"name"`
	TypeHint string `json:"typeHint,omitempty"`
	Value    Field  `json:"value"`
}

// UpdateResult represents the result of a single update in a batch.
type UpdateResult struct {
	GeneratedFields []Field `json:"generatedFields"`
}

// SQLStatementResult represents the result of a single SQL statement in an ExecuteSql call.
type SQLStatementResult struct {
	NumberOfRecordsUpdated int64 `json:"numberOfRecordsUpdated"`
}
