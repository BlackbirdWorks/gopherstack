package iceberg

// Iceberg primitive type names and document keys used across the package.
const (
	typeBoolean     = "boolean"
	typeInt         = "int"
	typeLong        = "long"
	typeFloat       = "float"
	typeDouble      = "double"
	typeDate        = "date"
	typeTime        = "time"
	typeTimestamp   = "timestamp"
	typeTimestamptz = "timestamptz"
	typeString      = "string"
	typeUUID        = "uuid"
	typeBinary      = "binary"
	typeStruct      = "struct"
	typeMap         = "map"

	transformYear     = "year"
	transformMonth    = "month"
	transformHour     = "hour"
	transformDay      = "day"
	transformBucket   = "bucket"
	transformTruncate = "truncate"

	keyFormatVersion = "format-version"
	keyContent       = "content"
	keyFields        = "fields"
	keyDefault       = "default"
	avroRecord       = "record"
	keyName          = "name"
	keyType          = "type"
	keyKey           = "key"
	keyValue         = "value"
)

// Field ids fixed by the manifest and manifest-list schemas of the table spec.
const (
	idStatus           = 0
	idSnapshotID       = 1
	idDataFile         = 2
	idSequenceNumber   = 3
	idFileSequence     = 4
	idFilePath         = 100
	idFileFormat       = 101
	idPartition        = 102
	idRecordCount      = 103
	idFileSize         = 104
	idValueCounts      = 109
	idNullCounts       = 110
	idNullCountsKey    = 121
	idNullCountsValue  = 122
	idContent          = 134
	idValueCountsKey   = 119
	idValueCountsValue = 120
)
