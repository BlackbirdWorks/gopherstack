package athena

const (
	typeBigint    = "bigint"
	typeVarchar   = "varchar"
	typeDecimal   = "decimal"
	typeDate      = "date"
	typeTimestamp = "timestamp"
	typeInteger   = "integer"
	typeDouble    = "double"
	typeBoolean   = "boolean"
	typeInt       = "int"
	typeSmallint  = "smallint"
	typeTinyint   = "tinyint"
	typeReal      = "real"
	typeFloat     = "float"
	typeNumeric   = "numeric"
	sqlInteger    = "INTEGER"

	declDateText      = "DATE_TEXT"
	declTimestampText = "TIMESTAMP_TEXT"
	declVarchar       = "VARCHAR"
	strTrue           = "true"
	strFalse          = "false"

	unitSecond  = "second"
	unitMinute  = "minute"
	unitHour    = "hour"
	unitDay     = "day"
	unitWeek    = "week"
	unitMonth   = "month"
	unitQuarter = "quarter"
	unitYear    = "year"

	kwFrom = "from"

	argsTwo           = 2
	argsThree         = 3
	monthsPerYear     = 12
	readBufBytes      = 64 << 10
	gzipMagicLen      = 2
	commentDelimLen   = 2
	tokenSlackPerCall = 8
)

// syntaxError is a statement the engine cannot parse; its text is the full reason.
type syntaxError struct{ msg string }

func (e *syntaxError) Error() string { return e.msg }
