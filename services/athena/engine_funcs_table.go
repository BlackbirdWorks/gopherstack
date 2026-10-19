package athena

import (
	"database/sql/driver"
	"net/url"
	"time"

	sqlitedriver "modernc.org/sqlite"
)

type scalarFn = func(*fnCtx, []driver.Value) (driver.Value, error)

type scalarDef struct {
	fn    scalarFn
	name  string
	nargs int32
}

func scalarDefs() []scalarDef {
	return []scalarDef{
		{udfRegexpExtract, "regexp_extract", -1},
		{udfRegexpExtractAll, "regexp_extract_all", -1},
		{udfRegexpReplace, "regexp_replace", -1},
		{udfRegexpCount, "regexp_count", argsTwo}, {udfRegexpSplit, "regexp_split", argsTwo},
		{udfReverse, "reverse", 1}, {udfLpad, "lpad", argsThree}, {udfRpad, "rpad", argsThree},
		{udfStartsWith, "starts_with", argsTwo}, {udfChr, "chr", 1}, {udfCodepoint, "codepoint", 1},
		{udfLevenshtein, "levenshtein_distance", argsTwo},
		{hashFunc(sumMD5), "md5", 1}, {hashFunc(sumSHA1), "sha1", 1}, {hashFunc(sumSHA256), "sha256", 1},
		{hashFunc(sumSHA512), "sha512", 1}, {udfCRC32, "crc32", 1},
		{udfToHex, "to_hex", 1}, {udfFromHex, "from_hex", 1}, {udfToBase64, "to_base64", 1},
		{udfFromBase64, "from_base64", 1}, {identityFunc, "to_utf8", 1}, {identityFunc, "from_utf8", 1},
		{identityFunc, "try", 1}, {identityFunc, "json_format", 1}, {udfJSONParse, "json_parse", 1},
		{urlPart(func(u *url.URL) (string, bool) { return u.Hostname(), u.Host != "" }), "url_extract_host", 1},
		{urlPart(func(u *url.URL) (string, bool) { return u.Path, true }), "url_extract_path", 1},
		{urlPart(func(u *url.URL) (string, bool) { return u.Scheme, u.Scheme != "" }), "url_extract_protocol", 1},
		{urlPart(func(u *url.URL) (string, bool) { return u.RawQuery, true }), "url_extract_query", 1},
		{urlPart(func(u *url.URL) (string, bool) { return u.Fragment, true }), "url_extract_fragment", 1},
		{udfURLParameter, "url_extract_parameter", argsTwo},
		{udfRand, "rand", 0}, {udfCbrt, "cbrt", 1}, {udfIsNaN, "is_nan", 1}, {udfIsFinite, "is_finite", 1},
		{bitwise(func(a, b int64) int64 { return a & b }), "bitwise_and", argsTwo},
		{bitwise(func(a, b int64) int64 { return a | b }), "bitwise_or", argsTwo},
		{bitwise(func(a, b int64) int64 { return a ^ b }), "bitwise_xor", argsTwo},
		{udfJSONExtractScalar, "json_extract_scalar", argsTwo}, {udfJSONExtract, "athena_json_extract", argsTwo},
		{udfJSONSize, "json_size", argsTwo},
		{udfSplit, "split", -1}, {udfSplitPart, "split_part", argsThree},
		{udfElementAt, "element_at", argsTwo}, {udfCardinality, "cardinality", 1},
		{udfContains, "contains", argsTwo}, {udfArrayPosition, "array_position", argsTwo},
		{udfArrayJoin, "array_join", -1},
		{udfArrayDistinct, "array_distinct", 1}, {udfArraySort, "array_sort", 1},
		{udfArrayMax, "array_max", 1}, {udfArrayMin, "array_min", 1}, {udfArrayRemove, "array_remove", argsTwo},
		{udfArrayConcat, "array_concat", -1}, {udfSlice, "slice", argsThree},
		{udfMapKeys, "map_keys", 1}, {udfMapValues, "map_values", 1}, {udfMapFromArrays, "map", argsTwo},
		{udfDateFormat, "date_format", argsTwo}, {udfDateParse, "date_parse", argsTwo},
		{udfFormatDatetime, "format_datetime", argsTwo}, {udfParseDatetime, "parse_datetime", argsTwo},
		{udfFromISO8601Timestamp, "from_iso8601_timestamp", 1}, {udfFromISO8601Date, "from_iso8601_date", 1},
		{udfToISO8601, "to_iso8601", 1}, {udfFromUnixtime, "from_unixtime", 1}, {udfToUnixtime, "to_unixtime", 1},
		{udfLastDayOfMonth, "last_day_of_month", 1}, {udfToDate, "athena_to_date", 1},
		{tsPart(isoDayOfWeek), "day_of_week", 1}, {tsPart(isoDayOfWeek), "dow", 1},
		{tsPart(func(t time.Time) int64 { return int64(t.YearDay()) }), "day_of_year", 1},
		{tsPart(func(t time.Time) int64 { return int64(t.YearDay()) }), "doy", 1},
		{tsPart(isoWeek), "week", 1}, {tsPart(isoWeek), "week_of_year", 1}, {tsPart(quarterOf), "quarter", 1},
		{tsPart(func(t time.Time) int64 { return int64(t.Minute()) }), "minute", 1},
		{tsPart(func(t time.Time) int64 { return int64(t.Second()) }), "second", 1},
		{tsPart(func(t time.Time) int64 { return int64(t.Nanosecond() / int(time.Millisecond)) }), "millisecond", 1},
	}
}

func registerExtraFuncs() {
	for _, d := range scalarDefs() {
		if d.nargs < 0 {
			d.fn = atLeastArgs(variadicMin(d.name), d.fn)
		}

		if d.name == "rand" {
			sqlitedriver.MustRegisterScalarFunction(d.name, d.nargs, d.fn)

			continue
		}

		sqlitedriver.MustRegisterDeterministicScalarFunction(d.name, d.nargs, d.fn)
	}
}

func variadicMin(name string) int {
	if name == "array_concat" {
		return 1
	}

	return argsTwo
}

// atLeastArgs fails a variadic call with too few arguments instead of indexing past them.
func atLeastArgs(n int, fn scalarFn) scalarFn {
	return func(c *fnCtx, args []driver.Value) (driver.Value, error) {
		if len(args) < n {
			return nil, errBadValue
		}

		return fn(c, args)
	}
}
