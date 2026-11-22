package timestreamquery

import (
	"maps"
	"strings"
)

type callCtx struct {
	inf  *inferrer
	n    *node
	args []tsType
}

type funcRule func(c *callCtx) tsType

func fixed(k tsKind) funcRule { return func(*callCtx) tsType { return scalar(k) } }

func same(i int) funcRule {
	return func(c *callCtx) tsType {
		if i < len(c.args) {
			return c.args[i]
		}

		return tsType{}
	}
}

func elemOf(i int) funcRule {
	return func(c *callCtx) tsType {
		if i < len(c.args) && c.args[i].kind == tyArray {
			return *c.args[i].elem
		}

		return tsType{}
	}
}

func arrayOfFirstArg(c *callCtx) tsType {
	if len(c.args) > 0 && c.args[0].kind != tyInvalid {
		return arrayOf(c.args[0])
	}

	return tsType{}
}

func arrayOfKind(k tsKind) funcRule {
	return func(*callCtx) tsType { return arrayOf(scalar(k)) }
}

func seriesOfDouble(*callCtx) tsType { return seriesOf(scalar(tyDouble)) }

func superAll(c *callCtx) tsType {
	res := scalar(tyUnknown)
	for _, a := range c.args {
		res = supertype(res, a)
	}

	return res
}

func superFrom(from int) funcRule {
	return func(c *callCtx) tsType {
		res := scalar(tyUnknown)

		for i := from; i < len(c.args); i++ {
			res = supertype(res, c.args[i])
		}

		return res
	}
}

func internalFn(*callCtx) tsType { return tsType{} }

func unsupportedFn(reason string) funcRule {
	return func(c *callCtx) tsType { return c.inf.unsupportedf(reason) }
}

func sumRule(c *callCtx) tsType {
	if len(c.args) == 0 {
		return tsType{}
	}

	switch c.args[0].kind {
	case tyInteger, tyBigint:
		return scalar(tyBigint)
	case tyDouble:
		return scalar(tyDouble)
	default:
		return tsType{}
	}
}

func randomRule(c *callCtx) tsType {
	if len(c.args) == 1 && isIntegral(c.args[0]) {
		return c.args[0]
	}

	return scalar(tyDouble)
}

func concatRule(c *callCtx) tsType {
	for _, a := range c.args {
		if a.kind == tyArray {
			return superAll(c)
		}
	}

	return scalar(tyVarchar)
}

func createSeriesRule(c *callCtx) tsType {
	if len(c.args) == argsTwo && c.args[1].known() {
		return seriesOf(c.args[1])
	}

	return tsType{}
}

func interpolateRule(c *callCtx) tsType {
	if len(c.args) < argsTwo || c.args[0].kind != tyTimeseries {
		return tsType{}
	}

	if c.args[1].kind == tyTimestamp {
		return *c.args[0].elem
	}

	return c.args[0]
}

func binRule(c *callCtx) tsType {
	if len(c.args) > 0 && (c.args[0].isTemporal() || c.args[0].isNumeric()) {
		return c.args[0]
	}

	return tsType{}
}

func sequenceRule(c *callCtx) tsType {
	t := superFrom(0)(c)
	if t.kind == tyDate {
		return arrayOf(t)
	}

	if t.known() {
		return arrayOf(t)
	}

	return tsType{}
}

func zipRule(c *callCtx) tsType {
	fields := make([]tsField, len(c.args))

	for i, a := range c.args {
		if a.kind != tyArray {
			return tsType{}
		}

		fields[i] = tsField{typ: *a.elem}
	}

	return arrayOf(rowOf(fields))
}

func nthRule(n int) funcRule {
	return func(c *callCtx) tsType {
		if len(c.args) == n {
			return arrayOfFirstArg(c)
		}

		return same(0)(c)
	}
}

func percentileRule(c *callCtx) tsType {
	if len(c.args) < argsTwo {
		return tsType{}
	}

	for _, a := range c.args[1:] {
		if a.kind == tyArray {
			return arrayOfFirstArg(c)
		}
	}

	return same(0)(c)
}

func aggregateRules() map[string]funcRule {
	return map[string]funcRule{
		"max_by": nthRule(argsThree), "min_by": nthRule(argsThree), "max": nthRule(argsTwo), "min": nthRule(argsTwo),
		"approx_percentile": percentileRule,
	}
}

func funcRules() map[string]funcRule {
	m := map[string]funcRule{}
	for _, group := range []map[string]funcRule{
		bigintRules(), doubleRules(), varcharRules(), booleanRules(), temporalRules(), passThroughRules(),
		collectionRules(), seriesRules(), aggregateRules(),
	} {
		maps.Copy(m, group)
	}

	return m
}

func ruleSet(r funcRule, names string) map[string]funcRule {
	m := map[string]funcRule{}
	for n := range strings.FieldsSeq(names) {
		m[n] = r
	}

	return m
}

func union(sets ...map[string]funcRule) map[string]funcRule {
	m := map[string]funcRule{}
	for _, s := range sets {
		maps.Copy(m, s)
	}

	return m
}

func bigintRules() map[string]funcRule {
	return union(
		ruleSet(fixed(tyInteger), "codepoint"),
		ruleSet(
			fixed(tyBigint),
			"count count_if approx_distinct length char_length character_length cardinality from_base "+
				"strpos strrpos instr position levenshtein_distance hamming_distance unicode json_array_length json_size "+
				"year month day day_of_month hour minute second millisecond quarter week week_of_year day_of_week dow "+
				"day_of_year doy year_of_week yow nanosecond to_milliseconds to_nanoseconds date_diff regexp_count "+
				"bitwise_and bitwise_or bitwise_xor bitwise_not bit_count crc32 row_number rank dense_rank ntile "+
				"array_position extract width_bucket bitwise_and_agg bitwise_or_agg",
		),
	)
}

func doubleRules() map[string]funcRule {
	return ruleSet(fixed(tyDouble), "avg stddev stddev_pop stddev_samp variance var_pop var_samp corr covar_pop "+
		"covar_samp geometric_mean skewness sqrt cbrt exp ln log log10 log2 pi e radians degrees sin cos tan asin "+
		"acos atan atan2 sinh cosh tanh asinh acosh atanh power pow to_unixtime integral_trapezoidal "+
		"correlate_pearson correlate_spearman cume_dist percent_rank total infinity nan regr_slope regr_intercept")
}

func varcharRules() map[string]funcRule {
	return union(
		ruleSet(fixed(tyVarchar), "lower upper trim ltrim rtrim replace substr substring lpad rpad split_part "+
			"regexp_extract regexp_replace to_hex md5 sha1 sha256 sha512 to_base64 from_base64 from_hex to_utf8 "+
			"from_utf8 date_format format_datetime to_iso8601 json_format json_extract_scalar json_extract json_parse "+
			"typeof to_base url_extract_fragment url_extract_host url_extract_parameter url_extract_path "+
			"url_extract_protocol url_extract_query chr array_join format current_timezone concat_ws "+
			"printf hex char quote group_concat string_agg listagg soundex json_array json_object"),
		map[string]funcRule{"concat": concatRule},
	)
}

func booleanRules() map[string]funcRule {
	return ruleSet(fixed(tyBoolean), "contains regexp_like starts_with ends_with is_nan is_finite is_infinite "+
		"any_match all_match none_match bool_and bool_or every arrays_overlap json_array_contains is_json_scalar "+
		"like glob")
}

func temporalRules() map[string]funcRule {
	return union(
		ruleSet(fixed(tyTimestamp), "now current_timestamp localtimestamp ago ts_ago from_unixtime from_milliseconds "+
			"from_nanoseconds from_iso8601_timestamp parse_datetime date_parse"),
		ruleSet(fixed(tyDate), "current_date from_iso8601_date to_date date last_day_of_month"),
		ruleSet(fixed(tyTime), "current_time localtime"),
		ruleSet(fixed(tyIntervalDS), "parse_duration ts_tsdiff"),
		map[string]funcRule{"date_trunc": same(1), "date_add": same(argsTwo), "bin": binRule, "ts_bin": binRule},
	)
}

func passThroughRules() map[string]funcRule {
	return union(
		ruleSet(same(0), "min max arbitrary any_value first_value last_value lag lead nth_value max_by min_by "+
			"approx_percentile nullif abs round ceil ceiling floor truncate trunc sign try mod"),
		ruleSet(superAll, "coalesce greatest least ifnull"),
		map[string]funcRule{"sum": sumRule, "if": superFrom(1), "iif": superFrom(1)},
		map[string]funcRule{"random": randomRule, "rand": randomRule},
	)
}

func collectionRules() map[string]funcRule {
	return union(
		ruleSet(same(0), "array_distinct array_sort array_remove slice reverse array_concat shuffle"),
		ruleSet(superAll, "array_intersect array_union array_except"),
		map[string]funcRule{"zip": zipRule, "repeat": arrayOfFirstArg},
		ruleSet(arrayOfKind(tyVarchar), "split regexp_extract_all regexp_split"),
		ruleSet(elemOf(0), "array_max array_min element_at"),
		map[string]funcRule{"array_agg": arrayOfFirstArg, "sequence": sequenceRule},
		ruleSet(unsupportedFn("map type"), "map map_agg map_keys map_values histogram"),
		ruleSet(internalFn, "array elem field fold index_list iv_cast iv_dt iv_op iv_part iv_str iv_ym row row_cast "+
			"row_compare row_max row_min row_sortkey subscript unmark"),
	)
}

func seriesRules() map[string]funcRule {
	return union(
		ruleSet(interpolateRule, "interpolate_linear interpolate_spline_cubic interpolate_locf interpolate_fill"),
		ruleSet(seriesOfDouble, "derivative_linear non_negative_derivative_linear"),
		map[string]funcRule{"create_time_series": createSeriesRule, "ts_series_json": internalFn},
	)
}
