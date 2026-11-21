package timestreamquery

import (
	"slices"
	"strconv"
	"strings"
)

// translateTimestream translates a rewritten query, expanding timeseries filter/reduce lambdas and UNNEST.
func translateTimestream(q string, set loadedSet) (string, error) {
	q, subs, unnests := extractSeriesLambdas(q)

	out, err := translate(q, set)
	if err != nil {
		return "", err
	}

	for i, u := range unnests {
		arg, argErr := fragment(u.arg)
		if argErr != nil {
			return "", argErr
		}

		mod := unnestModuleFor(u.cols)
		out = strings.ReplaceAll(out, `"main".`+quoteIdent(unnestPlaceholder(i)), mod+"("+arg+")")
	}

	for i, sub := range slices.Backward(subs) {
		sql, expandErr := sub.sql()
		if expandErr != nil {
			return "", expandErr
		}

		out = strings.ReplaceAll(out, placeholderPrefix+strconv.Itoa(i)+"__", sql)
	}

	return out, nil
}

// fragment translates a standalone expression (no table references).
func fragment(expr string) (string, error) {
	sql, err := translateTimestream("SELECT "+strings.TrimSpace(expr), loadedSet{})
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(sql), "SELECT")), nil
}

func (s seriesSub) sql() (string, error) {
	ts, err := fragment(s.args[0])
	if err != nil {
		return "", err
	}

	if !s.reduce {
		return s.filterSQL(ts)
	}

	return s.reduceSQL(ts)
}

func (s seriesSub) filterSQL(ts string) (string, error) {
	body, err := fragment(colRefs(s.lam[1], s.lam[0], `"_x"`))
	if err != nil {
		return "", err
	}

	body = strings.ReplaceAll(body, `"_x"."time"`, `json_extract("_j".value, '$[0]')`)
	body = strings.ReplaceAll(body, `"_x"."value"`, `json_extract("_j".value, '$[1]')`)

	return `(CASE WHEN (` + ts + `) IS NULL THEN NULL ELSE (SELECT create_time_series(json_extract("_j".value, '$[0]'), ` +
		`json_extract("_j".value, '$[1]')) FROM json_each(ts_series_json(` + ts + `)) AS "_j" WHERE ` + body + `) END)`, nil
}

func (s seriesSub) reduceSQL(ts string) (string, error) {
	state, elem, step, outParam, out := s.lam[0], s.lam[1], s.lam[2], s.lam[3], s.lam[4]

	init, err := fragment(fieldRefs(rowCasts(s.args[1]), state))
	if err != nil {
		return "", err
	}

	stepSQL, err := fragment(fieldRefs(colRefs(rowCasts(step), elem, `"_e"`), state))
	if err != nil {
		return "", err
	}

	outSQL, err := fragment(fieldRefs(rowCasts(out), outParam))
	if err != nil {
		return "", err
	}

	qs, qo := `"`+state+`"`, `"`+outParam+`"`
	cell := func(field int) string {
		return `json_extract("_r"."_arr", '$[' || ("_r"."_i" + 1) || '][` + strconv.Itoa(field) + `]')`
	}

	stepSQL = strings.ReplaceAll(strings.ReplaceAll(stepSQL, `"_e"."time"`, cell(0)), `"_e"."value"`, cell(1))

	return `(CASE WHEN (` + ts + `) IS NULL THEN NULL ELSE (WITH RECURSIVE "_r"("_i", ` + qs + `, "_arr") AS (` +
		`SELECT -1, ` + init + `, ts_series_json(` + ts + `) UNION ALL SELECT "_r"."_i" + 1, ` + stepSQL +
		`, "_r"."_arr" FROM "_r" WHERE "_r"."_i" + 1 < json_array_length("_r"."_arr")) SELECT ` + outSQL +
		` FROM (SELECT ` + qs + ` AS ` + qo + ` FROM "_r" ORDER BY "_i" DESC LIMIT 1)) END)`, nil
}
