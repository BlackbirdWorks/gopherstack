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

		mod, modErr := unnestModuleFor(u.cols)
		if modErr != nil {
			return "", modErr
		}

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

const seriesRows = `(SELECT json_extract(e.value, '$[0]') AS "time", ` +
	`json_extract(e.value, '$[1]') AS "value", e.key AS "_i" ` +
	`FROM json_each(ts_series_json(%s)) AS e)`

func seriesRowsOf(ts string) string { return strings.Replace(seriesRows, "%s", ts, 1) }

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

	return `(CASE WHEN (` + ts + `) IS NULL THEN NULL ELSE (SELECT create_time_series("_x"."time", "_x"."value") ` +
		`FROM (SELECT "time", "value" FROM ` + seriesRowsOf(ts) + ` ORDER BY "_i") AS "_x" WHERE ` + body + `) END)`, nil
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

	return `(CASE WHEN (` + ts + `) IS NULL THEN NULL ELSE (WITH RECURSIVE "_e" AS (SELECT "time", "value", "_i" FROM ` +
		seriesRowsOf(
			ts,
		) + `), "_r"("_i", ` + qs + `) AS (SELECT -1, ` + init + ` UNION ALL SELECT "_e"."_i", ` + stepSQL +
		` FROM "_r" JOIN "_e" ON "_e"."_i" = "_r"."_i" + 1) SELECT ` + outSQL + ` FROM (SELECT ` + qs + ` AS ` + qo +
		` FROM "_r" ORDER BY "_i" DESC LIMIT 1)) END)`, nil
}
