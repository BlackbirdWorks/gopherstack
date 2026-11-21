package azurearm

import (
	_ "embed" // locale table
	"errors"
	"fmt"
	"slices"
	"strings"
)

var errLogicLocaleRow = errors.New("malformed locale row")

//go:embed rp_logic_locales.txt
var logicLocaleTable string

const (
	logicLocaleCols   = 9
	logicLocaleMonths = 12
	logicLocaleDays   = 7
	logicLocaleStd    = "dDfFgGtT"
)

//nolint:gochecknoglobals // static locale tables decoded once from the embedded CLDR export
var logicLocales = mustDecodeLogicLocales(logicLocaleTable)

func mustDecodeLogicLocales(table string) map[string]logicLocale {
	out, err := decodeLogicLocales(table)
	if err != nil {
		panic(err)
	}

	return out
}

// decodeLogicLocales parses the embedded table; a malformed row is an error, never a silently missing culture.
func decodeLogicLocales(table string) (map[string]logicLocale, error) {
	out := map[string]logicLocale{}

	for n, line := range strings.Split(table, "\n") {
		if line == "" || line[0] == '#' {
			continue
		}

		key, l, err := decodeLogicLocaleRow(line)
		if err != nil {
			return nil, fmt.Errorf("logic locale table line %d: %w", n+1, err)
		}

		out[key] = l
	}

	return out, nil
}

func decodeLogicLocaleRow(line string) (string, logicLocale, error) {
	cols := strings.Split(line, "\t")
	if len(cols) != logicLocaleCols {
		return "", logicLocale{}, fmt.Errorf("%w: %d columns, want %d", errLogicLocaleRow, len(cols), logicLocaleCols)
	}

	l := logicLocale{am: cols[5], pm: cols[6], std: map[string]string{}}

	for _, f := range []struct {
		src  string
		dst  []string
		want int
	}{
		{cols[1], l.months[:], logicLocaleMonths},
		{cols[2], l.abbrMonths[:], logicLocaleMonths},
		{cols[3], l.days[:], logicLocaleDays},
		{cols[4], l.abbrDays[:], logicLocaleDays},
	} {
		parts := strings.Split(f.src, "|")
		if len(parts) != f.want || slices.Contains(parts, "") {
			return "", logicLocale{}, fmt.Errorf("%w: %q is not %d names", errLogicLocaleRow, f.src, f.want)
		}

		copy(f.dst, parts)
	}

	std := strings.Split(cols[7], "|")
	if len(std) != len(logicLocaleStd) {
		return "", logicLocale{}, fmt.Errorf("%w: %d standard patterns", errLogicLocaleRow, len(std))
	}

	for i, p := range std {
		l.std[logicLocaleStd[i:i+1]] = p
	}

	l.parse = strings.Split(cols[8], "|")

	return cols[0], l, nil
}
