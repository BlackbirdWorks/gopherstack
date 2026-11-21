package azurearm

import (
	_ "embed" // locale table
	"strings"
)

//go:embed rp_logic_locales.txt
var logicLocaleTable string

const (
	logicLocaleCols   = 9
	logicLocaleMonths = 12
	logicLocaleDays   = 7
	logicLocaleStd    = "dDfFgGtT"
)

//nolint:gochecknoglobals // static locale tables decoded once from the embedded CLDR export
var logicLocales = decodeLogicLocales(logicLocaleTable)

func decodeLogicLocales(table string) map[string]logicLocale {
	out := map[string]logicLocale{}

	for line := range strings.SplitSeq(table, "\n") {
		cols := strings.Split(line, "\t")
		if line == "" || line[0] == '#' || len(cols) != logicLocaleCols {
			continue
		}

		l := logicLocale{am: cols[5], pm: cols[6], std: map[string]string{}}
		copy(l.months[:], strings.Split(cols[1], "|"))
		copy(l.abbrMonths[:], strings.Split(cols[2], "|"))
		copy(l.days[:], strings.Split(cols[3], "|"))
		copy(l.abbrDays[:], strings.Split(cols[4], "|"))

		for i, p := range strings.Split(cols[7], "|") {
			if i < len(logicLocaleStd) {
				l.std[logicLocaleStd[i:i+1]] = p
			}
		}

		l.parse = strings.Split(cols[8], "|")
		out[cols[0]] = l
	}

	return out
}
