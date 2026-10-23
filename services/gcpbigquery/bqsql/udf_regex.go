package bqsql

import (
	"database/sql/driver"
	"encoding/json"
	"regexp"
	"strings"
)

func compileRe(v driver.Value) (*regexp.Regexp, error) {
	re, err := regexp.Compile(toStr(v))
	if err != nil {
		return nil, errBQ("Cannot parse regular expression: %v", err)
	}

	return re, nil
}

func addRegexUDFs(d map[string]udfDef) {
	d["regexp_contains"] = udfDef{n: argc2, fn: func(a []driver.Value) (driver.Value, error) {
		if anyNull(a) {
			return nil, nil //nolint:nilnil // NULL in, NULL out
		}

		re, err := compileRe(a[1])
		if err != nil {
			return nil, err
		}

		return boolVal(re.MatchString(toStr(a[0]))), nil
	}}
	d["regexp_extract"] = udfDef{n: -1, fn: udfRegexExtract}
	d["regexp_extract_all"] = udfDef{n: argc2, fn: udfRegexExtractAll}
	d["regexp_replace"] = udfDef{n: argc3, fn: func(a []driver.Value) (driver.Value, error) {
		if anyNull(a) {
			return nil, nil //nolint:nilnil // NULL in, NULL out
		}

		re, err := compileRe(a[1])
		if err != nil {
			return nil, err
		}

		return re.ReplaceAllString(toStr(a[0]), convertReplacement(toStr(a[2]))), nil
	}}
}

// convertReplacement maps BigQuery's \1 backreferences to Go's ${1} and escapes literal dollars.
func convertReplacement(s string) string {
	var sb strings.Builder

	for i := 0; i < len(s); i++ {
		switch {
		case s[i] == '\\' && i+1 < len(s) && s[i+1] >= '0' && s[i+1] <= '9':
			sb.WriteString("${" + string(s[i+1]) + "}")

			i++
		case s[i] == '\\' && i+1 < len(s) && s[i+1] == '\\':
			sb.WriteByte('\\')

			i++
		case s[i] == '$':
			sb.WriteString("$$")
		default:
			sb.WriteByte(s[i])
		}
	}

	return sb.String()
}

func firstGroup(m []string) string {
	if len(m) > 1 {
		return m[1]
	}

	return m[0]
}

func udfRegexExtract(a []driver.Value) (driver.Value, error) {
	if a[0] == nil || a[1] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	re, err := compileRe(a[1])
	if err != nil {
		return nil, err
	}

	if re.NumSubexp() > 1 {
		return nil, errBQ(
			"Regular expressions passed into extraction functions must not have more than 1 capturing group",
		)
	}

	s := toStr(a[0])
	pos := 1

	if len(a) > 2 && a[2] != nil {
		if pos, err = toSmallInt(a[2]); err != nil {
			return nil, err
		}
	}

	r := []rune(s)
	if pos < 1 || pos > len(r)+1 {
		return nil, errBQ("Position must be between 1 and the string length")
	}

	occ := 1

	if len(a) > 3 && a[3] != nil {
		if occ, err = toSmallInt(a[3]); err != nil {
			return nil, err
		}
	}

	ms := re.FindAllStringSubmatch(string(r[pos-1:]), -1)
	if occ < 1 || occ > len(ms) {
		return nil, nil //nolint:nilnil // no match is NULL
	}

	return firstGroup(ms[occ-1]), nil
}

func udfRegexExtractAll(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	re, err := compileRe(a[1])
	if err != nil {
		return nil, err
	}

	out := []string{}
	for _, m := range re.FindAllStringSubmatch(toStr(a[0]), -1) {
		out = append(out, firstGroup(m))
	}

	b, _ := json.Marshal(out)

	return string(b), nil
}
