package timestreamquery

import (
	"database/sql/driver"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"regexp"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const maxRegexpMatches = 100000

var reRegexpReplace = regexp.MustCompile(`(?i)\bregexp_replace\s*\(`)

// rewriteRegexpLambdas turns regexp_replace(s, p, x -> body) into a splice of transform() over the match groups.
func rewriteRegexpLambdas(q string) string {
	if !reRegexpReplace.MatchString(q) {
		return q
	}

	mask := quoteMask(q)

	var (
		sb  strings.Builder
		pos int
	)

	for _, loc := range reRegexpReplace.FindAllStringIndex(q, -1) {
		if loc[0] < pos || mask[loc[0]] {
			continue
		}

		open := loc[1] - 1

		closeAt := closeParen(q, open)
		if closeAt < 0 {
			continue
		}

		args := splitTop(q[open+1 : closeAt])
		if len(args) != argsThree || reLambdaOne.FindStringSubmatch(args[argsTwo]) == nil {
			continue
		}

		subject, pattern := rewriteRegexpLambdas(args[0]), rewriteRegexpLambdas(args[1])
		lam := rewriteRegexpLambdas(args[argsTwo])

		sb.WriteString(q[pos:loc[0]])
		sb.WriteString("ts_regexp_splice(" + subject + "," + pattern + ", transform(ts_regexp_groups(" +
			subject + "," + pattern + "), " + lam + "))")

		pos = closeAt + 1
	}

	sb.WriteString(q[pos:])

	return sb.String()
}

func compileJava(pattern string) (*regexp.Regexp, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, evalError("Invalid regular expression: " + pattern)
	}

	return re, nil
}

func regexpArgs(args []driver.Value) (string, *regexp.Regexp, bool, error) {
	if args[0] == nil || args[1] == nil {
		return "", nil, true, nil
	}

	s, ok1 := textOf(args[0])
	p, ok2 := textOf(args[1])

	if !ok1 || !ok2 {
		return "", nil, false, errOperandType
	}

	re, err := compileJava(p)

	return s, re, false, err
}

// udfRegexpGroups is the capturing groups (numbered from one) of every match, as an array of arrays.
func udfRegexpGroups(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, re, isNull, err := regexpArgs(args)
	if err != nil || isNull {
		return nil, err
	}

	matches := re.FindAllStringSubmatchIndex(s, maxRegexpMatches)
	out := make(rawElems, len(matches))

	for i, m := range matches {
		groups := make(rawElems, 0, len(m)/argsTwo)

		for g := 1; g < len(m)/argsTwo; g++ {
			if m[g*argsTwo] < 0 {
				groups = append(groups, kwNull)

				continue
			}

			groups = append(groups, quoteJSON(s[m[g*argsTwo]:m[g*argsTwo+1]]))
		}

		out[i] = groups.text()
	}

	return out.encode(), nil
}

// udfRegexpSplice replaces each match of the pattern in the subject with the matching replacement.
func udfRegexpSplice(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, re, isNull, err := regexpArgs(args[:argsTwo])
	if err != nil || isNull || args[argsTwo] == nil {
		return nil, err
	}

	repl, null, err := decodeRawArray(args[argsTwo])
	if err != nil || null {
		return nil, err
	}

	matches := re.FindAllStringIndex(s, maxRegexpMatches)
	if len(matches) != len(repl) {
		return nil, errOperandType
	}

	var sb strings.Builder

	cur := 0

	for i, m := range matches {
		text, isNil := replacementText(repl[i])
		if isNil {
			return nil, nil //nolint:nilnil // a null replacement makes the result SQL NULL
		}

		sb.WriteString(s[cur:m[0]])
		sb.WriteString(text)

		cur = m[1]
	}

	sb.WriteString(s[cur:])

	return sb.String(), nil
}

func replacementText(raw string) (string, bool) {
	if raw == kwNull {
		return "", true
	}

	var s string
	if json.Unmarshal([]byte(raw), &s, jsonx.V1()) == nil {
		return s, false
	}

	return string(jsontext.Value(raw)), false
}
