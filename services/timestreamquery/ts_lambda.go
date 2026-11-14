package timestreamquery

import (
	"regexp"
	"strconv"
	"strings"
)

const placeholderPrefix = "__tsl"

var (
	reLambdaOne  = regexp.MustCompile(`(?s)^\s*([A-Za-z_]\w*)\s*->\s*(.*)$`)
	reLambdaTwo  = regexp.MustCompile(`(?s)^\s*\(\s*([A-Za-z_]\w*)\s*,\s*([A-Za-z_]\w*)\s*\)\s*->\s*(.*)$`)
	reCallStart  = regexp.MustCompile(`(?i)\b(filter|reduce|unnest)\s*\(`)
	reOrdinal    = regexp.MustCompile(`(?is)^\s+with\s+ordinality\b`)
	reUnnestAs   = regexp.MustCompile(`(?is)^\s+as\s+([A-Za-z_]\w*)(?:\s*\(([^)]*)\))?`)
	reUnnestBare = regexp.MustCompile(`(?is)^\s+([A-Za-z_]\w*)\s*\(([^)]*)\)`)
	reRowCast    = regexp.MustCompile(`(?i)\bcast\s*\(\s*row\s*\(`)
	reAsRow      = regexp.MustCompile(`(?is)^\s*as\s+row\s*\(`)
)

// seriesSub is one filter/reduce over a timeseries awaiting expansion after translation.
type seriesSub struct {
	args   []string
	lam    []string
	reduce bool
}

// closeParen returns the index of the parenthesis matching q[open], skipping quoted text; -1 when unbalanced.
func closeParen(q string, open int) int {
	depth := 0

	for i := open; i < len(q); i++ {
		switch q[i] {
		case '\'', '"':
			i = closingQuote(q, i) - 1
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}

	return -1
}

func splitTop(s string) []string {
	var (
		parts []string
		depth int
		start int
	)

	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'', '"':
			i = closingQuote(s, i) - 1
		case '(', '[':
			depth++
		case ')', ']':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}

	return append(parts, s[start:])
}

func paramRef(p string) *regexp.Regexp {
	return regexp.MustCompile(`(?i)(^|[^\w."])` + regexp.QuoteMeta(p) + `\.(value|time)\b`)
}

// seriesLambda reports whether the lambda body reads p.value or p.time, i.e. iterates a timeseries.
func seriesLambda(p, body string) bool { return paramRef(p).MatchString(body) }

func classify(name string, args []string) (seriesSub, bool) {
	switch strings.ToLower(name) {
	case "filter":
		if len(args) != argsTwo {
			return seriesSub{}, false
		}

		m := reLambdaOne.FindStringSubmatch(args[1])
		if m == nil || !seriesLambda(m[1], m[2]) {
			return seriesSub{}, false
		}

		return seriesSub{args: args[:1], lam: m[1:]}, true
	case "reduce":
		const reduceArgs = 4
		if len(args) != reduceArgs {
			return seriesSub{}, false
		}

		step := reLambdaTwo.FindStringSubmatch(args[2])
		out := reLambdaOne.FindStringSubmatch(args[3])

		if step == nil || out == nil || !seriesLambda(step[2], step[3]) {
			return seriesSub{}, false
		}

		return seriesSub{
			reduce: true,
			args:   args[:argsTwo],
			lam:    []string{step[1], step[2], step[3], out[1], out[2]},
		}, true
	default:
		return seriesSub{}, false
	}
}

// quoteMask marks every byte of q that sits inside a quoted literal or identifier.
func quoteMask(q string) []bool {
	mask := make([]bool, len(q))

	for i := 0; i < len(q); i++ {
		if q[i] != '\'' && q[i] != '"' {
			continue
		}

		end := closingQuote(q, i)
		for j := i; j < end; j++ {
			mask[j] = true
		}

		i = end - 1
	}

	return mask
}

// extractSeriesLambdas swaps timeseries filter/reduce calls and UNNEST(timeseries) for placeholders.
func extractSeriesLambdas(q string) (string, []seriesSub, []unnestSub) {
	var (
		out    strings.Builder
		subs   []seriesSub
		unnest []unnestSub
		pos    int
	)

	mask := quoteMask(q)

	for _, loc := range reCallStart.FindAllStringSubmatchIndex(q, -1) {
		if loc[0] < pos || mask[loc[0]] {
			continue
		}

		open := loc[1] - 1
		closeIdx := closeParen(q, open)

		if closeIdx < 0 {
			continue
		}

		name := q[loc[2]:loc[3]]
		inner := q[open+1 : closeIdx]

		if strings.EqualFold(name, "unnest") {
			if end, ok := rewriteUnnest(q, loc[0], closeIdx, inner, &out, &unnest, pos); ok {
				pos = end
			}

			continue
		}

		if sub, ok := classify(name, splitTop(inner)); ok {
			out.WriteString(q[pos:loc[0]])
			out.WriteString(placeholderPrefix + strconv.Itoa(len(subs)) + "__")

			subs = append(subs, sub)
			pos = closeIdx + 1
		}
	}

	out.WriteString(q[pos:])

	return out.String(), subs, unnest
}

func colRefs(text, p, target string) string {
	re := paramRef(p)

	return re.ReplaceAllString(text, `${1}`+target+`."$2"`)
}

func fieldRefs(text, p string) string {
	re := regexp.MustCompile(`(?i)(^|[^\w."])` + regexp.QuoteMeta(p) + `\.([A-Za-z_]\w*)`)

	return re.ReplaceAllString(text, `${1}json_extract("`+p+`",'$.$2')`)
}

// rowCasts rewrites CAST(ROW(a, b) AS ROW(n1 T, n2 T)) into json_object('n1', a, 'n2', b).
func rowCasts(text string) string {
	for {
		loc := reRowCast.FindStringIndex(text)
		if loc == nil {
			return text
		}

		open := loc[1] - 1
		closeVals := closeParen(text, open)

		if closeVals < 0 {
			return text
		}

		tail := reAsRow.FindString(text[closeVals+1:])
		if tail == "" {
			return text
		}

		typeOpen := closeVals + len(tail)
		closeTypes := closeParen(text, typeOpen)

		if closeTypes < 0 || closeTypes+1 >= len(text) {
			return text
		}

		castClose := strings.Index(text[closeTypes+1:], ")")
		if castClose < 0 {
			return text
		}

		vals := splitTop(text[open+1 : closeVals])
		types := splitTop(text[typeOpen+1 : closeTypes])
		pairs := make([]string, 0, len(vals)*argsTwo)

		for i, v := range vals {
			name := ""
			if i < len(types) {
				name = strings.Fields(strings.TrimSpace(types[i]))[0]
			}

			pairs = append(pairs, "'"+name+"'", "("+strings.TrimSpace(v)+")")
		}

		text = text[:loc[0]] + "json_object(" + strings.Join(pairs, ", ") + ")" + text[closeTypes+1+castClose+1:]
	}
}

// unnestSub is one UNNEST over a non-literal argument, served by a column-named ts_unnest module.
type unnestSub struct {
	arg  string
	cols []string
}

// rewriteUnnest swaps UNNEST(arg) [AS t [(a, b)]] for a placeholder table; ok=false leaves it to the stock engine.
func rewriteUnnest(
	q string, start, closeIdx int, arg string, out *strings.Builder, subs *[]unnestSub, pos int,
) (int, bool) {
	tail := q[closeIdx+1:]
	if strings.Contains(strings.ToUpper(arg), "ARRAY[") || reOrdinal.MatchString(tail) {
		return pos, false
	}

	alias, cols, end := "", []string{"time", "value"}, closeIdx+1

	m := reUnnestAs.FindStringSubmatch(tail)
	if m == nil {
		m = reUnnestBare.FindStringSubmatch(tail)
	}

	if m != nil {
		alias, end = m[1], closeIdx+1+len(m[0])
		if strings.TrimSpace(m[2]) != "" {
			cols = cols[:0]
			for c := range strings.SplitSeq(m[2], ",") {
				cols = append(cols, strings.Trim(strings.TrimSpace(c), `"`))
			}
		}
	}

	out.WriteString(q[pos:start])
	out.WriteString(unnestPlaceholder(len(*subs)))

	if alias != "" {
		out.WriteString(" AS " + alias)
	}

	*subs = append(*subs, unnestSub{arg: arg, cols: cols})

	return end, true
}

func unnestPlaceholder(n int) string { return "__tsu" + strconv.Itoa(n) + "__" }
