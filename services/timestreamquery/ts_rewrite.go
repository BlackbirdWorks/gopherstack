package timestreamquery

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	reAgoInterval = regexp.MustCompile(`(?i)\bago\(\s*interval\s+'(\d+)'\s+([a-z]+)\s*\)`)
	reBinInterval = regexp.MustCompile(`(?is)\bbin\((.+?),\s*interval\s+'(\d+)'\s+([a-z]+)\s*\)`)
	reMeasureCol  = regexp.MustCompile(`(?i)\bmeasure_value::(double|bigint|varchar|boolean|timestamp)\b`)
	reDuration    = regexp.MustCompile(
		`(?i)\b(\d+)(nanoseconds?|ns|microseconds?|us|milliseconds?|ms|seconds?|s|minutes?|m|hours?|h|days?|d)\b`)
	reSignedDur = regexp.MustCompile(
		`(?i)([+\-])\s*(\d+)(nanoseconds?|ns|microseconds?|us|milliseconds?|ms|seconds?|s|minutes?|m|hours?|h|days?|d)\b`)
	reAgo      = regexp.MustCompile(`(?i)\bago\(`)
	reBin      = regexp.MustCompile(`(?i)\bbin\(`)
	reNow      = regexp.MustCompile(`(?i)\bnow\(\s*\)|\bcurrent_timestamp\b(\s*\(\s*\))?|\blocaltimestamp\b`)
	reDate     = regexp.MustCompile(`(?i)\bcurrent_date\b`)
	reTimezone = regexp.MustCompile(`(?i)\bcurrent_timezone\(\s*\)`)
)

func unitNanos(unit string) int64 {
	u := strings.ToLower(unit)

	switch {
	case u == "ns" || strings.HasPrefix(u, "nano"):
		return 1
	case u == "us" || strings.HasPrefix(u, "micro"):
		return int64(time.Microsecond)
	case u == "ms" || strings.HasPrefix(u, "milli"):
		return int64(time.Millisecond)
	case u == "s" || strings.HasPrefix(u, "sec"):
		return int64(time.Second)
	case u == "m" || strings.HasPrefix(u, "min"):
		return int64(time.Minute)
	case u == "h" || strings.HasPrefix(u, "hour"):
		return int64(time.Hour)
	default:
		return int64(24 * time.Hour)
	}
}

func durationNanos(n, unit string) string {
	v, _ := strconv.ParseInt(n, 10, 64)

	return strconv.FormatInt(v*unitNanos(unit), 10)
}

// rewriteTimestream rewrites Timestream-only syntax (interval literals, ago/bin/now,
// measure_value::T columns, @scheduled_runtime) into the Trino subset the SQLite
// engine translates. String literals and quoted identifiers are left untouched.
func rewriteTimestream(q string, now time.Time, vars map[string]string) string {
	q = reAgoInterval.ReplaceAllStringFunc(q, func(m string) string {
		g := reAgoInterval.FindStringSubmatch(m)

		return "ago(" + durationNanos(g[1], g[2]) + "ns)"
	})
	q = reBinInterval.ReplaceAllStringFunc(q, func(m string) string {
		g := reBinInterval.FindStringSubmatch(m)

		return "bin(" + g[1] + ", " + durationNanos(g[2], g[3]) + "ns)"
	})

	nowLit := "'" + formatTS(now) + "'"
	dateLit := "'" + now.UTC().Format("2006-01-02") + "'"

	var b strings.Builder

	for len(q) > 0 {
		i := strings.IndexAny(q, `'"`)
		if i < 0 {
			b.WriteString(rewriteCode(q, nowLit, dateLit, vars))

			break
		}

		b.WriteString(rewriteCode(q[:i], nowLit, dateLit, vars))

		end := closingQuote(q, i)
		b.WriteString(q[i:end])
		q = q[end:]
	}

	return b.String()
}

// closingQuote returns the index just past the quoted run starting at q[i] ('' and "" escape).
func closingQuote(q string, i int) int {
	quote := q[i]

	for j := i + 1; j < len(q); j++ {
		if q[j] != quote {
			continue
		}

		if j+1 < len(q) && q[j+1] == quote {
			j++

			continue
		}

		return j + 1
	}

	return len(q)
}

func rewriteCode(seg, nowLit, dateLit string, vars map[string]string) string {
	for name, v := range vars {
		seg = strings.ReplaceAll(seg, name, "'"+v+"'")
	}

	seg = reMeasureCol.ReplaceAllString(seg, `"measure_value::$1"`)
	seg = reSignedDur.ReplaceAllStringFunc(seg, func(m string) string {
		g := reSignedDur.FindStringSubmatch(m)

		ns, _ := strconv.ParseInt(durationNanos(g[2], g[3]), 10, 64)
		if ns%int64(time.Second) != 0 {
			return m
		}

		return g[1] + " INTERVAL '" + strconv.FormatInt(ns/int64(time.Second), 10) + "' second"
	})
	seg = reDuration.ReplaceAllStringFunc(seg, func(m string) string {
		g := reDuration.FindStringSubmatch(m)

		return durationNanos(g[1], g[2])
	})
	seg = reAgo.ReplaceAllString(seg, "ts_ago("+nowLit+", ")
	seg = reBin.ReplaceAllString(seg, "ts_bin(")
	seg = reNow.ReplaceAllString(seg, nowLit)
	seg = reDate.ReplaceAllString(seg, dateLit)
	seg = reTimezone.ReplaceAllString(seg, "'UTC'")

	return seg
}
