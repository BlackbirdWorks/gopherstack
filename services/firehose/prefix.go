package firehose

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	errTypeProcessing = "processing-failed"
	errTypeConversion = "format-conversion-failed"
	errTypeHTTP       = "http-endpoint-failed"
	errTypeOpenSearch = "AmazonOpenSearchService-failed"
	errTypeSplunk     = "splunk-failed"

	errorOutputTypeToken = "!{firehose:error-output-type}"
	timestampNamespace   = "!{timestamp:"
	randomStringLen      = 11
	firehoseNamespace    = "firehose"
	shortYearDigits      = 2
	nanoDigits           = 9
	shortDayNameLen      = 3
	monthNumberWidth     = 2
	monthAbbrevWidth     = 3
)

type timeZoneKey struct{}

// expandPrefix evaluates !{timestamp:..} and !{firehose:random-string}; partition tokens are left intact.
func expandPrefix(prefix string, t time.Time) string {
	var out strings.Builder

	rest := prefix
	for {
		start := strings.Index(rest, "!{")
		if start < 0 {
			break
		}

		end := strings.IndexByte(rest[start:], '}')
		if end < 0 {
			break
		}

		out.WriteString(rest[:start])

		token := rest[start : start+end+1]
		out.WriteString(evalPrefixToken(token, t))
		rest = rest[start+end+1:]
	}

	out.WriteString(rest)

	return out.String()
}

func evalPrefixToken(token string, t time.Time) string {
	ns, val, _ := strings.Cut(token[2:len(token)-1], ":")

	switch {
	case ns == "timestamp":
		return formatJavaTime(t, val)
	case ns == firehoseNamespace && val == "random-string":
		return strings.ReplaceAll(uuid.NewString(), "-", "")[:randomStringLen]
	default:
		return token
	}
}

// formatJavaTime renders t with a java.time.DateTimeFormatter pattern (the common letters).
func formatJavaTime(t time.Time, pattern string) string {
	var out strings.Builder

	runes := []rune(pattern)
	for i := 0; i < len(runes); {
		r := runes[i]
		if r == '\'' {
			i = copyQuoted(&out, runes, i)

			continue
		}

		j := i
		for j < len(runes) && runes[j] == r {
			j++
		}

		out.WriteString(javaField(t, r, j-i))
		i = j
	}

	return out.String()
}

func copyQuoted(out *strings.Builder, runes []rune, i int) int {
	i++
	if i < len(runes) && runes[i] == '\'' {
		out.WriteRune('\'')

		return i + 1
	}

	for i < len(runes) && runes[i] != '\'' {
		out.WriteRune(runes[i])
		i++
	}

	return i + 1
}

func javaField(t time.Time, r rune, n int) string {
	switch r {
	case 'y', 'u':
		return pick(t, n, shortYearDigits, "06", "2006")
	case 'M', 'L':
		return monthField(t, n)
	case 'd':
		return pick(t, n, 1, "2", "02")
	case 'D':
		return fmt.Sprintf("%0*d", n, t.YearDay())
	case 'H':
		return fmt.Sprintf("%0*d", n, t.Hour())
	case 'h':
		return pick(t, n, 1, "3", "03")
	case 'm':
		return fmt.Sprintf("%0*d", n, t.Minute())
	case 's':
		return fmt.Sprintf("%0*d", n, t.Second())
	case 'S':
		return fmt.Sprintf("%09d", t.Nanosecond())[:min(n, nanoDigits)]
	case 'w':
		_, w := t.ISOWeek()

		return fmt.Sprintf("%0*d", n, w)
	case 'E':
		return pick(t, n, shortDayNameLen, "Mon", "Monday")
	case 'a':
		return t.Format("PM")
	default:
		return strings.Repeat(string(r), n)
	}
}

func monthField(t time.Time, n int) string {
	switch n {
	case 1:
		return t.Format("1")
	case monthNumberWidth:
		return t.Format("01")
	case monthAbbrevWidth:
		return t.Format("Jan")
	default:
		return t.Format("January")
	}
}

// pick returns short layout when n <= limit, else long.
func pick(t time.Time, n, limit int, short, long string) string {
	if n <= limit {
		return t.Format(short)
	}

	return t.Format(long)
}

func hasTimestampExpression(prefix string) bool {
	return strings.Contains(prefix, timestampNamespace)
}

// errorPrefix resolves a destination's error-output prefix for errType.
func errorPrefix(errorOutputPrefix, prefix, errType string) string {
	if errorOutputPrefix != "" {
		return strings.ReplaceAll(errorOutputPrefix, errorOutputTypeToken, errType)
	}

	if strings.Contains(prefix, "!{") {
		return errType + "/"
	}

	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	return prefix + errType + "/"
}
