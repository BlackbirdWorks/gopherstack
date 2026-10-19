package appsync

import (
	"fmt"
	"strings"
	"time"
)

const (
	isoMillis = "2006-01-02T15:04:05.000Z"
	quotePair = 2
	yy        = 2
	threeCh   = 3
)

func timeArg(a []any, i int) time.Time {
	ms, _, _ := toNum(argAt(a, i))

	return time.UnixMilli(int64(ms)).UTC()
}

func locArg(a []any, i int) (*time.Location, error) {
	name := argStrIf(a, i)
	if name == "" {
		return time.UTC, nil
	}

	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("%w: unknown time zone %q", ErrValidation, name)
	}

	return loc, nil
}

func utilTime() map[string]utilFn {
	return map[string]utilFn{
		"time.nowISO8601": func(_ *utilEnv, _ []any) (any, error) {
			return time.Now().UTC().Format(isoMillis), nil
		},
		"time.nowEpochSeconds": func(_ *utilEnv, _ []any) (any, error) { return time.Now().Unix(), nil },
		"time.nowEpochMilliSeconds": func(_ *utilEnv, _ []any) (any, error) {
			return time.Now().UnixMilli(), nil
		},
		"time.nowFormatted": func(_ *utilEnv, a []any) (any, error) {
			loc, err := locArg(a, 1)
			if err != nil {
				return nil, err
			}

			return time.Now().In(loc).Format(javaLayout(argStr(a, 0))), nil
		},
		"time.epochMilliSecondsToSeconds": func(_ *utilEnv, a []any) (any, error) {
			return timeArg(a, 0).Unix(), nil
		},
		"time.epochMilliSecondsToISO8601": func(_ *utilEnv, a []any) (any, error) {
			return timeArg(a, 0).Format(isoMillis), nil
		},
		"time.epochMilliSecondsToFormatted": func(_ *utilEnv, a []any) (any, error) {
			loc, err := locArg(a, argThird)
			if err != nil {
				return nil, err
			}

			return timeArg(a, 0).In(loc).Format(javaLayout(argStr(a, 1))), nil
		},
		"time.parseISO8601ToEpochMilliSeconds": func(_ *utilEnv, a []any) (any, error) {
			t, err := time.Parse(time.RFC3339Nano, argStr(a, 0))
			if err != nil {
				return nil, fmt.Errorf("%w: invalid ISO8601 timestamp", ErrValidation)
			}

			return t.UnixMilli(), nil
		},
		"time.parseFormattedToEpochMilliSeconds": func(_ *utilEnv, a []any) (any, error) {
			loc, err := locArg(a, argThird)
			if err != nil {
				return nil, err
			}

			t, err := time.ParseInLocation(javaLayout(argStr(a, 1)), argStr(a, 0), loc)
			if err != nil {
				return nil, fmt.Errorf("%w: timestamp does not match format", ErrValidation)
			}

			return t.UnixMilli(), nil
		},
	}
}

// javaLayout converts a java.text.SimpleDateFormat pattern to a Go layout.
func javaLayout(f string) string {
	var b strings.Builder

	for i := 0; i < len(f); {
		c := f[i]

		if c == '\'' {
			end := strings.IndexByte(f[i+1:], '\'')
			if end < 0 {
				b.WriteString(f[i+1:])

				break
			}

			b.WriteString(f[i+1 : i+1+end])

			i += end + quotePair

			continue
		}

		j := i
		for j < len(f) && f[j] == c {
			j++
		}

		b.WriteString(javaToken(c, j-i))
		i = j
	}

	return b.String()
}

func javaToken(c byte, n int) string {
	switch c {
	case 'y', 'u':
		if n == yy {
			return "06"
		}

		return "2006"
	case 'M':
		return pick(n, "1", "01", "Jan", "January")
	case 'd':
		return pick(n, "2", "02", "02", "02")
	case 'H':
		return "15"
	case 'h':
		return pick(n, "3", "03", "03", "03")
	case 'm':
		return pick(n, "4", "04", "04", "04")
	case 's':
		return pick(n, "5", "05", "05", "05")
	case 'S':
		return strings.Repeat("0", n)
	case 'a':
		return "PM"
	case 'E':
		return pick(n, "Mon", "Mon", "Mon", "Monday")
	case 'Z':
		return "-0700"
	case 'X':
		return pick(n, "Z07", "Z0700", "Z07:00", "Z07:00")
	case 'z':
		return "MST"
	}

	return strings.Repeat(string(c), n)
}

func pick(n int, one, two, three, four string) string {
	switch {
	case n <= 1:
		return one
	case n == yy:
		return two
	case n == threeCh:
		return three
	}

	return four
}
