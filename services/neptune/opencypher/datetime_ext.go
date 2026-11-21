package opencypher

import (
	"strconv"
	"strings"
	"time"
)

const (
	datetimeExtLayout = "2006-01-02T15:04:05.000Z07:00"
	datetimeMinYear   = 1400
	datetimeMaxYear   = 9999
	datetimeMaxOffset = 14 * 60 * 60
	secPerMinute      = 60
	secPerHour        = 3600
	offsetHourDigits  = 2
	offsetHHMMDigits  = 4
	offsetColonLen    = 5
)

// SetExtendedDatetime installs the per-call check for the DatetimeMillisecond lab mode (Neptune 1.3.2.0+).
func (g *Graph) SetExtendedDatetime(enabled func() bool) {
	g.extDatetime.Store(&enabled)
}

func (g *Graph) extendedDatetime() bool {
	f := g.extDatetime.Load()

	return f != nil && *f != nil && (*f)()
}

func datetimeExtDateLayouts(utc bool) []string {
	if utc {
		return datetimeDateLayouts()
	}

	return []string{"2006-01-02", "2006-002", "2006002"}
}

func datetimeExtTimeLayouts(utc bool) []string {
	if utc {
		return []string{timeHMSMillis, timeHMS, "150405.000", "150405", "15:04", "1504", "15"}
	}

	return []string{timeHMS, timeHMSMillis}
}

// splitDatetimeZone separates a trailing Z or +/-HH[:mm|mm] zone from the time part.
func splitDatetimeZone(tm string) (string, int, bool, bool) {
	if before, ok := strings.CutSuffix(tm, "Z"); ok {
		return before, 0, true, true
	}

	i := strings.LastIndexAny(tm, "+-")
	if i < 0 {
		return tm, 0, true, true
	}

	off, ok := parseZoneOffset(tm[i:])
	if !ok {
		return "", 0, false, false
	}

	return tm[:i], off, off == 0, true
}

func parseZoneOffset(z string) (int, bool) {
	sign := 1
	if z[0] == '-' {
		sign = -1
	}

	z = z[1:]

	var hh, mm string

	switch {
	case len(z) == offsetHourDigits:
		hh = z
	case len(z) == offsetHHMMDigits:
		hh, mm = z[:2], z[2:]
	case len(z) == offsetColonLen && z[2] == ':':
		hh, mm = z[:2], z[3:]
	default:
		return 0, false
	}

	if !allDigits(hh) || (mm != "" && !allDigits(mm)) {
		return 0, false
	}

	h, _ := strconv.Atoi(hh)
	m, _ := strconv.Atoi(mm)

	off := sign * (h*secPerHour + m*secPerMinute)
	if m >= secPerMinute || off > datetimeMaxOffset || off < -datetimeMaxOffset {
		return 0, false
	}

	return off, true
}

func allDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}

	return s != ""
}

// parseDatetimeExt parses [Date]T[Time][Timezone] per the Neptune 1.3.2.0 extended grammar, keeping the offset.
func parseDatetimeExt(s string) (time.Time, bool) {
	datePart, timePart, hasTime := strings.Cut(s, "T")
	off, utc := 0, true

	if hasTime {
		var ok bool
		if timePart, off, utc, ok = splitDatetimeZone(timePart); !ok {
			return time.Time{}, false
		}
	}

	base, ok := parseExtDateTime(datePart, timePart, hasTime, utc)
	if !ok || base.Year() < datetimeMinYear || base.Year() > datetimeMaxYear {
		return time.Time{}, false
	}

	loc := time.UTC
	if off != 0 {
		loc = time.FixedZone("", off)
	}

	return time.Date(
		base.Year(),
		base.Month(),
		base.Day(),
		base.Hour(),
		base.Minute(),
		base.Second(),
		base.Nanosecond(),
		loc,
	), true
}

func parseExtDateTime(datePart, timePart string, hasTime, utc bool) (time.Time, bool) {
	for _, dl := range datetimeExtDateLayouts(utc) {
		if !hasTime {
			if t, err := time.Parse(dl, datePart); err == nil {
				return t, true
			}

			continue
		}

		for _, tl := range datetimeExtTimeLayouts(utc) {
			if t, err := time.Parse(dl+"T"+tl, datePart+"T"+timePart); err == nil {
				return t, true
			}
		}
	}

	return time.Time{}, false
}

func funcDatetimeExt(args []any) (any, error) {
	switch len(args) {
	case 0:
		return time.Now().UTC().Format(datetimeExtLayout), nil
	case 1:
		if args[0] == nil {
			return null()
		}

		s, ok := args[0].(string)
		if !ok {
			return nil, malformed("Type mismatch: expected String but was %s", typeName(args[0]))
		}

		t, parsed := parseDatetimeExt(s)
		if !parsed {
			return nil, &Error{
				Code:   CodeInvalidParameter,
				Detail: "Unable to parse '" + s + "' as a datetime",
			}
		}

		return t.Format(datetimeExtLayout), nil
	default:
		return nil, malformed("Function datetime() takes at most 1 argument")
	}
}
