package azurearm

import (
	"strings"
	"time"
	_ "time/tzdata" // IANA zones for time zone conversions in minimal containers
	"unicode"
)

func normalizeZoneName(s string) string {
	var sb strings.Builder

	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '+' || r == '-' {
			sb.WriteRune(r)
		}
	}

	return sb.String()
}

func logicZoneIndex() map[string]string {
	idx := make(map[string]string, len(logicWindowsZones))
	for name, iana := range logicWindowsZones {
		idx[normalizeZoneName(name)] = iana
	}

	return idx
}

// logicLoadZone resolves a Windows time zone name (punctuation optional) or an IANA name.
func logicLoadZone(name string) (*time.Location, error) {
	if iana, ok := logicZoneIndex()[normalizeZoneName(name)]; ok {
		if loc, err := time.LoadLocation(iana); err == nil {
			return loc, nil
		}
	}

	if loc, err := time.LoadLocation(name); err == nil && name != "" && !strings.EqualFold(name, "local") {
		return loc, nil
	}

	return nil, exprErr("the time zone '%s' was not found", name)
}

func inZoneWall(t time.Time, loc *time.Location) time.Time {
	w := t.In(loc)

	return time.Date(w.Year(), w.Month(), w.Day(), w.Hour(), w.Minute(), w.Second(), w.Nanosecond(), time.UTC)
}

func wallToUTC(t time.Time, loc *time.Location) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), loc).UTC()
}

func zoneConvert(a []any, srcIdx, dstIdx, fmtIdx int) (any, error) {
	ts, err := parseLogicTime(logicString(a[0]))
	if err != nil {
		return nil, err
	}

	t := ts.t
	utc := true

	if srcIdx >= 0 {
		src, serr := logicLoadZone(logicString(a[srcIdx]))
		if serr != nil {
			return nil, serr
		}

		t = wallToUTC(t, src)
	}

	if dstIdx >= 0 {
		dst, derr := logicLoadZone(logicString(a[dstIdx]))
		if derr != nil {
			return nil, derr
		}

		t, utc = inZoneWall(t, dst), false
	}

	return formatLogicTime(logicTS{t: t, hasZone: utc}, optFormat(a, fmtIdx), "")
}

func addLogicZoneFuncs(m map[string]logicFunc) {
	m["convertfromutc"] = eagerLogic("convertFromUtc", argsTwo, argsThree, func(a []any) (any, error) {
		return zoneConvert(a, -1, 1, argsTwo)
	})
	m["converttoutc"] = eagerLogic("convertToUtc", argsTwo, argsThree, func(a []any) (any, error) {
		return zoneConvert(a, 1, -1, argsTwo)
	})
	m["converttimezone"] = eagerLogic("convertTimeZone", argsThree, argsFour, func(a []any) (any, error) {
		return zoneConvert(a, 1, argsTwo, argsThree)
	})
	m["parsedatetime"] = eagerLogic("parseDateTime", 1, argsThree, logicParseDateTime)
}
