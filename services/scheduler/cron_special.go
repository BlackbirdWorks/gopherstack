package scheduler

import (
	"strconv"
	"strings"
	"time"
)

const (
	cronHashParts   = 2
	cronSaturdayDW  = 7
	cronWeekendRoll = 2
)

func daysInMonth(t time.Time) int {
	return time.Date(t.Year(), t.Month()+1, 0, 0, 0, 0, 0, t.Location()).Day()
}

func isWeekdayDate(t time.Time, day int) bool {
	wd := time.Date(t.Year(), t.Month(), day, 0, 0, 0, 0, t.Location()).Weekday()

	return wd != time.Saturday && wd != time.Sunday
}

// nearestWeekday returns the weekday day-of-month closest to target without leaving the month.
func nearestWeekday(t time.Time, target int) (int, bool) {
	last := daysInMonth(t)
	if target < 1 || target > last {
		return 0, false
	}

	wd := time.Date(t.Year(), t.Month(), target, 0, 0, 0, 0, t.Location()).Weekday()

	switch wd {
	case time.Saturday:
		if target == 1 {
			return target + cronWeekendRoll, true
		}

		return target - 1, true
	case time.Sunday:
		if target == last {
			return target - cronWeekendRoll, true
		}

		return target + 1, true
	default:
		return target, true
	}
}

func lastWeekday(t time.Time) int {
	day := daysInMonth(t)
	for !isWeekdayDate(t, day) {
		day--
	}

	return day
}

// matchesCronDayOfMonth matches a day-of-month field, supporting L, LW, L-n and nW tokens.
func matchesCronDayOfMonth(t time.Time, field string) bool {
	if field == "*" || field == "?" {
		return true
	}

	for part := range strings.SplitSeq(field, ",") {
		if matchesDayOfMonthPart(t, strings.TrimSpace(part)) {
			return true
		}
	}

	return false
}

func matchesDayOfMonthPart(t time.Time, part string) bool {
	switch {
	case part == "L":
		return t.Day() == daysInMonth(t)
	case part == "LW":
		return t.Day() == lastWeekday(t)
	case strings.HasPrefix(part, "L-"):
		n, err := strconv.Atoi(part[2:])

		return err == nil && t.Day() == daysInMonth(t)-n
	case strings.HasSuffix(part, "W"):
		n, err := strconv.Atoi(strings.TrimSuffix(part, "W"))
		if err != nil {
			return false
		}

		day, ok := nearestWeekday(t, n)

		return ok && t.Day() == day
	}

	return matchesCronPart(part, t.Day())
}

// matchesCronDayOfWeek matches a day-of-week field, supporting L, nL and n#m tokens.
func matchesCronDayOfWeek(t time.Time, field string) bool {
	if field == "*" || field == "?" {
		return true
	}

	dow := dayOfWeekAWS(t.Weekday())

	for part := range strings.SplitSeq(field, ",") {
		if matchesDayOfWeekPart(t, dow, strings.TrimSpace(part)) {
			return true
		}
	}

	return false
}

func matchesDayOfWeekPart(t time.Time, dow int, part string) bool {
	switch {
	case part == "L":
		return dow == cronSaturdayDW
	case strings.Contains(part, "#"):
		return matchesNthWeekday(t, dow, part)
	case strings.HasSuffix(part, "L"):
		want, err := parseCronValue(strings.TrimSuffix(part, "L"))

		return err == nil && want == dow && t.Day()+daysPerWeek > daysInMonth(t)
	}

	return matchesCronPart(part, dow)
}

const daysPerWeek = 7

func matchesNthWeekday(t time.Time, dow int, part string) bool {
	fields := strings.Split(part, "#")
	if len(fields) != cronHashParts {
		return false
	}

	want, err1 := parseCronValue(fields[0])
	nth, err2 := strconv.Atoi(fields[1])
	if err1 != nil || err2 != nil {
		return false
	}

	return want == dow && (t.Day()-1)/daysPerWeek+1 == nth
}
