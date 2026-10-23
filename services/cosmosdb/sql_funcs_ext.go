package cosmosdb

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

const (
	ticksPerSecond  = 10_000_000
	datePartsHour   = 3
	datePartsMinute = 4
	datePartsSecond = 5
	datePartsFrac   = 6
)

// parseJSONContainer implements StringToArray and StringToObject: the string must hold exactly that JSON kind.
func parseJSONContainer(name, s string) any {
	dec := json.NewDecoder(bytes.NewReader([]byte(s)))
	dec.UseNumber()

	var v any
	if err := dec.Decode(&v); err != nil || rejectTrailingJSON(dec) != nil {
		return undef{}
	}

	switch v.(type) {
	case []any:
		if name == biStringtoarray {
			return v
		}
	case map[string]any:
		if name == biStringtoobject {
			return v
		}
	}

	return undef{}
}

// stripFreeSpacing removes whitespace and #-comments outside character classes and escapes (regex modifier x).
func stripFreeSpacing(pat string) string {
	var sb strings.Builder

	inClass, escaped, comment := false, false, false

	for _, r := range pat {
		switch {
		case comment:
			comment = r != '\n'
		case escaped:
			sb.WriteRune(r)

			escaped = false
		case r == '\\':
			sb.WriteRune(r)

			escaped = true
		case inClass:
			sb.WriteRune(r)

			inClass = r != ']'
		case r == '[':
			sb.WriteRune(r)

			inClass = true
		case r == '#':
			comment = true
		case r == ' ' || r == '\t' || r == '\n' || r == '\r':
		default:
			sb.WriteRune(r)
		}
	}

	return sb.String()
}

// dateTicksOp implements DateTimeFromParts, DateTimeToTicks and TicksToDateTime (ticks are 100ns since 1970).
func dateTicksOp(name string, args []any) any {
	switch name {
	case biDatetimetoticks:
		t, ok := parseDateTime(args[0])
		if !ok {
			return undef{}
		}

		return json.Number(strconv.FormatInt(t.Unix()*ticksPerSecond+int64(t.Nanosecond())/ticksPerNano, 10))
	case biTickstodatetime:
		n, ok := toNumber(args[0])
		if !ok {
			return undef{}
		}

		ticks, err := n.Int64()
		if err != nil {
			return undef{}
		}

		return time.Unix(ticks/ticksPerSecond, (ticks%ticksPerSecond)*ticksPerNano).UTC().Format(dateTimeLayout)
	default:
		return dateTimeFromParts(args)
	}
}

func dateTimeFromParts(args []any) any {
	vals := make([]int, len(args))

	for i, a := range args {
		v, ok := intArg(a)
		if !ok {
			return undef{}
		}

		vals[i] = v
	}

	part := func(i int) int {
		if i < len(vals) {
			return vals[i]
		}

		return 0
	}

	y, mo, d := vals[0], vals[1], vals[2]
	t := time.Date(y, time.Month(mo), d, part(datePartsHour), part(datePartsMinute), part(datePartsSecond),
		part(datePartsFrac)*ticksPerNano, time.UTC)

	if t.Year() != y || int(t.Month()) != mo || t.Day() != d || part(datePartsHour) > 23 ||
		part(datePartsMinute) > 59 ||
		part(datePartsSecond) > 59 ||
		part(datePartsFrac) < 0 ||
		part(datePartsFrac) >= ticksPerSecond {
		return undef{}
	}

	return t.Format(dateTimeLayout)
}
