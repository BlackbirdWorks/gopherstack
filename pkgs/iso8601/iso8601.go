// Package iso8601 parses and formats the PnDTnHnMnS/PTnHnMnS subset of ISO
// 8601 durations used by Azure Service Bus's LockDuration/
// DefaultMessageTimeToLive XML properties, and by ARM's equivalent JSON
// properties on Microsoft.ServiceBus queue/topic/subscription resources.
// Extracted from services/azureservicebus (M5) when services/azurearm (M9)
// became a second caller needing the exact same logic -- mirroring
// pkgs/odatatable's earlier extraction for the same reason (AZURE.md
// section 9's M6 milestone).
package iso8601

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ErrInvalidDuration is returned by Parse for a string that doesn't match
// the supported PnDTnHnMnS/PTnHnMnS subset.
var ErrInvalidDuration = errors.New("iso8601: invalid duration")

// Seconds-per-unit conversion factors for the D/H/M components of an ISO
// 8601 duration.
const (
	secondsPerDay    = 24 * 60 * 60
	secondsPerHour   = 60 * 60
	secondsPerMinute = 60
)

// durationPattern matches the PnDTnHnMnS / PTnHnMnS subset of ISO 8601
// durations that real Service Bus's LockDuration/MaxDeliveryCount/
// DefaultMessageTimeToLive XML elements (and ARM's equivalent JSON
// properties) use (e.g. "PT1M", "PT30S", "P14D", "PT5M30.5S"). Weeks (PnW),
// months, and years are deliberately not supported -- Service Bus never
// emits or expects them for these properties.
var durationPattern = regexp.MustCompile(
	`^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$`,
)

// Parse parses the PnDTnHnMnS/PTnHnMnS subset of ISO 8601 durations used by
// Service Bus's LockDuration and DefaultMessageTimeToLive properties
// (MaxDeliveryCount, unlike those two, is a plain integer, not a duration,
// and does not go through this parser). Fractional seconds are supported. A
// value that would overflow time.Duration's int64-nanosecond range is
// clamped to the maximum representable time.Duration (~292 years) rather
// than erroring -- real Service Bus's own "infinite" sentinel for these
// fields, "P10675199DT2H48M5.4775807S", is itself just .NET's
// TimeSpan.MaxValue (which is exactly math.MaxInt64 100-nanosecond ticks,
// i.e. very close to Go's own int64-nanosecond time.Duration ceiling)
// spelled out in ISO 8601, so this clamp is a deliberate, documented
// approximation of that sentinel rather than a real precision loss for any
// duration Service Bus itself would ever produce.
func Parse(s string) (time.Duration, error) {
	m := durationPattern.FindStringSubmatch(s)
	if m == nil || (m[1] == "" && m[2] == "" && m[3] == "" && m[4] == "") {
		return 0, fmt.Errorf("%w: %q", ErrInvalidDuration, s)
	}

	var totalSeconds float64

	for i, unitSeconds := range [...]float64{secondsPerDay, secondsPerHour, secondsPerMinute, 1} {
		if m[i+1] == "" {
			continue
		}

		v, err := strconv.ParseFloat(m[i+1], 64)
		if err != nil {
			return 0, fmt.Errorf("%w: %q: %w", ErrInvalidDuration, s, err)
		}

		totalSeconds += v * unitSeconds
	}

	nanos := totalSeconds * float64(time.Second)
	// float64(math.MaxInt64) rounds UP to exactly 2^63 (one more than the
	// actual int64 maximum, 2^63-1, which has no exact float64
	// representation) -- so this comparison must be >=, not >. With a plain
	// >, nanos == 2^63 would fail the clamp check and fall through to
	// time.Duration(nanos), converting an out-of-range float64 to int64:
	// implementation-defined behavior that yields a negative duration on
	// amd64/arm64, silently corrupting LockDuration/DefaultMessageTimeToLive
	// into something negative instead of clamping to the documented maximum.
	if nanos >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64), nil
	}

	return time.Duration(nanos), nil
}

// Zero is the shortest valid ISO 8601 representation of "no duration",
// used by Format for a non-positive input.
const Zero = "PT0S"

// durationParts holds the D/H/M/S components (plus any sub-second
// remainder) that Format decomposes a positive time.Duration into.
type durationParts struct {
	days, hours, minutes, seconds int64
	fracNanos                     int64
}

// decompose breaks a positive time.Duration down into its ISO 8601
// D/H/M/S components. d must be > 0; Format handles the non-positive case
// itself before calling this.
func decompose(d time.Duration) durationParts {
	totalWhole := int64(d / time.Second)
	fracNanos := int64(d % time.Second)

	days := totalWhole / secondsPerDay
	rem := totalWhole % secondsPerDay
	hours := rem / secondsPerHour
	rem %= secondsPerHour
	minutes := rem / secondsPerMinute
	seconds := rem % secondsPerMinute

	return durationParts{days: days, hours: hours, minutes: minutes, seconds: seconds, fracNanos: fracNanos}
}

// formatSeconds renders the "nS" or "n.fffS" trailing component for the
// given whole seconds and sub-second nanosecond remainder, or "" if both
// are zero (i.e. there's no seconds component to emit at all).
func formatSeconds(seconds, fracNanos int64) string {
	switch {
	case fracNanos > 0:
		secondsFloat := float64(seconds) + float64(fracNanos)/float64(time.Second)

		return strconv.FormatFloat(secondsFloat, 'f', -1, 64) + "S"
	case seconds > 0:
		return strconv.FormatInt(seconds, 10) + "S"
	default:
		return ""
	}
}

// Format is Parse's inverse. A non-positive duration formats as Zero.
func Format(d time.Duration) string {
	if d <= 0 {
		return Zero
	}

	p := decompose(d)

	var b strings.Builder

	b.WriteString("P")

	if p.days > 0 {
		fmt.Fprintf(&b, "%dD", p.days)
	}

	if p.hours == 0 && p.minutes == 0 && p.seconds == 0 && p.fracNanos == 0 {
		return b.String()
	}

	b.WriteString("T")

	if p.hours > 0 {
		fmt.Fprintf(&b, "%dH", p.hours)
	}

	if p.minutes > 0 {
		fmt.Fprintf(&b, "%dM", p.minutes)
	}

	b.WriteString(formatSeconds(p.seconds, p.fracNanos))

	return b.String()
}
