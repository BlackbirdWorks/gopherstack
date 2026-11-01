package iceberg

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	murmurC1       = 0xcc9e2d51
	murmurC2       = 0x1b873593
	murmurN        = 0xe6546b64
	murmurMix1     = 0x85ebca6b
	murmurMix2     = 0xc2b2ae35
	rotl1          = 15
	rotl2          = 13
	secondsPerDay  = 86400
	microsPerSec   = 1_000_000
	microsPerHour  = 3_600_000_000
	epochYear      = 1970
	maxInt32Bucket = 0x7fffffff
)

// Transform is a parsed partition transform.
type Transform struct {
	Name  string
	Width int
}

// ParseTransform parses identity, year, month, day, hour, void, bucket[N] and truncate[W].
func ParseTransform(s string) (Transform, error) {
	name, arg, hasArg := strings.Cut(strings.ToLower(strings.TrimSpace(s)), "[")
	if !hasArg {
		switch name {
		case "identity", transformYear, transformMonth, transformDay, transformHour, "void":
			return Transform{Name: name}, nil
		}

		return Transform{}, fmt.Errorf("%w: transform %q", ErrUnsupportedType, s)
	}

	n64, err := strconv.ParseInt(strings.TrimSuffix(arg, "]"), 10, 32)
	if err != nil || n64 <= 0 || (name != "bucket" && name != "truncate") {
		return Transform{}, fmt.Errorf("%w: transform %q", ErrUnsupportedType, s)
	}

	return Transform{Name: name, Width: int(n64)}, nil
}

// String renders the transform as it appears in a partition spec.
func (t Transform) String() string {
	if t.Width > 0 {
		return fmt.Sprintf("%s[%d]", t.Name, t.Width)
	}

	return t.Name
}

// ResultType is the Iceberg type of the transform's output for a source type.
func (t Transform) ResultType(source string) string {
	switch t.Name {
	case transformYear, transformMonth, transformDay, transformHour, transformBucket:
		return typeInt
	default:
		return source
	}
}

// Apply transforms the textual value of a source column. A nil result is null.
// Results are bool, int32, int64, float32, float64 or string.
func (t Transform) Apply(source string, raw *string) (any, error) {
	if raw == nil || t.Name == "void" {
		return nil, nil //nolint:nilnil // a null partition value is not an error
	}

	switch t.Name {
	case "identity":
		return identityValue(source, *raw)
	case transformYear, transformMonth, transformDay, transformHour:
		return t.temporal(source, *raw)
	case transformBucket:
		return t.bucket(source, *raw)
	default:
		return t.truncate(source, *raw)
	}
}

func parseInstant(source, raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)

	for _, l := range []string{
		"2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05.999999999", time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 MST", "2006-01-02",
	} {
		if v, err := time.Parse(l, raw); err == nil {
			return v.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("%w: %q as %s", ErrUnsupportedType, raw, source)
}

func identityValue(source, raw string) (any, error) {
	switch source {
	case typeBoolean:
		return strings.EqualFold(raw, "true"), nil
	case typeInt:
		n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32)

		return narrow(n), err
	case typeLong:
		return strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	case typeFloat:
		f, err := strconv.ParseFloat(strings.TrimSpace(raw), 32)

		return float32(f), err
	case typeDouble:
		return strconv.ParseFloat(strings.TrimSpace(raw), 64)
	case typeDate:
		d, err := parseInstant(source, raw)

		return narrow(d.Unix() / secondsPerDay), err
	case typeTimestamp, typeTimestamptz:
		d, err := parseInstant(source, raw)

		return d.UnixMicro(), err
	case typeString, typeUUID:
		return raw, nil
	default:
		return nil, fmt.Errorf("%w: identity partition on %s", ErrUnsupportedType, source)
	}
}

func (t Transform) temporal(source, raw string) (any, error) {
	if source != "date" && source != "timestamp" && source != "timestamptz" {
		return nil, fmt.Errorf("%w: %s partition on %s", ErrUnsupportedType, t.Name, source)
	}

	if t.Name == transformHour && source == "date" {
		return nil, fmt.Errorf("%w: hour partition on date", ErrUnsupportedType)
	}

	d, err := parseInstant(source, raw)
	if err != nil {
		return nil, err
	}

	switch t.Name {
	case transformYear:
		return narrow(int64(d.Year() - epochYear)), nil
	case transformMonth:
		return narrow(int64((d.Year()-epochYear)*12 + int(d.Month()) - 1)), nil
	case transformDay:
		return narrow(floorDiv(d.Unix(), secondsPerDay)), nil
	default:
		return narrow(floorDiv(d.UnixMicro(), microsPerHour)), nil
	}
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}

	return q
}

func (t Transform) bucket(source, raw string) (any, error) {
	var in []byte

	switch source {
	case typeInt, typeLong, typeDate, typeTimestamp, typeTimestamptz:
		v, err := identityValue(source, raw)
		if err != nil {
			return nil, err
		}

		var n int64

		switch x := v.(type) {
		case int32:
			n = int64(x)
		case int64:
			n = x
		}

		in = binary.LittleEndian.AppendUint64(nil, uint64(n))
	case typeString, typeUUID:
		in = []byte(raw)
	case typeBinary:
		in = []byte(raw)
	default:
		return nil, fmt.Errorf("%w: bucket partition on %s", ErrUnsupportedType, source)
	}

	if t.Width <= 0 || t.Width > math.MaxInt32 {
		return nil, fmt.Errorf("%w: bucket width %d", ErrUnsupportedType, t.Width)
	}

	width := uint32(t.Width)

	return narrow(int64((murmur3(in) & maxInt32Bucket) % width)), nil
}

func (t Transform) truncate(source, raw string) (any, error) {
	switch source {
	case typeInt, typeLong:
		v, err := identityValue(source, raw)
		if err != nil {
			return nil, err
		}

		w := int64(t.Width)

		if x, ok := v.(int32); ok {
			n := int64(x)

			return narrow(n - ((n%w)+w)%w), nil
		}

		n, _ := v.(int64)

		return n - ((n%w)+w)%w, nil
	case typeString:
		if utf8.RuneCountInString(raw) <= t.Width {
			return raw, nil
		}

		return string([]rune(raw)[:t.Width]), nil
	case "decimal":
		return nil, fmt.Errorf("%w: truncate partition on decimal", ErrUnsupportedType)
	default:
		return nil, fmt.Errorf("%w: truncate partition on %s", ErrUnsupportedType, source)
	}
}

//nolint:mnd // MurmurHash3 x86_32 mixing constants
func murmur3(data []byte) uint32 {
	var h uint32

	n := len(data) / 4 * 4
	for i := 0; i < n; i += 4 {
		k := binary.LittleEndian.Uint32(data[i:])
		k *= murmurC1
		k = k<<rotl1 | k>>(32-rotl1)
		k *= murmurC2
		h ^= k
		h = h<<rotl2 | h>>(32-rotl2)
		h = h*5 + murmurN
	}

	var k uint32

	switch len(data) & 3 {
	case 3:
		k ^= uint32(data[n+2]) << 16

		fallthrough
	case 2:
		k ^= uint32(data[n+1]) << 8

		fallthrough
	case 1:
		k ^= uint32(data[n])
		k *= murmurC1
		k = k<<rotl1 | k>>(32-rotl1)
		k *= murmurC2
		h ^= k
	}

	h ^= uint32(len(data)) //nolint:gosec // length fits uint32 for hashed values
	h ^= h >> 16
	h *= murmurMix1
	h ^= h >> 13
	h *= murmurMix2
	h ^= h >> 16

	return h
}

// AvroTypeOf is the Avro primitive a value of the given Iceberg primitive is stored as in a manifest.
func AvroTypeOf(icebergType string) (string, error) {
	switch icebergType {
	case typeBoolean:
		return typeBoolean, nil
	case typeInt, typeDate:
		return typeInt, nil
	case typeLong, typeTime, typeTimestamp, typeTimestamptz:
		return typeLong, nil
	case typeFloat:
		return typeFloat, nil
	case typeDouble:
		return typeDouble, nil
	case typeString, typeUUID:
		return typeString, nil
	default:
		return "", fmt.Errorf("%w: partition value of type %s", ErrUnsupportedType, icebergType)
	}
}

// narrow converts a transform result that is bounded by calendar or width arithmetic to an Avro int.
func narrow(n int64) int32 {
	return int32(n) //nolint:gosec // years, days, hours and truncated ints fit 32 bits
}
