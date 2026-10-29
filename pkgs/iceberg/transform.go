package iceberg

import (
	"encoding/binary"
	"fmt"
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
		case "identity", "year", "month", "day", "hour", "void":
			return Transform{Name: name}, nil
		}

		return Transform{}, fmt.Errorf("%w: transform %q", ErrUnsupportedType, s)
	}

	n, err := strconv.Atoi(strings.TrimSuffix(arg, "]"))
	if err != nil || n <= 0 || (name != "bucket" && name != "truncate") {
		return Transform{}, fmt.Errorf("%w: transform %q", ErrUnsupportedType, s)
	}

	return Transform{Name: name, Width: n}, nil
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
	case "year", "month", "day", "hour", "bucket":
		return "int"
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
	case "year", "month", "day", "hour":
		return t.temporal(source, *raw)
	case "bucket":
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
	case "boolean":
		return strings.EqualFold(raw, "true"), nil
	case "int":
		n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32)

		return int32(n), err
	case "long":
		return strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	case "float":
		f, err := strconv.ParseFloat(strings.TrimSpace(raw), 32)

		return float32(f), err
	case "double":
		return strconv.ParseFloat(strings.TrimSpace(raw), 64)
	case "date":
		d, err := parseInstant(source, raw)

		return int32(d.Unix() / secondsPerDay), err
	case "timestamp", "timestamptz":
		d, err := parseInstant(source, raw)

		return d.UnixMicro(), err
	case "string", "uuid":
		return raw, nil
	default:
		return nil, fmt.Errorf("%w: identity partition on %s", ErrUnsupportedType, source)
	}
}

func (t Transform) temporal(source, raw string) (any, error) {
	if source != "date" && source != "timestamp" && source != "timestamptz" {
		return nil, fmt.Errorf("%w: %s partition on %s", ErrUnsupportedType, t.Name, source)
	}

	if t.Name == "hour" && source == "date" {
		return nil, fmt.Errorf("%w: hour partition on date", ErrUnsupportedType)
	}

	d, err := parseInstant(source, raw)
	if err != nil {
		return nil, err
	}

	switch t.Name {
	case "year":
		return int32(d.Year() - epochYear), nil
	case "month":
		return int32((d.Year()-epochYear)*12 + int(d.Month()) - 1), nil
	case "day":
		return int32(floorDiv(d.Unix(), secondsPerDay)), nil
	default:
		return int32(floorDiv(d.UnixMicro(), microsPerHour)), nil
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
	case "int", "long", "date", "timestamp", "timestamptz":
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
	case "string", "uuid":
		in = []byte(raw)
	case "binary":
		in = []byte(raw)
	default:
		return nil, fmt.Errorf("%w: bucket partition on %s", ErrUnsupportedType, source)
	}

	return int32((murmur3(in) & maxInt32Bucket) % uint32(t.Width)), nil //nolint:gosec // width is validated positive
}

func (t Transform) truncate(source, raw string) (any, error) {
	switch source {
	case "int", "long":
		v, err := identityValue(source, raw)
		if err != nil {
			return nil, err
		}

		w := int64(t.Width)

		if x, ok := v.(int32); ok {
			n := int64(x)

			return int32(n - ((n%w)+w)%w), nil
		}

		n, _ := v.(int64)

		return n - ((n%w)+w)%w, nil
	case "string":
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
	case "boolean":
		return "boolean", nil
	case "int", "date":
		return "int", nil
	case "long", "time", "timestamp", "timestamptz":
		return "long", nil
	case "float":
		return "float", nil
	case "double":
		return "double", nil
	case "string", "uuid":
		return "string", nil
	default:
		return "", fmt.Errorf("%w: partition value of type %s", ErrUnsupportedType, icebergType)
	}
}
