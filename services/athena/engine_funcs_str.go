package athena

import (
	"crypto/md5"  //nolint:gosec // Athena exposes md5()
	"crypto/sha1" //nolint:gosec // Athena exposes sha1()
	"crypto/sha256"
	"crypto/sha512"
	"database/sql/driver"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand/v2"
	"net/url"
	"regexp"
	"strings"
	"unicode/utf8"
)

var (
	errBadValue   = errors.New("invalid argument")
	errPadTooLong = errors.New("pad length too large")
)

const maxPadLength = 1 << 24

func compileRegexp(v driver.Value) (*regexp.Regexp, error) {
	p, _ := asString(v)

	re, err := regexp.Compile(p)
	if err != nil {
		return nil, fmt.Errorf("invalid regular expression: %w", err)
	}

	return re, nil
}

func udfRegexpExtract(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, ok := asString(args[0])
	if !ok {
		return sqlNull(), nil
	}

	re, err := compileRegexp(args[1])
	if err != nil {
		return nil, err
	}

	group := int64(0)
	if len(args) == argsThree {
		group, _ = toInt(args[2])
	}

	m := re.FindStringSubmatch(s)
	if m == nil || group < 0 || group >= int64(len(m)) {
		return sqlNull(), nil
	}

	return m[group], nil
}

func udfRegexpExtractAll(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, ok := asString(args[0])
	if !ok {
		return sqlNull(), nil
	}

	re, err := compileRegexp(args[1])
	if err != nil {
		return nil, err
	}

	group := int64(0)
	if len(args) == argsThree {
		group, _ = toInt(args[2])
	}

	out := []any{}

	for _, m := range re.FindAllStringSubmatch(s, -1) {
		if group >= 0 && group < int64(len(m)) {
			out = append(out, m[group])
		}
	}

	return jsonText(out), nil
}

// goReplacement turns Trino's $1 / ${name} references into Go's ${1} form.
func goReplacement(r string) string {
	return regexp.MustCompile(`\$(\d+)`).ReplaceAllString(r, "$${$1}")
}

func udfRegexpReplace(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, ok := asString(args[0])
	if !ok {
		return sqlNull(), nil
	}

	re, err := compileRegexp(args[1])
	if err != nil {
		return nil, err
	}

	repl := ""
	if len(args) == argsThree {
		repl, _ = asString(args[2])
	}

	return re.ReplaceAllString(s, goReplacement(repl)), nil
}

func udfRegexpCount(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, ok := asString(args[0])
	if !ok {
		return sqlNull(), nil
	}

	re, err := compileRegexp(args[1])
	if err != nil {
		return nil, err
	}

	return int64(len(re.FindAllStringIndex(s, -1))), nil
}

func udfRegexpSplit(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, ok := asString(args[0])
	if !ok {
		return sqlNull(), nil
	}

	re, err := compileRegexp(args[1])
	if err != nil {
		return nil, err
	}

	return jsonText(re.Split(s, -1)), nil
}

func udfReverse(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, ok := asString(args[0])
	if !ok {
		return sqlNull(), nil
	}

	r := []rune(s)
	for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}

	return string(r), nil
}

func pad(args []driver.Value, left bool) (driver.Value, error) {
	s, sok := asString(args[0])
	n, nok := toInt(args[1])
	fill, fok := asString(args[2])

	if !sok || !nok || !fok || n < 0 || fill == "" {
		return sqlNull(), nil
	}

	r := []rune(s)
	if n <= int64(len(r)) {
		return string(r[:n]), nil
	}

	if n > maxPadLength {
		return nil, fmt.Errorf("%w: pad length %d exceeds %d", errPadTooLong, n, maxPadLength)
	}

	f := []rune(fill)
	padding := make([]rune, n-int64(len(r)))

	for i := range padding {
		padding[i] = f[i%len(f)]
	}

	if left {
		return string(padding) + s, nil
	}

	return s + string(padding), nil
}

func udfLpad(_ *fnCtx, args []driver.Value) (driver.Value, error) { return pad(args, true) }

func udfRpad(_ *fnCtx, args []driver.Value) (driver.Value, error) { return pad(args, false) }

func udfStartsWith(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, sok := asString(args[0])
	p, pok := asString(args[1])

	if !sok || !pok {
		return sqlNull(), nil
	}

	return boolInt(strings.HasPrefix(s, p)), nil
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}

	return 0
}

func udfChr(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	n, ok := toInt(args[0])
	if !ok || n < 0 || n > utf8.MaxRune {
		return sqlNull(), nil
	}

	return string(rune(n)), nil
}

func udfCodepoint(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, ok := asString(args[0])
	if !ok || utf8.RuneCountInString(s) != 1 {
		return sqlNull(), nil
	}

	r, _ := utf8.DecodeRuneInString(s)

	return int64(r), nil
}

func levenshtein(a, b []rune) int64 {
	prev := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}

	for i := 1; i <= len(a); i++ {
		cur := make([]int, len(b)+1)
		cur[0] = i

		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}

			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}

		prev = cur
	}

	return int64(prev[len(b)])
}

func udfLevenshtein(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	a, aok := asString(args[0])
	b, bok := asString(args[1])

	if !aok || !bok {
		return sqlNull(), nil
	}

	return levenshtein([]rune(a), []rune(b)), nil
}

func bytesArg(v driver.Value) ([]byte, bool) {
	switch t := v.(type) {
	case string:
		return []byte(t), true
	case []byte:
		return t, true
	default:
		return nil, false
	}
}

func hashFunc(sum func([]byte) []byte) func(*fnCtx, []driver.Value) (driver.Value, error) {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		b, ok := bytesArg(args[0])
		if !ok {
			return sqlNull(), nil
		}

		return hex.EncodeToString(sum(b)), nil
	}
}

func sumMD5(b []byte) []byte {
	s := md5.Sum(b) //nolint:gosec // Athena exposes md5()

	return s[:]
}

func sumSHA1(b []byte) []byte {
	s := sha1.Sum(b) //nolint:gosec // Athena exposes sha1()

	return s[:]
}

func sumSHA256(b []byte) []byte {
	s := sha256.Sum256(b)

	return s[:]
}

func sumSHA512(b []byte) []byte {
	s := sha512.Sum512(b)

	return s[:]
}

func udfCRC32(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	b, ok := bytesArg(args[0])
	if !ok {
		return sqlNull(), nil
	}

	return int64(crc32.ChecksumIEEE(b)), nil
}

func udfToHex(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	b, ok := bytesArg(args[0])
	if !ok {
		return sqlNull(), nil
	}

	return strings.ToUpper(hex.EncodeToString(b)), nil
}

func udfFromHex(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, ok := asString(args[0])
	if !ok {
		return sqlNull(), nil
	}

	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid hex %q", errBadValue, s)
	}

	return string(b), nil
}

func udfToBase64(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	b, ok := bytesArg(args[0])
	if !ok {
		return sqlNull(), nil
	}

	return base64.StdEncoding.EncodeToString(b), nil
}

func udfFromBase64(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, ok := asString(args[0])
	if !ok {
		return sqlNull(), nil
	}

	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid base64", errBadValue)
	}

	return string(b), nil
}

func identityFunc(_ *fnCtx, args []driver.Value) (driver.Value, error) { return args[0], nil }

func urlPart(part func(*url.URL) (string, bool)) func(*fnCtx, []driver.Value) (driver.Value, error) {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		s, ok := asString(args[0])
		if !ok {
			return sqlNull(), nil
		}

		u, err := url.Parse(s)
		if err != nil {
			return sqlNull(), nil //nolint:nilerr // Trino returns NULL for an unparsable URL
		}

		if v, found := part(u); found {
			return v, nil
		}

		return sqlNull(), nil
	}
}

func udfURLParameter(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, ok := asString(args[0])
	name, nok := asString(args[1])

	if !ok || !nok {
		return sqlNull(), nil
	}

	u, err := url.Parse(s)
	if err != nil || !u.Query().Has(name) {
		return sqlNull(), nil //nolint:nilerr // Trino returns NULL for an unparsable URL
	}

	return u.Query().Get(name), nil
}

func udfRand(_ *fnCtx, _ []driver.Value) (driver.Value, error) {
	return rand.Float64(), nil //nolint:gosec // not security-sensitive
}

func udfCbrt(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	f, ok := toFloatArg(args[0])
	if !ok {
		return sqlNull(), nil
	}

	return math.Cbrt(f), nil
}

func toFloatArg(v driver.Value) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case int64:
		return float64(t), true
	default:
		return 0, false
	}
}

func udfIsNaN(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	f, ok := toFloatArg(args[0])
	if !ok {
		return sqlNull(), nil
	}

	return boolInt(math.IsNaN(f)), nil
}

func udfIsFinite(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	f, ok := toFloatArg(args[0])
	if !ok {
		return sqlNull(), nil
	}

	return boolInt(!math.IsNaN(f) && !math.IsInf(f, 0)), nil
}

func bitwise(op func(a, b int64) int64) func(*fnCtx, []driver.Value) (driver.Value, error) {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		a, aok := toInt(args[0])
		b, bok := toInt(args[1])

		if !aok || !bok {
			return sqlNull(), nil
		}

		return op(a, b), nil
	}
}
