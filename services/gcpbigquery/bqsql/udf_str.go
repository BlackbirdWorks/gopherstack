package bqsql

import (
	"crypto/md5"  //nolint:gosec // MD5 is a BigQuery function
	"crypto/sha1" //nolint:gosec // SHA1 is a BigQuery function
	"crypto/sha256"
	"crypto/sha512"
	"database/sql/driver"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"unicode"
	"unicode/utf8"
)

func strFn(n int32, f func(s string, a []driver.Value) (driver.Value, error)) udfDef {
	return udfDef{n: n, fn: func(a []driver.Value) (driver.Value, error) {
		if a[0] == nil {
			return nil, nil
		}

		return f(toStr(a[0]), a)
	}}
}

func addStringUDFs(d map[string]udfDef) {
	d["lower"] = strFn(1, func(s string, _ []driver.Value) (driver.Value, error) { return strings.ToLower(s), nil })
	d["upper"] = strFn(1, func(s string, _ []driver.Value) (driver.Value, error) { return strings.ToUpper(s), nil })
	d["initcap"] = strFn(-1, func(s string, a []driver.Value) (driver.Value, error) { return initcap(s, a), nil })
	d["reverse"] = strFn(1, func(s string, _ []driver.Value) (driver.Value, error) { return reverseRunes(s), nil })
	d["length"] = lengthFn(true)
	d["char_length"] = lengthFn(true)
	d["byte_length"] = lengthFn(false)
	d["ascii"] = strFn(1, func(s string, _ []driver.Value) (driver.Value, error) {
		if s == "" {
			return int64(0), nil
		}

		return int64(s[0]), nil
	})
	d["chr"] = udfDef{n: 1, fn: udfChr}
	d["repeat"] = udfDef{n: argc2, fn: udfRepeat}
	d["replace"] = udfDef{n: argc3, fn: udfReplace}
	d["trim"] = udfDef{n: -1, fn: trimFn(strings.TrimFunc, strings.Trim)}
	d["ltrim"] = udfDef{n: -1, fn: trimFn(strings.TrimLeftFunc, strings.TrimLeft)}
	d["rtrim"] = udfDef{n: -1, fn: trimFn(strings.TrimRightFunc, strings.TrimRight)}
	d["lpad"] = udfDef{n: -1, fn: padFn(true)}
	d["rpad"] = udfDef{n: -1, fn: padFn(false)}
	d["left"] = udfDef{n: argc2, fn: sideFn(true)}
	d["right"] = udfDef{n: argc2, fn: sideFn(false)}
	d["substr"] = udfDef{n: -1, fn: udfSubstr}
	d["concat"] = udfDef{n: -1, fn: udfConcat}
	d["strpos"] = udfDef{n: argc2, fn: udfStrpos}
	d["instr"] = udfDef{n: -1, fn: udfStrpos}
	d["starts_with"] = udfDef{n: argc2, fn: predFn(strings.HasPrefix)}
	d["ends_with"] = udfDef{n: argc2, fn: predFn(strings.HasSuffix)}
	d["contains_substr"] = udfDef{n: argc2, fn: predFn(func(s, sub string) bool {
		return strings.Contains(strings.ToLower(s), strings.ToLower(sub))
	})}
	d["split"] = udfDef{n: -1, fn: udfSplit}
	d["to_base64"] = udfDef{n: 1, fn: encFn(func(b []byte) string { return base64.StdEncoding.EncodeToString(b) })}
	d["to_hex"] = udfDef{n: 1, fn: encFn(hex.EncodeToString)}
	d["from_base64"] = udfDef{n: 1, fn: decFn(base64.StdEncoding.DecodeString)}
	d["from_hex"] = udfDef{n: 1, fn: decFn(hex.DecodeString)}
	d["b64enc"] = udfDef{n: 1, fn: encFn(func(b []byte) string { return base64.StdEncoding.EncodeToString(b) })}
	d["b64dec"] = udfDef{n: 1, fn: decFn(base64.StdEncoding.DecodeString)}
	d["md5"] = hashFn(sumMD5)
	d["sha1"] = hashFn(sumSHA1)
	d["sha256"] = hashFn(sumSHA256)
	d["sha512"] = hashFn(sumSHA512)
	d["format"] = udfDef{n: -1, fn: udfFormat}
	d["string"] = udfDef{n: -1, fn: udfStringOf}
	d["generate_uuid"] = udfDef{n: 0, rand: true, fn: udfUUID}
	d["error"] = udfDef{n: 1, fn: func(a []driver.Value) (driver.Value, error) { return nil, errBQ("%s", toStr(a[0])) }}
	d["session_user"] = udfDef{
		n:  0,
		fn: func([]driver.Value) (driver.Value, error) { return "gopherstack@localhost", nil },
	}
	d["like"] = udfDef{n: argc2, fn: udfLike}
	d["json_value"] = udfDef{n: -1, fn: udfJSONValue(true)}
	d["json_query"] = udfDef{n: -1, fn: udfJSONValue(false)}
	d["parse_json"] = udfDef{n: -1, fn: udfParseJSON}
}

func initcap(s string, a []driver.Value) string {
	delims := " \t\n\r\f\v[](){}/|\\<>!?@\"^#$&~_,.:;*%+-"
	if len(a) > 1 && a[1] != nil {
		delims = toStr(a[1])
	}

	var sb strings.Builder

	start := true

	for _, r := range s {
		if start {
			sb.WriteRune(unicode.ToUpper(r))
		} else {
			sb.WriteRune(unicode.ToLower(r))
		}

		start = strings.ContainsRune(delims, r)
	}

	return sb.String()
}

func reverseRunes(s string) string {
	r := []rune(s)
	for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}

	return string(r)
}

func lengthFn(chars bool) udfDef {
	return udfDef{n: 1, fn: func(a []driver.Value) (driver.Value, error) {
		switch t := a[0].(type) {
		case nil:
			return nil, nil
		case []byte:
			return int64(len(t)), nil
		}

		s := toStr(a[0])
		if chars {
			return int64(utf8.RuneCountInString(s)), nil
		}

		return int64(len(s)), nil
	}}
}

func udfChr(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	n, err := toInt(a[0])
	if err != nil {
		return nil, err
	}

	if n == 0 {
		return "", nil
	}

	if n < 0 || n > unicode.MaxRune {
		return nil, errBQ("invalid code point %d", n)
	}

	return string(rune(n)), nil
}

func udfRepeat(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	n, err := toSmallInt(a[1])
	if err != nil {
		return nil, err
	}

	if n < 0 || n*len(toStr(a[0])) > 1<<24 {
		return nil, errBQ("invalid REPEAT count %d", n)
	}

	return strings.Repeat(toStr(a[0]), n), nil
}

func udfReplace(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	if toStr(a[1]) == "" {
		return toStr(a[0]), nil
	}

	return strings.ReplaceAll(toStr(a[0]), toStr(a[1]), toStr(a[2])), nil
}

func trimFn(
	trimFunc func(string, func(rune) bool) string, trimSet func(string, string) string,
) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if anyNull(a) {
			return nil, nil
		}

		if len(a) > 1 {
			return trimSet(toStr(a[0]), toStr(a[1])), nil
		}

		return trimFunc(toStr(a[0]), unicode.IsSpace), nil
	}
}

func padFn(left bool) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if anyNull(a) {
			return nil, nil
		}

		n, err := toInt(a[1])
		if err != nil {
			return nil, err
		}

		if n < 0 || n > 1<<24 {
			return nil, errBQ("invalid pad length %d", n)
		}

		pad := " "
		if len(a) > argc2 {
			pad = toStr(a[2])
		}

		return padString(toStr(a[0]), int(n), pad, left), nil
	}
}

func padString(s string, n int, pad string, left bool) string {
	r := []rune(s)
	if len(r) >= n {
		return string(r[:n])
	}

	pr := []rune(pad)
	if len(pr) == 0 {
		return s
	}

	fill := make([]rune, 0, n-len(r))
	for i := 0; len(fill) < n-len(r); i++ {
		fill = append(fill, pr[i%len(pr)])
	}

	if left {
		return string(fill) + s
	}

	return s + string(fill)
}

func sideFn(left bool) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if anyNull(a) {
			return nil, nil
		}

		n, err := toSmallInt(a[1])
		if err != nil || n < 0 {
			return nil, errBQ("invalid length argument")
		}

		r := []rune(toStr(a[0]))
		if n >= len(r) {
			return string(r), nil
		}

		if left {
			return string(r[:n]), nil
		}

		return string(r[len(r)-n:]), nil
	}
}

func udfSubstr(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	pos, err := toInt(a[1])
	if err != nil {
		return nil, err
	}

	r := []rune(toStr(a[0]))
	n := int64(len(r))

	switch {
	case pos < 0:
		pos = max(n+pos, 0)
	case pos > 0:
		pos--
	}

	if pos > n {
		pos = n
	}

	end := n

	if len(a) > argc2 {
		l, lerr := toInt(a[2])
		if lerr != nil {
			return nil, lerr
		}

		if l < 0 {
			return nil, errBQ("third argument of SUBSTR must not be negative")
		}

		end = min(pos+l, n)
	}

	return string(r[pos:end]), nil
}

func udfConcat(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	var sb strings.Builder
	for _, v := range a {
		sb.WriteString(toStr(v))
	}

	return sb.String(), nil
}

func udfStrpos(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	s, sub := toStr(a[0]), toStr(a[1])
	before, _, found := strings.Cut(s, sub)
	if !found {
		return int64(0), nil
	}

	return int64(utf8.RuneCountInString(before) + 1), nil
}

func predFn(f func(s, sub string) bool) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if anyNull(a) {
			return nil, nil
		}

		return boolVal(f(toStr(a[0]), toStr(a[1]))), nil
	}
}

func udfSplit(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	delim := ","
	if len(a) > 1 {
		if a[1] == nil {
			return nil, nil //nolint:nilnil // NULL in, NULL out
		}

		delim = toStr(a[1])
	}

	s := toStr(a[0])

	var parts []string

	if delim == "" {
		for _, r := range s {
			parts = append(parts, string(r))
		}
	} else {
		parts = strings.Split(s, delim)
	}

	if s == "" {
		parts = []string{""}
	}

	b, _ := json.Marshal(parts)

	return string(b), nil
}

func bytesOf(v driver.Value) []byte {
	if b, ok := v.([]byte); ok {
		return b
	}

	return []byte(toStr(v))
}

func encFn(enc func([]byte) string) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if a[0] == nil {
			return nil, nil
		}

		return enc(bytesOf(a[0])), nil
	}
}

func decFn(dec func(string) ([]byte, error)) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if a[0] == nil {
			return nil, nil
		}

		b, err := dec(toStr(a[0]))
		if err != nil {
			return nil, errBQ("invalid encoded input")
		}

		return b, nil
	}
}

func hashFn(h func([]byte) []byte) udfDef {
	return udfDef{n: 1, fn: func(a []driver.Value) (driver.Value, error) {
		if a[0] == nil {
			return nil, nil
		}

		return h(bytesOf(a[0])), nil
	}}
}

// udfLike implements SQL LIKE with % and _ and a case-sensitive match.
func udfLike(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	return boolVal(likeMatch([]rune(toStr(a[0])), []rune(toStr(a[1])))), nil
}

func likeMatch(s, p []rune) bool {
	si, pi := 0, 0
	star, mark := -1, 0

	for si < len(s) {
		switch {
		case pi < len(p) && p[pi] == '%':
			star, mark = pi, si
			pi++
		case pi < len(p) && likeRuneMatches(p, pi, s[si]):
			if p[pi] == '\\' && pi+1 < len(p) {
				pi++
			}

			pi++
			si++
		case star >= 0:
			mark++
			si, pi = mark, star+1
		default:
			return false
		}
	}

	for pi < len(p) && p[pi] == '%' {
		pi++
	}

	return pi == len(p)
}

// likeRuneMatches reports whether the pattern element at pi matches r; a backslash escapes the next element.
func likeRuneMatches(p []rune, pi int, r rune) bool {
	switch {
	case p[pi] == '_':
		return true
	case p[pi] == '\\' && pi+1 < len(p):
		return p[pi+1] == r
	}

	return p[pi] == r
}

func sumMD5(b []byte) []byte {
	s := md5.Sum(b) //nolint:gosec // BigQuery MD5()

	return s[:]
}

func sumSHA1(b []byte) []byte {
	s := sha1.Sum(b) //nolint:gosec // BigQuery SHA1()

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
