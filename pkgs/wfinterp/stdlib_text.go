package wfinterp

import (
	"crypto/hmac"
	"crypto/md5"  //nolint:gosec // MD5 is a documented hash.compute_checksum algorithm
	"crypto/sha1" //nolint:gosec // SHA1 is a documented hash.compute_checksum algorithm
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"hash"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

func (e *Engine) addText() {
	e.reg(
		"text.substring",
		[]string{paramSource, "start", "end"},
		arity3,
		fnSubstring,
	)
	e.reg(
		"text.split",
		[]string{paramSource, "separator"},
		arity2,
		fnSplit,
	)
	e.reg(
		"text.find_all",
		[]string{paramSource, "substr"},
		arity2,
		fnFindAll,
	)
	e.reg(
		"text.find_all_regex",
		[]string{paramSource, paramRegexp},
		arity2,
		fnFindAllRegex,
	)
	e.reg(
		"text.match_regex",
		[]string{paramSource, paramRegexp},
		arity2,
		fnMatchRegex,
	)
	e.reg(
		"text.replace_all",
		[]string{paramSource, "substr", "repl"},
		arity3,
		fnReplaceAll,
	)
	e.reg(
		"text.replace_all_regex",
		[]string{paramSource, paramRegexp, "replacement"},
		arity3,
		fnReplaceAllRegex,
	)
	e.reg("text.to_lower", []string{paramSource}, 1, strFn("text.to_lower", strings.ToLower))
	e.reg("text.to_upper", []string{paramSource}, 1, strFn("text.to_upper", strings.ToUpper))
	e.reg("text.url_encode", []string{paramSource}, 1, strFn("text.url_encode", func(s string) string {
		return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
	}))
	e.reg("text.url_encode_plus", []string{paramSource}, 1, strFn("text.url_encode_plus", url.QueryEscape))
	e.reg("text.url_decode", []string{paramSource}, 1, func(_ *callCtx, a []any) (any, error) {
		s, err := argString("text.url_decode", paramSource, a[0])
		if err != nil {
			return nil, err
		}

		out, derr := url.QueryUnescape(s)
		if derr != nil {
			return nil, valueErr("text.url_decode: %v", derr)
		}

		return out, nil
	})
	e.reg("text.encode", []string{"text", "charset"}, 1, func(_ *callCtx, a []any) (any, error) {
		s, err := argString("text.encode", "text", a[0])
		if err != nil {
			return nil, err
		}

		return []byte(s), checkCharset("text.encode", a[1])
	})
	e.reg("text.decode", []string{paramData, "charset"}, 1, func(_ *callCtx, a []any) (any, error) {
		b, err := argBytes("text.decode", paramData, a[0])
		if err != nil {
			return nil, err
		}

		return strings.ToValidUTF8(string(b), "�"), checkCharset("text.decode", a[1])
	})
}

func checkCharset(fn string, v any) error {
	if v == nil {
		return nil
	}

	s, err := argString(fn, "charset", v)
	if err != nil {
		return err
	}

	switch strings.ToUpper(s) {
	case "UTF-8", "UTF8", "US-ASCII", "ASCII":
		return nil
	default:
		return valueErr("%s: unsupported charset %q", fn, s)
	}
}

func strFn(name string, f func(string) string) func(*callCtx, []any) (any, error) {
	return func(_ *callCtx, a []any) (any, error) {
		s, err := argString(name, paramSource, a[0])
		if err != nil {
			return nil, err
		}

		return f(s), nil
	}
}

func twoStrings(fn string, a []any, n1, n2 string) (string, string, error) {
	s1, err := argString(fn, n1, a[0])
	if err != nil {
		return "", "", err
	}

	s2, err := argString(fn, n2, a[1])

	return s1, s2, err
}

func fnSubstring(_ *callCtx, a []any) (any, error) {
	src, err := argString("text.substring", paramSource, a[0])
	if err != nil {
		return nil, err
	}

	start, err := argInt("text.substring", "start", a[1])
	if err != nil {
		return nil, err
	}

	end, err := argInt("text.substring", "end", a[2])
	if err != nil {
		return nil, err
	}

	rs := []rune(src)
	n := int64(len(rs))
	clamp := func(i int64) int64 {
		if i < 0 {
			i += n
		}

		return max(0, min(i, n))
	}
	s, e := clamp(start), clamp(end)

	if s >= e {
		return "", nil
	}

	return string(rs[s:e]), nil
}

func fnSplit(_ *callCtx, a []any) (any, error) {
	src, sep, err := twoStrings("text.split", a, paramSource, "separator")
	if err != nil {
		return nil, err
	}

	if sep == "" {
		return nil, valueErr("text.split: separator must not be empty")
	}

	parts := strings.Split(src, sep)
	out := make([]any, len(parts))

	for i, p := range parts {
		out[i] = p
	}

	return out, nil
}

func fnFindAll(_ *callCtx, a []any) (any, error) {
	src, sub, err := twoStrings("text.find_all", a, paramSource, "substr")
	if err != nil {
		return nil, err
	}

	out := []any{}
	if sub == "" {
		return out, nil
	}

	for i := 0; i <= len(src)-len(sub); {
		j := strings.Index(src[i:], sub)
		if j < 0 {
			break
		}

		out = append(out, int64(utf8.RuneCountInString(src[:i+j])))
		i += j + len(sub)
	}

	return out, nil
}

func compileRegex(fn, expr string) (*regexp.Regexp, error) {
	re, err := regexp.Compile(expr)
	if err != nil {
		return nil, valueErr("%s: invalid regular expression: %v", fn, err)
	}

	return re, nil
}

func fnFindAllRegex(_ *callCtx, a []any) (any, error) {
	src, expr, err := twoStrings("text.find_all_regex", a, paramSource, paramRegexp)
	if err != nil {
		return nil, err
	}

	re, err := compileRegex("text.find_all_regex", expr)
	if err != nil {
		return nil, err
	}

	out := []any{}

	for _, loc := range re.FindAllStringIndex(src, -1) {
		out = append(out, map[string]any{
			"index": int64(utf8.RuneCountInString(src[:loc[0]])),
			"match": src[loc[0]:loc[1]],
		})
	}

	return out, nil
}

func fnMatchRegex(_ *callCtx, a []any) (any, error) {
	src, expr, err := twoStrings("text.match_regex", a, paramSource, paramRegexp)
	if err != nil {
		return nil, err
	}

	re, err := compileRegex("text.match_regex", expr)
	if err != nil {
		return nil, err
	}

	return re.MatchString(src), nil
}

func fnReplaceAll(_ *callCtx, a []any) (any, error) {
	src, err := argString("text.replace_all", paramSource, a[0])
	if err != nil {
		return nil, err
	}

	sub, repl, err := twoStrings("text.replace_all", a[1:], "substr", "repl")
	if err != nil {
		return nil, err
	}

	return strings.ReplaceAll(src, sub, repl), nil
}

var backrefRe = regexp.MustCompile(`\\(\d)`)

func fnReplaceAllRegex(_ *callCtx, a []any) (any, error) {
	src, expr, err := twoStrings("text.replace_all_regex", a, paramSource, paramRegexp)
	if err != nil {
		return nil, err
	}

	repl, err := argString("text.replace_all_regex", "replacement", a[2])
	if err != nil {
		return nil, err
	}

	re, err := compileRegex("text.replace_all_regex", expr)
	if err != nil {
		return nil, err
	}

	return re.ReplaceAllString(src, backrefRe.ReplaceAllString(repl, "$${$1}")), nil
}

func (e *Engine) addSys() {
	e.reg("sys.now", nil, 0, func(c *callCtx, _ []any) (any, error) {
		return float64(c.clock().Now().UnixNano()) / 1e9, nil //nolint:mnd // ns to s
	})
	e.reg("uuid.generate", nil, 0, func(*callCtx, []any) (any, error) { return uuid.NewString(), nil })
	e.reg("time.format", []string{"seconds", "timezone"}, 1, fnTimeFormat)
	e.reg("time.parse", []string{"value"}, 1, func(_ *callCtx, a []any) (any, error) {
		s, err := argString("time.parse", "value", a[0])
		if err != nil {
			return nil, err
		}

		t, perr := time.Parse(time.RFC3339Nano, s)
		if perr != nil {
			return nil, valueErr("time.parse: %q is not an RFC 3339 timestamp", s)
		}

		return float64(t.UnixNano()) / 1e9, nil //nolint:mnd // ns to s
	})
	e.reg("base64.encode", []string{paramData, "padding"}, 1, func(_ *callCtx, a []any) (any, error) {
		b, err := argBytes("base64.encode", paramData, a[0])
		if err != nil {
			return nil, err
		}

		if p, ok := a[1].(bool); ok && !p {
			return base64.RawStdEncoding.EncodeToString(b), nil
		}

		return base64.StdEncoding.EncodeToString(b), nil
	})
	e.reg("base64.decode", []string{paramData}, 1, fnBase64Decode)
	e.reg(
		"hash.compute_checksum",
		[]string{paramData, "algorithm"},
		arity2,
		func(_ *callCtx, a []any) (any, error) {
			data, err := argBytes("hash.compute_checksum", paramData, a[0])
			if err != nil {
				return nil, err
			}

			newHash, err := hashFor("hash.compute_checksum", a[1])
			if err != nil {
				return nil, err
			}

			h := newHash()
			h.Write(data)

			return h.Sum(nil), nil
		},
	)
	e.reg(
		"hash.compute_hmac",
		[]string{"key", paramData, "algorithm"},
		arity3,
		func(_ *callCtx, a []any) (any, error) {
			key, err := argBytes("hash.compute_hmac", "key", a[0])
			if err != nil {
				return nil, err
			}

			data, err := argBytes("hash.compute_hmac", paramData, a[1])
			if err != nil {
				return nil, err
			}

			newHash, err := hashFor("hash.compute_hmac", a[2])
			if err != nil {
				return nil, err
			}

			m := hmac.New(newHash, key)
			m.Write(data)

			return m.Sum(nil), nil
		},
	)
}

func (c *callCtx) clock() Clock {
	if c == nil || c.x == nil {
		return RealClock{}
	}

	return c.x.host.clock()
}

func fnTimeFormat(_ *callCtx, a []any) (any, error) {
	secs, err := argNumber("time.format", "seconds", a[0])
	if err != nil {
		return nil, err
	}

	loc := time.UTC

	if a[1] != nil {
		name, serr := argString("time.format", "timezone", a[1])
		if serr != nil {
			return nil, serr
		}

		if name != "" && name != "GMT" {
			l, lerr := time.LoadLocation(name)
			if lerr != nil {
				return nil, valueErr("time.format: unknown timezone %q", name)
			}

			loc = l
		}
	}

	t := time.Unix(0, int64(secs*1e9)).In(loc) //nolint:mnd // s to ns

	return t.Format("2006-01-02T15:04:05.000000Z07:00"), nil
}

func fnBase64Decode(_ *callCtx, a []any) (any, error) {
	s, err := argString("base64.decode", paramData, a[0])
	if err != nil {
		return nil, err
	}

	for _, enc := range []*base64.Encoding{
		base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding,
	} {
		if b, derr := enc.DecodeString(s); derr == nil {
			return b, nil
		}
	}

	return nil, valueErr("base64.decode: invalid base64 input")
}

func hashFor(fn string, v any) (func() hash.Hash, error) {
	alg, err := argString(fn, "algorithm", v)
	if err != nil {
		return nil, err
	}

	switch alg {
	case "MD5":
		return md5.New, nil
	case "SHA1":
		return sha1.New, nil
	case "SHA224":
		return sha256.New224, nil
	case "SHA256":
		return sha256.New, nil
	case "SHA384":
		return sha512.New384, nil
	case "SHA512":
		return sha512.New, nil
	default:
		return nil, valueErr("%s: unsupported algorithm %q", fn, alg)
	}
}
