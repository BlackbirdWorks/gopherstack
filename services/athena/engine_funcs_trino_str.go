package athena

import (
	"database/sql/driver"
	"fmt"
	"net/url"
	"strings"
)

const urlSafeMarks = ".-*_"

func udfEndsWith(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, sok := asString(args[0])
	p, pok := asString(args[1])

	if !sok || !pok {
		return sqlNull(), nil
	}

	return boolInt(strings.HasSuffix(s, p)), nil
}

func udfTranslate(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, sok := asString(args[0])
	from, fok := asString(args[1])
	to, tok := asString(args[argsTwo])

	if !sok || !fok || !tok {
		return sqlNull(), nil
	}

	toRunes := []rune(to)
	mapping := map[rune]int{}

	for i, r := range []rune(from) {
		if _, dup := mapping[r]; !dup {
			mapping[r] = i
		}
	}

	var b strings.Builder

	for _, r := range s {
		i, found := mapping[r]

		switch {
		case !found:
			b.WriteRune(r)
		case i < len(toRunes):
			b.WriteRune(toRunes[i])
		}
	}

	return b.String(), nil
}

func isURLPlain(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
		strings.IndexByte(urlSafeMarks, c) >= 0
}

func udfURLEncode(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, ok := asString(args[0])
	if !ok {
		return sqlNull(), nil
	}

	var b strings.Builder

	for i := range len(s) {
		switch c := s[i]; {
		case isURLPlain(c):
			b.WriteByte(c)
		case c == ' ':
			b.WriteByte('+')
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}

	return b.String(), nil
}

func udfURLDecode(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, ok := asString(args[0])
	if !ok {
		return sqlNull(), nil
	}

	out, err := url.QueryUnescape(s)
	if err != nil {
		return nil, errArg("url_decode: %s", err)
	}

	return out, nil
}

func registerTrinoStringFuncs() {
	registerScalar("ends_with", argsTwo, udfEndsWith)
	registerScalar("translate", argsThree, udfTranslate)
	registerScalar("url_encode", 1, udfURLEncode)
	registerNondeterministic("url_decode", 1, udfURLDecode)
}
