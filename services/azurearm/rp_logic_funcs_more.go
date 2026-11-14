package azurearm

import (
	"crypto/rand"
	"encoding/base64"
	"math/big"
	"net/url"
	"strings"
)

const logicDataURIPrefix = "data:"

func addLogicMoreFuncs(m map[string]logicFunc) {
	m["decodebase64"] = eagerLogic("decodeBase64", 1, 1, logicBase64Decode)
	m["decodeuricomponent"] = eagerLogic("decodeUriComponent", 1, 1, logicURIDecode)
	m["encodeuricomponent"] = m["uricomponent"]
	m["datauri"] = eagerLogic("dataUri", 1, 1, func(a []any) (any, error) {
		enc := base64.StdEncoding.EncodeToString([]byte(logicString(a[0])))

		return "data:text/plain;charset=utf-8;base64," + enc, nil
	})
	m["datauritostring"] = eagerLogic("dataUriToString", 1, 1, logicDataURIToString)
	m["rand"] = eagerLogic("rand", argsTwo, argsTwo, logicRand)
	m["getfuturetime"] = relativeTimeFunc("getFutureTime", 1)
	m["getpasttime"] = relativeTimeFunc("getPastTime", -1)

	for name, f := range map[string]func(ts logicTS) int64{
		"dayofmonth": func(ts logicTS) int64 { return int64(ts.t.Day()) },
		"dayofweek":  func(ts logicTS) int64 { return int64(ts.t.Weekday()) },
		"dayofyear":  func(ts logicTS) int64 { return int64(ts.t.YearDay()) },
	} {
		m[name] = eagerLogic(name, 1, 1, func(a []any) (any, error) {
			ts, err := parseLogicTime(logicString(a[0]))
			if err != nil {
				return nil, err
			}

			return f(ts), nil
		})
	}
}

func relativeTimeFunc(name string, sign int64) logicFunc {
	return func(c *logicEvalCtx, args []*logicExpr) (any, error) {
		if len(args) < argsTwo || len(args) > argsThree {
			return nil, exprErr("function '%s' has an invalid number of arguments: %d", name, len(args))
		}

		vals := make([]any, len(args))

		for i, n := range args {
			v, err := c.eval(n)
			if err != nil {
				return nil, err
			}

			vals[i] = v
		}

		now, err := formatLogicTime(logicTS{t: c.now().UTC(), hasZone: true}, logicRoundTrip, "")
		if err != nil {
			return nil, err
		}

		return logicAddTime(now, vals[0], strings.ToLower(logicString(vals[1])), sign, optFormat(vals, argsTwo))
	}
}

func logicDataURIToString(a []any) (any, error) {
	s := logicString(a[0])
	if len(s) < len(logicDataURIPrefix) || !strings.EqualFold(s[:len(logicDataURIPrefix)], logicDataURIPrefix) {
		return nil, exprErr("the value is not a valid data URI")
	}

	meta, payload, ok := strings.Cut(s[len(logicDataURIPrefix):], ",")
	if !ok {
		return nil, exprErr("the value is not a valid data URI")
	}

	if strings.HasSuffix(strings.ToLower(meta), ";base64") {
		b, err := base64.StdEncoding.DecodeString(payload)
		if err != nil {
			return nil, exprErr("the data URI payload is not valid base64")
		}

		return string(b), nil
	}

	out, err := url.PathUnescape(payload)
	if err != nil {
		return nil, exprErr("the data URI payload is not valid")
	}

	return out, nil
}

// logicRand returns an integer in [min, max): the upper bound is exclusive per the rand function reference.
func logicRand(a []any) (any, error) {
	lo, ok1 := logicInt(a[0])
	hi, ok2 := logicInt(a[1])

	if !ok1 || !ok2 {
		return nil, exprErr("the arguments of function 'rand' must be integers")
	}

	if lo >= hi {
		return nil, exprErr("the minimum value must be less than the maximum value in function 'rand'")
	}

	span := new(big.Int).Sub(big.NewInt(hi), big.NewInt(lo))

	n, err := rand.Int(rand.Reader, span)
	if err != nil {
		return nil, exprErr("function 'rand' failed: %v", err)
	}

	return n.Add(n, big.NewInt(lo)).Int64(), nil
}
