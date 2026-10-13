package sts

import (
	"bytes"
	"encoding/base64"
	"net"
	"net/http"
	"strconv"
	"strings"
)

const condKeySourceIP = "aws:sourceip"

// remoteIP returns the host part of the request's peer address.
func remoteIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return ""
	}

	return host
}

// withSourceIP adds aws:SourceIp to ctx when ip is a parseable address.
func withSourceIP(ctx map[string]string, ip string) map[string]string {
	if net.ParseIP(ip) != nil {
		ctx[condKeySourceIP] = ip
	}

	return ctx
}

// lookupConditionOperator resolves a normalized operator, including the
// ForAllValues:/ForAnyValue: set qualifiers, to its matcher.
func lookupConditionOperator(normOp string) (func(want []string, actual string) bool, bool) {
	if base, ok := strings.CutPrefix(normOp, "forallvalues:"); ok {
		fn, found := lookupBaseOperator(base)
		if !found {
			return nil, false
		}

		return func(want []string, actual string) bool {
			for _, v := range splitSet(actual) {
				if !fn(want, v) {
					return false
				}
			}

			return true
		}, true
	}

	if base, ok := strings.CutPrefix(normOp, "foranyvalue:"); ok {
		fn, found := lookupBaseOperator(base)
		if !found {
			return nil, false
		}

		return func(want []string, actual string) bool {
			for _, v := range splitSet(actual) {
				if fn(want, v) {
					return true
				}
			}

			return false
		}, true
	}

	return lookupBaseOperator(normOp)
}

func lookupBaseOperator(op string) (func(want []string, actual string) bool, bool) {
	if fn, ok := conditionOperatorFuncs[op]; ok {
		return fn, true
	}
	if fn, ok := numericOperatorFuncs[op]; ok {
		return numericCompare(fn), true
	}

	switch op {
	case "ipaddress":
		return anyIPMatch, true
	case "notipaddress":
		return func(want []string, actual string) bool { return !anyIPMatch(want, actual) }, true
	case "binaryequals":
		return binaryEquals, true
	}

	return nil, false
}

func splitSet(actual string) []string {
	if actual == "" {
		return nil
	}

	return strings.Split(actual, ",")
}

//nolint:gochecknoglobals // read-only dispatch table
var numericOperatorFuncs = map[string]func(a, b float64) bool{
	"numericequals":            func(a, b float64) bool { return a == b },
	"numericnotequals":         func(a, b float64) bool { return a != b },
	"numericlessthan":          func(a, b float64) bool { return a < b },
	"numericlessthanequals":    func(a, b float64) bool { return a <= b },
	"numericgreaterthan":       func(a, b float64) bool { return a > b },
	"numericgreaterthanequals": func(a, b float64) bool { return a >= b },
}

func numericCompare(cmp func(a, b float64) bool) func(want []string, actual string) bool {
	return func(want []string, actual string) bool {
		a, err := strconv.ParseFloat(actual, 64)
		if err != nil {
			return false
		}

		for _, w := range want {
			if b, errParse := strconv.ParseFloat(w, 64); errParse == nil && cmp(a, b) {
				return true
			}
		}

		return false
	}
}

// anyIPMatch reports whether actual falls in any CIDR, or equals any bare IP,
// in want. A bare address is its own /32 (/128).
func anyIPMatch(want []string, actual string) bool {
	ip := net.ParseIP(actual)
	if ip == nil {
		return false
	}

	for _, w := range want {
		if strings.Contains(w, "/") {
			if _, n, err := net.ParseCIDR(w); err == nil && n.Contains(ip) {
				return true
			}

			continue
		}

		if c := net.ParseIP(w); c != nil && c.Equal(ip) {
			return true
		}
	}

	return false
}

func binaryEquals(want []string, actual string) bool {
	a, err := base64.StdEncoding.DecodeString(actual)
	if err != nil {
		return false
	}

	for _, w := range want {
		if b, errDec := base64.StdEncoding.DecodeString(w); errDec == nil && bytes.Equal(a, b) {
			return true
		}
	}

	return false
}
