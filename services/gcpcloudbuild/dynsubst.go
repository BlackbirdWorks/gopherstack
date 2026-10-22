package gcpcloudbuild

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
)

const maxDynamicValue = 64 << 10

var (
	dynRef = regexp.MustCompile(`\$\$|\$\{(#?[A-Z_][A-Z0-9_]*)([^}]*)\}|\$([A-Z_][A-Z0-9_]*)`)

	errDynRequired = errors.New("parameter is empty or not set")
	errDynOp       = errors.New("unsupported parameter expansion")
	errDynTooLong  = errors.New("value is too long for a bash-style substitution")
)

// globRegexp turns a bash glob (* and ?) into an anchored regular expression.
func globRegexp(glob string) *regexp.Regexp {
	var sb strings.Builder

	sb.WriteString("^")

	for _, c := range glob {
		switch c {
		case '*':
			sb.WriteString(".*")
		case '?':
			sb.WriteString(".")
		default:
			sb.WriteString(regexp.QuoteMeta(string(c)))
		}
	}

	sb.WriteString("$")

	return regexp.MustCompile("(?s)" + sb.String())
}

// dynamicExpand applies the bash parameter expansion op (the text after the name) to the value of a variable.
func dynamicExpand(val string, set bool, op string) (string, error) {
	if len(val) > maxDynamicValue {
		return "", errDynTooLong
	}

	switch {
	case op == "":
		return val, nil
	case strings.HasPrefix(op, ":-"), strings.HasPrefix(op, ":="):
		return orDefault(val, op[2:]), nil
	case strings.HasPrefix(op, "-"), strings.HasPrefix(op, "="):
		if set {
			return val, nil
		}

		return op[1:], nil
	case strings.HasPrefix(op, ":+"):
		if val == "" {
			return "", nil
		}

		return op[2:], nil
	case strings.HasPrefix(op, ":?"):
		if val == "" {
			return "", errDynRequired
		}

		return val, nil
	}

	return dynamicTransform(val, op)
}

func orDefault(val, def string) string {
	if val == "" {
		return def
	}

	return val
}

func dynamicTransform(val, op string) (string, error) {
	switch {
	case strings.HasPrefix(op, "//"):
		return replaceGlob(val, op[2:], true), nil
	case strings.HasPrefix(op, "/"):
		return replaceGlob(val, op[1:], false), nil
	case strings.HasPrefix(op, "##"):
		return trimGlob(val, op[2:], true, true), nil
	case strings.HasPrefix(op, "#"):
		return trimGlob(val, op[1:], true, false), nil
	case strings.HasPrefix(op, "%%"):
		return trimGlob(val, op[2:], false, true), nil
	case strings.HasPrefix(op, "%"):
		return trimGlob(val, op[1:], false, false), nil
	case strings.HasPrefix(op, ":"):
		return substring(val, op[1:])
	}

	return changeCase(val, op)
}

func changeCase(val, op string) (string, error) {
	switch op {
	case "^^":
		return strings.ToUpper(val), nil
	case ",,":
		return strings.ToLower(val), nil
	case "^", ",":
		if val == "" {
			return val, nil
		}

		if op == "^" {
			return strings.ToUpper(val[:1]) + val[1:], nil
		}

		return strings.ToLower(val[:1]) + val[1:], nil
	}

	return "", errDynOp
}

// substring implements ${v:offset} and ${v:offset:length}; negative offsets count from the end.
func substring(val, spec string) (string, error) {
	offS, lenS, hasLen := strings.Cut(spec, ":")

	off, err := strconv.Atoi(strings.TrimSpace(offS))
	if err != nil {
		return "", errDynOp
	}

	if off < 0 {
		off = max(len(val)+off, 0)
	}

	off = min(off, len(val))
	end := len(val)

	if hasLen {
		n, nErr := strconv.Atoi(strings.TrimSpace(lenS))
		if nErr != nil {
			return "", errDynOp
		}

		if n < 0 {
			end = max(len(val)+n, off)
		} else {
			end = min(off+n, len(val))
		}
	}

	return val[off:end], nil
}

func trimGlob(val, glob string, prefix, longest bool) string {
	re := globRegexp(glob)

	for k := range len(val) + 1 {
		n := k
		if longest {
			n = len(val) - k
		}

		if prefix && re.MatchString(val[:n]) {
			return val[n:]
		}

		if !prefix && re.MatchString(val[len(val)-n:]) {
			return val[:len(val)-n]
		}
	}

	return val
}

func replaceGlob(val, spec string, all bool) string {
	glob, rep, _ := strings.Cut(spec, "/")
	if glob == "" {
		return val
	}

	re := globRegexp(glob)

	var sb strings.Builder

	for i := 0; i < len(val); {
		n := longestMatch(re, val[i:])
		if n == 0 {
			sb.WriteByte(val[i])

			i++

			continue
		}

		sb.WriteString(rep)

		i += n

		if !all {
			sb.WriteString(val[i:])

			return sb.String()
		}
	}

	return sb.String()
}

func longestMatch(re *regexp.Regexp, s string) int {
	for n := len(s); n > 0; n-- {
		if re.MatchString(s[:n]) {
			return n
		}
	}

	return 0
}

// expandDynamic expands $VAR, ${VAR} and bash-style ${VAR<op>} references.
func (s *substituter) expandDynamic(in string) string {
	return dynRef.ReplaceAllStringFunc(in, func(m string) string {
		if m == "$$" {
			return "$"
		}

		sub := dynRef.FindStringSubmatch(m)
		if sub[3] != "" {
			return s.lookup(sub[3], m)
		}

		name, op := sub[1], sub[2]
		if strings.HasPrefix(name, "#") {
			return strconv.Itoa(len(s.vals[name[1:]]))
		}

		val, set := s.vals[name]
		if !set && !strings.ContainsAny(op[:min(1, len(op))], ":-=") {
			return s.lookup(name, m)
		}

		out, err := dynamicExpand(val, set, op)
		if err != nil {
			s.fail(name, err)

			return m
		}

		return out
	})
}

func (s *substituter) lookup(name, raw string) string {
	if v, ok := s.vals[name]; ok {
		return v
	}

	if s.loose && strings.HasPrefix(name, "_") {
		return ""
	}

	s.fail(name, nil)

	return raw
}

func (s *substituter) fail(name string, err error) {
	if s.err != nil {
		return
	}

	if err == nil {
		s.err = errInvalid("key in the template %q is not matched in the substitutions map.", name)

		return
	}

	s.err = errInvalid("invalid substitution of %q: %v.", name, err)
}
