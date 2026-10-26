package appsync

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"math/big"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// resolverError is an error raised from a mapping template or handler
// ($util.error, util.error, unauthorized, validation); it surfaces in the
// GraphQL errors[] array.
type resolverError struct {
	Data       any
	Info       any
	Message    string
	Type       string
	CodeErrors []codeError
}

// codeError is one EvaluateCode CodeError (types.CodeError / CodeErrorLocation).
type codeError struct {
	ErrorType string
	Value     string
	Line      int
	Column    int
	Span      int
}

func (e *resolverError) Error() string { return e.Message }

func (e *resolverError) graphql() graphqlError {
	return graphqlError{Message: e.Message, ErrorType: e.Type, Data: e.Data, ErrorInfo: e.Info}
}

const (
	errDataArg          = 2
	errInfoArg          = 3
	errTypeUnknown      = "UnknownError"
	errTypeUnauthorized = "Unauthorized"
	errTypeValidation   = "ValidationError"
)

// earlyReturnError signals #return / runtime.earlyReturnError out of a request mapping.
type earlyReturnError struct{ value any }

func (e *earlyReturnError) Error() string { return "early return" }

// utilEnv is the per-call environment behind $util / util.
type utilEnv struct {
	appendErr  func(*resolverError)
	headerSink func(name, value string)
	fns        map[string]utilFn
	field      string
	typeName   string
	authType   AuthenticationType
	ext        []map[string]any
	logs       []string
}

type utilFn func(u *utilEnv, a []any) (any, error)

func isUtilNamespace(name string) bool {
	switch name {
	case "time", "dynamodb", "transform", "str", "math", "list", "map", "xml", "http", "rds", "log":
		return true
	}

	return false
}

func (u *utilEnv) registry() map[string]utilFn {
	if u.fns != nil {
		return u.fns
	}

	u.fns = map[string]utilFn{}

	for _, m := range []map[string]utilFn{
		utilCore(), utilTypes(), utilTime(), utilDynamo(), utilTransform(), utilCollections(),
		utilExtensions(), utilExtra(), utilRDS(), utilLog(),
	} {
		maps.Copy(u.fns, m)
	}

	return u.fns
}

func (u *utilEnv) call(name string, args []any) (any, error) {
	fn, ok := u.registry()[name]
	if !ok {
		return nil, fmt.Errorf("%w: unknown util function %q", ErrValidation, name)
	}

	return fn(u, args)
}

func (u *utilEnv) names() []string {
	reg := u.registry()
	out := make([]string, 0, len(reg))

	for k := range reg {
		out = append(out, k)
	}

	return out
}

func newResolverError(a []any, def string) *resolverError {
	return &resolverError{
		Message: argStr(a, 0),
		Type:    firstNonEmpty(argStrIf(a, 1), def),
		Data:    argAt(a, errDataArg),
		Info:    argAt(a, errInfoArg),
	}
}

func argStrIf(a []any, i int) string {
	if v := argAt(a, i); v != nil {
		return vrender(v)
	}

	return ""
}

func utilCore() map[string]utilFn {
	return map[string]utilFn{
		rtMsgError: func(_ *utilEnv, a []any) (any, error) { return nil, newResolverError(a, errTypeUnknown) },
		"appendError": func(u *utilEnv, a []any) (any, error) {
			if u.appendErr != nil {
				u.appendErr(newResolverError(a, errTypeUnknown))
			}

			return "", nil
		},
		"authType": func(u *utilEnv, _ []any) (any, error) { return authTypeLabel(u.authType), nil },
		"unauthorized": func(u *utilEnv, _ []any) (any, error) {
			msg := "Unauthorized"
			if u.field != "" {
				msg = "Not Authorized to access " + u.field + " on type " + u.typeName
			}

			return nil, &resolverError{Message: msg, Type: errTypeUnauthorized}
		},
		"validate": func(_ *utilEnv, a []any) (any, error) {
			if len(a) > 0 && vtruthy(a[0]) {
				return nil, nil
			}

			if len(a) == 0 {
				return nil, newResolverError(nil, errTypeValidation)
			}

			return nil, newResolverError(a[1:], errTypeValidation)
		},
		"autoId":   func(_ *utilEnv, _ []any) (any, error) { return uuid.NewString(), nil },
		"autoUlid": func(_ *utilEnv, _ []any) (any, error) { return newULID(), nil },
		"autoKsuid": func(_ *utilEnv, _ []any) (any, error) {
			return newKSUID(), nil
		},
		"toJson": func(_ *utilEnv, a []any) (any, error) {
			b, err := json.Marshal(argAt(a, 0))

			return string(b), err
		},
		"parseJson": func(_ *utilEnv, a []any) (any, error) {
			var v any
			if err := json.Unmarshal([]byte(argStr(a, 0)), &v); err != nil {
				return nil, fmt.Errorf("%w: invalid JSON", ErrValidation)
			}

			return v, nil
		},
		"qr":    func(_ *utilEnv, _ []any) (any, error) { return "", nil },
		"quiet": func(_ *utilEnv, _ []any) (any, error) { return "", nil },
		mMatches: func(_ *utilEnv, a []any) (any, error) {
			re, err := regexp.Compile(argStr(a, 0))
			if err != nil {
				return nil, fmt.Errorf("%w: invalid regular expression", ErrValidation)
			}

			return re.MatchString(argStr(a, 1)), nil
		},
		"escapeJavaScript": func(_ *utilEnv, a []any) (any, error) { return escapeJS(argStr(a, 0)), nil },
		"urlEncode": func(_ *utilEnv, a []any) (any, error) {
			return url.QueryEscape(argStr(a, 0)), nil
		},
		"urlDecode": func(_ *utilEnv, a []any) (any, error) {
			s, err := url.QueryUnescape(argStr(a, 0))
			if err != nil {
				return nil, fmt.Errorf("%w: invalid URL encoding", ErrValidation)
			}

			return s, nil
		},
		"base64Encode": func(_ *utilEnv, a []any) (any, error) {
			return base64.StdEncoding.EncodeToString([]byte(argStr(a, 0))), nil
		},
		"base64Decode": func(_ *utilEnv, a []any) (any, error) {
			b, err := base64.StdEncoding.DecodeString(argStr(a, 0))
			if err != nil {
				return nil, fmt.Errorf("%w: invalid base64", ErrValidation)
			}

			return string(b), nil
		},
	}
}

func escapeJS(s string) string {
	var b strings.Builder

	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\'':
			b.WriteString(`\'`)
		case '/':
			b.WriteString(`\/`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		default:
			if r < ' ' {
				fmt.Fprintf(&b, `\u%04x`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}

	return b.String()
}

const (
	crockford    = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	ulidLen      = 26
	ulidMask     = 31
	ulidShift    = 5
	ulidTimeLen  = 6
	base62       = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	ksuidEpoch   = 1400000000
	ksuidStrLen  = 27
	ksuidPayload = 16
	bitsPerByte  = 8
)

func newULID() string {
	var raw [16]byte

	ms := time.Now().UnixMilli()
	for i := ulidTimeLen - 1; i >= 0; i-- {
		raw[i] = byte(ms)
		ms >>= 8
	}

	_, _ = rand.Read(raw[ulidTimeLen:])

	n := new(big.Int).SetBytes(raw[:])
	out := make([]byte, ulidLen)
	mask := big.NewInt(ulidMask)

	for i := ulidLen - 1; i >= 0; i-- {
		out[i] = crockford[new(big.Int).And(n, mask).Int64()]
		n.Rsh(n, ulidShift)
	}

	return string(out)
}

func newKSUID() string {
	payload := make([]byte, ksuidPayload)
	_, _ = rand.Read(payload)

	n := big.NewInt(time.Now().Unix() - ksuidEpoch)
	n.Lsh(n, ksuidPayload*bitsPerByte)
	n.Or(n, new(big.Int).SetBytes(payload))

	out := make([]byte, ksuidStrLen)
	base := big.NewInt(int64(len(base62)))
	rem := new(big.Int)

	for i := ksuidStrLen - 1; i >= 0; i-- {
		n.DivMod(n, base, rem)
		out[i] = base62[rem.Int64()]
	}

	return string(out)
}

func utilTypes() map[string]utilFn {
	return map[string]utilFn{
		"isNull":        func(_ *utilEnv, a []any) (any, error) { return argAt(a, 0) == nil, nil },
		"isNullOrEmpty": func(_ *utilEnv, a []any) (any, error) { return emptyVal(argAt(a, 0), false), nil },
		"isNullOrBlank": func(_ *utilEnv, a []any) (any, error) { return emptyVal(argAt(a, 0), true), nil },
		"defaultIfNull": func(_ *utilEnv, a []any) (any, error) {
			if argAt(a, 0) == nil {
				return argAt(a, 1), nil
			}

			return a[0], nil
		},
		"defaultIfNullOrEmpty": func(_ *utilEnv, a []any) (any, error) {
			if emptyVal(argAt(a, 0), false) {
				return argAt(a, 1), nil
			}

			return a[0], nil
		},
		"defaultIfNullOrBlank": func(_ *utilEnv, a []any) (any, error) {
			if emptyVal(argAt(a, 0), true) {
				return argAt(a, 1), nil
			}

			return a[0], nil
		},
		"isString": func(_ *utilEnv, a []any) (any, error) { return typeName(argAt(a, 0)) == "String", nil },
		"isNumber": func(_ *utilEnv, a []any) (any, error) { return typeName(argAt(a, 0)) == "Number", nil },
		"isBoolean": func(_ *utilEnv, a []any) (any, error) {
			return typeName(argAt(a, 0)) == "Boolean", nil
		},
		"isList": func(_ *utilEnv, a []any) (any, error) { return typeName(argAt(a, 0)) == "List", nil },
		"isMap":  func(_ *utilEnv, a []any) (any, error) { return typeName(argAt(a, 0)) == "Map", nil },
		"typeOf": func(_ *utilEnv, a []any) (any, error) { return typeName(argAt(a, 0)), nil },
	}
}

func emptyVal(v any, blank bool) bool {
	switch t := v.(type) {
	case nil:
		return true
	case string:
		if blank {
			t = strings.TrimSpace(t)
		}

		return t == ""
	}

	return false
}

func typeName(v any) string {
	switch v.(type) {
	case nil:
		return "Null"
	case string:
		return "String"
	case bool:
		return "Boolean"
	case map[string]any:
		return "Map"
	case []any, *vlist:
		return "List"
	}

	if _, _, ok := toNum(v); ok {
		return "Number"
	}

	return "Object"
}

func utilExtensions() map[string]utilFn {
	rec := func(kind string) utilFn {
		return func(u *utilEnv, a []any) (any, error) {
			u.ext = append(u.ext, map[string]any{"kind": kind, "args": a})

			return "", nil
		}
	}

	return map[string]utilFn{
		"extensions.evictFromApiCache":                 rec("evictFromApiCache"),
		"extensions.invalidateSubscriptions":           rec("invalidateSubscriptions"),
		"extensions.setSubscriptionFilter":             rec("setSubscriptionFilter"),
		"extensions.setSubscriptionInvalidationFilter": rec("setSubscriptionInvalidationFilter"),
	}
}

func fmtNum(v any) string {
	switch t := v.(type) {
	case int64:
		return strconv.FormatInt(t, 10)
	case int:
		return strconv.Itoa(t)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case json.Number:
		return t.String()
	}

	return vrender(v)
}
