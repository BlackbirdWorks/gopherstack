package bqsql

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	sqlitedriver "modernc.org/sqlite"
)

const (
	argc2 = 2
	argc3 = 3
	argc4 = 4
)

type udfFn func(args []driver.Value) (driver.Value, error)

type udfDef struct {
	fn   udfFn
	n    int32
	rand bool
}

//nolint:gochecknoglobals // the driver's function registry is process-wide
var registerOnce sync.Once

var errBQBase = errors.New("bq")

func errBQ(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errBQBase, fmt.Sprintf(format, args...))
}

func registerUDFs() {
	registerOnce.Do(func() {
		defs := map[string]udfDef{}

		addMathUDFs(defs)
		addStringUDFs(defs)
		addRegexUDFs(defs)
		addTimeUDFs(defs)
		addArrayUDFs(defs)
		addCastUDFs(defs)

		for name, d := range defs {
			register("bq_"+name, d, d.fn)
			register("bq_safe_"+name, d, safeWrap(d.fn))
		}

		registerAggregates()
	})
}

func register(name string, d udfDef, fn udfFn) {
	impl := func(_ *sqlitedriver.FunctionContext, args []driver.Value) (driver.Value, error) { return fn(args) }
	if d.rand {
		sqlitedriver.MustRegisterScalarFunction(name, d.n, impl)

		return
	}

	sqlitedriver.MustRegisterDeterministicScalarFunction(name, d.n, impl)
}

func safeWrap(fn udfFn) udfFn {
	return func(args []driver.Value) (driver.Value, error) {
		v, err := fn(args)
		if err != nil {
			return nil, nil //nolint:nilerr // SAFE. functions turn errors into NULL
		}

		return v, nil
	}
}

func anyNull(args []driver.Value) bool {
	for _, a := range args {
		if a == nil {
			return true
		}
	}

	return false
}

func toStr(v driver.Value) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case int64:
		return strconv.FormatInt(t, decimalBase)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, bitSize)
	case nil:
		return ""
	}

	return fmt.Sprint(v)
}

func toFloat(v driver.Value) (float64, error) {
	switch t := v.(type) {
	case int64:
		return float64(t), nil
	case float64:
		return t, nil
	case string:
		f, err := parseFloatText(strings.TrimSpace(t))
		if err != nil {
			return 0, errBQ("Bad double value: %s", t)
		}

		return f, nil
	case []byte:
		return toFloat(string(t))
	}

	return 0, errBQ("expected a number")
}

func toInt(v driver.Value) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case float64:
		return int64(t), nil
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(t), decimalBase, bitSize)
		if err != nil {
			return 0, errBQ("Bad int64 value: %s", t)
		}

		return n, nil
	case []byte:
		return toInt(string(t))
	}

	return 0, errBQ("expected an integer")
}

func boolVal(b bool) driver.Value {
	if b {
		return int64(1)
	}

	return int64(0)
}

func isFloatVal(v driver.Value) bool {
	_, ok := v.(float64)

	return ok
}
