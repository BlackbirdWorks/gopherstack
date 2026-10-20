package appsync

import (
	"fmt"
	"maps"
	"regexp"
	"strings"
)

const (
	mToString   = "toString"
	mContains   = "contains"
	mBeginsWith = "beginsWith"
	mIsEmpty    = "isEmpty"
	mMatches    = "matches"
	mSet        = "set"
	mSize       = "size"
	mGet        = "get"
	mRemove     = "remove"
	mClear      = "clear"
	mEquals     = "equals"
	mIndexOf    = "indexOf"
	mReplaceAll = "replaceAll"
	argSecond   = 1
	argThird    = 2
)

func argStr(args []any, i int) string {
	if i < len(args) {
		return vrender(args[i])
	}

	return ""
}

func argInt(args []any, i, def int) int {
	if i < len(args) {
		if n, ok := toInt(args[i]); ok {
			return n
		}
	}

	return def
}

func argAt(args []any, i int) any {
	if i < len(args) {
		return args[i]
	}

	return nil
}

type javaFn[T any] func(recv T, args []any) any

// javaMethod dispatches the java.lang/java.util methods AppSync templates use.
func javaMethod(cur any, name string, args []any) (any, error) {
	switch t := cur.(type) {
	case string:
		if isRegexMethod(name) {
			return regexMethod(t, name, args)
		}

		return callJava(stringMethods(), t, name, args), nil
	case *vlist:
		return callJava(listMethods(), t, name, args), nil
	case map[string]any:
		return callJava(mapMethods(), t, name, args), nil
	}

	switch name {
	case mToString:
		return vrender(cur), nil
	case mEquals:
		return vequal(cur, argAt(args, 0)), nil
	case "intValue", rdsLongValue:
		if f, _, ok := toNum(cur); ok {
			return int64(f), nil
		}
	case "doubleValue", "floatValue":
		if f, _, ok := toNum(cur); ok {
			return f, nil
		}
	}

	return unknownMethod(), nil
}

func unknownMethod() any { return nil }

func callJava[T any](table map[string]javaFn[T], recv T, name string, args []any) any {
	if fn, ok := table[name]; ok {
		return fn(recv, args)
	}

	return unknownMethod()
}

func isRegexMethod(name string) bool {
	switch name {
	case mReplaceAll, "replaceFirst", mMatches, "split":
		return true
	}

	return false
}

func stringMethods() map[string]javaFn[string] {
	return map[string]javaFn[string]{
		"length":      func(s string, _ []any) any { return int64(len(s)) },
		mIsEmpty:      func(s string, _ []any) any { return s == "" },
		"toLowerCase": func(s string, _ []any) any { return strings.ToLower(s) },
		"toUpperCase": func(s string, _ []any) any { return strings.ToUpper(s) },
		"trim":        func(s string, _ []any) any { return strings.TrimSpace(s) },
		mToString:     func(s string, _ []any) any { return s },
		mContains:     func(s string, a []any) any { return strings.Contains(s, argStr(a, 0)) },
		"startsWith":  func(s string, a []any) any { return strings.HasPrefix(s, argStr(a, 0)) },
		"endsWith":    func(s string, a []any) any { return strings.HasSuffix(s, argStr(a, 0)) },
		mEquals:       func(s string, a []any) any { return s == argStr(a, 0) },
		"equalsIgnoreCase": func(s string, a []any) any {
			return strings.EqualFold(s, argStr(a, 0))
		},
		"concat":  func(s string, a []any) any { return s + argStr(a, 0) },
		mIndexOf:  func(s string, a []any) any { return int64(strings.Index(s, argStr(a, 0))) },
		"replace": func(s string, a []any) any { return strings.ReplaceAll(s, argStr(a, 0), argStr(a, argSecond)) },
		"substring": func(s string, a []any) any {
			lo, hi := argInt(a, 0, 0), argInt(a, argSecond, len(s))
			if lo < 0 || hi > len(s) || lo > hi {
				return unknownMethod()
			}

			return s[lo:hi]
		},
		"charAt": func(s string, a []any) any {
			i := argInt(a, 0, 0)
			if i < 0 || i >= len(s) {
				return unknownMethod()
			}

			return string(s[i])
		},
	}
}

func compileJavaRegex(s string) (*regexp.Regexp, error) {
	re, err := regexp.Compile(s)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid regular expression %q", ErrValidation, s)
	}

	return re, nil
}

func regexMethod(s, name string, args []any) (any, error) {
	re, err := compileJavaRegex(argStr(args, 0))
	if err != nil {
		return nil, err
	}

	switch name {
	case mMatches:
		loc := re.FindStringIndex(s)

		return loc != nil && loc[0] == 0 && loc[1] == len(s), nil
	case mReplaceAll:
		return re.ReplaceAllString(s, argStr(args, argSecond)), nil
	case "replaceFirst":
		done := false

		return re.ReplaceAllStringFunc(s, func(m string) string {
			if done {
				return m
			}

			done = true

			return re.ReplaceAllString(m, argStr(args, argSecond))
		}), nil
	}

	parts := re.Split(s, -1)
	items := make([]any, len(parts))

	for i, p := range parts {
		items[i] = p
	}

	return &vlist{items: items}, nil
}

func listMethods() map[string]javaFn[*vlist] {
	return map[string]javaFn[*vlist]{
		mSize:     func(l *vlist, _ []any) any { return int64(len(l.items)) },
		mIsEmpty:  func(l *vlist, _ []any) any { return len(l.items) == 0 },
		mGet:      func(l *vlist, a []any) any { return getIndex(l, argAt(a, 0)) },
		"add":     listAdd,
		"addAll":  listAddAll,
		mClear:    listClear,
		mContains: func(l *vlist, a []any) any { return listIndexOf(l, argAt(a, 0)) >= 0 },
		mIndexOf:  func(l *vlist, a []any) any { return int64(listIndexOf(l, argAt(a, 0))) },
		mRemove:   func(l *vlist, a []any) any { return listRemove(l, argAt(a, 0)) },
		mSet:      listSet,
		"subList": func(l *vlist, a []any) any {
			lo, hi := argInt(a, 0, 0), argInt(a, argSecond, len(l.items))
			if lo < 0 || hi > len(l.items) || lo > hi {
				return unknownMethod()
			}

			return &vlist{items: append([]any(nil), l.items[lo:hi]...)}
		},
		mToString: func(l *vlist, _ []any) any { return vrender(l) },
	}
}

func listAddAll(l *vlist, args []any) any {
	if o, ok := argAt(args, 0).(*vlist); ok {
		l.items = append(l.items, o.items...)

		return true
	}

	return false
}

func listAdd(l *vlist, args []any) any {
	if len(args) > argSecond {
		i := argInt(args, 0, len(l.items))
		if i < 0 || i > len(l.items) {
			return unknownMethod()
		}

		l.items = append(l.items[:i], append([]any{args[argSecond]}, l.items[i:]...)...)

		return unknownMethod()
	}

	l.items = append(l.items, argAt(args, 0))

	return true
}

func listSet(l *vlist, args []any) any {
	i := argInt(args, 0, -1)
	if i < 0 || i >= len(l.items) {
		return unknownMethod()
	}

	old := l.items[i]
	l.items[i] = argAt(args, argSecond)

	return old
}

func listIndexOf(l *vlist, v any) int {
	for i, it := range l.items {
		if vequal(it, v) {
			return i
		}
	}

	return -1
}

func listRemove(l *vlist, v any) any {
	if f, isInt, ok := toNum(v); ok && isInt {
		i := int(f)
		if i < 0 || i >= len(l.items) {
			return unknownMethod()
		}

		old := l.items[i]
		l.items = append(l.items[:i], l.items[i+1:]...)

		return old
	}

	i := listIndexOf(l, v)
	if i < 0 {
		return false
	}

	l.items = append(l.items[:i], l.items[i+1:]...)

	return true
}

func mapMethods() map[string]javaFn[map[string]any] {
	return map[string]javaFn[map[string]any]{
		mSize:    func(m map[string]any, _ []any) any { return int64(len(m)) },
		mIsEmpty: func(m map[string]any, _ []any) any { return len(m) == 0 },
		mGet:     func(m map[string]any, a []any) any { return m[argStr(a, 0)] },
		"put": func(m map[string]any, a []any) any {
			k := argStr(a, 0)
			old := m[k]
			m[k] = argAt(a, argSecond)

			return old
		},
		"putAll": func(m map[string]any, a []any) any {
			if o, ok := argAt(a, 0).(map[string]any); ok {
				maps.Copy(m, o)
			}

			return unknownMethod()
		},
		mRemove: func(m map[string]any, a []any) any {
			k := argStr(a, 0)
			old := m[k]
			delete(m, k)

			return old
		},
		mClear: mapClear,
		"containsKey": func(m map[string]any, a []any) any {
			_, ok := m[argStr(a, 0)]

			return ok
		},
		"containsValue": mapContainsValue,
		"keySet":        func(m map[string]any, _ []any) any { return mapKeysOrValues(m, true) },
		"values":        func(m map[string]any, _ []any) any { return mapKeysOrValues(m, false) },
		mToString:       func(m map[string]any, _ []any) any { return vrender(m) },
	}
}

func mapContainsValue(m map[string]any, args []any) any {
	for _, v := range m {
		if vequal(v, argAt(args, 0)) {
			return true
		}
	}

	return false
}

func mapKeysOrValues(m map[string]any, keys bool) *vlist {
	out := &vlist{}

	for _, k := range sortedKeys(m) {
		if keys {
			out.items = append(out.items, k)
		} else {
			out.items = append(out.items, m[k])
		}
	}

	return out
}

func listClear(l *vlist, _ []any) any {
	l.items = nil

	return unknownMethod()
}

func mapClear(m map[string]any, _ []any) any {
	clear(m)

	return unknownMethod()
}
