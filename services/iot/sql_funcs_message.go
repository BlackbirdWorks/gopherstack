package iot

import (
	"crypto/md5"  //nolint:gosec // IoT SQL exposes md5()
	"crypto/sha1" //nolint:gosec // IoT SQL exposes sha1()
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"hash"
	"strings"

	"github.com/google/uuid"
)

func messageFuncs() map[string]funcDef {
	return map[string]funcDef{
		"topic":       {impl: topicFunc},
		"timestamp":   {impl: func(c *sqlCtx, _ []any) any { return c.msg.received.UnixMilli() }},
		"clientid":    {impl: clientIDFunc},
		"newuuid":     {impl: func(*sqlCtx, []any) any { return uuid.NewString() }},
		"accountid":   {impl: func(c *sqlCtx, _ []any) any { return c.msg.account }},
		"sql_version": {impl: func(c *sqlCtx, _ []any) any { return c.version }},
		"encode":      {since2016: true, impl: encodeFunc},
		"decode":      {since2016: true, impl: decodeFunc},
	}
}

func clientIDFunc(c *sqlCtx, _ []any) any {
	if c.msg.clientID == "" {
		return "n/a"
	}

	return c.msg.clientID
}

// topicFunc returns the topic, or its 1-based n-th segment.
func topicFunc(c *sqlCtx, args []any) any {
	if len(args) == 0 {
		return c.msg.topic
	}

	n, ok := toIntConv(args[0])
	segs := strings.Split(c.msg.topic, "/")

	if !ok || n < 1 || n > int64(len(segs)) {
		return sqlUndefined{}
	}

	return segs[n-1]
}

func encodeFunc(_ *sqlCtx, args []any) any {
	scheme, ok := strArg(args, 1)
	if !ok || !strings.EqualFold(scheme, "base64") {
		return sqlUndefined{}
	}

	s, ok := strArg(args, 0)
	if !ok {
		return sqlUndefined{}
	}

	return base64.StdEncoding.EncodeToString([]byte(s))
}

// decodeFunc base64-decodes; JSON results become addressable values, failures are Null.
func decodeFunc(c *sqlCtx, args []any) any {
	scheme, _ := strArg(args, 1)
	s, ok := strArg(args, 0)

	if !ok || !strings.EqualFold(scheme, "base64") {
		return nil
	}

	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil
	}

	if v, jerr := decodeJSONValue(raw, c.v2016); jerr == nil {
		return v
	}

	return string(raw)
}

func hashFunc(newHash func() hash.Hash) funcDef {
	return funcDef{impl: func(_ *sqlCtx, args []any) any {
		s, ok := strArg(args, 0)
		if !ok {
			return sqlUndefined{}
		}

		h := newHash()
		h.Write([]byte(s))

		return hex.EncodeToString(h.Sum(nil))
	}}
}

func hashFuncs() map[string]funcDef {
	return map[string]funcDef{
		"md2": {impl: func(_ *sqlCtx, args []any) any {
			in, ok := strArg(args, 0)
			if !ok {
				return sqlUndefined{}
			}

			sum := md2Sum([]byte(in))

			return hex.EncodeToString(sum[:])
		}},
		"md5":    hashFunc(md5.New),
		"sha1":   hashFunc(sha1.New),
		"sha224": hashFunc(sha256.New224),
		"sha256": hashFunc(sha256.New),
		"sha384": hashFunc(sha512.New384),
		"sha512": hashFunc(sha512.New),
	}
}

func valueFuncs() map[string]funcDef {
	return map[string]funcDef{
		"get":            {impl: getFunc},
		"transform":      {since2016: true, impl: transformFunc},
		"get_or_default": {since2016: true, impl: getOrDefaultFunc},
		"isnull": {
			since2016: true,
			impl:      func(_ *sqlCtx, args []any) any { return len(args) > 0 && args[0] == nil },
		},
		"isundefined": {since2016: true, impl: func(_ *sqlCtx, args []any) any { return isUndef(arg(args, 0)) }},
	}
}

func getFunc(_ *sqlCtx, args []any) any {
	switch coll := arg(args, 0).(type) {
	case []any:
		return indexOf(coll, arg(args, 1))
	case string:
		i, ok := toIntConv(arg(args, 1))
		runes := []rune(coll)

		if !ok || i < 0 || i >= int64(len(runes)) {
			return sqlUndefined{}
		}

		return string(runes[i])
	case *sqlObject:
		if k, ok := arg(args, 1).(string); ok {
			return memberOf(coll, k)
		}
	}

	return sqlUndefined{}
}

func getOrDefaultFunc(_ *sqlCtx, args []any) any {
	if v := arg(args, 0); v != nil && !isUndef(v) {
		return v
	}

	return arg(args, 1)
}

const transformSourceArg = 2

// transformFunc supports the enrichArray mode: each source element gains the enrichment object's attributes.
func transformFunc(_ *sqlCtx, args []any) any {
	mode, ok := arg(args, 0).(string)
	enrich, objOK := arg(args, 1).(*sqlObject)
	src, arrOK := arg(args, transformSourceArg).([]any)

	if !ok || !strings.EqualFold(mode, "enrichArray") || !objOK || !arrOK {
		return sqlUndefined{}
	}

	out := make([]any, 0, len(src))

	for _, el := range src {
		obj, isObj := el.(*sqlObject)
		if !isObj {
			return sqlUndefined{}
		}

		merged := newSQLObject()

		for _, k := range obj.keys {
			merged.set(k, obj.vals[k])
		}

		for _, k := range enrich.keys {
			merged.set(k, enrich.vals[k])
		}

		out = append(out, merged)
	}

	return out
}
