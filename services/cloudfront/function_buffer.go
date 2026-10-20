package cloudfront

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/dop251/goja"

	"github.com/blackbirdworks/gopherstack/pkgs/jsruntime"
)

const (
	hexPairLen   = 2
	encUTF8      = "utf8"
	encHex       = "hex"
	encBase64    = "base64"
	encBase64URL = "base64url"
)

func normalizeEncoding(enc string) (string, error) {
	switch e := strings.ToLower(enc); e {
	case "", encUTF8, "utf-8":
		return encUTF8, nil
	case encHex, encBase64, encBase64URL:
		return e, nil
	}

	return "", fnError("Unknown encoding: " + enc)
}

func encodeBytes(b []byte, enc string) (string, error) {
	e, err := normalizeEncoding(enc)
	if err != nil {
		return "", err
	}

	switch e {
	case encHex:
		return hex.EncodeToString(b), nil
	case encBase64:
		return base64.StdEncoding.EncodeToString(b), nil
	case encBase64URL:
		return base64.RawURLEncoding.EncodeToString(b), nil
	}

	return string(b), nil
}

// decodeString is lenient like Node: invalid hex/base64 tails are dropped, not rejected.
func decodeString(s, enc string) ([]byte, error) {
	e, err := normalizeEncoding(enc)
	if err != nil {
		return nil, err
	}

	switch e {
	case encHex:
		return decodeHexPrefix(s), nil
	case encBase64, encBase64URL:
		return decodeBase64Lenient(s), nil
	}

	return []byte(s), nil
}

func decodeHexPrefix(s string) []byte {
	out := make([]byte, 0, len(s)/hexPairLen)

	for i := 0; i+1 < len(s); i += 2 {
		b, err := hex.DecodeString(s[i : i+2])
		if err != nil {
			break
		}

		out = append(out, b[0])
	}

	return out
}

func decodeBase64Lenient(s string) []byte {
	s = strings.NewReplacer("-", "+", "_", "/", "\n", "", "\r", "", " ", "").Replace(s)
	s = strings.TrimRight(s, "=")
	buf := make([]byte, base64.RawStdEncoding.DecodedLen(len(s)))
	n, _ := base64.RawStdEncoding.Decode(buf, []byte(s))

	return buf[:n]
}

func bindBufferGlobals(sb *jsruntime.Sandbox) {
	vm := sb.VM

	sb.Set("__bufEnc", func(call goja.FunctionCall) goja.Value {
		b, _ := call.Argument(0).Export().([]byte)

		s, err := encodeBytes(b, encArg(call.Argument(1)))
		if err != nil {
			panic(vm.NewTypeError(err.Error()))
		}

		return vm.ToValue(s)
	})
	sb.Set("__bufDec", func(call goja.FunctionCall) goja.Value {
		b, err := decodeString(call.Argument(0).String(), encArg(call.Argument(1)))
		if err != nil {
			panic(vm.NewTypeError(err.Error()))
		}

		return vm.ToValue(vm.NewArrayBuffer(b))
	})
}

func encArg(v goja.Value) string {
	if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
		return ""
	}

	return fmt.Sprint(v.Export())
}

// fnBufferPrelude defines the cloudfront-js-2.0 buffer module on top of Uint8Array.
const fnBufferPrelude = `
var __mod_buffer = (function () {
  var proto = Object.create(Uint8Array.prototype);
  function wrap(u8) { return Object.setPrototypeOf(u8, proto); }
  function Buffer() { throw new TypeError('Use Buffer.from() or Buffer.alloc()'); }
  Buffer.prototype = proto;
  proto.constructor = Buffer;
  Object.defineProperty(Buffer, Symbol.species, { get: function () { return Uint8Array; } });
  function bytesOf(s, enc) { return new Uint8Array(__bufDec(s, enc)); }
  function isBytes(v) { return v instanceof Uint8Array; }
  function clamp(i, len, dflt) {
    if (i === undefined) { return dflt; }
    i = Math.trunc(Number(i)) || 0;
    if (i < 0) { i += len; }
    return Math.min(Math.max(i, 0), len);
  }
  Buffer.from = function (v, enc) {
    if (typeof v === 'string') { return wrap(bytesOf(v, enc)); }
    if (v instanceof ArrayBuffer) { return wrap(new Uint8Array(v)); }
    if (isBytes(v) || Array.isArray(v)) { return wrap(Uint8Array.from(v)); }
    throw new TypeError('The first argument must be a string, Array, ArrayBuffer or Buffer');
  };
  Buffer.alloc = function (size, fill, enc) {
    if (typeof size !== 'number' || size < 0) {
      throw new TypeError('The "size" argument must be a non-negative number');
    }
    var b = wrap(new Uint8Array(size));
    if (fill === undefined) { return b; }
    if (typeof fill === 'number') { b.fill(fill & 255); return b; }
    var src = typeof fill === 'string' ? bytesOf(fill, enc) : fill;
    for (var i = 0; src.length > 0 && i < size; i++) { b[i] = src[i % src.length]; }
    return b;
  };
  Buffer.allocUnsafe = function (size) { return Buffer.alloc(size); };
  Buffer.isBuffer = function (o) { return o instanceof Buffer; };
  Buffer.isEncoding = function (e) {
    return ['utf8', 'utf-8', 'hex', 'base64', 'base64url'].indexOf(String(e).toLowerCase()) >= 0;
  };
  Buffer.byteLength = function (v, enc) {
    if (typeof v === 'string') { return bytesOf(v, enc).length; }
    if (isBytes(v) || v instanceof ArrayBuffer) { return v.byteLength; }
    throw new TypeError('The "string" argument must be a string, Buffer or ArrayBuffer');
  };
  Buffer.concat = function (list, total) {
    if (!Array.isArray(list)) { throw new TypeError('The "list" argument must be an Array of Buffers'); }
    var len = 0;
    list.forEach(function (b) { len += b.length; });
    var out = Buffer.alloc(total === undefined ? len : total), pos = 0;
    list.forEach(function (b) {
      for (var i = 0; i < b.length && pos < out.length; i++) { out[pos++] = b[i]; }
    });
    return out;
  };
  Buffer.compare = function (a, b) {
    var n = Math.min(a.length, b.length);
    for (var i = 0; i < n; i++) { if (a[i] !== b[i]) { return a[i] < b[i] ? -1 : 1; } }
    return a.length === b.length ? 0 : (a.length < b.length ? -1 : 1);
  };
  proto.compare = function (o) { return Buffer.compare(this, o); };
  proto.equals = function (o) {
    if (!isBytes(o)) { throw new TypeError('The "otherBuffer" argument must be a Buffer'); }
    return Buffer.compare(this, o) === 0;
  };
  proto.toString = function (enc, start, end) {
    var s = clamp(start, this.length, 0), e = clamp(end, this.length, this.length);
    return __bufEnc(s >= e ? new Uint8Array(0) : new Uint8Array(this.buffer, this.byteOffset + s, e - s), enc);
  };
  proto.subarray = function (start, end) {
    var s = clamp(start, this.length, 0), e = Math.max(clamp(end, this.length, this.length), s);
    return wrap(new Uint8Array(this.buffer, this.byteOffset + s, e - s));
  };
  proto.slice = proto.subarray;
  proto.toJSON = function () { return { type: 'Buffer', data: Array.prototype.slice.call(this) }; };
  return { Buffer: Buffer };
})();
__mods.buffer = __mod_buffer;
if (typeof __js2 !== 'undefined') { var Buffer = __mod_buffer.Buffer; }
`
