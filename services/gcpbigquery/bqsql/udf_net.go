package bqsql

import (
	"database/sql/driver"
	"net/netip"
	"regexp"
	"strings"
	"sync"

	"golang.org/x/net/idna"
	"golang.org/x/net/publicsuffix"
)

const (
	ipv4Len      = 4
	ipv6Len      = 16
	bitsPerByte  = 8
	fullByte     = 0xFF
	shift24      = 24
	shift16      = 16
	shift8       = 8
	maxUint32    = 0xFFFFFFFF
	minInt32     = -0x80000000
	argcNetMask  = 2
	hostBracket  = "["
	authorityEnd = "/?#"
)

//nolint:gochecknoglobals // compiled once, read-only
var schemePrefix = sync.OnceValue(func() *regexp.Regexp { return regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.\-]*://`) })

// addNetUDFs registers NET.* (https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/net_functions).
func addNetUDFs(d map[string]udfDef) {
	d["net_ip_from_string"] = udfDef{n: 1, fn: udfIPFromString(true)}
	d["net_safe_ip_from_string"] = udfDef{n: 1, fn: udfIPFromString(false)}
	d["net_ip_to_string"] = udfDef{n: 1, fn: udfIPToString}
	d["net_ip_net_mask"] = udfDef{n: argcNetMask, fn: udfIPNetMask}
	d["net_ip_trunc"] = udfDef{n: argcNetMask, fn: udfIPTrunc}
	d["net_ipv4_from_int64"] = udfDef{n: 1, fn: udfIPv4FromInt64}
	d["net_ipv4_to_int64"] = udfDef{n: 1, fn: udfIPv4ToInt64}
	d["net_host"] = udfDef{n: 1, fn: urlPart(func(h string) (any, bool) { return h, h != "" })}
	d["net_public_suffix"] = udfDef{n: 1, fn: urlPart(publicSuffixOf)}
	d["net_reg_domain"] = udfDef{n: 1, fn: urlPart(regDomainOf)}
}

func udfIPFromString(strict bool) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if a[0] == nil {
			return nil, nil
		}

		s := toStr(a[0])

		addr, err := netip.ParseAddr(s)
		if err != nil || addr.Zone() != "" {
			if strict {
				return nil, errBQ("Invalid IP address: %s", s)
			}

			return nil, nil
		}

		if addr.Is4() {
			b := addr.As4()

			return b[:], nil
		}

		b := addr.As16()

		return b[:], nil
	}
}

func udfIPToString(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	b := bytesOf(a[0])

	switch len(b) {
	case ipv4Len:
		return netip.AddrFrom4([ipv4Len]byte(b)).String(), nil
	case ipv6Len:
		return netip.AddrFrom16([ipv6Len]byte(b)).String(), nil
	}

	return nil, errBQ("Invalid IP address length: %d", len(b))
}

func udfIPNetMask(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	size, err := toInt(a[0])
	if err != nil {
		return nil, err
	}

	prefix, err := toInt(a[1])
	if err != nil {
		return nil, err
	}

	if size != ipv4Len && size != ipv6Len {
		return nil, errBQ("Output bytes must be 4 or 16, but was %d", size)
	}

	if prefix < 0 || prefix > size*bitsPerByte {
		return nil, errBQ("Prefix length must be between 0 and %d, but was %d", size*bitsPerByte, prefix)
	}

	return maskBytes(int(size), int(prefix)), nil
}

func maskBytes(size, prefix int) []byte {
	out := make([]byte, size)

	for i := range out {
		switch {
		case prefix >= bitsPerByte:
			out[i] = fullByte
		case prefix > 0:
			out[i] = byte(fullByte << (bitsPerByte - prefix))
		}

		prefix = max(prefix-bitsPerByte, 0)
	}

	return out
}

func udfIPTrunc(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	addr := bytesOf(a[0])
	if len(addr) != ipv4Len && len(addr) != ipv6Len {
		return nil, errBQ("Invalid IP address length: %d", len(addr))
	}

	prefix, err := toInt(a[1])
	if err != nil {
		return nil, err
	}

	if prefix < 0 || prefix > int64(len(addr)*bitsPerByte) {
		return nil, errBQ("Prefix length must be between 0 and %d, but was %d", len(addr)*bitsPerByte, prefix)
	}

	mask := maskBytes(len(addr), int(prefix))
	out := make([]byte, len(addr))

	for i := range addr {
		out[i] = addr[i] & mask[i]
	}

	return out, nil
}

func udfIPv4FromInt64(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	n, err := toInt(a[0])
	if err != nil {
		return nil, err
	}

	if n < minInt32 || n > maxUint32 {
		return nil, errBQ("Value %d is out of range for an IPv4 address [-0x80000000, 0xFFFFFFFF]", n)
	}

	u := uint32(n)

	return []byte{byte(u >> shift24), byte(u >> shift16), byte(u >> shift8), byte(u)}, nil
}

func udfIPv4ToInt64(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	b := bytesOf(a[0])
	if len(b) != ipv4Len {
		return nil, errBQ("Invalid IPv4 address length: %d", len(b))
	}

	return int64(b[0])<<24 | int64(b[1])<<16 | int64(b[2])<<8 | int64(b[3]), nil
}

func urlPart(f func(host string) (any, bool)) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if a[0] == nil {
			return nil, nil
		}

		host, ok := urlHost(toStr(a[0]))
		if !ok {
			return nil, nil
		}

		v, ok := f(host)
		if !ok {
			return nil, nil
		}

		return v, nil
	}
}

// urlHost is a best-effort host extraction that does not normalise (NET.HOST).
func urlHost(raw string) (string, bool) {
	s := strings.TrimSpace(raw)
	s = schemePrefix().ReplaceAllString(s, "")
	s = strings.TrimPrefix(s, "//")

	if i := strings.IndexAny(s, authorityEnd); i >= 0 {
		s = s[:i]
	}

	if i := strings.LastIndex(s, "@"); i >= 0 {
		s = s[i+1:]
	}

	if strings.HasPrefix(s, hostBracket) {
		end := strings.Index(s, "]")
		if end < 0 {
			return "", false
		}

		return s[:end+1], true
	}

	if i := strings.Index(s, ":"); i >= 0 {
		s = s[:i]
	}

	return s, s != ""
}

// icannSuffixLabels returns the number of labels of host's ICANN public suffix; private suffixes are ignored.
func icannSuffixLabels(host string) (int, bool) {
	if strings.HasPrefix(host, hostBracket) || strings.Contains(host, "..") {
		return 0, false
	}

	if _, err := netip.ParseAddr(host); err == nil {
		return 0, false
	}

	ascii, err := idna.ToASCII(strings.ToLower(strings.TrimSuffix(host, ".")))
	if err != nil {
		return 0, false
	}

	labels := strings.Split(ascii, ".")

	for i := range labels {
		cand := strings.Join(labels[i:], ".")

		suffix, icann := publicsuffix.PublicSuffix(cand)
		if icann && suffix == cand {
			return len(labels) - i, true
		}
	}

	return 0, false
}

func lastLabels(host string, n int) string {
	labels := strings.Split(strings.TrimSuffix(host, "."), ".")

	return strings.Join(labels[len(labels)-n:], ".")
}

func publicSuffixOf(host string) (any, bool) {
	n, ok := icannSuffixLabels(host)
	if !ok {
		return nil, false
	}

	return lastLabels(host, n), true
}

func regDomainOf(host string) (any, bool) {
	n, ok := icannSuffixLabels(host)
	if !ok || len(strings.Split(strings.TrimSuffix(host, "."), ".")) <= n {
		return nil, false
	}

	return lastLabels(host, n+1), true
}

func addNetFuncs(t map[string]*fnDef) {
	t["NET.IP_FROM_STRING"] = def("net_ip_from_string", 1, 1, fixed(KindBytes))
	t["NET.SAFE_IP_FROM_STRING"] = def("net_safe_ip_from_string", 1, 1, fixed(KindBytes))
	t["NET.IP_TO_STRING"] = def("net_ip_to_string", 1, 1, fixed(KindString))
	t["NET.IP_NET_MASK"] = def("net_ip_net_mask", arityTwo, arityTwo, fixed(KindBytes))
	t["NET.IP_TRUNC"] = def("net_ip_trunc", arityTwo, arityTwo, fixed(KindBytes))
	t["NET.IPV4_FROM_INT64"] = def("net_ipv4_from_int64", 1, 1, fixed(KindBytes))
	t["NET.IPV4_TO_INT64"] = def("net_ipv4_to_int64", 1, 1, fixed(KindInt))
	t["NET.HOST"] = def("net_host", 1, 1, fixed(KindString))
	t["NET.PUBLIC_SUFFIX"] = def("net_public_suffix", 1, 1, fixed(KindString))
	t["NET.REG_DOMAIN"] = def("net_reg_domain", 1, 1, fixed(KindString))
}
