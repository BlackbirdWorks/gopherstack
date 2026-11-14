package dashboard

import (
	"encoding/json/v2"
	"math"
	"net/http"
	"net/netip"
	"regexp"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	netPxMinPriority = 100
	netPxMaxPriority = 4096
	netPxMaxDesc     = 140
	netPxMaxList     = 50
	netPxVNetPutIdx  = 8
	netPxProps       = "properties"
	netPxNameKey     = "name"
	netPxProtocolKey = "protocol"
)

var (
	netPxLocationRE = regexp.MustCompile(`^[a-z0-9]{2,40}$`)
	netPxNameRE     = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._-]{0,78}[A-Za-z0-9_])?$`)
	netPxPortRE     = regexp.MustCompile(`^(\*|\d{1,5}(-\d{1,5})?)$`)
	netPxTagRE      = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9.]{0,63}$`)
	netPxDescRE     = regexp.MustCompile(`^[^<>"\\]*$`)
	netPxServiceRE  = regexp.MustCompile(`^Microsoft\.[A-Za-z]{2,40}/[A-Za-z]{2,40}$`)
	netPxAssocRE    = regexp.MustCompile(
		`(?i)^/subscriptions/[0-9a-f-]{1,40}/resourceGroups/[A-Za-z0-9._()-]{1,90}/providers/Microsoft\.Network/` +
			`(networkSecurityGroups|natGateways)/[A-Za-z0-9._-]{1,80}$`)
	netPxDirections = []string{"Inbound", "Outbound"}                  //nolint:gochecknoglobals // lookup
	netPxAccesses   = []string{"Allow", "Deny"}                        //nolint:gochecknoglobals // lookup
	netPxProtocols  = []string{"Tcp", "Udp", "Icmp", "Esp", "Ah", "*"} //nolint:gochecknoglobals // lookup
)

type netPxObj = map[string]any

// netProxyBody rebuilds the body of every write from fixed fields; anything the caller adds is dropped.
func netProxyBody(method string, p []string, in []byte) ([]byte, bool) {
	if method == http.MethodGet || method == http.MethodDelete {
		return in, true
	}

	if method == http.MethodPost {
		return []byte("{}"), true
	}

	var req netPxObj
	if json.Unmarshal(in, &req, jsonx.V1()) != nil {
		return nil, false
	}

	props, _ := req[netPxProps].(netPxObj)

	var out netPxObj

	switch {
	case len(p) == netPxVNetPutIdx && p[netPxRGTypeIdx] == netPxVNets:
		out = netVNetBody(req, props)
	case len(p) > netPxVNetPutIdx && p[netPxRGTypeIdx] == netPxVNets && p[netPxVNetPutIdx] == netPxSubnets:
		out = netSubnetBody(props)
	case len(p) > netPxVNetPutIdx && p[netPxRGTypeIdx] == netPxNSGs && p[netPxVNetPutIdx] == netPxRules:
		out = netRuleBody(props)
	}

	if out == nil {
		return nil, false
	}

	b, err := json.Marshal(out, jsonx.V1())

	return b, err == nil
}

func netVNetBody(req, props netPxObj) netPxObj {
	loc, _ := req["location"].(string)
	space, _ := props["addressSpace"].(netPxObj)
	prefixes := netCIDRList(space["addressPrefixes"])

	if !netPxLocationRE.MatchString(loc) || len(prefixes) == 0 {
		return nil
	}

	out := netPxObj{"addressSpace": netPxObj{"addressPrefixes": prefixes}}

	if raw, present := props["subnets"]; present {
		list, _ := raw.([]any)
		subnets := make([]any, 0, len(list))

		for _, e := range list {
			m, _ := e.(netPxObj)
			name, _ := m[netPxNameKey].(string)
			prefix, _ := propsOfAny(m)["addressPrefix"].(string)

			if !netPxNameRE.MatchString(name) || !netValidCIDR(prefix) {
				return nil
			}

			subnets = append(subnets, netPxObj{netPxNameKey: name, netPxProps: netPxObj{"addressPrefix": prefix}})
		}

		out["subnets"] = subnets
	}

	return netPxObj{"location": loc, netPxProps: out}
}

func propsOfAny(m netPxObj) netPxObj {
	p, _ := m[netPxProps].(netPxObj)

	return p
}

func netSubnetBody(props netPxObj) netPxObj {
	prefix, _ := props["addressPrefix"].(string)
	if !netValidCIDR(prefix) {
		return nil
	}

	out := netPxObj{"addressPrefix": prefix}

	for _, k := range []string{"networkSecurityGroup", "natGateway"} {
		ref, present := props[k].(netPxObj)
		if !present {
			continue
		}

		id, _ := ref["id"].(string)
		if !netPxAssocRE.MatchString(id) {
			return nil
		}

		out[k] = netPxObj{"id": id}
	}

	if raw, present := props["delegations"]; present {
		list, _ := raw.([]any)
		dels := make([]any, 0, len(list))

		for _, e := range list {
			m, _ := e.(netPxObj)
			name, _ := m[netPxNameKey].(string)
			svc, _ := propsOfAny(m)["serviceName"].(string)

			if !netPxNameRE.MatchString(name) || !netPxServiceRE.MatchString(svc) {
				return nil
			}

			dels = append(dels, netPxObj{netPxNameKey: name, netPxProps: netPxObj{"serviceName": svc}})
		}

		out["delegations"] = dels
	}

	return netPxObj{netPxProps: out}
}

func netRuleBody(props netPxObj) netPxObj {
	prio, ok := props["priority"].(float64)
	if !ok || prio != math.Trunc(prio) || prio < netPxMinPriority || prio > netPxMaxPriority {
		return nil
	}

	out := netPxObj{"priority": int(prio)}

	for k, allowed := range map[string][]string{
		"direction": netPxDirections, "access": netPxAccesses, netPxProtocolKey: netPxProtocols,
	} {
		v, _ := props[k].(string)
		if !slices.Contains(allowed, v) {
			return nil
		}

		out[k] = v
	}

	if d, present := props["description"].(string); present {
		if len(d) > netPxMaxDesc || !netPxDescRE.MatchString(d) {
			return nil
		}

		out["description"] = d
	}

	for _, f := range []struct {
		valid     func(string) bool
		one, many string
	}{
		{netPxPortRE.MatchString, "sourcePortRange", "sourcePortRanges"},
		{netPxPortRE.MatchString, "destinationPortRange", "destinationPortRanges"},
		{netValidPrefix, "sourceAddressPrefix", "sourceAddressPrefixes"},
		{netValidPrefix, "destinationAddressPrefix", "destinationAddressPrefixes"},
	} {
		if !netCopyValues(out, props, f.one, f.many, f.valid) {
			return nil
		}
	}

	return netPxObj{netPxProps: out}
}

// netCopyValues copies exactly one of the singular or plural fields once every value validates.
func netCopyValues(out, props netPxObj, one, many string, valid func(string) bool) bool {
	if v, ok := props[one].(string); ok && props[many] == nil {
		out[one] = v

		return valid(v)
	}

	list, ok := props[many].([]any)
	if !ok || len(list) == 0 || len(list) > netPxMaxList || props[one] != nil {
		return false
	}

	vals := make([]string, 0, len(list))

	for _, e := range list {
		s, isStr := e.(string)
		if !isStr || !valid(s) {
			return false
		}

		vals = append(vals, s)
	}

	out[many] = vals

	return true
}

func netValidPrefix(s string) bool {
	return s == "*" || netValidCIDR(s) || netPxTagRE.MatchString(s) || netValidAddr(s)
}

func netValidAddr(s string) bool {
	_, err := netip.ParseAddr(s)

	return err == nil
}

func netValidCIDR(s string) bool {
	p, err := netip.ParsePrefix(s)

	return err == nil && !strings.Contains(s, "%") && p.IsValid()
}

func netCIDRList(v any) []string {
	list, _ := v.([]any)
	if len(list) == 0 || len(list) > netPxMaxList {
		return nil
	}

	out := make([]string, 0, len(list))

	for _, e := range list {
		s, ok := e.(string)
		if !ok || !netValidCIDR(s) {
			return nil
		}

		out = append(out, s)
	}

	return out
}
