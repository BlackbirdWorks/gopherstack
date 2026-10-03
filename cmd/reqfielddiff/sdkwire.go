package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	serializerFuncRe  = regexp.MustCompile(`^func aws\w+_serializeOp(?:Document|HttpBindings)(\w+)Input\(`)
	serializerPayload = regexp.MustCompile(
		`^func \(m \*aws\w+_serializeOp(\w+)\) HandleSerialize`)
	payloadMemberRe = regexp.MustCompile(
		`serializeDocument\w+\(input\.(\w+), (?:jsonEncoder\.Value|xmlEncoder\.RootElement\(payloadRoot\))\)`)
	serializerFieldRe = regexp.MustCompile(`^\tif (?:len\()?v\.(\w+)`)
	serializerKeyRe   = regexp.MustCompile(
		`(?:\.(?:Key|FlatKey|SetQuery|SetHeader|AddHeader|SetURI)\(|locationName :?= )"([^"]+)"`)
)

// opWire holds one op's SDK serializer facts: wire keys per member, and
// whole-body payload members (never wire keys themselves).
type opWire struct {
	Keys    map[string][]string
	Payload map[string]bool
}

// loadWireKeys parses the pinned SDK's serializers.go, e.g. ec2query
// FlatKey("SecurityGroupId") for Groups.
func loadWireKeys(modPath string) map[string]*opWire {
	src, err := os.ReadFile(filepath.Join(modPath, "serializers.go"))
	if err != nil {
		return nil
	}

	return parseWireKeys(string(src))
}

func parseWireKeys(src string) map[string]*opWire {
	out := map[string]*opWire{}
	entry := func(op string) *opWire {
		if out[op] == nil {
			out[op] = &opWire{Keys: map[string][]string{}, Payload: map[string]bool{}}
		}

		return out[op]
	}

	var op, field string

	for line := range strings.SplitSeq(src, "\n") {
		if m := serializerFuncRe.FindStringSubmatch(line); m != nil {
			op, field = m[1], ""

			continue
		}

		if m := serializerPayload.FindStringSubmatch(line); m != nil {
			op, field = m[1], ""

			continue
		}

		if op == "" {
			continue
		}

		if m := payloadMemberRe.FindStringSubmatch(line); m != nil {
			entry(op).Payload[m[1]] = true

			continue
		}

		if m := serializerFieldRe.FindStringSubmatch(line); m != nil {
			field = m[1]

			continue
		}

		if m := serializerKeyRe.FindStringSubmatch(line); m != nil && field != "" {
			w := entry(op)
			w.Keys[field] = append(w.Keys[field], stripHeaderPrefix(m[1]))
		}
	}

	return out
}

func attachWireKeys(ops []sdkOp, wire map[string]*opWire) {
	for i := range ops {
		if w := wire[ops[i].Name]; w != nil {
			ops[i].WireKeys, ops[i].Payload = w.Keys, w.Payload
		}
	}
}
