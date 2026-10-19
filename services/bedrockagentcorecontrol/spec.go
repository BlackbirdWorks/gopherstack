package bedrockagentcorecontrol

import (
	_ "embed" // opsJSON
	"encoding/json"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

//go:embed ops.json
var opsJSON []byte

type outField struct {
	Times map[string]string `json:"t"`
	Key   string            `json:"k"`
	Elem  []string          `json:"e"`
	List  bool              `json:"l"`
}

// opSpec is one SDK operation's HTTP binding and member sets, extracted from the pinned SDK serializers,
// deserializers and validators.
type opSpec struct {
	Enums      map[string][]string `json:"en"`
	OutTimes   map[string]string   `json:"ot"`
	Method     string              `json:"m"`
	Path       string              `json:"p"`
	Query      []string            `json:"q"`
	Body       []string            `json:"b"`
	Required   []string            `json:"r"`
	Out        []outField          `json:"o"`
	labelNames []string
}

type specFile struct {
	Ops map[string]*opSpec `json:"ops"`
}

var specs = sync.OnceValue(func() map[string]*opSpec { //nolint:gochecknoglobals // immutable embedded spec
	var f specFile
	if err := json.Unmarshal(opsJSON, &f); err != nil {
		panic("bedrockagentcorecontrol: corrupt ops.json: " + err.Error())
	}

	for _, s := range f.Ops {
		for seg := range strings.SplitSeq(s.Path, "/") {
			if len(seg) > 2 && seg[0] == '{' {
				s.labelNames = append(s.labelNames, seg[1:len(seg)-1])
			}
		}
	}

	return f.Ops
})

func (s *opSpec) isQuery(k string) bool {
	return slices.Contains(s.Query, k)
}

// routes orders literal paths ahead of labelled ones so a template never shadows a literal sibling.
func routes() []restroute.Route {
	all := specs()
	names := make([]string, 0, len(all))

	for n := range all {
		names = append(names, n)
	}

	sort.Slice(names, func(i, j int) bool {
		a, b := all[names[i]], all[names[j]]
		if len(a.labelNames) != len(b.labelNames) {
			return len(a.labelNames) < len(b.labelNames)
		}

		return names[i] < names[j]
	})

	out := make([]restroute.Route, 0, len(names))
	for _, n := range names {
		out = append(out, restroute.Route{Method: all[n].Method, Path: all[n].Path, Op: n})
	}

	return out
}
