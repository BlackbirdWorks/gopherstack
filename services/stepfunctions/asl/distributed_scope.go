package asl

import (
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
)

var varRefRe = regexp.MustCompile(
	`(?:^|[^$\p{L}\p{Nl}\p{Mn}\p{Mc}\p{Nd}\p{Pc}])\$([\p{L}\p{Nl}][\p{L}\p{Nl}\p{Mn}\p{Mc}\p{Nd}\p{Pc}]*)`)

func isDistributedProcessor(st *State) bool {
	return st.ItemProcessor != nil && st.ItemProcessor.ProcessorConfig != nil &&
		st.ItemProcessor.ProcessorConfig.Mode == processorModeDistributed
}

// checkDistributedScope rejects a Distributed Map child workflow that reads an
// outer-scope variable ("Distributed Map states cannot currently reference
// variables in outer scopes", AWS variables guide, "Variable scope").
func checkDistributedScope(mapName string, states map[string]*State, outer map[string]struct{}) error {
	if len(outer) == 0 {
		return nil
	}

	for _, name := range slices.Sorted(maps.Keys(states)) {
		st := states[name]
		if st == nil {
			continue
		}

		b, err := json.Marshal(st)
		if err != nil {
			return fmt.Errorf("%w: state %q: %w", ErrParseError, name, err)
		}

		var doc any
		if err = json.Unmarshal(b, &doc); err != nil {
			return fmt.Errorf("%w: state %q: %w", ErrParseError, name, err)
		}

		if ref := firstOuterRef(doc, outer); ref != "" {
			return fmt.Errorf(
				"%w: state %q in the Distributed Map %q references outer-scope variable $%s; "+
					"Distributed Map child workflows cannot read outer-scope variables",
				ErrParseError, name, mapName, ref)
		}

		for _, sub := range nestedMachines(st) {
			if err = checkDistributedScope(mapName, sub.states, outer); err != nil {
				return err
			}
		}
	}

	return nil
}

func firstOuterRef(v any, outer map[string]struct{}) string {
	switch t := v.(type) {
	case string:
		for _, m := range varRefRe.FindAllStringSubmatch(t, -1) {
			if _, ok := outer[m[1]]; ok {
				return m[1]
			}
		}
	case []any:
		for _, x := range t {
			if r := firstOuterRef(x, outer); r != "" {
				return r
			}
		}
	case map[string]any:
		for _, k := range slices.Sorted(maps.Keys(t)) {
			if r := firstOuterRef(t[k], outer); r != "" {
				return r
			}
		}
	}

	return ""
}
