package cloudwatchlogs

import (
	"maps"
	"regexp"
	"strings"
)

type tagFilter struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
}

// logGroupTagKeeper returns a predicate ANDing every filter, or nil when there are none.
func (h *Handler) logGroupTagKeeper(filters []tagFilter) func(LogGroup) bool {
	if len(filters) == 0 {
		return nil
	}

	return func(g LogGroup) bool {
		h.tagsMu.RLock("logGroupTagKeeper")
		defer h.tagsMu.RUnlock()

		kv := map[string]string{}
		for _, id := range []string{g.LogGroupName, g.Arn, strings.TrimSuffix(g.Arn, ":*")} {
			if t := h.tags[id]; t != nil {
				maps.Copy(kv, t.Clone())
			}
		}
		for _, f := range filters {
			v, ok := kv[f.Key]
			if !ok || !tagValueMatches(v, f.Values) {
				return false
			}
		}

		return true
	}
}

// tagValueMatches follows the TagFilter.Values doc: no values matches any, "!" negates (case
// sensitive), "*" wildcards (case insensitive), several values are ORed.
func tagValueMatches(value string, patterns []string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, p := range patterns {
		switch {
		case strings.HasPrefix(p, "!"):
			if value != p[1:] {
				return true
			}
		case strings.Contains(p, "*"):
			re := "(?i)^" + strings.ReplaceAll(regexp.QuoteMeta(p), `\*`, ".*") + "$"
			if regexp.MustCompile(re).MatchString(value) {
				return true
			}
		case value == p:
			return true
		}
	}

	return false
}
