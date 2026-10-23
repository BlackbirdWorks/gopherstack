package bedrockagentcore

import (
	"slices"
	"strings"
)

const (
	opEquals    = "EQUALS_TO"
	opExists    = "EXISTS"
	opNotExists = "NOT_EXISTS"
)

const mainBranch = "main"

type eventFilter struct {
	cutoffs map[string]uint64
	branch  string
	meta    []metaCond
	parents bool
}

type metaCond struct {
	right any
	key   string
	op    string
}

func parseEventFilter(f doc) (eventFilter, error) {
	out := eventFilter{}

	if br := objOf(f, "branch"); br != nil {
		out.branch, out.parents = strOf(br, "name"), boolOf(br, "includeParentBranches", false)
	}

	for _, e := range listOf(f, "eventMetadata") {
		c, err := parseCond(e, []string{opEquals, opExists, opNotExists})
		if err != nil {
			return out, err
		}

		out.meta = append(out.meta, c)
	}

	return out, nil
}

func parseCond(e any, ops []string) (metaCond, error) {
	m, _ := e.(map[string]any)
	op := strOf(m, "operator")

	if !slices.Contains(ops, op) {
		return metaCond{}, invalid("operator %q is not supported", op)
	}

	key := strOf(objOf(m, "left"), "metadataKey")
	if key == "" {
		return metaCond{}, invalid("left.metadataKey is required")
	}

	c := metaCond{key: key, op: op}

	if op != opExists && op != opNotExists {
		right := objOf(m, "right")
		if _, ok := right["metadataValue"]; !ok {
			return c, invalid("right.metadataValue is required for operator %s", op)
		}

		c.right = right["metadataValue"]
	}

	return c, nil
}

func (f eventFilter) match(e *Event) bool {
	if f.branch != "" && !f.branchMatch(e) {
		return false
	}

	for _, c := range f.meta {
		v, has := e.Metadata[c.key]

		switch c.op {
		case opExists:
			if !has {
				return false
			}
		case opNotExists:
			if has {
				return false
			}
		case opEquals:
			if !has || !jsonEqual(v, c.right) {
				return false
			}
		}
	}

	return true
}

func branchName(e *Event) string {
	if n := strOf(e.Branch, "name"); n != "" {
		return n
	}

	return mainBranch
}

func (f eventFilter) branchMatch(e *Event) bool {
	name := branchName(e)
	if name == f.branch {
		return true
	}

	cut, ok := f.cutoffs[name]

	return f.parents && ok && e.Seq <= cut
}

// withParents resolves the ancestor branches of the filtered branch from the session's events: each branch forks
// at the root event of its first event, and ancestors contribute only the events up to that fork point.
func (f eventFilter) withParents(session []*Event) eventFilter {
	if !f.parents || f.branch == "" {
		return f
	}

	byID := make(map[string]*Event, len(session))
	root := map[string]string{}

	for _, e := range session {
		byID[e.EventID] = e

		if r := strOf(e.Branch, "rootEventId"); r != "" {
			root[branchName(e)] = r
		}
	}

	f.cutoffs = map[string]uint64{}

	for cur := f.branch; root[cur] != ""; {
		fork, ok := byID[root[cur]]
		if !ok {
			break
		}

		parent := branchName(fork)
		if _, seen := f.cutoffs[parent]; seen || parent == f.branch {
			break
		}

		f.cutoffs[parent] = fork.Seq
		cur = parent
	}

	return f
}

func jsonEqual(a, b any) bool {
	return string(mustJSON(a)) == string(mustJSON(b))
}

func hasNamespace(namespaces []string, q string) bool {
	if q == "" {
		return true
	}

	prefix := strings.TrimSuffix(q, "/") + "/"

	return slices.ContainsFunc(namespaces, func(ns string) bool {
		return ns == q || strings.HasPrefix(ns, prefix)
	})
}
