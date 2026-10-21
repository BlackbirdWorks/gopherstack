package gcplogging

import (
	"encoding/json"
	"hash/fnv"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// entryDoc lazily renders a log entry as JSON-shaped data for filter evaluation.
type entryDoc struct {
	e    *loggingpb.LogEntry
	m    map[string]any
	done bool
}

func newDoc(e *loggingpb.LogEntry) *entryDoc { return &entryDoc{e: e} }

func (d *entryDoc) doc() map[string]any {
	if d.done {
		return d.m
	}

	d.done = true

	raw, err := protojson.Marshal(d.e)
	if err != nil {
		stripped, ok := proto.Clone(d.e).(*loggingpb.LogEntry)
		if !ok {
			return nil
		}

		stripped.Payload = nil
		raw, _ = protojson.Marshal(stripped)
	}

	_ = json.Unmarshal(raw, &d.m)

	return d.m
}

func (n *andNode) match(d *entryDoc) bool {
	for _, k := range n.kids {
		if !k.match(d) {
			return false
		}
	}

	return true
}

func (n *orNode) match(d *entryDoc) bool {
	for _, k := range n.kids {
		if k.match(d) {
			return true
		}
	}

	return false
}

func (n *notNode) match(d *entryDoc) bool { return !n.kid.match(d) }

func (n *textNode) match(d *entryDoc) bool { return anyString(d.doc(), n.text) }

func (n *funcNode) match(d *entryDoc) bool {
	switch n.name {
	case "log_id":
		return logIDOf(d.e.GetLogName()) == n.args[0]
	case "search":
		return anyString(d.doc(), strings.ToLower(n.args[0]))
	}

	vals := lookup(d.doc(), splitPath(n.args[0]))
	if len(vals) == 0 {
		return false
	}

	frac, _ := strconv.ParseFloat(n.args[1], 64)
	h := fnv.New32a()
	_, _ = h.Write([]byte(scalarString(vals[0])))

	return float64(h.Sum32())/float64(math.MaxUint32) < frac
}

// logIDOf returns the decoded log id of a full log name.
func logIDOf(logName string) string {
	_, id, ok := strings.Cut(logName, "/logs/")
	if !ok {
		return ""
	}

	if dec, err := url.PathUnescape(id); err == nil {
		return dec
	}

	return id
}

// anyString reports whether any string leaf contains needle (already lower-case).
func anyString(v any, needle string) bool {
	switch t := v.(type) {
	case string:
		return strings.Contains(strings.ToLower(t), needle)
	case map[string]any:
		for _, x := range t {
			if anyString(x, needle) {
				return true
			}
		}
	case []any:
		for _, x := range t {
			if anyString(x, needle) {
				return true
			}
		}
	}

	return false
}

// lookup resolves a path, fanning out across arrays; it returns every value found.
func lookup(root any, path []string) []any {
	cur := []any{root}

	for _, seg := range path {
		name, idx, hasIdx := splitIndex(seg)

		var next []any

		for _, c := range cur {
			m, ok := c.(map[string]any)
			if !ok {
				continue
			}

			v, found := m[name]
			if !found {
				continue
			}

			next = append(next, expandIndex(v, idx, hasIdx)...)
		}

		cur = next
	}

	return cur
}

func expandIndex(v any, idx int, hasIdx bool) []any {
	arr, isArr := v.([]any)

	switch {
	case hasIdx:
		if isArr && idx >= 0 && idx < len(arr) {
			return []any{arr[idx]}
		}

		return nil
	case isArr:
		return arr
	}

	return []any{v}
}

func splitIndex(seg string) (string, int, bool) {
	open := strings.IndexByte(seg, '[')
	if open <= 0 || !strings.HasSuffix(seg, "]") {
		return seg, 0, false
	}

	idx, err := strconv.Atoi(seg[open+1 : len(seg)-1])
	if err != nil {
		return seg, 0, false
	}

	return seg[:open], idx, true
}

func scalarString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	case nil:
		return ""
	}

	b, _ := json.Marshal(v)

	return string(b)
}

func (n *cmpNode) match(d *entryDoc) bool {
	vals := lookup(d.doc(), n.path)
	if len(vals) == 0 {
		return n.op == opNe || n.op == opNre
	}

	switch n.op {
	case opNe, opNre:
		for _, v := range vals {
			if n.cmpOne(v, true) {
				return false
			}
		}

		return true
	}

	for _, v := range vals {
		if n.cmpOne(v, false) {
			return true
		}
	}

	return false
}

// cmpOne compares one value; negated flips != and !~ to their positive form.
func (n *cmpNode) cmpOne(v any, negated bool) bool {
	op := n.op

	if negated {
		op = map[string]string{opNe: opEq, opNre: opRe}[op]
	}

	switch op {
	case opRe:
		return n.re.MatchString(scalarString(v))
	case opHas:
		return n.has(v)
	}

	if _, isMap := v.(map[string]any); isMap {
		return false
	}

	return n.ordered(v, op)
}

func (n *cmpNode) has(v any) bool {
	if n.val == "*" {
		return true
	}

	return anyString(stringify(v), strings.ToLower(n.val))
}

// stringify wraps scalars so anyString sees numbers and booleans as text.
func stringify(v any) any {
	switch t := v.(type) {
	case string, map[string]any, []any:
		return t
	}

	return scalarString(v)
}

func (n *cmpNode) ordered(v any, op string) bool {
	c, ok := n.compare(v)
	if !ok {
		return false
	}

	switch op {
	case opEq:
		return c == 0
	case "<":
		return c < 0
	case "<=":
		return c <= 0
	case ">":
		return c > 0
	case ">=":
		return c >= 0
	}

	return false
}

func (n *cmpNode) compare(v any) (int, bool) {
	lead := n.path[0]

	switch {
	case lead == "severity" && len(n.path) == 1:
		return compareSeverity(v, n.val)
	case lead == "timestamp" || lead == "receiveTimestamp":
		return compareTime(v, n.val)
	}

	if b, ok := v.(bool); ok {
		want, err := strconv.ParseBool(n.val)

		return cmpInt(boolInt(b), boolInt(want)), err == nil
	}

	ls := scalarString(v)

	lf, lerr := strconv.ParseFloat(ls, 64)
	rf, rerr := strconv.ParseFloat(n.val, 64)

	if lerr == nil && rerr == nil {
		return cmpFloat(lf, rf), true
	}

	return strings.Compare(strings.ToLower(ls), strings.ToLower(n.val)), true
}

func compareSeverity(v any, val string) (int, bool) {
	l, ok := severityNum(scalarString(v))
	if !ok {
		return 0, false
	}

	r, ok := severityNum(val)
	if !ok {
		return 0, false
	}

	return cmpInt(l, r), true
}

func severityNum(s string) (int, bool) {
	if n, ok := ltype.LogSeverity_value[strings.ToUpper(s)]; ok {
		return int(n), true
	}

	n, err := strconv.Atoi(s)

	return n, err == nil
}

func compareTime(v any, val string) (int, bool) {
	l, ok := parseFilterTime(scalarString(v))
	if !ok {
		return 0, false
	}

	r, ok := parseFilterTime(val)
	if !ok {
		return 0, false
	}

	return l.Compare(r), true
}

func parseFilterTime(s string) (time.Time, bool) {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05", "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}

	return time.Time{}, false
}

func boolInt(b bool) int {
	if b {
		return 1
	}

	return 0
}

func cmpInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}

	return 0
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}

	return 0
}
