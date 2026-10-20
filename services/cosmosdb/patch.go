package cosmosdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// maxPatchOperations is the Cosmos limit on operations in one patch request.
const maxPatchOperations = 10

const (
	patchAdd     = "add"
	patchSet     = "set"
	patchReplace = "replace"
	patchRemove  = "remove"
	patchIncr    = "incr"
	patchMove    = "move"

	arrayAppend = "-"
)

type patchOp struct {
	Op    string          `json:"op"`
	Path  string          `json:"path"`
	From  string          `json:"from,omitempty"`
	Value json.RawMessage `json:"value,omitempty"`
}

type patchRequest struct {
	Condition  string    `json:"condition"`
	Operations []patchOp `json:"operations"`
}

func invalidPatchf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidPatch, fmt.Sprintf(format, args...))
}

// parsePatchRequest decodes and validates a patch body.
func parsePatchRequest(raw []byte) (patchRequest, error) {
	var req patchRequest

	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&req); err != nil || rejectTrailingJSON(dec) != nil {
		return patchRequest{}, invalidPatchf("the patch body is not valid JSON")
	}

	if len(req.Operations) == 0 {
		return patchRequest{}, invalidPatchf("at least one patch operation is required")
	}

	if len(req.Operations) > maxPatchOperations {
		return patchRequest{}, invalidPatchf("a patch request holds at most %d operations", maxPatchOperations)
	}

	return req, nil
}

// patchPath splits a "/a/b/0" path into unescaped segments (~1 is "/", ~0 is "~").
func patchPath(path string) ([]string, error) {
	if !strings.HasPrefix(path, "/") || len(path) < 2 {
		return nil, invalidPatchf("invalid path %q", path)
	}

	segs := strings.Split(path[1:], "/")
	for i, s := range segs {
		segs[i] = strings.ReplaceAll(strings.ReplaceAll(s, "~1", "/"), "~0", "~")
	}

	switch segs[0] {
	case "id", sysPropRID, sysPropETag, sysPropTS, sysPropSelf, sysPropAttachments:
		if len(segs) == 1 {
			return nil, invalidPatchf("the property %q cannot be patched", segs[0])
		}
	}

	return segs, nil
}

func decodePatchValue(raw json.RawMessage) (any, error) {
	if len(raw) == 0 {
		return nil, invalidPatchf("the operation requires a value")
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()

	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, invalidPatchf("invalid value: %v", err)
	}

	return v, nil
}

// applyPatch applies ops in order to a copy of body; any failure leaves body untouched.
func applyPatch(body map[string]any, ops []patchOp) (map[string]any, error) {
	work, err := deepCopyBody(body)
	if err != nil {
		return nil, err
	}

	var root any = work

	for _, op := range ops {
		if root, err = applyPatchOp(root, op); err != nil {
			return nil, err
		}
	}

	out, ok := root.(map[string]any)
	if !ok {
		return nil, invalidPatchf("the document root must remain an object")
	}

	return out, nil
}

func applyPatchOp(root any, op patchOp) (any, error) {
	segs, err := patchPath(op.Path)
	if err != nil {
		return nil, err
	}

	switch op.Op {
	case patchMove:
		return applyMove(root, op, segs)
	case patchAdd, patchSet, patchReplace, patchIncr:
		val, valErr := decodePatchValue(op.Value)
		if valErr != nil {
			return nil, valErr
		}

		return patchNode(root, segs, op.Op, val)
	case patchRemove:
		return patchNode(root, segs, op.Op, nil)
	default:
		return nil, invalidPatchf("unknown operation %q", op.Op)
	}
}

func applyMove(root any, op patchOp, dst []string) (any, error) {
	src, err := patchPath(op.From)
	if err != nil {
		return nil, err
	}

	val, err := patchGet(root, src)
	if err != nil {
		return nil, err
	}

	if root, err = patchNode(root, src, patchRemove, nil); err != nil {
		return nil, err
	}

	return patchNode(root, dst, patchAdd, val)
}

func patchGet(node any, segs []string) (any, error) {
	for _, seg := range segs {
		child, ok := childOf(node, seg)
		if !ok {
			return nil, invalidPatchf("path segment %q does not exist", seg)
		}

		node = child
	}

	return node, nil
}

func childOf(node any, seg string) (any, bool) {
	switch n := node.(type) {
	case map[string]any:
		v, ok := n[seg]

		return v, ok
	case []any:
		i, ok := arrayIndex(seg, len(n))

		if !ok || i >= len(n) {
			return nil, false
		}

		return n[i], true
	default:
		return nil, false
	}
}

// arrayIndex parses seg as an index in [0, upper]; the caller decides whether upper itself is acceptable.
func arrayIndex(seg string, upper int) (int, bool) {
	i, err := strconv.Atoi(seg)
	if err != nil || i < 0 || i > upper {
		return 0, false
	}

	return i, true
}

// patchNode applies op at segs below node and returns the (possibly replaced) node.
func patchNode(node any, segs []string, op string, val any) (any, error) {
	if len(segs) > 1 {
		child, ok := childOf(node, segs[0])
		if !ok {
			return nil, invalidPatchf("path segment %q does not exist", segs[0])
		}

		newChild, err := patchNode(child, segs[1:], op, val)
		if err != nil {
			return nil, err
		}

		return setChild(node, segs[0], newChild), nil
	}

	switch n := node.(type) {
	case map[string]any:
		return patchObject(n, segs[0], op, val)
	case []any:
		return patchArray(n, segs[0], op, val)
	default:
		return nil, invalidPatchf("cannot patch below a scalar at %q", segs[0])
	}
}

func setChild(node any, seg string, child any) any {
	switch n := node.(type) {
	case map[string]any:
		n[seg] = child
	case []any:
		i, _ := arrayIndex(seg, len(n))
		n[i] = child
	}

	return node
}

func patchObject(m map[string]any, key, op string, val any) (any, error) {
	cur, exists := m[key]

	switch op {
	case patchAdd, patchSet:
		m[key] = val
	case patchReplace:
		if !exists {
			return nil, invalidPatchf("replace target %q does not exist", key)
		}

		m[key] = val
	case patchRemove:
		if !exists {
			return nil, invalidPatchf("remove target %q does not exist", key)
		}

		delete(m, key)
	case patchIncr:
		next, err := incrValue(cur, exists, val)
		if err != nil {
			return nil, err
		}

		m[key] = next
	}

	return m, nil
}

func patchArray(a []any, seg, op string, val any) (any, error) {
	if seg == arrayAppend && (op == patchAdd || op == patchSet) {
		return append(a, val), nil
	}

	i, ok := arrayIndex(seg, len(a))
	if !ok {
		return nil, invalidPatchf("invalid array index %q", seg)
	}

	switch op {
	case patchAdd:
		return append(a[:i], append([]any{val}, a[i:]...)...), nil
	case patchSet:
		if i == len(a) {
			return append(a, val), nil
		}

		a[i] = val
	case patchReplace:
		return replaceAt(a, i, val)
	case patchRemove:
		if i >= len(a) {
			return nil, invalidPatchf("array index %d is out of range", i)
		}

		return append(a[:i], a[i+1:]...), nil
	case patchIncr:
		if i >= len(a) {
			return nil, invalidPatchf("array index %d is out of range", i)
		}

		next, err := incrValue(a[i], true, val)
		if err != nil {
			return nil, err
		}

		a[i] = next
	}

	return a, nil
}

func replaceAt(a []any, i int, val any) (any, error) {
	if i >= len(a) {
		return nil, invalidPatchf("array index %d is out of range", i)
	}

	a[i] = val

	return a, nil
}

// incrValue adds delta to cur; an absent object property is created with delta.
func incrValue(cur any, exists bool, delta any) (any, error) {
	d, ok := toNumber(delta)
	if !ok {
		return nil, invalidPatchf("incr requires a numeric value")
	}

	if !exists {
		return d, nil
	}

	c, ok := toNumber(cur)
	if !ok {
		return nil, invalidPatchf("incr target is not a number")
	}

	return addNumbers(c, d)
}

func addNumbers(a, b json.Number) (json.Number, error) {
	x, errX := a.Int64()
	y, errY := b.Int64()

	if errX == nil && errY == nil {
		sum := x + y
		if (x >= 0) != (y >= 0) || (sum >= 0) == (x >= 0) {
			return json.Number(strconv.FormatInt(sum, 10)), nil
		}
	}

	fx, errFX := a.Float64()
	fy, errFY := b.Float64()

	if errFX != nil || errFY != nil || math.IsInf(fx+fy, 0) {
		return "", invalidPatchf("incr result is not a finite number")
	}

	return json.Number(strconv.FormatFloat(fx+fy, 'g', -1, 64)), nil
}

// evalPatchCondition runs a "from c where ..." predicate against one document.
func evalPatchCondition(condition string, info DocumentInfo) (bool, error) {
	query := strings.TrimSpace(condition)
	if len(query) >= len("from") && strings.EqualFold(query[:len("from")], "from") {
		query = "SELECT * " + query
	}

	rows, err := ExecuteQuery(query, nil, []DocumentInfo{info})
	if err != nil {
		return false, err
	}

	return len(rows) > 0, nil
}
