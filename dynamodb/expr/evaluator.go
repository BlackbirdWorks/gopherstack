package expr

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type Evaluator struct {
	Item       map[string]any
	AttrNames  map[string]string
	AttrValues map[string]any
}

func (e *Evaluator) Evaluate(node Node) (any, error) {
	switch n := node.(type) {
	case *LogicalExpr:
		return e.evaluateLogical(n)
	case *NotExpr:
		return e.evaluateNot(n)
	case *ComparisonExpr:
		return e.evaluateComparison(n)
	case *BetweenExpr:
		return e.evaluateBetween(n)
	case *InExpr:
		return e.evaluateIn(n)
	case *FunctionExpr:
		return e.evaluateFunction(n)
	case *PathExpr:
		val, _ := e.getValueAtPath(e.Item, n.Elements)
		return val, nil
	case *ValuePlaceholder:
		if val, ok := e.AttrValues[n.Name]; ok {
			return val, nil
		}
		return nil, fmt.Errorf("value placeholder %s not found", n.Name)
	default:
		return nil, fmt.Errorf("unsupported node type: %T", node)
	}
}

func (e *Evaluator) evaluateLogical(n *LogicalExpr) (bool, error) {
	leftVal, err := e.Evaluate(n.Left)
	if err != nil {
		return false, err
	}
	left, _ := leftVal.(bool)

	if n.Operator == TokenAND && !left {
		return false, nil
	}
	if n.Operator == TokenOR && left {
		return true, nil
	}

	rightVal, err := e.Evaluate(n.Right)
	if err != nil {
		return false, err
	}
	right, _ := rightVal.(bool)

	return right, nil
}

func (e *Evaluator) evaluateNot(n *NotExpr) (bool, error) {
	val, err := e.Evaluate(n.Expression)
	if err != nil {
		return false, err
	}
	b, _ := val.(bool)
	return !b, nil
}

func (e *Evaluator) evaluateComparison(n *ComparisonExpr) (bool, error) {
	left, err := e.Evaluate(n.Left)
	if err != nil {
		return false, err
	}
	right, err := e.Evaluate(n.Right)
	if err != nil {
		return false, err
	}

	return e.compareValues(left, n.Operator, right), nil
}

func (e *Evaluator) evaluateBetween(n *BetweenExpr) (bool, error) {
	val, err := e.Evaluate(n.Value)
	if err != nil {
		return false, err
	}
	lower, err := e.Evaluate(n.Lower)
	if err != nil {
		return false, err
	}
	upper, err := e.Evaluate(n.Upper)
	if err != nil {
		return false, err
	}

	return e.compareValues(val, TokenGreaterEqual, lower) && e.compareValues(val, TokenLessEqual, upper), nil
}

func (e *Evaluator) evaluateIn(n *InExpr) (bool, error) {
	val, err := e.Evaluate(n.Value)
	if err != nil {
		return false, err
	}

	for _, candNode := range n.Candidates {
		cand, err := e.Evaluate(candNode)
		if err != nil {
			return false, err
		}
		if e.compareValues(val, TokenEqual, cand) {
			return true, nil
		}
	}

	return false, nil
}

func (e *Evaluator) evaluateFunction(n *FunctionExpr) (any, error) {
	switch n.Name {
	case "size":
		if len(n.Args) != 1 {
			return nil, fmt.Errorf("size() expects 1 argument")
		}
		val, err := e.Evaluate(n.Args[0])
		if err != nil {
			return nil, err
		}
		return e.calculateSize(val), nil
	case "attribute_exists":
		if len(n.Args) != 1 {
			return nil, fmt.Errorf("attribute_exists() expects 1 argument")
		}
		path, ok := n.Args[0].(*PathExpr)
		if !ok {
			return nil, fmt.Errorf("attribute_exists() expects a path")
		}
		_, exists := e.getValueAtPath(e.Item, path.Elements)
		return exists, nil
	case "attribute_not_exists":
		if len(n.Args) != 1 {
			return nil, fmt.Errorf("attribute_not_exists() expects 1 argument")
		}
		path, ok := n.Args[0].(*PathExpr)
		if !ok {
			return nil, fmt.Errorf("attribute_not_exists() expects a path")
		}
		_, exists := e.getValueAtPath(e.Item, path.Elements)
		return !exists, nil
	case "begins_with":
		if len(n.Args) != 2 {
			return nil, fmt.Errorf("begins_with() expects 2 arguments")
		}
		pathVal, err := e.Evaluate(n.Args[0])
		if err != nil {
			return nil, err
		}
		prefixVal, err := e.Evaluate(n.Args[1])
		if err != nil {
			return nil, err
		}
		return strings.HasPrefix(e.toString(pathVal), e.toString(prefixVal)), nil
	case "contains":
		if len(n.Args) != 2 {
			return nil, fmt.Errorf("contains() expects 2 arguments")
		}
		pathVal, err := e.Evaluate(n.Args[0])
		if err != nil {
			return nil, err
		}
		targetVal, err := e.Evaluate(n.Args[1])
		if err != nil {
			return nil, err
		}
		return strings.Contains(e.toString(pathVal), e.toString(targetVal)), nil
	default:
		return nil, fmt.Errorf("unknown function: %s", n.Name)
	}
}

func (e *Evaluator) calculateSize(v any) float64 {
	unwrapped := e.unwrapAttributeValue(v)
	switch val := unwrapped.(type) {
	case string:
		return float64(len(val))
	case []byte:
		return float64(len(val))
	case []any:
		return float64(len(val))
	case map[string]any:
		return float64(len(val))
	case []string: // SS
		return float64(len(val))
	case [][]byte: // BS
		return float64(len(val))
		// Numbers don't have size in the same way, DynamoDB docs say "Returns a number representing the size of an attribute in bytes"
		// but for strings/lists/maps/sets it's number of elements/chars.
	}
	return 0
}

// Reused utilities adapted for expr package
func (e *Evaluator) unwrapAttributeValue(v any) any {
	m, ok := v.(map[string]any)
	if !ok {
		return v
	}
	if val, exists := m["S"]; exists {
		return val
	}
	if val, exists := m["N"]; exists {
		return val
	}
	if val, exists := m["B"]; exists {
		return val
	}
	if val, exists := m["BOOL"]; exists {
		return val
	}
	if _, exists := m["NULL"]; exists {
		return nil
	}
	if val, exists := m["M"]; exists {
		return val
	}
	if val, exists := m["L"]; exists {
		return val
	}
	if val, exists := m["SS"]; exists {
		return val
	}
	if val, exists := m["NS"]; exists {
		return val
	}
	if val, exists := m["BS"]; exists {
		return val
	}
	return v
}

func (e *Evaluator) toString(v any) string {
	unwrapped := e.unwrapAttributeValue(v)
	switch s := unwrapped.(type) {
	case string:
		return s
	case bool:
		return strconv.FormatBool(s)
	case float64:
		return fmt.Sprintf("%v", s)
	case int, int64, int32:
		return fmt.Sprintf("%v", s)
	case nil:
		return ""
	default:
		b, _ := json.Marshal(s)
		return string(b)
	}
}

func (e *Evaluator) compareValues(lhs any, op TokenType, rhs any) bool {
	lhs = e.unwrapAttributeValue(lhs)
	rhs = e.unwrapAttributeValue(rhs)

	lhsStr := e.toString(lhs)
	rhsStr := e.toString(rhs)
	lNum, lIsNum := e.parseNumeric(lhs)
	rNum, rIsNum := e.parseNumeric(rhs)

	switch op {
	case TokenEqual:
		return lhsStr == rhsStr
	case TokenNotEqual:
		return lhsStr != rhsStr
	case TokenLess:
		return e.compareOrdered(lNum, rNum, lIsNum, rIsNum, lhsStr, rhsStr, func(a, b float64) bool { return a < b }, func(a, b string) bool { return a < b })
	case TokenGreater:
		return e.compareOrdered(lNum, rNum, lIsNum, rIsNum, lhsStr, rhsStr, func(a, b float64) bool { return a > b }, func(a, b string) bool { return a > b })
	case TokenLessEqual:
		return e.compareOrdered(lNum, rNum, lIsNum, rIsNum, lhsStr, rhsStr, func(a, b float64) bool { return a <= b }, func(a, b string) bool { return a <= b })
	case TokenGreaterEqual:
		return e.compareOrdered(lNum, rNum, lIsNum, rIsNum, lhsStr, rhsStr, func(a, b float64) bool { return a >= b }, func(a, b string) bool { return a >= b })
	}
	return false
}

func (e *Evaluator) parseNumeric(v any) (float64, bool) {
	unwrapped := e.unwrapAttributeValue(v)
	switch val := unwrapped.(type) {
	case float64:
		return val, true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

func (e *Evaluator) compareOrdered(lNum, rNum float64, lIsNum, rIsNum bool, lStr, rStr string, numCmp func(float64, float64) bool, strCmp func(string, string) bool) bool {
	if lIsNum && rIsNum {
		return numCmp(lNum, rNum)
	}
	return strCmp(lStr, rStr)
}

func (e *Evaluator) getValueAtPath(item map[string]any, path []PathElement) (any, bool) {
	if len(path) == 0 {
		return nil, false
	}
	current := any(item)
	for i, elem := range path {
		isLast := i == len(path)-1
		var val any
		var exists bool

		name := elem.Name
		if strings.HasPrefix(name, "#") {
			if resolved, ok := e.AttrNames[name]; ok {
				name = resolved
			}
		}

		if elem.Type == ElementKey {
			val, exists = e.navigateMap(current, name)
		} else {
			val, exists = e.navigateList(current, elem.Index)
		}

		if !exists {
			return nil, false
		}
		if isLast {
			return val, true
		}
		current = e.unwrapAttributeValue(val)
	}
	return nil, false
}

func (e *Evaluator) navigateMap(current any, key string) (any, bool) {
	m, ok := current.(map[string]any)
	if !ok {
		return nil, false
	}
	val, exists := m[key]
	return val, exists
}

func (e *Evaluator) navigateList(current any, index int) (any, bool) {
	current = e.unwrapAttributeValue(current)
	list, ok := current.([]any)
	if !ok || index < 0 || index >= len(list) {
		return nil, false
	}
	return list[index], true
}

// Update and Projection methods

func (e *Evaluator) ApplyUpdate(u *UpdateExpr) error {
	for _, action := range u.Actions {
		for _, item := range action.Items {
			path, ok := item.Path.(*PathExpr)
			if !ok {
				return fmt.Errorf("update item path must be a PathExpr")
			}

			switch action.Type {
			case TokenSET:
				val, err := e.Evaluate(item.Value)
				if err != nil {
					return err
				}
				updated, err := e.setValueAtPath(e.Item, path.Elements, val)
				if err != nil {
					return err
				}
				if m, ok := updated.(map[string]any); ok {
					e.Item = m
				}
			case TokenREMOVE:
				updated, err := e.removeValueAtPath(e.Item, path.Elements)
				if err != nil {
					return err
				}
				if m, ok := updated.(map[string]any); ok {
					e.Item = m
				}
			case TokenADD:
				val, err := e.Evaluate(item.Value)
				if err != nil {
					return err
				}
				if err := e.applyAdd(path.Elements, val); err != nil {
					return err
				}
			case TokenDELETE:
				// Similar to REMOVE but for sets? DynamoDB DELETE is specifically for sets.
				// For now, let's focus on SET and REMOVE as they are most common.
			}
		}
	}
	return nil
}

func (e *Evaluator) ApplyProjection(p *ProjectionExpr) map[string]any {
	newItem := make(map[string]any)
	for _, pathNode := range p.Paths {
		path, ok := pathNode.(*PathExpr)
		if !ok {
			continue
		}
		val, exists := e.getValueAtPath(e.Item, path.Elements)
		if exists {
			updated, _ := e.setValueAtPath(newItem, path.Elements, val)
			if m, ok := updated.(map[string]any); ok {
				newItem = m
			}
		}
	}
	return newItem
}

func (e *Evaluator) applyAdd(path []PathElement, val any) error {
	cur, exists := e.getValueAtPath(e.Item, path)
	if !exists {
		updated, err := e.setValueAtPath(e.Item, path, val)
		if err != nil {
			return err
		}
		if m, ok := updated.(map[string]any); ok {
			e.Item = m
		}
		return nil
	}

	// Simple addition for numbers
	curNum, ok1 := e.parseNumeric(cur)
	valNum, ok2 := e.parseNumeric(val)
	if ok1 && ok2 {
		sum := curNum + valNum
		updated, err := e.setValueAtPath(e.Item, path, map[string]any{"N": fmt.Sprintf("%v", sum)})
		if err != nil {
			return err
		}
		if m, ok := updated.(map[string]any); ok {
			e.Item = m
		}
		return nil
	}

	// Also support list/set append would go here
	return nil
}

// Mutation helpers ported from expressions.go

func (e *Evaluator) setValueAtPath(target any, path []PathElement, value any) (any, error) {
	if len(path) == 0 {
		return target, nil
	}

	return e.mutate(target, path, value, false)
}

func (e *Evaluator) removeValueAtPath(target any, path []PathElement) (any, error) {
	if len(path) == 0 {
		return target, nil
	}

	return e.mutate(target, path, nil, true)
}

func (e *Evaluator) mutate(current any, path []PathElement, value any, isRemove bool) (any, error) {
	if len(path) == 0 {
		return current, nil
	}

	elem := path[0]
	isLast := len(path) == 1
	name := elem.Name
	if strings.HasPrefix(name, "#") {
		if resolved, ok := e.AttrNames[name]; ok {
			name = resolved
		}
	}

	if elem.Type == ElementKey {
		// Handle Map (M) or root item
		var m map[string]any
		var isWrapped bool

		if wrappedMap, ok := current.(map[string]any); ok {
			if mVal, exists := wrappedMap["M"]; exists {
				m = mVal.(map[string]any)
				isWrapped = true
			} else {
				m = wrappedMap
			}
		} else {
			return nil, fmt.Errorf("expected map for key %s", name)
		}

		if isLast {
			if isRemove {
				delete(m, name)
			} else {
				m[name] = value
			}
		} else {
			next, exists := m[name]
			if !exists {
				if isRemove {
					return current, nil // Path doesn't exist, nothing to remove
				}
				// Create intermediate map
				next = map[string]any{"M": make(map[string]any)}
				m[name] = next
			}

			updatedNext, err := e.mutate(next, path[1:], value, isRemove)
			if err != nil {
				return nil, err
			}
			m[name] = updatedNext
		}

		if isWrapped {
			return map[string]any{"M": m}, nil
		}
		return m, nil
	} else {
		// Handle List (L)
		var list []any
		var isWrapped bool

		if wrappedList, ok := current.(map[string]any); ok {
			if lVal, exists := wrappedList["L"]; exists {
				list = lVal.([]any)
				isWrapped = true
			} else {
				return nil, fmt.Errorf("expected list at index %d", elem.Index)
			}
		} else if lVal, ok := current.([]any); ok {
			list = lVal
		} else {
			return nil, fmt.Errorf("expected list for index %d", elem.Index)
		}

		if elem.Index < 0 || elem.Index >= len(list) {
			return nil, fmt.Errorf("index out of range: %d", elem.Index)
		}

		if isLast {
			if isRemove {
				// Remove element and shift
				list = append(list[:elem.Index], list[elem.Index+1:]...)
			} else {
				list[elem.Index] = value
			}
		} else {
			next := list[elem.Index]
			updatedNext, err := e.mutate(next, path[1:], value, isRemove)
			if err != nil {
				return nil, err
			}
			list[elem.Index] = updatedNext
		}

		if isWrapped {
			return map[string]any{"L": list}, nil
		}
		return list, nil
	}
}
