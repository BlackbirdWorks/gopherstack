package expr

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Sentinel errors for evaluator.
var (
	ErrValuePlaceholderNotFound    = errors.New("value placeholder not found")
	ErrUnsupportedNodeType         = errors.New("unsupported node type")
	ErrSizeExpectsOneArg           = errors.New("size() expects 1 argument")
	ErrAttributeExistsExpectsPath  = errors.New("attribute_exists() expects a path")
	ErrAttributeExistsExpectsOne   = errors.New("attribute_exists() expects 1 argument")
	ErrAttributeNExistsExpectsPath = errors.New("attribute_not_exists() expects a path")
	ErrAttributeNExistsExpectsOne  = errors.New("attribute_not_exists() expects 1 argument")
	ErrBeginsWithExpectsTwo        = errors.New("begins_with() expects 2 arguments")
	ErrContainsExpectsTwo          = errors.New("contains() expects 2 arguments")
	ErrAttributeTypeExpectsTwo     = errors.New("attribute_type() expects 2 arguments")
	ErrListAppendExpectsTwo        = errors.New("list_append() expects 2 arguments")
	ErrUnknownFunction             = errors.New("unknown function")
	ErrUpdatePathMustBePathExpr    = errors.New("update item path must be a PathExpr")
	ErrExpectedMapForKey           = errors.New("expected map for key")
	ErrExpectedListAtIndex         = errors.New("expected list at index")
	ErrExpectedListForIndex        = errors.New("expected list for index")
	ErrIndexOutOfRange             = errors.New("index out of range")
	ErrExpectedMapForM             = errors.New("expected map for M")
	ErrExpectedListForL            = errors.New("expected list for L")
)

// twoArgs is the expected argument count for two-argument functions.
const twoArgs = 2

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
		// Check if this is an arithmetic operation instead of a comparison
		if n.Operator == TokenPlus || n.Operator == TokenMinus {
			return e.evaluateArithmetic(n)
		}
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

		return nil, fmt.Errorf("%w: %s", ErrValuePlaceholderNotFound, n.Name)
	default:
		return nil, fmt.Errorf("%w: %T", ErrUnsupportedNodeType, node)
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

// evaluateArithmetic evaluates arithmetic operations (+ and -) for UPDATE expressions.
// Returns the result in DynamoDB attribute value format (e.g., map[string]any{"N": "5"}).
func (e *Evaluator) evaluateArithmetic(n *ComparisonExpr) (any, error) {
	left, err := e.Evaluate(n.Left)
	if err != nil {
		return nil, err
	}
	right, err := e.Evaluate(n.Right)
	if err != nil {
		return nil, err
	}

	// Extract numeric values from DynamoDB attribute format
	leftNum := e.toNumber(left)
	rightNum := e.toNumber(right)

	var result float64
	switch n.Operator {
	case TokenPlus:
		result = leftNum + rightNum
	case TokenMinus:
		result = leftNum - rightNum
	default:
		return nil, fmt.Errorf("unsupported arithmetic operator: %v", n.Operator)
	}

	// Return in DynamoDB attribute value format
	return map[string]any{"N": fmt.Sprintf("%g", result)}, nil
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
		cand, candErr := e.Evaluate(candNode)
		if candErr != nil {
			return false, candErr
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
		return e.evalSizeFunc(n)
	case "attribute_exists":
		return e.evalAttributeExistsFunc(n)
	case "attribute_not_exists":
		return e.evalAttributeNotExistsFunc(n)
	case "begins_with":
		return e.evalBeginsWithFunc(n)
	case "contains":
		return e.evalContainsFunc(n)
	case "attribute_type":
		return e.evalAttributeTypeFunc(n)
	case "if_not_exists":
		return e.evalIfNotExistsFunc(n)
	case "list_append":
		return e.evalListAppendFunc(n)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownFunction, n.Name)
	}
}

func (e *Evaluator) evalSizeFunc(n *FunctionExpr) (any, error) {
	if len(n.Args) != 1 {
		return nil, ErrSizeExpectsOneArg
	}
	val, err := e.Evaluate(n.Args[0])
	if err != nil {
		return nil, err
	}

	return e.calculateSize(val), nil
}

func (e *Evaluator) evalAttributeExistsFunc(n *FunctionExpr) (any, error) {
	if len(n.Args) != 1 {
		return nil, ErrAttributeExistsExpectsOne
	}
	path, ok := n.Args[0].(*PathExpr)
	if !ok {
		return nil, ErrAttributeExistsExpectsPath
	}
	_, exists := e.getValueAtPath(e.Item, path.Elements)

	return exists, nil
}

func (e *Evaluator) evalAttributeNotExistsFunc(n *FunctionExpr) (any, error) {
	if len(n.Args) != 1 {
		return nil, ErrAttributeNExistsExpectsOne
	}
	path, ok := n.Args[0].(*PathExpr)
	if !ok {
		return nil, ErrAttributeNExistsExpectsPath
	}
	_, exists := e.getValueAtPath(e.Item, path.Elements)

	return !exists, nil
}

func (e *Evaluator) evalBeginsWithFunc(n *FunctionExpr) (any, error) {
	if len(n.Args) != twoArgs {
		return nil, ErrBeginsWithExpectsTwo
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
}

func (e *Evaluator) evalContainsFunc(n *FunctionExpr) (any, error) {
	if len(n.Args) != twoArgs {
		return nil, ErrContainsExpectsTwo
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
}

// evalIfNotExistsFunc implements the if_not_exists() function for UPDATE expressions.
// if_not_exists(path, value) returns the value at path if it exists, otherwise returns value.
func (e *Evaluator) evalIfNotExistsFunc(n *FunctionExpr) (any, error) {
	if len(n.Args) != twoArgs {
		return nil, fmt.Errorf("if_not_exists expects exactly 2 arguments, got %d", len(n.Args))
	}

	// Try to evaluate the path argument
	pathVal, err := e.Evaluate(n.Args[0])

	// If path doesn't exist or evaluation fails, return the default value
	if err != nil || pathVal == nil {
		return e.Evaluate(n.Args[1])
	}

	// Path exists, return its value
	return pathVal, nil
}

// evalAttributeTypeFunc implements the attribute_type() function.
// attribute_type(path, type) returns true if the attribute at path is of the specified type.
// Valid types: S, SS, N, NS, B, BS, BOOL, NULL, L, M
func (e *Evaluator) evalAttributeTypeFunc(n *FunctionExpr) (any, error) {
	if len(n.Args) != twoArgs {
		return nil, ErrAttributeTypeExpectsTwo
	}

	pathVal, err := e.Evaluate(n.Args[0])
	if err != nil || pathVal == nil {
		return false, nil
	}

	expectedType, err := e.Evaluate(n.Args[1])
	if err != nil {
		return nil, err
	}

	expectedTypeStr := e.toString(expectedType)

	// Get the DynamoDB type from the attribute value
	if attrMap, ok := pathVal.(map[string]any); ok {
		for attrType := range attrMap {
			return attrType == expectedTypeStr, nil
		}
	}

	return false, nil
}

// evalListAppendFunc implements the list_append() function for UPDATE expressions.
// list_append(list1, list2) appends list2 to list1 and returns the combined list.
func (e *Evaluator) evalListAppendFunc(n *FunctionExpr) (any, error) {
	if len(n.Args) != twoArgs {
		return nil, ErrListAppendExpectsTwo
	}

	list1Val, err := e.Evaluate(n.Args[0])
	if err != nil {
		return nil, err
	}

	list2Val, err := e.Evaluate(n.Args[1])
	if err != nil {
		return nil, err
	}

	// Extract the L (list) values from DynamoDB format
	var list1, list2 []any

	if list1Map, ok := list1Val.(map[string]any); ok {
		if l, ok := list1Map["L"].([]any); ok {
			list1 = l
		}
	}

	if list2Map, ok := list2Val.(map[string]any); ok {
		if l, ok := list2Map["L"].([]any); ok {
			list2 = l
		}
	}

	// Append and return in DynamoDB format
	combined := append(list1, list2...)
	return map[string]any{"L": combined}, nil
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
		// Numbers don't have size in the same way; for strings/lists/maps/sets it's number of elements/chars.
	}

	return 0
}

// Reused utilities adapted for expr package.
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

// toNumber converts a DynamoDB attribute value to a float64.
func (e *Evaluator) toNumber(v any) float64 {
	unwrapped := e.unwrapAttributeValue(v)
	switch n := unwrapped.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case int32:
		return float64(n)
	case string:
		// DynamoDB numbers are stored as strings
		f, err := strconv.ParseFloat(n, 64)
		if err != nil {
			return 0
		}
		return f
	default:
		return 0
	}
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

	switch op { //nolint:exhaustive // Only comparison operators expected
	case TokenEqual:
		return lhsStr == rhsStr
	case TokenNotEqual:
		return lhsStr != rhsStr
	case TokenLess:
		return e.compareOrdered(
			lNum,
			rNum,
			lIsNum,
			rIsNum,
			lhsStr,
			rhsStr,
			func(a, b float64) bool { return a < b },
			func(a, b string) bool { return a < b },
		)
	case TokenGreater:
		return e.compareOrdered(
			lNum,
			rNum,
			lIsNum,
			rIsNum,
			lhsStr,
			rhsStr,
			func(a, b float64) bool { return a > b },
			func(a, b string) bool { return a > b },
		)
	case TokenLessEqual:
		return e.compareOrdered(
			lNum,
			rNum,
			lIsNum,
			rIsNum,
			lhsStr,
			rhsStr,
			func(a, b float64) bool { return a <= b },
			func(a, b string) bool { return a <= b },
		)
	case TokenGreaterEqual:
		return e.compareOrdered(
			lNum,
			rNum,
			lIsNum,
			rIsNum,
			lhsStr,
			rhsStr,
			func(a, b float64) bool { return a >= b },
			func(a, b string) bool { return a >= b },
		)
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
		if f, parseErr := strconv.ParseFloat(val, 64); parseErr == nil {
			return f, true
		}
	}

	return 0, false
}

func (e *Evaluator) compareOrdered(
	lNum, rNum float64,
	lIsNum, rIsNum bool,
	lStr, rStr string,
	numCmp func(float64, float64) bool,
	strCmp func(string, string) bool,
) bool {
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
		if err := e.applyUpdateAction(action); err != nil {
			return err
		}
	}

	return nil
}

func (e *Evaluator) applyUpdateAction(action UpdateAction) error {
	for _, item := range action.Items {
		path, ok := item.Path.(*PathExpr)
		if !ok {
			return ErrUpdatePathMustBePathExpr
		}

		if err := e.applyUpdateItem(action.Type, path, item); err != nil {
			return err
		}
	}

	return nil
}

func (e *Evaluator) applyUpdateItem(actionType TokenType, path *PathExpr, item UpdateItem) error {
	switch actionType { //nolint:exhaustive // Only update action types are expected
	case TokenSET:
		return e.applySet(path, item)
	case TokenREMOVE:
		return e.applyRemove(path)
	case TokenADD:
		return e.applyAddAction(path, item)
	case TokenDELETE:
		return e.applyDeleteAction(path, item)
	}

	return nil
}

func (e *Evaluator) applySet(path *PathExpr, item UpdateItem) error {
	val, err := e.Evaluate(item.Value)
	if err != nil {
		return err
	}
	updated, err := e.setValueAtPath(e.Item, path.Elements, val)
	if err != nil {
		return err
	}
	if m, isMap := updated.(map[string]any); isMap {
		e.Item = m
	}

	return nil
}

func (e *Evaluator) applyRemove(path *PathExpr) error {
	updated, err := e.removeValueAtPath(e.Item, path.Elements)
	if err != nil {
		return err
	}
	if m, isMap := updated.(map[string]any); isMap {
		e.Item = m
	}

	return nil
}

func (e *Evaluator) applyAddAction(path *PathExpr, item UpdateItem) error {
	val, err := e.Evaluate(item.Value)
	if err != nil {
		return err
	}

	return e.applyAdd(path.Elements, val)
}

func (e *Evaluator) applyDeleteAction(path *PathExpr, item UpdateItem) error {
	// DELETE removes values from a set
	val, err := e.Evaluate(item.Value)
	if err != nil {
		return err
	}

	return e.applyDelete(path.Elements, val)
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
			if m, isMap := updated.(map[string]any); isMap {
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

func (e *Evaluator) applyDelete(path []PathElement, val any) error {
	// DELETE removes elements from a set (SS, NS, BS)
	cur, exists := e.getValueAtPath(e.Item, path)
	if !exists {
		// If the attribute doesn't exist, DELETE has no effect
		return nil
	}

	// Get the set to remove from the current value
	valMap, ok := val.(map[string]any)
	if !ok {
		return fmt.Errorf("DELETE value must be a set")
	}

	// Handle string sets (SS)
	if ssToRemove, ok := valMap["SS"]; ok {
		toRemove, ok := ssToRemove.([]string)
		if !ok {
			return fmt.Errorf("SS value must be []string")
		}

		curMap, ok := cur.(map[string]any)
		if !ok {
			return fmt.Errorf("current value must be a map")
		}

		if ssCurrent, ok := curMap["SS"]; ok {
			currentSet, ok := ssCurrent.([]string)
			if !ok {
				return fmt.Errorf("current SS value must be []string")
			}

			// Remove items from the set
			newSet := make([]string, 0)
			for _, item := range currentSet {
				shouldRemove := false
				for _, toRem := range toRemove {
					if item == toRem {
						shouldRemove = true
						break
					}
				}
				if !shouldRemove {
					newSet = append(newSet, item)
				}
			}

			// Update the item
			updated, err := e.setValueAtPath(e.Item, path, map[string]any{"SS": newSet})
			if err != nil {
				return err
			}
			if m, ok := updated.(map[string]any); ok {
				e.Item = m
			}
		}
	}

	// Handle number sets (NS)
	if nsToRemove, ok := valMap["NS"]; ok {
		toRemove, ok := nsToRemove.([]string)
		if !ok {
			return fmt.Errorf("NS value must be []string")
		}

		curMap, ok := cur.(map[string]any)
		if !ok {
			return fmt.Errorf("current value must be a map")
		}

		if nsCurrent, ok := curMap["NS"]; ok {
			currentSet, ok := nsCurrent.([]string)
			if !ok {
				return fmt.Errorf("current NS value must be []string")
			}

			// Remove items from the set
			newSet := make([]string, 0)
			for _, item := range currentSet {
				shouldRemove := false
				for _, toRem := range toRemove {
					if item == toRem {
						shouldRemove = true
						break
					}
				}
				if !shouldRemove {
					newSet = append(newSet, item)
				}
			}

			// Update the item
			updated, err := e.setValueAtPath(e.Item, path, map[string]any{"NS": newSet})
			if err != nil {
				return err
			}
			if m, ok := updated.(map[string]any); ok {
				e.Item = m
			}
		}
	}

	// TODO: Handle binary sets (BS) similarly if needed

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
		return e.mutateMap(current, path, name, isLast, value, isRemove)
	}

	return e.mutateList(current, path, elem, isLast, value, isRemove)
}

func (e *Evaluator) mutateMap(
	current any,
	path []PathElement,
	name string,
	isLast bool,
	value any,
	isRemove bool,
) (any, error) {
	var m map[string]any
	var isWrapped bool

	wrappedMap, isMap := current.(map[string]any)
	if !isMap {
		return nil, fmt.Errorf("%w: %s", ErrExpectedMapForKey, name)
	}

	if mVal, exists := wrappedMap["M"]; exists {
		if mValMap, ok := mVal.(map[string]any); ok {
			m = mValMap
			isWrapped = true
		} else {
			return nil, fmt.Errorf("%w: got %T", ErrExpectedMapForM, mVal)
		}
	} else {
		m = wrappedMap
	}

	if isLast {
		if isRemove {
			delete(m, name)
		} else {
			m[name] = value
		}
	} else {
		if err := e.mutateMapNested(m, path, name, value, isRemove); err != nil {
			return nil, err
		}
	}

	if isWrapped {
		return map[string]any{"M": m}, nil
	}

	return m, nil
}

func (e *Evaluator) mutateMapNested(
	m map[string]any,
	path []PathElement,
	name string,
	value any,
	isRemove bool,
) error {
	next, exists := m[name]
	if !exists {
		if isRemove {
			return nil // Path doesn't exist, nothing to remove
		}
		// Create intermediate map
		next = map[string]any{"M": make(map[string]any)}
		m[name] = next
	}

	updatedNext, err := e.mutate(next, path[1:], value, isRemove)
	if err != nil {
		return err
	}
	m[name] = updatedNext

	return nil
}

func (e *Evaluator) mutateList(
	current any,
	path []PathElement,
	elem PathElement,
	isLast bool,
	value any,
	isRemove bool,
) (any, error) {
	list, isWrapped, err := e.resolveList(current, elem.Index)
	if err != nil {
		return nil, err
	}

	if isLast {
		list = e.mutateListAtIndex(list, elem.Index, value, isRemove)
	} else {
		next := list[elem.Index]
		updatedNext, mutErr := e.mutate(next, path[1:], value, isRemove)
		if mutErr != nil {
			return nil, mutErr
		}
		list[elem.Index] = updatedNext
	}

	if isWrapped {
		return map[string]any{"L": list}, nil
	}

	return list, nil
}

func (e *Evaluator) resolveList(current any, index int) ([]any, bool, error) {
	var list []any
	var isWrapped bool

	switch v := current.(type) {
	case map[string]any:
		lVal, exists := v["L"]
		if !exists {
			return nil, false, fmt.Errorf("%w: %d", ErrExpectedListAtIndex, index)
		}
		if lValList, ok := lVal.([]any); ok {
			list = lValList
			isWrapped = true
		} else {
			return nil, false, fmt.Errorf("%w: got %T", ErrExpectedListForL, lVal)
		}
	case []any:
		list = v
	default:
		return nil, false, fmt.Errorf("%w: %d", ErrExpectedListForIndex, index)
	}

	if index < 0 || index >= len(list) {
		return nil, false, fmt.Errorf("%w: %d", ErrIndexOutOfRange, index)
	}

	return list, isWrapped, nil
}

func (e *Evaluator) mutateListAtIndex(list []any, index int, value any, isRemove bool) []any {
	if isRemove {
		// Remove element and shift
		return append(list[:index], list[index+1:]...)
	}
	list[index] = value

	return list
}
