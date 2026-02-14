package dynamodb

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

const (
	comparisonOpEqual              = "="
	comparisonOpNotEqual           = "<>"
	comparisonOpLessThan           = "<"
	comparisonOpGreaterThan        = ">"
	comparisonOpLessThanOrEqual    = "<="
	comparisonOpGreaterThanOrEqual = ">="
)

const (
	expectedPartsBetween = 2
	expectedPartsRange   = 2
	expectedSetKVParts   = 2 // For SET expressions, key=value
)

// evaluateExpression is a naive implementation of DynamoDB expression evaluation.
// It supports basic comparisons and some functions.
// This is NOT a full parser; it uses simple string splitting/tokenizing which is fragile but sufficient for V1.
func evaluateExpression(
	expression string,
	item map[string]any,
	attrValues map[string]any,
	attrNames map[string]string,
) (bool, error) {
	if expression == "" {
		return true, nil
	}

	// Split by " OR " first (lower precedence), then by " AND " (higher precedence).
	for orPart := range strings.SplitSeq(expression, " OR ") {
		match, err := evaluateANDGroup(orPart, item, attrValues, attrNames)
		if err != nil {
			return false, err
		}

		if match {
			return true, nil
		}
	}

	return false, nil
}

// splitANDConditions splits an expression by " AND " while preserving BETWEEN ... AND ... clauses.
func splitANDConditions(expression string) []string {
	rawParts := strings.Split(expression, " AND ")
	conditions := make([]string, 0, len(rawParts))

	for i := 0; i < len(rawParts); i++ {
		part := rawParts[i]
		if strings.Contains(part, " BETWEEN ") && i+1 < len(rawParts) {
			part = part + " AND " + rawParts[i+1]
			i++
		}

		conditions = append(conditions, part)
	}

	return conditions
}

// evaluateANDGroup evaluates all AND-connected conditions and returns true only if all match.
func evaluateANDGroup(
	orPart string,
	item map[string]any,
	attrValues map[string]any,
	attrNames map[string]string,
) (bool, error) {
	for _, cond := range splitANDConditions(orPart) {
		cond = strings.TrimSpace(cond)
		negate := strings.HasPrefix(cond, "NOT ")

		if negate {
			cond = strings.TrimPrefix(cond, "NOT ")
		}

		if strings.HasPrefix(cond, "(") && strings.HasSuffix(cond, ")") {
			cond = cond[1 : len(cond)-1]
		}

		result, err := evaluateCondition(cond, item, attrValues, attrNames)
		if err != nil {
			return false, err
		}

		if negate {
			result = !result
		}

		if !result {
			return false, nil
		}
	}

	return true, nil
}

func evaluateCondition(
	cond string,
	item map[string]any,
	attrValues map[string]any,
	attrNames map[string]string,
) (bool, error) {
	// 1. Functions
	if isFunction(cond) {
		return evaluateFunction(cond, item, attrValues, attrNames)
	}

	if strings.Contains(cond, " BETWEEN ") {
		return evaluateBetween(cond, item, attrValues, attrNames)
	}

	// 2. Comparisons
	return evaluateComparison(cond, item, attrValues, attrNames)
}

func isFunction(cond string) bool {
	return strings.HasPrefix(cond, "attribute_exists(") ||
		strings.HasPrefix(cond, "attribute_not_exists(") ||
		strings.HasPrefix(cond, "begins_with(") ||
		strings.HasPrefix(cond, "contains(")
}

func evaluateFunction(
	cond string,
	item map[string]any,
	attrValues map[string]any,
	attrNames map[string]string,
) (bool, error) {
	switch {
	case strings.HasPrefix(cond, "attribute_exists("):
		path := extractFunctionArg(cond)
		pathElems, err := parseAndResolvePath(path, attrNames)
		if err != nil {
			return false, err
		}

		_, exists := getValueAtPath(item, pathElems)

		return exists, nil
	case strings.HasPrefix(cond, "attribute_not_exists("):
		path := extractFunctionArg(cond)
		pathElems, err := parseAndResolvePath(path, attrNames)
		if err != nil {
			return false, err
		}

		_, exists := getValueAtPath(item, pathElems)

		return !exists, nil
	case strings.HasPrefix(cond, "begins_with("):
		return evaluateBeginsWith(cond, item, attrValues, attrNames)
	case strings.HasPrefix(cond, "contains("):
		return evaluateContains(cond, item, attrValues, attrNames)
	default:
		return false, NewValidationException(fmt.Sprintf("unknown function: %s", cond))
	}
}

func evaluateBeginsWith(
	cond string,
	item map[string]any,
	attrValues map[string]any,
	attrNames map[string]string,
) (bool, error) {
	const expectedArgs = 2
	args := extractFunctionArgs(cond)
	if len(args) != expectedArgs {
		return false, NewValidationException(fmt.Sprintf("invalid begins_with args: %s", cond))
	}

	path := args[0]
	targetVal := resolveValue(args[1], attrValues)

	pathElems, err := parseAndResolvePath(path, attrNames)
	if err != nil {
		return false, err
	}

	val, exists := getValueAtPath(item, pathElems)
	if !exists {
		return false, nil
	}

	val = unwrapAttributeValue(val)
	targetVal = unwrapAttributeValue(targetVal)

	return strings.HasPrefix(toString(val), toString(targetVal)), nil
}

func evaluateContains(
	cond string,
	item map[string]any,
	attrValues map[string]any,
	attrNames map[string]string,
) (bool, error) {
	const expectedArgs = 2
	args := extractFunctionArgs(cond)
	if len(args) != expectedArgs {
		return false, NewValidationException(
			fmt.Sprintf("invalid contains args: %v (len=%d) from cond: %s", args, len(args), cond))
	}

	path := args[0]
	targetVal := resolveValue(args[1], attrValues)

	pathElems, err := parseAndResolvePath(path, attrNames)
	if err != nil {
		return false, err
	}

	val, exists := getValueAtPath(item, pathElems)
	if !exists {
		return false, nil
	}

	val = unwrapAttributeValue(val)
	targetVal = unwrapAttributeValue(targetVal)

	return strings.Contains(toString(val), toString(targetVal)), nil
}

func evaluateBetween(
	cond string,
	item map[string]any,
	attrValues map[string]any,
	attrNames map[string]string,
) (bool, error) {
	parts := strings.Split(cond, " BETWEEN ")
	if len(parts) != expectedPartsBetween {
		return false, NewValidationException("invalid BETWEEN syntax")
	}

	path := strings.TrimSpace(parts[0])
	rangePart := strings.TrimSpace(parts[1])
	rangeParts := strings.Split(rangePart, " AND ")
	if len(rangeParts) != expectedPartsRange {
		return false, NewValidationException("invalid BETWEEN range syntax")
	}

	minVal := resolveValue(strings.TrimSpace(rangeParts[0]), attrValues)
	maxVal := resolveValue(strings.TrimSpace(rangeParts[1]), attrValues)

	pathElems, err := parseAndResolvePath(path, attrNames)
	if err != nil {
		return false, err
	}

	val, exists := getValueAtPath(item, pathElems)
	if !exists {
		return false, nil
	}

	ge := compareValues(val, ">=", minVal)
	le := compareValues(val, "<=", maxVal)

	return ge && le, nil
}

func evaluateComparison(
	cond string,
	item map[string]any,
	attrValues map[string]any,
	attrNames map[string]string,
) (bool, error) {
	ops := []string{
		comparisonOpEqual,
		comparisonOpNotEqual,
		comparisonOpLessThanOrEqual,
		comparisonOpGreaterThanOrEqual,
		comparisonOpLessThan,
		comparisonOpGreaterThan,
	}
	const numSplitParts = 2

	for _, op := range ops {
		opWithSpaces := " " + op + " "
		if strings.Contains(cond, opWithSpaces) {
			parts := strings.SplitN(cond, opWithSpaces, numSplitParts)
			left := strings.TrimSpace(parts[0])
			right := strings.TrimSpace(parts[1])

			lhsPathElems, err := parseAndResolvePath(left, attrNames)
			if err != nil {
				return false, err
			}

			lhsVal, lhsExists := getValueAtPath(item, lhsPathElems)
			rhsVal := resolveValue(right, attrValues)

			if !lhsExists {
				return false, nil
			}

			return compareValues(lhsVal, op, rhsVal), nil
		}
	}

	return false, NewValidationException(fmt.Sprintf("unknown or unsupported condition: %s", cond))
}

func extractFunctionArg(s string) string {
	start := strings.Index(s, "(")
	end := strings.LastIndex(s, ")")
	if start == -1 || end == -1 {
		return ""
	}

	return strings.TrimSpace(s[start+1 : end])
}

func extractFunctionArgs(s string) []string {
	argStr := extractFunctionArg(s)
	if argStr == "" {
		return nil
	}

	parts := strings.Split(argStr, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}

	return parts
}

func unwrapAttributeValue(v any) any {
	m, ok := v.(map[string]any)
	if !ok {
		return v
	}

	if val, okS := m["S"]; okS {
		return val
	}

	if val, okN := m["N"]; okN {
		if s, okNS := val.(string); okNS {
			return s
		}

		return val
	}

	if val, okBool := m["BOOL"]; okBool {
		return val
	}

	if _, okNull := m["NULL"]; okNull {
		return nil
	}

	return v
}

func toString(v any) string {
	v = unwrapAttributeValue(v)
	switch s := v.(type) {
	case string:
		return s
	case bool:
		return strconv.FormatBool(s)
	case float64:
		return fmt.Sprintf("%v", s)
	case nil:
		return ""
	default:
		b, _ := json.Marshal(s)

		return string(b)
	}
}

func compareValues(lhs any, op string, rhs any) bool {
	lhs = unwrapAttributeValue(lhs)
	rhs = unwrapAttributeValue(rhs)

	lhsStr := toString(lhs)
	rhsStr := toString(rhs)
	lNum, lIsNum := parseNumeric(lhs, lhsStr)
	rNum, rIsNum := parseNumeric(rhs, rhsStr)

	switch op {
	case comparisonOpEqual:
		return lhsStr == rhsStr
	case comparisonOpNotEqual:
		return lhsStr != rhsStr
	case comparisonOpLessThan:
		return compareOrdered(
			lNum, rNum, lIsNum, rIsNum, lhsStr, rhsStr,
			func(a, b float64) bool { return a < b },
			func(a, b string) bool { return a < b },
		)
	case comparisonOpGreaterThan:
		return compareOrdered(
			lNum, rNum, lIsNum, rIsNum, lhsStr, rhsStr,
			func(a, b float64) bool { return a > b },
			func(a, b string) bool { return a > b },
		)
	case comparisonOpLessThanOrEqual:
		return compareOrdered(
			lNum, rNum, lIsNum, rIsNum, lhsStr, rhsStr,
			func(a, b float64) bool { return a <= b },
			func(a, b string) bool { return a <= b },
		)
	case comparisonOpGreaterThanOrEqual:
		return compareOrdered(
			lNum, rNum, lIsNum, rIsNum, lhsStr, rhsStr,
			func(a, b float64) bool { return a >= b },
			func(a, b string) bool { return a >= b },
		)
	}

	return false
}

func parseNumeric(val any, str string) (float64, bool) {
	if f, ok := val.(float64); ok {
		return f, true
	}

	if parsed, err := strconv.ParseFloat(str, 64); err == nil {
		return parsed, true
	}

	return 0, false
}

func compareOrdered(
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

func projectItem(item map[string]any, projectionExpression string, attrNames map[string]string) map[string]any {
	if projectionExpression == "" {
		return item
	}

	newItem := make(map[string]any)
	for path := range strings.SplitSeq(projectionExpression, ",") {
		path = strings.TrimSpace(path)
		pathElems, err := parseAndResolvePath(path, attrNames)
		if err != nil {
			continue
		}

		val, exists := getValueAtPath(item, pathElems)
		if exists {
			_ = setValueAtPath(newItem, pathElems, val)
		}
	}

	return newItem
}

func applyUpdate(item map[string]any, expression string, attrNames map[string]string, attrValues map[string]any) error {
	expression = strings.TrimSpace(expression)
	upperExpr := strings.ToUpper(expression)

	switch {
	case strings.HasPrefix(upperExpr, "SET "):
		return applySetUpdate(item, expression, attrNames, attrValues)
	case strings.HasPrefix(upperExpr, "REMOVE "):
		return applyRemoveUpdate(item, expression, attrNames)
	case strings.HasPrefix(upperExpr, "ADD "):
		return applyAddUpdate(item, expression, attrNames, attrValues)
	case strings.HasPrefix(upperExpr, "DELETE "):
		return nil // No-op for now
	default:
		return NewValidationException(fmt.Sprintf("Invalid UpdateExpression: %s", expression))
	}
}

func applySetUpdate(
	item map[string]any,
	expression string,
	attrNames map[string]string,
	attrValues map[string]any,
) error {
	const setPrefixLen = 4
	for p := range strings.SplitSeq(expression[setPrefixLen:], ",") {
		kv := strings.Split(p, "=")
		if len(kv) >= expectedSetKVParts {
			pathStr := strings.TrimSpace(kv[0])
			valToken := strings.TrimSpace(
				strings.Join(kv[1:], "="),
			) // Handle optional = in value? No, just split 2 normally.

			// Resolve attribute names in path
			resolvedPath := resolveComplexPathNames(pathStr, attrNames)

			parsedPath, err := parsePath(resolvedPath)
			if err != nil {
				return err
			}

			val := resolveValue(valToken, attrValues)

			if serr := setValueAtPath(item, parsedPath, val); serr != nil {
				return serr
			}
		}
	}

	return nil
}

func applyRemoveUpdate(item map[string]any, expression string, attrNames map[string]string) error {
	const removePrefixLen = 7
	for p := range strings.SplitSeq(expression[removePrefixLen:], ",") {
		pathStr := strings.TrimSpace(p)
		resolvedPath := resolveComplexPathNames(pathStr, attrNames)

		parsedPath, err := parsePath(resolvedPath)
		if err != nil {
			return err
		}

		if serr := removeValueAtPath(item, parsedPath); serr != nil {
			return serr
		}
	}

	return nil
}

func applyAddUpdate(
	item map[string]any,
	expression string,
	attrNames map[string]string,
	attrValues map[string]any,
) error {
	// Not fully genericizing ADD yet as it's deprecated/specific behavior
	const addPrefixLen = 4
	const pairLen = 2
	for p := range strings.SplitSeq(expression[addPrefixLen:], ",") {
		kv := strings.Split(p, " ")
		if len(kv) >= pairLen {
			path := strings.TrimSpace(kv[0])
			valToken := strings.TrimSpace(kv[1])
			val := resolveValue(valToken, attrValues)

			parsedPath, err := parseAndResolvePath(path, attrNames)
			if err != nil {
				return err
			}

			// Generic ADD is harder because we need to read then write.
			// Reusing applyAddValue logic but with path traversal?
			// For now, let's just support simple paths for ADD as it's legacy.
			// Or support basic resolution.
			// applyAddValue takes a dot-path string which is legacy.
			// Let's rely on simple resolution.

			// Actually, let's try to do it right.
			// Get current value
			// Get current value
			_, exists := getValueAtPath(item, parsedPath)
			if !exists {
				_ = setValueAtPath(item, parsedPath, val)
			} else {
				// Perform add
				applyAddValueGeneric(item, parsedPath, val)
			}
		}
	}

	return nil
}

func applyAddValueGeneric(item map[string]any, path []PathElement, val any) {
	cur, _ := getValueAtPath(item, path)
	// Same logic as old applyAddValue but using objects
	curMap, okMap := cur.(map[string]any)
	if !okMap {
		return
	}

	nStr, okN := curMap["N"]
	if !okN {
		return
	}

	valMap, okVal := val.(map[string]any)
	if !okVal {
		return
	}

	vStr, okV := valMap["N"]
	if !okV {
		return
	}

	n1 := parseNumber(nStr)
	n2 := parseNumber(vStr)
	sum := n1 + n2

	_ = setValueAtPath(item, path, map[string]any{"N": fmt.Sprintf("%v", sum)})
}

// PathElement represents a segment of a document path.
type PathElement struct {
	Key   string
	Type  PathElementType
	Index int
}

type PathElementType int

const (
	PathElementKey PathElementType = iota
	PathElementIndex
)

// parsePath parses a document path into a slice of PathElements
// Supports dot notation "a.b" and list indexing "a[1]".
func parsePath(path string) ([]PathElement, error) {
	var elements []PathElement
	var current strings.Builder

	for i := 0; i < len(path); i++ {
		char := path[i]

		switch char {
		case '.':
			elements = appendKeyIfNotEmpty(elements, &current)
		case '[':
			elements = appendKeyIfNotEmpty(elements, &current)
			idx, end, err := parseIndex(path, i)
			if err != nil {
				return nil, err
			}
			elements = append(elements, PathElement{Type: PathElementIndex, Index: idx})
			i = end
		default:
			current.WriteByte(char)
		}
	}

	elements = appendKeyIfNotEmpty(elements, &current)

	return elements, nil
}

func appendKeyIfNotEmpty(elements []PathElement, current *strings.Builder) []PathElement {
	if current.Len() > 0 {
		elements = append(elements, PathElement{Type: PathElementKey, Key: current.String()})
		current.Reset()
	}

	return elements
}

func parseIndex(path string, start int) (int, int, error) {
	// Find closing bracket
	end := -1
	for j := start + 1; j < len(path); j++ {
		if path[j] == ']' {
			end = j

			break
		}
	}
	if end == -1 {
		return 0, 0, fmt.Errorf("%w: %s", ErrUnclosedBracket, path)
	}

	indexStr := path[start+1 : end]
	index, err := strconv.Atoi(indexStr)
	if err != nil {
		return 0, 0, fmt.Errorf("%w: %s", ErrInvalidIndex, indexStr)
	}

	return index, end, nil
}

func resolvePathElements(elements []PathElement, attrNames map[string]string) []PathElement {
	resolved := make([]PathElement, len(elements))
	for i, elem := range elements {
		if elem.Type == PathElementKey && strings.HasPrefix(elem.Key, "#") {
			if name, ok := attrNames[elem.Key]; ok {
				resolved[i] = PathElement{Type: PathElementKey, Key: name}

				continue
			}
		}
		resolved[i] = elem
	}

	return resolved
}

func parseAndResolvePath(path string, attrNames map[string]string) ([]PathElement, error) {
	elements, err := parsePath(path)
	if err != nil {
		return nil, err
	}

	return resolvePathElements(elements, attrNames), nil
}

func getValueAtPath(item map[string]any, path []PathElement) (any, bool) {
	if len(path) == 0 {
		return nil, false
	}

	current := any(item)
	for i, elem := range path {
		isLast := i == len(path)-1
		var val any
		var exists bool

		if elem.Type == PathElementKey {
			val, exists = navigateMap(current, elem.Key)
		} else {
			val, exists = navigateList(current, elem.Index)
		}

		if !exists {
			return nil, false
		}

		if isLast {
			return val, true
		}
		current = unwrapContainer(val)
	}

	return nil, false
}

func navigateMap(current any, key string) (any, bool) {
	m, ok := current.(map[string]any)
	if !ok {
		return nil, false
	}
	val, exists := m[key]

	return val, exists
}

func navigateList(current any, index int) (any, bool) {
	current = unwrapL(current)
	list, ok := current.([]any)
	if !ok || index < 0 || index >= len(list) {
		return nil, false
	}

	return list[index], true
}

func unwrapContainer(val any) any {
	if m, ok := val.(map[string]any); ok {
		if inner, okM := m["M"]; okM {
			return inner
		}
		if inner, okL := m["L"]; okL {
			return inner
		}
	}

	return val
}

func unwrapL(val any) any {
	if m, ok := val.(map[string]any); ok {
		if inner, okL := m["L"]; okL {
			return inner
		}
	}

	return val
}

func resolveValue(token string, attrValues map[string]any) any {
	if strings.HasPrefix(token, ":") {
		if val, ok := attrValues[token]; ok {
			return val
		}
	}

	return token
}

// resolveComplexPathNames resolves #names in a path string without full parsing logic splitting by dot
// This handles #a.#b -> realA.realB
// But it should preserve [1].
func resolveComplexPathNames(path string, attrNames map[string]string) string {
	// Simple replace for now, but safer to respect boundaries
	// Ideally we parse first then resolve, but we want to feed resolved string to parsePath
	// Implementation: Replace all occurrences of keys in attrNames
	for k, v := range attrNames {
		path = strings.ReplaceAll(path, k, v)
	}

	return path
}

func setValueAtPath(item map[string]any, path []PathElement, value any) error {
	if len(path) == 0 {
		return nil
	}

	current := any(item)
	for i, elem := range path {
		isLast := i == len(path)-1

		if elem.Type == PathElementKey {
			var err error
			current, err = setKeyStep(current, elem.Key, value, isLast)
			if err != nil {
				return err
			}
		} else {
			var err error
			current, err = setIndexStep(current, elem.Index, value, isLast)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func setKeyStep(current any, key string, value any, isLast bool) (any, error) {
	m, ok := current.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNonMapAccess, key)
	}

	if isLast {
		m[key] = value

		return current, nil
	}

	next, exists := m[key]
	if !exists {
		newMap := make(map[string]any)
		m[key] = map[string]any{"M": newMap}

		return newMap, nil
	}

	return unwrapOrError(next, key)
}

func setIndexStep(current any, index int, value any, isLast bool) (any, error) {
	current = unwrapL(current)
	list, ok := current.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrNonListAccess, index)
	}

	if index < 0 || index >= len(list) {
		return nil, fmt.Errorf("%w: %d (len %d)", ErrIndexOutOfRange, index, len(list))
	}

	if isLast {
		list[index] = value

		return current, nil
	}

	return list[index], nil
}

func unwrapOrError(next any, key string) (any, error) {
	if wrapped, okWrap := next.(map[string]any); okWrap {
		if inner, okInner := wrapped["M"]; okInner {
			return inner, nil
		}
	}
	if _, okMap := next.(map[string]any); okMap {
		return next, nil
	}

	return nil, fmt.Errorf("%w: %s", ErrNonMapItem, key)
}

func removeValueAtPath(item map[string]any, path []PathElement) error {
	if len(path) == 0 {
		return nil
	}

	current, ok := navigateToParent(item, path)
	if !ok {
		return nil
	}

	return performRemoval(current, path[len(path)-1])
}

func performRemoval(container any, target PathElement) error {
	if target.Type == PathElementKey {
		if m, ok := container.(map[string]any); ok {
			delete(m, target.Key)
		}

		return nil
	}

	return removeIndex(container, target.Index)
}

func navigateToParent(item map[string]any, path []PathElement) (any, bool) {
	current := any(item)
	for i := range len(path) - 1 {
		elem := path[i]
		next := path[i+1]

		if elem.Type == PathElementKey {
			val, ok := navigateMapStep(current, elem.Key, next.Type == PathElementIndex)
			if !ok {
				return nil, false
			}

			current = val
		} else {
			val, ok := navigateListStep(current, elem.Index)
			if !ok {
				return nil, false
			}

			current = val
		}
	}

	return current, true
}

func navigateMapStep(current any, key string, isNextIndex bool) (any, bool) {
	m, ok := current.(map[string]any)
	if !ok {
		return nil, false
	}

	val, okVal := m[key]
	if !okVal {
		return nil, false
	}

	if isNextIndex {
		return val, true
	}

	return unwrapContainer(val), true
}

func navigateListStep(current any, index int) (any, bool) {
	current = unwrapL(current)
	list, ok := current.([]any)
	if !ok || index < 0 || index >= len(list) {
		return nil, false
	}

	return unwrapContainer(list[index]), true
}

func removeIndex(container any, index int) error {
	wrapper, ok := container.(map[string]any)
	if !ok {
		return nil
	}

	list, okList := wrapper["L"].([]any)
	if !okList {
		return nil
	}

	if index < 0 || index >= len(list) {
		return nil
	}

	newList := make([]any, 0, len(list)-1)
	newList = append(newList, list[:index]...)
	newList = append(newList, list[index+1:]...)
	wrapper["L"] = newList

	return nil
}
