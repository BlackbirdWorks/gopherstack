package dynamodb

import (
	"fmt"
	"maps"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/expr"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// EvaluateExpression evaluates a DynamoDB condition expression against an item.
func EvaluateExpression(
	expression string,
	item map[string]any,
	attrValues map[string]any,
	attrNames map[string]string,
) (bool, error) {
	if expression == "" {
		return true, nil
	}

	l := expr.NewLexer(expression)
	p := expr.NewParser(l)
	node, err := p.ParseCondition()
	if err != nil {
		return false, err
	}

	eval := &expr.Evaluator{
		Item:       item,
		AttrNames:  attrNames,
		AttrValues: attrValues,
	}

	result, err := eval.Evaluate(node)
	if err != nil {
		return false, err
	}

	if b, ok := result.(bool); ok {
		return b, nil
	}

	return false, nil
}

// applyUpdate is an internal entry point for updating an item using an UpdateExpression.
// It returns the set of top-level attribute names that were touched by the expression.
func applyUpdate(
	item map[string]any,
	expression string,
	attrNames map[string]string,
	attrValues map[string]any,
) (map[string]struct{}, error) {
	if expression == "" {
		return nil, nil //nolint:nilnil // no paths touched when expression is empty
	}

	l := expr.NewLexer(expression)
	p := expr.NewParser(l)
	u, err := p.ParseUpdate()
	if err != nil {
		return nil, err
	}

	eval := &expr.Evaluator{
		Item:       item,
		AttrNames:  attrNames,
		AttrValues: attrValues,
	}

	if applyErr := eval.ApplyUpdate(u); applyErr != nil {
		return nil, applyErr
	}

	return eval.UpdatedPaths, nil
}

// projectItem creates a new item containing only the attributes specified in the ProjectionExpression.
func projectItem(
	item map[string]any,
	projectionExpression string,
	attrNames map[string]string,
) map[string]any {
	if projectionExpression == "" {
		return item
	}

	l := expr.NewLexer(projectionExpression)
	p := expr.NewParser(l)
	proj, err := p.ParseProjection()
	if err != nil {
		return item // Return full item if projection fails? Or error? Standard seems to be quiet.
	}

	eval := &expr.Evaluator{
		Item:      item,
		AttrNames: attrNames,
	}

	return eval.ApplyProjection(proj)
}

// Projector holds a pre-parsed projection expression for efficient repeated use.
type Projector struct {
	projection *expr.ProjectionExpr
	attrNames  map[string]string
}

// ParseProjector parses a ProjectionExpression and returns a Projector.
func ParseProjector(expression string, attrNames map[string]string) (*Projector, error) {
	if expression == "" {
		return &Projector{}, nil
	}

	l := expr.NewLexer(expression)
	p := expr.NewParser(l)
	proj, err := p.ParseProjection()
	if err != nil {
		return nil, err
	}

	return &Projector{
		projection: proj,
		attrNames:  attrNames,
	}, nil
}

// Project applies the pre-parsed projection to an item.
func (p *Projector) Project(item map[string]any) map[string]any {
	if p == nil || p.projection == nil {
		return item
	}

	eval := &expr.Evaluator{
		Item:      item,
		AttrNames: p.attrNames,
	}

	return eval.ApplyProjection(p.projection)
}

// Compatibility layer for unexported calls within the package.
func evaluateExpression(
	expression string,
	item map[string]any,
	attrValues map[string]any,
	attrNames map[string]string,
) (bool, error) {
	return EvaluateExpression(expression, item, attrValues, attrNames)
}

// ParsedCondition is a pre-parsed condition or filter expression AST.
// Pre-parsing once and reusing across many items avoids per-item lexing overhead.
type ParsedCondition struct {
	node expr.Node
}

// ParseConditionStr parses a DynamoDB condition expression string once.
// Returns a zero ParsedCondition when expression is empty (always matches).
func ParseConditionStr(expression string) (*ParsedCondition, error) {
	if expression == "" {
		return &ParsedCondition{}, nil
	}

	l := expr.NewLexer(expression)
	p := expr.NewParser(l)
	node, err := p.ParseCondition()
	if err != nil {
		return nil, err
	}

	return &ParsedCondition{node: node}, nil
}

// Evaluate runs the pre-parsed condition against item.
// A zero ParsedCondition (nil node) always returns true (matches everything).
func (c *ParsedCondition) Evaluate(
	item map[string]any,
	attrValues map[string]any,
	attrNames map[string]string,
) bool {
	if c == nil || c.node == nil {
		return true
	}

	eval := &expr.Evaluator{
		Item:       item,
		AttrNames:  attrNames,
		AttrValues: attrValues,
	}

	result, err := eval.Evaluate(c.node)
	if err != nil {
		return false
	}

	b, ok := result.(bool)

	return ok && b
}

// validateEAVTypes checks that expression attribute values are structurally valid
// wire-format attribute values (each must be a single-key type map).
func validateEAVTypes(eav map[string]any) error {
	for name, val := range eav {
		m, ok := val.(map[string]any)
		if !ok {
			return NewValidationException(fmt.Sprintf(
				"ExpressionAttributeValues contains invalid value for key %q: "+
					"must be a DynamoDB attribute value map",
				name,
			))
		}

		if len(m) != 1 {
			return NewValidationException(
				fmt.Sprintf(
					"ExpressionAttributeValues[%q]: expected exactly one type key, got %d",
					name,
					len(m),
				),
			)
		}

		for typeKey := range m {
			if !isValidDynamoDBTypeKey(typeKey) {
				return NewValidationException(
					fmt.Sprintf(
						"ExpressionAttributeValues[%q]: unknown type key %q",
						name,
						typeKey,
					),
				)
			}
		}
	}

	return nil
}

// isValidDynamoDBTypeKey returns true for recognised DynamoDB attribute type keys.
func isValidDynamoDBTypeKey(key string) bool {
	switch key {
	case "S", "N", "B", "BOOL", "NULL", "SS", "NS", "BS", "M", "L":
		return true
	}

	return false
}

// validateExpressionAttributeNames checks that every key starts with '#' and
// every value is a non-empty string. AWS rejects both conditions.
func validateExpressionAttributeNames(ean map[string]string) error {
	for k, v := range ean {
		if !strings.HasPrefix(k, "#") {
			return NewValidationException(
				fmt.Sprintf(
					"ExpressionAttributeNames contains invalid key: "+
						"Syntax error; token: '%s', near: '%s'",
					k, k,
				),
			)
		}

		if v == "" {
			return NewValidationException(
				fmt.Sprintf(
					"ExpressionAttributeNames contains invalid value: "+
						"empty string for key %s",
					k,
				),
			)
		}
	}

	return nil
}

// checkUnusedExpressionAttributeNames returns a ValidationException when any key
// in ean is not referenced in any of the supplied expression strings.
func checkUnusedExpressionAttributeNames(ean map[string]string, exprs ...string) error {
	if len(ean) == 0 {
		return nil
	}

	combined := strings.Join(exprs, " ")
	var unused []string

	for k := range ean {
		if !strings.Contains(combined, k) {
			unused = append(unused, k)
		}
	}

	if len(unused) == 0 {
		return nil
	}

	sort.Strings(unused)

	return NewValidationException(
		fmt.Sprintf(
			"Value provided in ExpressionAttributeNames unused in expressions: keys: {%s}",
			strings.Join(unused, ", "),
		),
	)
}

// checkUnusedExpressionAttributeValues returns a ValidationException when any key
// in eav is not referenced in any of the supplied expression strings.
func checkUnusedExpressionAttributeValues(eav map[string]any, exprs ...string) error {
	if len(eav) == 0 {
		return nil
	}

	combined := strings.Join(exprs, " ")
	var unused []string

	for k := range eav {
		if !strings.Contains(combined, k) {
			unused = append(unused, k)
		}
	}

	if len(unused) == 0 {
		return nil
	}

	sort.Strings(unused)

	return NewValidationException(
		fmt.Sprintf(
			"Value provided in ExpressionAttributeValues unused in expressions: keys: {%s}",
			strings.Join(unused, ", "),
		),
	)
}

// validateUpdateDoesNotModifyKeys returns a ValidationException when the
// UpdateExpression attempts to SET or REMOVE a primary-key attribute.
// AWS DynamoDB forbids mutations to the partition key or sort key.
func validateUpdateDoesNotModifyKeys(
	updateExpr string,
	ean map[string]string,
	keySchema []models.KeySchemaElement,
) error {
	if updateExpr == "" {
		return nil
	}

	keyAttrs := make(map[string]struct{}, len(keySchema))
	for _, k := range keySchema {
		keyAttrs[k.AttributeName] = struct{}{}
	}

	// Build reverse map: #alias → real attribute name.
	reverseEAN := make(map[string]string, len(ean))
	maps.Copy(reverseEAN, ean)

	// Tokenise the expression to find attribute references in SET/REMOVE clauses.
	// We look for bare identifiers and #aliases that map to key attributes.
	tokens := tokenizeUpdateExpression(updateExpr)
	inSetOrRemove := false

	for _, tok := range tokens {
		upper := strings.ToUpper(tok)
		if upper == "SET" || upper == "REMOVE" {
			inSetOrRemove = true

			continue
		}

		if upper == "ADD" || upper == replicationOpDelete {
			inSetOrRemove = false

			continue
		}

		if !inSetOrRemove {
			continue
		}

		// Resolve alias if present.
		attrName := tok
		if strings.HasPrefix(tok, "#") {
			if resolved, ok := reverseEAN[tok]; ok {
				attrName = resolved
			}
		}

		if _, isKey := keyAttrs[attrName]; isKey {
			return NewValidationException(
				fmt.Sprintf(
					"One or more parameter values were invalid: Cannot update attribute %s. "+
						"This attribute is part of the key",
					attrName,
				),
			)
		}
	}

	return nil
}

// tokenizeUpdateExpression splits an UpdateExpression into tokens (keywords and
// attribute names), ignoring punctuation and EAV placeholders.
func tokenizeUpdateExpression(expr string) []string {
	var tokens []string

	for _, part := range strings.FieldsFunc(expr, func(r rune) bool {
		return r == ',' || r == '=' || r == '+' || r == '-' || r == '(' || r == ')' || r == ' '
	}) {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" || strings.HasPrefix(trimmed, ":") {
			continue
		}

		tokens = append(tokens, trimmed)
	}

	return tokens
}
