// legacy_conditions.go translates the legacy pre-2013 conditional-write and
// update parameters (Expected, ConditionalOperator, AttributeUpdates) into
// their modern expression equivalents (ConditionExpression / UpdateExpression,
// plus synthesized ExpressionAttributeNames / ExpressionAttributeValues) so
// PutItem, UpdateItem, and DeleteItem can evaluate them through the existing
// services/dynamodb/expr evaluator instead of a second, parallel
// condition-evaluation engine.
//
// KeyConditions, QueryFilter, and ScanFilter (the Query/Scan legacy
// parameters) are deliberately NOT covered here -- see PARITY.md and
// gopherstack-lze5 for why.

package dynamodb

import (
	"fmt"
	"maps"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// legacyPlaceholders synthesizes #name / :value expression placeholders while
// translating a legacy request. prefix keeps two translators that might both
// run for the same request (Expected and AttributeUpdates) from ever
// generating colliding aliases; the collision check against existing also
// guards against a caller's own ExpressionAttributeNames/Values happening to
// use the same alias.
type legacyPlaceholders struct {
	ean         map[string]string
	eav         map[string]types.AttributeValue
	existingEAN map[string]string
	existingEAV map[string]types.AttributeValue
	prefix      string
	n           int
}

func newLegacyPlaceholders(
	prefix string,
	existingEAN map[string]string,
	existingEAV map[string]types.AttributeValue,
) *legacyPlaceholders {
	return &legacyPlaceholders{
		prefix:      prefix,
		ean:         map[string]string{},
		eav:         map[string]types.AttributeValue{},
		existingEAN: existingEAN,
		existingEAV: existingEAV,
	}
}

func (p *legacyPlaceholders) nameFor(attr string) string {
	for {
		alias := fmt.Sprintf("#%s_name%d", p.prefix, p.n)
		p.n++
		if _, taken := p.existingEAN[alias]; taken {
			continue
		}
		p.ean[alias] = attr

		return alias
	}
}

func (p *legacyPlaceholders) valueFor(v types.AttributeValue) string {
	for {
		alias := fmt.Sprintf(":%s_val%d", p.prefix, p.n)
		p.n++
		if _, taken := p.existingEAV[alias]; taken {
			continue
		}
		p.eav[alias] = v

		return alias
	}
}

// mergeEAN returns a new map containing existing's entries plus extra's,
// without mutating either input.
func mergeEAN(existing, extra map[string]string) map[string]string {
	if len(extra) == 0 {
		return existing
	}

	out := make(map[string]string, len(existing)+len(extra))
	maps.Copy(out, existing)
	maps.Copy(out, extra)

	return out
}

// mergeEAV returns a new map containing existing's entries plus extra's,
// without mutating either input.
func mergeEAV(
	existing, extra map[string]types.AttributeValue,
) map[string]types.AttributeValue {
	if len(extra) == 0 {
		return existing
	}

	out := make(map[string]types.AttributeValue, len(existing)+len(extra))
	maps.Copy(out, existing)
	maps.Copy(out, extra)

	return out
}

// rejectMixedLegacyAndExpressionParams enforces AWS's documented rule
// (LegacyConditionalParameters guide) that a single request may use either
// the legacy pre-2013 parameters (Expected, ConditionalOperator,
// AttributeUpdates) or their modern expression equivalents
// (ConditionExpression, UpdateExpression), never both. Real DynamoDB groups
// this rejection per-operation -- any legacy field mixed with any expression
// field on the same request is rejected, not just directly-corresponding
// pairs. The aws-sdk-go-v2 client performs no validation of this itself
// (enforced server-side only), so this message is our own wording rather
// than a verified verbatim AWS string.
func rejectMixedLegacyAndExpressionParams(hasLegacy, hasExpression bool) error {
	if hasLegacy && hasExpression {
		return NewValidationException(
			"Cannot use both legacy parameters (Expected, ConditionalOperator, " +
				"AttributeUpdates) and expression parameters (ConditionExpression, " +
				"UpdateExpression) in the same request",
		)
	}

	return nil
}

// legacyConditionalJoiner returns the logical-AND/OR joiner text for
// combining multiple Expected conditions per ConditionalOperator. Default
// (unset) is AND. types/enums.go: ConditionalOperatorAnd = "AND",
// ConditionalOperatorOr = "OR".
func legacyConditionalJoiner(op types.ConditionalOperator) (string, error) {
	switch op {
	case "", types.ConditionalOperatorAnd:
		return " AND ", nil
	case types.ConditionalOperatorOr:
		return " OR ", nil
	default:
		return "", NewValidationException(fmt.Sprintf("Invalid ConditionalOperator: %s", op))
	}
}

// normalizeExpected resolves one ExpectedAttributeValue entry to a single
// (ComparisonOperator, values) pair, handling both legacy styles documented
// at types/types.go:1240-1256 (ExpectedAttributeValue doc comment):
//
//   - modern-legacy style: ComparisonOperator + AttributeValueList
//   - old-legacy style: Value (implies EQ) or Exists=false (implies the
//     attribute must not exist)
//
// "Value and Exists are incompatible with AttributeValueList and
// ComparisonOperator... if you use both sets of parameters at once, DynamoDB
// will return a ValidationException" -- types/types.go:1254-1256.
func normalizeExpected(ev types.ExpectedAttributeValue) (types.ComparisonOperator, []types.AttributeValue, error) {
	hasCmp := ev.ComparisonOperator != ""
	hasList := len(ev.AttributeValueList) > 0
	hasValue := ev.Value != nil
	hasExists := ev.Exists != nil

	if (hasCmp || hasList) && (hasValue || hasExists) {
		return "", nil, NewValidationException(
			"Expected: cannot specify both (ComparisonOperator or AttributeValueList) " +
				"and (Value or Exists)",
		)
	}

	if hasCmp || hasList {
		if !hasCmp {
			return "", nil, NewValidationException(
				"Expected: AttributeValueList requires ComparisonOperator",
			)
		}

		return ev.ComparisonOperator, ev.AttributeValueList, nil
	}

	exists := true
	if hasExists {
		exists = *ev.Exists
	}

	if !exists {
		if hasValue {
			return "", nil, NewValidationException(
				"Expected: Exists is false but Value was also provided",
			)
		}

		return types.ComparisonOperatorNull, nil, nil
	}

	if !hasValue {
		return "", nil, NewValidationException(
			"Expected: Exists is true but no Value was provided",
		)
	}

	return types.ComparisonOperatorEq, []types.AttributeValue{ev.Value}, nil
}

const legacyBetweenArgCount = 2

// requireArgCount returns a ValidationException when values does not have
// exactly want elements.
func requireArgCount(op types.ComparisonOperator, values []types.AttributeValue, want int) error {
	if len(values) != want {
		return NewValidationException(fmt.Sprintf(
			"ComparisonOperator %s requires exactly %d value(s) in AttributeValueList, got %d",
			op, want, len(values),
		))
	}

	return nil
}

// legacyBinarySymbols maps the ComparisonOperators that render as a plain
// infix comparison to their expression symbol. Deliberately partial: the
// remaining ComparisonOperator values are handled by legacyUnaryFuncs or the
// switch in renderComparison.
//
//nolint:gochecknoglobals,exhaustive // fixed lookup table, see comment above
var legacyBinarySymbols = map[types.ComparisonOperator]string{
	types.ComparisonOperatorEq: "=",
	types.ComparisonOperatorNe: "<>",
	types.ComparisonOperatorLe: "<=",
	types.ComparisonOperatorLt: "<",
	types.ComparisonOperatorGe: ">=",
	types.ComparisonOperatorGt: ">",
}

// legacyUnaryFuncOp describes a ComparisonOperator that renders as a
// single-argument function call, optionally negated with NOT.
type legacyUnaryFuncOp struct {
	fn     string
	negate bool
}

// legacyUnaryFuncs maps the ComparisonOperators that render as a
// fn(alias, value) call to their function name and negation. Deliberately
// partial: the remaining ComparisonOperator values are handled by
// legacyBinarySymbols or the switch in renderComparison.
//
//nolint:gochecknoglobals,exhaustive // fixed lookup table, see comment above
var legacyUnaryFuncs = map[types.ComparisonOperator]legacyUnaryFuncOp{
	types.ComparisonOperatorContains:    {fn: "contains"},
	types.ComparisonOperatorNotContains: {fn: "contains", negate: true},
	types.ComparisonOperatorBeginsWith:  {fn: "begins_with"},
}

// renderComparison renders one legacy Condition/ExpectedAttributeValue as a
// ConditionExpression fragment referencing alias (the already-synthesized
// #name placeholder for the attribute) plus zero or more synthesized :value
// placeholders. Operator set and argument-count rules per
// types/types.go:1279-1391 (ExpectedAttributeValue.ComparisonOperator doc,
// shared verbatim by Condition.ComparisonOperator at types/types.go:672-770).
func renderComparison(
	alias string,
	op types.ComparisonOperator,
	values []types.AttributeValue,
	ph *legacyPlaceholders,
) (string, error) {
	if sym, ok := legacyBinarySymbols[op]; ok {
		return renderBinary(alias, sym, values, ph)
	}
	if uf, ok := legacyUnaryFuncs[op]; ok {
		return renderFunc1(alias, uf.fn, values, ph, uf.negate)
	}

	switch op {
	case types.ComparisonOperatorNotNull:
		return renderExistsFunc(alias, "attribute_exists", op, values)
	case types.ComparisonOperatorNull:
		return renderExistsFunc(alias, "attribute_not_exists", op, values)
	case types.ComparisonOperatorIn:
		return renderIn(alias, values, ph)
	case types.ComparisonOperatorBetween:
		return renderBetween(alias, values, ph)
	default:
		return "", NewValidationException(fmt.Sprintf("Unsupported ComparisonOperator: %s", op))
	}
}

func renderBinary(
	alias, sym string, values []types.AttributeValue, ph *legacyPlaceholders,
) (string, error) {
	if err := requireArgCount(types.ComparisonOperator(sym), values, 1); err != nil {
		return "", err
	}

	return fmt.Sprintf("%s %s %s", alias, sym, ph.valueFor(values[0])), nil
}

func renderFunc1(
	alias, fn string, values []types.AttributeValue, ph *legacyPlaceholders, negate bool,
) (string, error) {
	if err := requireArgCount(types.ComparisonOperator(fn), values, 1); err != nil {
		return "", err
	}
	frag := fmt.Sprintf("%s(%s, %s)", fn, alias, ph.valueFor(values[0]))
	if negate {
		frag = "(NOT " + frag + ")"
	}

	return frag, nil
}

const legacyNoArgs = 0

func renderExistsFunc(
	alias, fn string, op types.ComparisonOperator, values []types.AttributeValue,
) (string, error) {
	if err := requireArgCount(op, values, legacyNoArgs); err != nil {
		return "", err
	}

	return fmt.Sprintf("%s(%s)", fn, alias), nil
}

func renderIn(alias string, values []types.AttributeValue, ph *legacyPlaceholders) (string, error) {
	if len(values) == 0 {
		return "", NewValidationException(
			"ComparisonOperator IN requires at least one value in AttributeValueList",
		)
	}
	placeholders := make([]string, len(values))
	for i, v := range values {
		placeholders[i] = ph.valueFor(v)
	}

	return fmt.Sprintf("%s IN (%s)", alias, strings.Join(placeholders, ", ")), nil
}

func renderBetween(alias string, values []types.AttributeValue, ph *legacyPlaceholders) (string, error) {
	if err := requireArgCount(types.ComparisonOperatorBetween, values, legacyBetweenArgCount); err != nil {
		return "", err
	}

	return fmt.Sprintf(
		"%s BETWEEN %s AND %s", alias, ph.valueFor(values[0]), ph.valueFor(values[1]),
	), nil
}

// translateExpectedToConditionExpression translates a legacy Expected map
// (combined per ConditionalOperator) into a ConditionExpression fragment plus
// the ExpressionAttributeNames/Values it references. Attribute keys are
// processed in sorted order for deterministic output.
func translateExpectedToConditionExpression(
	expected map[string]types.ExpectedAttributeValue,
	condOp types.ConditionalOperator,
	existingEAN map[string]string,
	existingEAV map[string]types.AttributeValue,
) (string, map[string]string, map[string]types.AttributeValue, error) {
	joiner, err := legacyConditionalJoiner(condOp)
	if err != nil {
		return "", nil, nil, err
	}

	keys := make([]string, 0, len(expected))
	for k := range expected {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	ph := newLegacyPlaceholders("expected", existingEAN, existingEAV)
	fragments := make([]string, 0, len(keys))

	for _, k := range keys {
		op, values, normErr := normalizeExpected(expected[k])
		if normErr != nil {
			return "", nil, nil, normErr
		}

		alias := ph.nameFor(k)
		frag, renderErr := renderComparison(alias, op, values, ph)
		if renderErr != nil {
			return "", nil, nil, renderErr
		}

		fragments = append(fragments, frag)
	}

	return strings.Join(fragments, joiner), ph.ean, ph.eav, nil
}

// translateAttributeUpdatesToUpdateExpression translates a legacy
// AttributeUpdates map into an UpdateExpression string plus the
// ExpressionAttributeNames/Values it references.
//
// Action semantics per types/types.go:197-269 (AttributeValueUpdate doc):
//   - PUT (default when Action is unset): SET the attribute to Value.
//   - DELETE with no Value: REMOVE the attribute entirely.
//   - DELETE with a Value: the value must be a set (SS/NS/BS); DELETE
//     subtracts it from the existing set (expr.Evaluator.applyDelete already
//     implements exactly this).
//   - ADD: the attribute does not exist -> create it with Value; a Number
//     attribute -> arithmetic add; a set attribute -> set union
//     (expr.Evaluator.applyAdd already implements both).
func translateAttributeUpdatesToUpdateExpression(
	updates map[string]types.AttributeValueUpdate,
	existingEAN map[string]string,
	existingEAV map[string]types.AttributeValue,
) (string, map[string]string, map[string]types.AttributeValue, error) {
	keys := make([]string, 0, len(updates))
	for k := range updates {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	ph := newLegacyPlaceholders("attrupd", existingEAN, existingEAV)

	var setItems, removeItems, addItems, deleteItems []string

	for _, k := range keys {
		u := updates[k]
		alias := ph.nameFor(k)
		action := u.Action
		if action == "" {
			action = types.AttributeActionPut
		}

		switch action {
		case types.AttributeActionPut:
			if u.Value == nil {
				return "", nil, nil, NewValidationException(fmt.Sprintf(
					"AttributeUpdates: PUT action for %q requires a Value", k,
				))
			}
			setItems = append(setItems, fmt.Sprintf("%s = %s", alias, ph.valueFor(u.Value)))
		case types.AttributeActionDelete:
			if u.Value == nil {
				removeItems = append(removeItems, alias)
			} else {
				deleteItems = append(deleteItems, fmt.Sprintf("%s %s", alias, ph.valueFor(u.Value)))
			}
		case types.AttributeActionAdd:
			if u.Value == nil {
				return "", nil, nil, NewValidationException(fmt.Sprintf(
					"AttributeUpdates: ADD action for %q requires a Value", k,
				))
			}
			addItems = append(addItems, fmt.Sprintf("%s %s", alias, ph.valueFor(u.Value)))
		default:
			return "", nil, nil, NewValidationException(fmt.Sprintf(
				"AttributeUpdates: unsupported Action %q for %q", action, k,
			))
		}
	}

	var clauses []string
	if len(setItems) > 0 {
		clauses = append(clauses, "SET "+strings.Join(setItems, ", "))
	}
	if len(removeItems) > 0 {
		clauses = append(clauses, "REMOVE "+strings.Join(removeItems, ", "))
	}
	if len(addItems) > 0 {
		clauses = append(clauses, "ADD "+strings.Join(addItems, ", "))
	}
	if len(deleteItems) > 0 {
		clauses = append(clauses, "DELETE "+strings.Join(deleteItems, ", "))
	}

	return strings.Join(clauses, " "), ph.ean, ph.eav, nil
}

// requireConditionalOperatorNeedsExpected rejects a ConditionalOperator set
// without any Expected entries -- it has nothing to combine.
func requireConditionalOperatorNeedsExpected(condOp types.ConditionalOperator, expectedLen int) error {
	if condOp != "" && expectedLen == 0 {
		return NewValidationException(
			"ConditionalOperator can only be used in conjunction with Expected",
		)
	}

	return nil
}

// applyLegacyPutParams rewrites input in place: translating Expected +
// ConditionalOperator into ConditionExpression (and the EAN/EAV it
// references) when present, after rejecting any mix with the modern
// expression parameters.
func applyLegacyPutParams(input *dynamodb.PutItemInput) error {
	hasLegacy := len(input.Expected) > 0 || input.ConditionalOperator != ""
	hasExpr := aws.ToString(input.ConditionExpression) != ""

	if err := rejectMixedLegacyAndExpressionParams(hasLegacy, hasExpr); err != nil {
		return err
	}
	if err := requireConditionalOperatorNeedsExpected(input.ConditionalOperator, len(input.Expected)); err != nil {
		return err
	}
	if len(input.Expected) == 0 {
		return nil
	}

	exprStr, ean, eav, err := translateExpectedToConditionExpression(
		input.Expected, input.ConditionalOperator,
		input.ExpressionAttributeNames, input.ExpressionAttributeValues,
	)
	if err != nil {
		return err
	}

	input.ConditionExpression = aws.String(exprStr)
	input.ExpressionAttributeNames = mergeEAN(input.ExpressionAttributeNames, ean)
	input.ExpressionAttributeValues = mergeEAV(input.ExpressionAttributeValues, eav)

	return nil
}

// applyLegacyDeleteParams is applyLegacyPutParams's DeleteItem counterpart --
// Expected and ConditionalOperator behave identically on both operations.
func applyLegacyDeleteParams(input *dynamodb.DeleteItemInput) error {
	hasLegacy := len(input.Expected) > 0 || input.ConditionalOperator != ""
	hasExpr := aws.ToString(input.ConditionExpression) != ""

	if err := rejectMixedLegacyAndExpressionParams(hasLegacy, hasExpr); err != nil {
		return err
	}
	if err := requireConditionalOperatorNeedsExpected(input.ConditionalOperator, len(input.Expected)); err != nil {
		return err
	}
	if len(input.Expected) == 0 {
		return nil
	}

	exprStr, ean, eav, err := translateExpectedToConditionExpression(
		input.Expected, input.ConditionalOperator,
		input.ExpressionAttributeNames, input.ExpressionAttributeValues,
	)
	if err != nil {
		return err
	}

	input.ConditionExpression = aws.String(exprStr)
	input.ExpressionAttributeNames = mergeEAN(input.ExpressionAttributeNames, ean)
	input.ExpressionAttributeValues = mergeEAV(input.ExpressionAttributeValues, eav)

	return nil
}

// applyLegacyUpdateParams rewrites input in place: translating Expected +
// ConditionalOperator into ConditionExpression, and AttributeUpdates into
// UpdateExpression, after rejecting any mix with the modern expression
// parameters. Both legacy translations may apply to the same request (that
// combination is legal -- it is the all-legacy equivalent of setting both
// ConditionExpression and UpdateExpression).
func applyLegacyUpdateParams(input *dynamodb.UpdateItemInput) error {
	hasLegacy := len(input.Expected) > 0 || input.ConditionalOperator != "" || len(input.AttributeUpdates) > 0
	hasExpr := aws.ToString(input.ConditionExpression) != "" || aws.ToString(input.UpdateExpression) != ""

	if err := rejectMixedLegacyAndExpressionParams(hasLegacy, hasExpr); err != nil {
		return err
	}
	if err := requireConditionalOperatorNeedsExpected(input.ConditionalOperator, len(input.Expected)); err != nil {
		return err
	}

	if len(input.Expected) > 0 {
		exprStr, ean, eav, err := translateExpectedToConditionExpression(
			input.Expected, input.ConditionalOperator,
			input.ExpressionAttributeNames, input.ExpressionAttributeValues,
		)
		if err != nil {
			return err
		}

		input.ConditionExpression = aws.String(exprStr)
		input.ExpressionAttributeNames = mergeEAN(input.ExpressionAttributeNames, ean)
		input.ExpressionAttributeValues = mergeEAV(input.ExpressionAttributeValues, eav)
	}

	if len(input.AttributeUpdates) > 0 {
		updateStr, ean, eav, err := translateAttributeUpdatesToUpdateExpression(
			input.AttributeUpdates, input.ExpressionAttributeNames, input.ExpressionAttributeValues,
		)
		if err != nil {
			return err
		}

		input.UpdateExpression = aws.String(updateStr)
		input.ExpressionAttributeNames = mergeEAN(input.ExpressionAttributeNames, ean)
		input.ExpressionAttributeValues = mergeEAV(input.ExpressionAttributeValues, eav)
	}

	return nil
}
