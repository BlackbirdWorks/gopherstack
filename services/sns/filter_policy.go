package sns

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
)

// parseFilterPolicy parses and validates a FilterPolicy JSON string. It returns
// an empty (non-nil) policy for an empty input, or an InvalidParameter-wrapped
// error for any malformed input. Validation enforces:
//   - JSON is well-formed and is an object whose values are arrays.
//   - Total encoded size ≤ maxFilterPolicySizeBytes (256 KiB).
//   - Total attribute conditions ≤ maxFilterPolicyConditions (150).
//   - Object-condition operator names are restricted to the AWS-supported set
//     (`prefix`, `suffix`, `equals-ignore-case`, `anything-but`, `exists`,
//     `numeric`, `wildcard`, `cidr`).
//   - Numeric operand shape (operator/number pairs) is well-formed.
//   - CIDR operand is a valid IPv4/IPv6 address or CIDR block.
//
// Nesting depth (for nested-object filter policies) is not yet enforced —
// issue #1679 item 13.
// maxFilterPolicyConditions is the AWS SNS cap on total attribute conditions
// across all keys in a single FilterPolicy (≈150 in production).
const maxFilterPolicyConditions = 150

// maxFilterPolicyKeys is the AWS SNS cap on the number of keys a single
// FilterPolicy may declare (5, per "Filter policy constraints" in the SNS
// developer guide). For MessageAttributes scope this is the number of
// top-level keys; this backend does not yet parse genuinely nested
// MessageBody policies (see the nesting-depth note above), so the same
// top-level count is used as the best available approximation for both scopes.
const maxFilterPolicyKeys = 5

func parseFilterPolicy(filterPolicy string) (parsedFilterPolicy, error) {
	if filterPolicy == "" {
		return parsedFilterPolicy{}, nil
	}

	if len(filterPolicy) > maxFilterPolicySizeBytes {
		return nil, fmt.Errorf(
			"%w: FilterPolicy exceeds %d bytes",
			ErrInvalidParameter, maxFilterPolicySizeBytes,
		)
	}

	var rawPolicy map[string]json.RawMessage
	if err := json.Unmarshal([]byte(filterPolicy), &rawPolicy); err != nil {
		return nil, fmt.Errorf(
			"%w: FilterPolicy is not valid JSON: %s",
			ErrInvalidParameter,
			err.Error(),
		)
	}

	if len(rawPolicy) > maxFilterPolicyKeys {
		return nil, fmt.Errorf(
			"%w: FilterPolicy exceeds %d keys",
			ErrInvalidParameter, maxFilterPolicyKeys,
		)
	}

	parsed := make(parsedFilterPolicy, len(rawPolicy))

	totalConditions := 0

	for key, rawConditions := range rawPolicy {
		conditions, count, err := parseFilterPolicyKey(key, rawConditions)
		if err != nil {
			return nil, err
		}

		totalConditions += count
		if budgetErr := checkConditionBudget(totalConditions); budgetErr != nil {
			return nil, budgetErr
		}

		parsed[key] = conditions
	}

	return parsed, nil
}

// nestedPolicyKey wraps a nested sub-policy (an object value in a MessageBody
// filter policy) so it can live in the []json.RawMessage condition slot.
const nestedPolicyKey = "$nestedPolicy"

// parseFilterPolicyKey validates one top-level FilterPolicy key and returns its
// stored conditions plus how many count toward the total-condition budget.
func parseFilterPolicyKey(key string, rawConditions json.RawMessage) ([]json.RawMessage, int, error) {
	if isNestedPolicyValue(rawConditions) {
		nested, err := parseNestedFilterPolicy(key, rawConditions)

		return nested, 0, err
	}

	var conditions []json.RawMessage
	if err := json.Unmarshal(rawConditions, &conditions); err != nil {
		return nil, 0, fmt.Errorf(
			"%w: FilterPolicy attribute %q must be a JSON array",
			ErrInvalidParameter, key,
		)
	}

	// "$or" carries nested sub-policy objects, not scalar/operator conditions. When
	// AWS's recognition rules are met (>=2 objects, none using a reserved keyword as
	// a top-level field) each sub-policy is validated recursively; otherwise "$or"
	// is an ordinary attribute name, matching AWS.
	if key == orOperatorKey && isRecognisedOrOperator(conditions) {
		subConditions, err := validateOrSubPolicies(conditions)

		return conditions, subConditions, err
	}

	// Validate numeric operand shapes eagerly so a malformed condition is rejected
	// at Subscribe/SetSubscriptionAttributes time rather than failing every match.
	return conditions, len(conditions), validateConditionShapes(key, conditions)
}

func isNestedPolicyValue(raw json.RawMessage) bool {
	return strings.HasPrefix(strings.TrimSpace(string(raw)), "{")
}

func checkConditionBudget(total int) error {
	if total > maxFilterPolicyConditions {
		return fmt.Errorf(
			"%w: FilterPolicy exceeds %d total attribute conditions",
			ErrInvalidParameter, maxFilterPolicyConditions,
		)
	}

	return nil
}

func parseNestedFilterPolicy(key string, raw json.RawMessage) ([]json.RawMessage, error) {
	if _, err := parseFilterPolicy(string(raw)); err != nil {
		return nil, fmt.Errorf("%w (nested key %q)", err, key)
	}

	wrapped, err := json.Marshal(map[string]json.RawMessage{nestedPolicyKey: raw})
	if err != nil {
		return nil, fmt.Errorf("%w: FilterPolicy attribute %q: %s", ErrInvalidParameter, key, err.Error())
	}

	return []json.RawMessage{wrapped}, nil
}

// nestedSubPolicy returns the nested sub-policy JSON stored by parseNestedFilterPolicy.
func nestedSubPolicy(conditions []json.RawMessage) (string, bool) {
	if len(conditions) != 1 {
		return "", false
	}

	var wrapper map[string]json.RawMessage
	if err := json.Unmarshal(conditions[0], &wrapper); err != nil {
		return "", false
	}

	sub, ok := wrapper[nestedPolicyKey]

	return string(sub), ok
}

// orOperatorKey is the reserved field name for SNS OR-operator filter policies.
const orOperatorKey = "$or"

// minOrOperatorElems is the minimum number of sub-policy objects a "$or" array
// must contain for AWS to recognise it as an OR relationship.
const minOrOperatorElems = 2

// isRecognisedOrOperator reports whether a "$or" array satisfies the AWS rules
// for being treated as an OR relationship rather than a literal attribute name:
//   - the value is an array of at least 2 elements,
//   - every element is a JSON object, and
//   - no element uses a reserved operator keyword as a top-level field name.
//
// When any rule is violated, AWS treats "$or" as an ordinary attribute key.
func isRecognisedOrOperator(elems []json.RawMessage) bool {
	if len(elems) < minOrOperatorElems {
		return false
	}

	for _, elem := range elems {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(elem, &obj); err != nil {
			return false
		}

		if len(obj) == 0 {
			return false
		}

		for field := range obj {
			if _, reserved := knownFilterPolicyOperators[field]; reserved {
				return false
			}
		}
	}

	return true
}

// validateOrSubPolicies recursively parses each sub-policy object of a "$or"
// array, rejecting malformed operators or numeric operands. It returns the total
// number of conditions contained across all sub-policies so the caller can
// enforce the global condition cap.
func validateOrSubPolicies(elems []json.RawMessage) (int, error) {
	total := 0

	for _, elem := range elems {
		sub, err := parseFilterPolicy(string(elem))
		if err != nil {
			return 0, err
		}

		for _, conds := range sub {
			total += len(conds)
		}
	}

	return total, nil
}

// knownFilterPolicyOperators is the set of object-condition keys recognised
// by AWS SNS subscription FilterPolicy. Conditions containing any other key
// are rejected at Subscribe / SetSubscriptionAttributes time so misconfigurations
// surface immediately rather than silently mis-routing messages.
//
//nolint:gochecknoglobals // read-only lookup
var knownFilterPolicyOperators = map[string]struct{}{
	"prefix":             {},
	"suffix":             {},
	"equals-ignore-case": {},
	"anything-but":       {},
	"exists":             {},
	"numeric":            {},
	"wildcard":           {},
	"cidr":               {},
}

// validateConditionShapes inspects each condition under a single FilterPolicy
// attribute and rejects unknown operator names and malformed numeric operand
// structures. Scalar conditions (plain strings, numbers, booleans, null) and
// known object operators are tolerated as-is and matched lazily at evaluation.
func validateConditionShapes(key string, conditions []json.RawMessage) error {
	for _, raw := range conditions {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &obj); err != nil {
			// Scalar conditions (e.g. plain strings) are valid; skip object-only checks.
			continue
		}

		for opName := range obj {
			if _, ok := knownFilterPolicyOperators[opName]; !ok {
				return fmt.Errorf(
					"%w: FilterPolicy attribute %q uses unsupported operator %q",
					ErrInvalidParameter, key, opName,
				)
			}
		}

		if numericRaw, ok := obj["numeric"]; ok {
			if err := validateNumericOperands(key, numericRaw); err != nil {
				return err
			}
		}

		if cidrRaw, ok := obj["cidr"]; ok {
			if err := validateCIDROperand(key, cidrRaw); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateCIDROperand enforces that a "cidr" condition operand is a string
// containing a valid IPv4/IPv6 address (bare host route) or CIDR block,
// rejecting it eagerly at Subscribe/SetSubscriptionAttributes time rather
// than letting it silently never match at evaluation (matchCIDR).
func validateCIDROperand(key string, raw json.RawMessage) error {
	var operand string
	if err := json.Unmarshal(raw, &operand); err != nil {
		return fmt.Errorf(
			"%w: FilterPolicy attribute %q cidr operand must be a string",
			ErrInvalidParameter, key,
		)
	}

	if strings.Contains(operand, "/") {
		if _, _, err := net.ParseCIDR(operand); err != nil {
			return fmt.Errorf(
				"%w: FilterPolicy attribute %q cidr operand %q is not a valid CIDR block",
				ErrInvalidParameter, key, operand,
			)
		}

		return nil
	}

	if net.ParseIP(operand) == nil {
		return fmt.Errorf(
			"%w: FilterPolicy attribute %q cidr operand %q is not a valid IP address",
			ErrInvalidParameter, key, operand,
		)
	}

	return nil
}

// validateNumericOperands enforces that a "numeric" condition operand is a JSON
// array of even length where each pair is (operator-string, number).
func validateNumericOperands(key string, raw json.RawMessage) error {
	var operands []json.RawMessage
	if err := json.Unmarshal(raw, &operands); err != nil {
		return fmt.Errorf(
			"%w: FilterPolicy attribute %q numeric operand must be a JSON array",
			ErrInvalidParameter, key,
		)
	}

	if len(operands)%2 != 0 || len(operands) == 0 {
		return fmt.Errorf(
			"%w: FilterPolicy attribute %q numeric operand must contain operator/number pairs",
			ErrInvalidParameter, key,
		)
	}

	// AWS's numeric-value-matching page documents exactly these five
	// operators; "<>" is not among them
	// (docs.aws.amazon.com/sns/latest/dg/numeric-value-matching.html).
	validNumericOps := map[string]struct{}{
		"=": {}, ">": {}, ">=": {}, "<": {}, "<=": {},
	}

	for i := 0; i+1 < len(operands); i += 2 {
		var op string
		if err := json.Unmarshal(operands[i], &op); err != nil {
			return fmt.Errorf(
				"%w: FilterPolicy attribute %q numeric operator must be a string",
				ErrInvalidParameter, key,
			)
		}

		if _, ok := validNumericOps[op]; !ok {
			return fmt.Errorf(
				"%w: FilterPolicy attribute %q numeric operator %q is not supported",
				ErrInvalidParameter, key, op,
			)
		}

		var num json.Number
		if err := json.Unmarshal(operands[i+1], &num); err != nil {
			return fmt.Errorf(
				"%w: FilterPolicy attribute %q numeric threshold must be a number",
				ErrInvalidParameter, key,
			)
		}

		if _, err := strconv.ParseFloat(num.String(), 64); err != nil {
			return fmt.Errorf(
				"%w: FilterPolicy attribute %q numeric threshold %s is not a finite number",
				ErrInvalidParameter, key, num.String(),
			)
		}
	}

	return nil
}

// validateRedrivePolicy validates the JSON redrive policy attached to a
// subscription. AWS requires deadLetterTargetArn to be a valid SQS queue ARN.
func validateRedrivePolicy(policy string) error {
	var parsed struct {
		DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	}

	if err := json.Unmarshal([]byte(policy), &parsed); err != nil {
		return fmt.Errorf(
			"%w: RedrivePolicy is not valid JSON: %s",
			ErrInvalidParameter,
			err.Error(),
		)
	}

	if parsed.DeadLetterTargetArn == "" {
		return fmt.Errorf("%w: RedrivePolicy must include deadLetterTargetArn", ErrInvalidParameter)
	}

	parts := strings.Split(parsed.DeadLetterTargetArn, ":")
	if len(parts) < 6 || parts[0] != "arn" || parts[2] != protocolSQS {
		return fmt.Errorf(
			"%w: RedrivePolicy.deadLetterTargetArn must be a valid SQS queue ARN, got %s",
			ErrInvalidParameter, parsed.DeadLetterTargetArn,
		)
	}

	return nil
}

// checkDLQExists verifies that the SQS queue named in a RedrivePolicy JSON exists,
// when a SQSQueueChecker is wired. Returns nil when no checker is configured.
func (b *InMemoryBackend) checkDLQExists(policy string) error {
	checker := b.sqsChecker
	if checker == nil {
		return nil
	}

	var parsed struct {
		DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	}
	// Shape is already validated by validateRedrivePolicy, so an unmarshal failure
	// here cannot happen for a well-formed policy; ignore it and skip verification.
	_ = json.Unmarshal([]byte(policy), &parsed)
	if parsed.DeadLetterTargetArn == "" {
		return nil
	}

	exists, err := checker.QueueExists(b.svcCtx, parsed.DeadLetterTargetArn)
	if err != nil {
		return fmt.Errorf(
			"%w: could not verify deadLetterTargetArn: %s",
			ErrInvalidParameter,
			err.Error(),
		)
	}

	if !exists {
		return fmt.Errorf(
			"%w: deadLetterTargetArn queue does not exist: %s",
			ErrInvalidParameter, parsed.DeadLetterTargetArn,
		)
	}

	return nil
}

// validMessageAttributeDataType reports whether the given DataType prefix is
// one of the SNS-supported scalar message-attribute data types.
func validMessageAttributeDataType(base string) bool {
	switch base {
	case "String", "String.Array", "Number", "Binary":
		return true
	}

	return false
}

// maxMessageAttributeNameLen is the AWS-documented cap for a message-attribute name.
const maxMessageAttributeNameLen = 256

// validateMessageAttributes enforces SNS validation rules on the per-message
// attribute map: each name 1..256 chars, each DataType is one of the supported
// scalar types or a "<base>.<subtype>" specialization, and the cumulative
// payload size (names + data types + values) does not exceed 256 KiB.
func validateMessageAttributes(attrs map[string]MessageAttribute) error {
	const maxAttrPayloadBytes = 256 * 1024

	total := 0

	for name, a := range attrs {
		if name == "" || len(name) > maxMessageAttributeNameLen {
			return fmt.Errorf(
				"%w: message attribute name must be 1..%d characters",
				ErrInvalidParameter, maxMessageAttributeNameLen,
			)
		}

		base := a.DataType
		if i := strings.Index(base, "."); i >= 0 {
			base = base[:i]
		}

		if !validMessageAttributeDataType(a.DataType) && !validMessageAttributeDataType(base) {
			return fmt.Errorf(
				"%w: message attribute %q has unsupported DataType %q",
				ErrInvalidParameter, name, a.DataType,
			)
		}

		total += len(name) + len(a.DataType) + len(a.StringValue)
		if total > maxAttrPayloadBytes {
			return fmt.Errorf(
				"%w: aggregate message attribute payload exceeds %d bytes",
				ErrInvalidParameter, maxAttrPayloadBytes,
			)
		}
	}

	return nil
}
