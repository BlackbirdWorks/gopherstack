package sns

import (
	"encoding/json"
	"fmt"
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
//     `numeric`).
//   - Numeric operand shape (operator/number pairs) is well-formed.
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
		var conditions []json.RawMessage
		if err := json.Unmarshal(rawConditions, &conditions); err != nil {
			return nil, fmt.Errorf(
				"%w: FilterPolicy attribute %q must be a JSON array",
				ErrInvalidParameter, key,
			)
		}

		// The "$or" operator carries an array of nested sub-policy objects rather
		// than scalar/operator conditions. When the AWS recognition rules are met
		// (>=2 objects, no object using a reserved keyword as a top-level field),
		// validate each sub-policy recursively and store the raw objects under the
		// "$or" key for OR evaluation. Otherwise "$or" is treated as a normal
		// attribute name, matching AWS.
		if key == orOperatorKey && isRecognisedOrOperator(conditions) {
			subConditions, err := validateOrSubPolicies(conditions)
			if err != nil {
				return nil, err
			}

			totalConditions += subConditions
			if totalConditions > maxFilterPolicyConditions {
				return nil, fmt.Errorf(
					"%w: FilterPolicy exceeds %d total attribute conditions",
					ErrInvalidParameter, maxFilterPolicyConditions,
				)
			}

			parsed[key] = conditions

			continue
		}

		totalConditions += len(conditions)
		if totalConditions > maxFilterPolicyConditions {
			return nil, fmt.Errorf(
				"%w: FilterPolicy exceeds %d total attribute conditions",
				ErrInvalidParameter, maxFilterPolicyConditions,
			)
		}

		// Eagerly validate numeric operand shapes so that a malformed numeric
		// condition is rejected at Subscribe/SetSubscriptionAttributes time
		// rather than silently failing every match at evaluation.
		if err := validateConditionShapes(key, conditions); err != nil {
			return nil, err
		}

		parsed[key] = conditions
	}

	return parsed, nil
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

		numericRaw, ok := obj["numeric"]
		if !ok {
			continue
		}

		if err := validateNumericOperands(key, numericRaw); err != nil {
			return err
		}
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
