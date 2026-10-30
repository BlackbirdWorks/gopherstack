package chaos

import (
	"errors"
	"fmt"
	"maps"
)

// EffectDynamoDBTransactionConflict makes DynamoDB treat the targeted items as
// part of an in-flight TransactWriteItems, so the documented transaction
// conflict errors fire.
const EffectDynamoDBTransactionConflict = "dynamodb:transaction-conflict"

// ErrInvalidFaultRule is returned for a rule that cannot be applied.
var ErrInvalidFaultRule = errors.New("invalid fault rule")

// EffectTarget narrows an effect rule to a table and, optionally, one item.
// Key maps key attribute names to their scalar (S/N/B) string values.
type EffectTarget struct {
	Key   map[string]string `json:"key,omitempty"`
	Table string            `json:"table,omitempty"`
}

func effectTargetsEqual(a, b *EffectTarget) bool {
	if a == nil || b == nil {
		return a == b
	}

	return a.Table == b.Table && maps.Equal(a.Key, b.Key)
}

// Validate reports whether the rule is well formed.
func (r FaultRule) Validate() error {
	if r.Effect == "" {
		if r.Target != nil || r.HoldOps != 0 || r.HoldMs != 0 {
			return fmt.Errorf("%w: target/holdOps/holdMs require an effect", ErrInvalidFaultRule)
		}

		return nil
	}

	if r.Effect != EffectDynamoDBTransactionConflict {
		return fmt.Errorf("%w: unknown effect %q", ErrInvalidFaultRule, r.Effect)
	}

	if r.Service != "" && r.Service != "dynamodb" {
		return fmt.Errorf("%w: effect %q applies to service dynamodb", ErrInvalidFaultRule, r.Effect)
	}

	if r.Error != nil {
		return fmt.Errorf("%w: an effect rule cannot carry an error", ErrInvalidFaultRule)
	}

	if r.HoldOps < 0 || r.HoldMs < 0 {
		return fmt.Errorf("%w: holdOps and holdMs must not be negative", ErrInvalidFaultRule)
	}

	if r.Target != nil && len(r.Target.Key) > 0 && r.Target.Table == "" {
		return fmt.Errorf("%w: a key target requires a table", ErrInvalidFaultRule)
	}

	return nil
}

func validateRules(rules []FaultRule) error {
	for _, r := range rules {
		if err := r.Validate(); err != nil {
			return err
		}
	}

	return nil
}
