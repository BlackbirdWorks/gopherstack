package databrew

import "fmt"

// validateRuleMembers enforces the required members of types.Rule (Name, CheckExpression).
func validateRuleMembers(rules []Rule) error {
	for i, r := range rules {
		if r.Name == "" {
			return fmt.Errorf("%w: Rules[%d].Name is required", ErrValidation, i)
		}

		if r.CheckExpression == "" {
			return fmt.Errorf("%w: Rules[%d].CheckExpression is required", ErrValidation, i)
		}
	}

	return nil
}

// validateRecipeStepMembers enforces types.RecipeStep.Action.Operation and the
// required Condition/TargetColumn of each types.ConditionExpression.
func validateRecipeStepMembers(steps []RecipeStep) error {
	for i, s := range steps {
		if op, _ := s.Action["Operation"].(string); op == "" {
			return fmt.Errorf("%w: Steps[%d].Action.Operation is required", ErrValidation, i)
		}

		for j, c := range s.ConditionExpressions {
			if cond, _ := c["Condition"].(string); cond == "" {
				return fmt.Errorf("%w: Steps[%d].ConditionExpressions[%d].Condition is required", ErrValidation, i, j)
			}

			if col, _ := c["TargetColumn"].(string); col == "" {
				return fmt.Errorf(
					"%w: Steps[%d].ConditionExpressions[%d].TargetColumn is required",
					ErrValidation,
					i,
					j,
				)
			}
		}
	}

	return nil
}
