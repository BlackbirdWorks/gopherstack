package athena

import (
	"fmt"
	"strconv"
)

const queryEngineClassification = "athena-query-engine-properties"

// validateQueryEngineConfiguration enforces the StartQueryExecution doc rule: the only classification is
// athena-query-engine-properties and its only properties are min-dpu-count and max-dpu-count.
func validateQueryEngineConfiguration(ec *EngineConfiguration) error {
	if ec == nil {
		return nil
	}

	for _, c := range ec.Classifications {
		if c.Name != queryEngineClassification {
			return fmt.Errorf("%w: Classification name must be %s", ErrValidation, queryEngineClassification)
		}

		for k, v := range c.Properties {
			if k != "min-dpu-count" && k != "max-dpu-count" {
				return fmt.Errorf(
					"%w: property %q is not allowed; use min-dpu-count or max-dpu-count",
					ErrValidation,
					k,
				)
			}

			if n, err := strconv.Atoi(v); err != nil || n <= 0 {
				return fmt.Errorf("%w: %s must be a positive integer", ErrValidation, k)
			}
		}
	}

	return nil
}
