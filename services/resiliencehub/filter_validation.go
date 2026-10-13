package resiliencehub

import (
	"fmt"
	"slices"
)

var statusEnum = []string{"Pending", "InProgress", "Failed", "Success"} //nolint:gochecknoglobals // read-only enum

func validateEnumValues(field string, values []string, allowed []string) error {
	for _, v := range values {
		if v != "" && !slices.Contains(allowed, v) {
			return validationError(fmt.Sprintf("%s: %q is not a valid value", field, v))
		}
	}

	return nil
}

func validateAssessmentFilter(f listAssessmentsFilter) error {
	if err := validateEnumValues("assessmentStatus", f.statuses, statusEnum); err != nil {
		return err
	}

	err := validateEnumValues(
		"complianceStatus", []string{f.complianceStatus},
		[]string{"PolicyBreached", "PolicyMet", "NotApplicable", "MissingPolicy"},
	)
	if err != nil {
		return err
	}

	return validateEnumValues("invoker", []string{f.invoker}, []string{"User", "System"})
}
