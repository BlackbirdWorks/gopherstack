package applicationautoscaling

import (
	"fmt"
	"slices"
	"strings"

	aastypes "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling/types"
)

// validateEnums rejects values outside the SDK ServiceNamespace/ScalableDimension enums; empty values pass.
func validateEnums(serviceNamespace, scalableDimension string) error {
	if serviceNamespace != "" &&
		!slices.Contains(aastypes.ServiceNamespace("").Values(), aastypes.ServiceNamespace(serviceNamespace)) {
		return fmt.Errorf("%w: invalid ServiceNamespace %q", ErrValidation, serviceNamespace)
	}

	if scalableDimension != "" &&
		!slices.Contains(aastypes.ScalableDimension("").Values(), aastypes.ScalableDimension(scalableDimension)) {
		return fmt.Errorf("%w: invalid ScalableDimension %q", ErrValidation, scalableDimension)
	}

	return nil
}

// validateNamespaceDimension is validateEnums plus a check that the dimension's leading segment
// names the namespace, for operations that create or change a target's configuration.
func validateNamespaceDimension(serviceNamespace, scalableDimension string) error {
	if err := validateEnums(serviceNamespace, scalableDimension); err != nil {
		return err
	}

	if prefix, _, _ := strings.Cut(scalableDimension, ":"); scalableDimension != "" && prefix != serviceNamespace {
		return fmt.Errorf(
			"%w: ScalableDimension %q does not belong to ServiceNamespace %q",
			ErrValidation, scalableDimension, serviceNamespace,
		)
	}

	return nil
}
