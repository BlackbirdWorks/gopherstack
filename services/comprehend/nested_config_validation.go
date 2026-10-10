package comprehend

import (
	"fmt"
	"slices"
)

//nolint:gochecknoglobals // static declarative tables of SDK enum values
var (
	maskModes = []string{"MASK", "REPLACE_WITH_PII_ENTITY_TYPE"}

	piiEntityTypes = []string{
		"BANK_ACCOUNT_NUMBER", "BANK_ROUTING", "CREDIT_DEBIT_NUMBER", "CREDIT_DEBIT_CVV", "CREDIT_DEBIT_EXPIRY",
		"PIN", piiTypeEmail, "ADDRESS", "NAME", "PHONE", piiTypeSSN, "DATE_TIME", "PASSPORT_NUMBER", "DRIVER_ID", "URL",
		"AGE", "USERNAME", "PASSWORD", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "IP_ADDRESS", "MAC_ADDRESS", "ALL",
		"LICENSE_PLATE", "VEHICLE_IDENTIFICATION_NUMBER", "UK_NATIONAL_INSURANCE_NUMBER",
		"CA_SOCIAL_INSURANCE_NUMBER", "US_INDIVIDUAL_TAX_IDENTIFICATION_NUMBER",
		"UK_UNIQUE_TAXPAYER_REFERENCE_NUMBER", "IN_PERMANENT_ACCOUNT_NUMBER", "IN_NREGA",
		"INTERNATIONAL_BANK_ACCOUNT_NUMBER", "SWIFT_CODE", "UK_NATIONAL_HEALTH_SERVICE_NUMBER",
		"CA_HEALTH_NUMBER", "IN_AADHAAR", "IN_VOTER_NUMBER",
	}
)

// validateNestedConfigs checks the VpcConfig and RedactionConfig members of a
// request body against types.VpcConfig and types.RedactionConfig.
func validateNestedConfigs(input map[string]any) error {
	if raw, ok := input["VpcConfig"]; ok && raw != nil {
		if err := validateVpcConfig(raw); err != nil {
			return err
		}
	}

	if raw, ok := input["RedactionConfig"]; ok && raw != nil {
		return validateRedactionConfig(raw)
	}

	return nil
}

func validateVpcConfig(raw any) error {
	cfg, ok := raw.(map[string]any)
	if !ok {
		return fmt.Errorf("%w: VpcConfig must be an object", ErrValidation)
	}

	for _, field := range []string{"SecurityGroupIds", "Subnets"} {
		list, isList := cfg[field].([]any)
		if !isList {
			return fmt.Errorf("%w: VpcConfig.%s is required", ErrValidation, field)
		}

		if len(list) == 0 {
			return fmt.Errorf("%w: VpcConfig.%s must contain at least one entry", ErrValidation, field)
		}

		for _, item := range list {
			if s, isString := item.(string); !isString || s == "" {
				return fmt.Errorf("%w: VpcConfig.%s entries must be non-empty strings", ErrValidation, field)
			}
		}
	}

	return nil
}

func validateRedactionConfig(raw any) error {
	cfg, ok := raw.(map[string]any)
	if !ok {
		return fmt.Errorf("%w: RedactionConfig must be an object", ErrValidation)
	}

	if mode := stringValue(cfg, "MaskMode", ""); mode != "" && !slices.Contains(maskModes, mode) {
		return fmt.Errorf("%w: RedactionConfig.MaskMode %q is not a valid mask mode", ErrValidation, mode)
	}

	if char := stringValue(cfg, "MaskCharacter", ""); len([]rune(char)) > 1 {
		return fmt.Errorf("%w: RedactionConfig.MaskCharacter must be a single character", ErrValidation)
	}

	types, _ := cfg["PiiEntityTypes"].([]any)
	for _, item := range types {
		if s, isString := item.(string); !isString || !slices.Contains(piiEntityTypes, s) {
			return fmt.Errorf("%w: RedactionConfig.PiiEntityTypes contains invalid entity type %v", ErrValidation, item)
		}
	}

	return nil
}
