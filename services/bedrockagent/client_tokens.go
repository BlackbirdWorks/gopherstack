package bedrockagent

import (
	"fmt"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func actionGroupSignatures() []string {
	return []string{
		"AMAZON.UserInput", "AMAZON.CodeInterpreter", "ANTHROPIC.Computer", "ANTHROPIC.Bash", "ANTHROPIC.TextEditor",
	}
}

func aliasInvocationStates() []string {
	return []string{aliasInvocationAccept, aliasInvocationReject}
}

func concurrencyTypes() []string { return []string{"Automatic", "Manual"} }

const (
	aliasInvocationAccept = "ACCEPT_INVOCATIONS"
	aliasInvocationReject = "REJECT_INVOCATIONS"
	includedDataMetadata  = "METADATA_ONLY"
	includedDataAll       = "ALL_DATA"
)

// findByClientToken returns the resource created with token, scoped by inScope.
func findByClientToken[V any](t *store.Table[V], token string, tokenOf func(*V) string, inScope func(*V) bool) *V {
	if token == "" {
		return nil
	}

	var found *V

	t.Range(func(v *V) bool {
		if tokenOf(v) == token && inScope(v) {
			found = v

			return false
		}

		return true
	})

	return found
}

func validateEnum(field, value string, allowed []string) error {
	if value == "" || slices.Contains(allowed, value) {
		return nil
	}

	return fmt.Errorf("%w: %s %q is not one of %v", ErrValidation, field, value, allowed)
}

func validateConcurrency(cc map[string]any) error {
	if cc == nil {
		return nil
	}

	typ, _ := cc["type"].(string)
	if typ == "" {
		return fmt.Errorf("%w: concurrencyConfiguration.type is required", ErrValidation)
	}

	return validateEnum("concurrencyConfiguration.type", typ, concurrencyTypes())
}

func validateIncludedData(v string) error {
	return validateEnum("includedData", v, []string{includedDataAll, includedDataMetadata})
}

func validateActionGroupSignature(cfg ActionGroupConfig) error {
	if cfg.ParentActionGroupSignature == "" {
		return nil
	}

	err := validateEnum("parentActionGroupSignature", cfg.ParentActionGroupSignature, actionGroupSignatures())
	if err != nil {
		return err
	}

	if cfg.Description != "" || cfg.APISchema != nil || cfg.ActionGroupExecutor != nil {
		return fmt.Errorf(
			"%w: description, apiSchema and actionGroupExecutor must be empty with a parentActionGroupSignature",
			ErrValidation,
		)
	}

	return nil
}
