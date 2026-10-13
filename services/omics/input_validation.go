package omics

import (
	"fmt"
	"slices"
)

const (
	maxWorkflowNameLength = 128
	networkingModeField   = "networkingMode"
)

func checkEnum(field, value string, allowed ...string) error {
	if value == "" || slices.Contains(allowed, value) {
		return nil
	}

	return fmt.Errorf("%w: %s %q is not a valid value", ErrValidation, field, value)
}

func validateWorkflowEnums(engine, storageType string) error {
	if err := checkEnum("engine", engine, "WDL", "NEXTFLOW", "CWL", "WDL_LENIENT"); err != nil {
		return err
	}

	return checkEnum("storageType", storageType, storageTypeStatic, "DYNAMIC")
}

func validateWorkflowName(name string) error {
	if len(name) > maxWorkflowNameLength {
		return fmt.Errorf("%w: name must be at most %d characters", ErrValidation, maxWorkflowNameLength)
	}

	return nil
}

func validateRunEnums(in StartRunInput) error {
	checks := []struct{ field, value string }{
		{"logLevel", in.LogLevel},
		{"retentionMode", in.RetentionMode},
		{networkingModeField, in.NetworkingMode},
		{"storageType", in.StorageType},
		{"workflowType", in.WorkflowType},
		{"cacheBehavior", in.CacheBehavior},
	}
	allowed := map[string][]string{
		"logLevel":          {"OFF", "FATAL", "ERROR", "ALL"},
		"retentionMode":     {"RETAIN", "REMOVE"},
		networkingModeField: {"RESTRICTED", "VPC"},
		"storageType":       {storageTypeStatic, "DYNAMIC"},
		"workflowType":      {workflowTypePrivate, workflowTypeReady2Run},
		"cacheBehavior":     {"CACHE_ON_FAILURE", "CACHE_ALWAYS"},
	}

	for _, c := range checks {
		if err := checkEnum(c.field, c.value, allowed[c.field]...); err != nil {
			return err
		}
	}

	return nil
}
