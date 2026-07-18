package cloudformation

import "fmt"

// ---- SWF ----

func (rc *ResourceCreator) createSWFDomain(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SWF == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	description := strProp(props, "Description", params, physicalIDs)
	retention := strProp(props, "WorkflowExecutionRetentionPeriodInDays", params, physicalIDs)

	if err := rc.backends.SWF.Backend.RegisterDomain(name, description, retention); err != nil {
		return "", fmt.Errorf("create SWF domain %s: %w", name, err)
	}

	return name, nil
}

func (rc *ResourceCreator) deleteSWFDomain(name string) error {
	if rc.backends.SWF == nil {
		return nil
	}

	return rc.backends.SWF.Backend.DeprecateDomain(name)
}
