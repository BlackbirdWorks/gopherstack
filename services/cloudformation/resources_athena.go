package cloudformation

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	athenabackend "github.com/blackbirdworks/gopherstack/services/athena"
)

const (
	resTypeAthenaWorkGroup           = "AWS::Athena::WorkGroup"
	resTypeAthenaDataCatalog         = "AWS::Athena::DataCatalog"
	resTypeAthenaNamedQuery          = "AWS::Athena::NamedQuery"
	resTypeAthenaPreparedStatement   = "AWS::Athena::PreparedStatement"
	resTypeAthenaCapacityReservation = "AWS::Athena::CapacityReservation"
)

// createAthenaResource handles the Athena resource types listed above.
// Returns handled=false when resourceType isn't one of them.
func (rc *ResourceCreator) createAthenaResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeAthenaWorkGroup:
		id, err := rc.createAthenaWorkGroup(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeAthenaDataCatalog:
		id, err := rc.createAthenaDataCatalog(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeAthenaNamedQuery:
		id, err := rc.createAthenaNamedQuery(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeAthenaPreparedStatement:
		id, err := rc.createAthenaPreparedStatement(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeAthenaCapacityReservation:
		id, err := rc.createAthenaCapacityReservation(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteAthenaResource handles deletion for the types described in
// createAthenaResource. AWS::Athena::PreparedStatement is handled separately
// via deleteAthenaPreparedStatement (deleteMorePropsBasedResource,
// resources_iam_more.go) since DeletePreparedStatement needs the sibling
// WorkGroup property, not just the physical ID.
func (rc *ResourceCreator) deleteAthenaResource(resourceType, physicalID string) (bool, error) {
	if rc.backends.Athena == nil {
		switch resourceType {
		case resTypeAthenaWorkGroup, resTypeAthenaDataCatalog, resTypeAthenaNamedQuery,
			resTypeAthenaCapacityReservation:
			return true, nil
		default:
			return false, nil
		}
	}

	b := rc.backends.Athena.Backend

	switch resourceType {
	case resTypeAthenaWorkGroup:
		return true, ignoreNotFound(ignoreNotFound(
			b.DeleteWorkGroup(physicalID, true), athenabackend.ErrNotFound), athenabackend.ErrProtected)
	case resTypeAthenaDataCatalog:
		_, err := b.DeleteDataCatalog(physicalID, false)

		return true, ignoreNotFound(ignoreNotFound(err, athenabackend.ErrNotFound), athenabackend.ErrProtected)
	case resTypeAthenaNamedQuery:
		return true, rc.deleteAthenaNamedQueryByName(physicalID)
	case resTypeAthenaCapacityReservation:
		return true, rc.deleteAthenaCapacityReservation(physicalID)
	default:
		return false, nil
	}
}

// ---- AWS::Athena::WorkGroup ----
// Ref returns the WorkGroup name (docs).

func (rc *ResourceCreator) createAthenaWorkGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Athena == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	cfg := athenabackend.WorkGroupConfiguration{}

	if wc, ok := props["WorkGroupConfiguration"].(map[string]any); ok {
		if rc2, ok2 := wc["ResultConfiguration"].(map[string]any); ok2 {
			cfg.ResultConfiguration.OutputLocation = strProp(rc2, "OutputLocation", params, physicalIDs)
		}
	}

	state := strProp(props, "State", params, physicalIDs)
	if state == "" {
		state = statusEnabled
	}

	err := rc.backends.Athena.Backend.CreateWorkGroup(
		name, strProp(props, "Description", params, physicalIDs), state, cfg,
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Athena workgroup %s: %w", name, err)
	}

	return name, nil
}

// ---- AWS::Athena::DataCatalog ----
// Ref returns the data catalog name (docs).

func (rc *ResourceCreator) createAthenaDataCatalog(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Athena == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	parameters := map[string]string{}

	if p, ok := props["Parameters"].(map[string]any); ok {
		for k, v := range p {
			if s, isStr := v.(string); isStr {
				parameters[k] = s
			}
		}
	}

	dc, err := rc.backends.Athena.Backend.CreateDataCatalog(
		name,
		strProp(props, "Type", params, physicalIDs),
		strProp(props, "Description", params, physicalIDs),
		parameters,
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Athena data catalog %s: %w", name, err)
	}

	return dc.Name, nil
}

// ---- AWS::Athena::NamedQuery ----
// The Template Reference's literal text ("Ref returns the resource name")
// cannot be this backend's (or real AWS's) physical identifier: NamedQuery
// is keyed by a generated NamedQueryId (CreateNamedQuery returns it, not the
// name; DeleteNamedQuery/GetNamedQuery are ID-keyed, with no name-keyed
// lookup), and GetAtt NamedQueryId is documented as a distinct attribute
// from Ref -- the same "docs text doesn't match this type's own identifier
// model" class already logged for AWS::Glue::Registry/AWS::ECR::
// RegistryPolicy in this file's PARITY.md. The generated ID is used as
// Ref/physical ID instead; Name is stashed as a GetAtt-style attribute.

func (rc *ResourceCreator) createAthenaNamedQuery(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Athena == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	id, err := rc.backends.Athena.Backend.CreateNamedQuery(
		name,
		strProp(props, "Description", params, physicalIDs),
		strProp(props, "Database", params, physicalIDs),
		strProp(props, "QueryString", params, physicalIDs),
		strProp(props, "WorkGroup", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Athena named query %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Name"] = name

	return id, nil
}

func (rc *ResourceCreator) deleteAthenaNamedQueryByName(id string) error {
	err := rc.backends.Athena.Backend.DeleteNamedQuery(id)

	return ignoreNotFound(err, athenabackend.ErrNotFound)
}

// ---- AWS::Athena::PreparedStatement ----
// Ref returns the name of the prepared statement (docs); delete needs the
// sibling WorkGroup property (PreparedStatement is keyed by
// workGroup+"/"+name), so it's handled via deleteMorePropsBasedResource.

func (rc *ResourceCreator) createAthenaPreparedStatement(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Athena == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "StatementName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	err := rc.backends.Athena.Backend.CreatePreparedStatement(
		name,
		strProp(props, "Description", params, physicalIDs),
		strProp(props, "WorkGroup", params, physicalIDs),
		strProp(props, "QueryStatement", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Athena prepared statement %s: %w", name, err)
	}

	return name, nil
}

func (rc *ResourceCreator) deleteAthenaPreparedStatement(
	props map[string]any, stackPhysicalIDs map[string]string,
) error {
	if rc.backends.Athena == nil {
		return nil
	}

	name := strProp(props, "StatementName", nil, stackPhysicalIDs)
	workGroup := strProp(props, "WorkGroup", nil, stackPhysicalIDs)

	err := rc.backends.Athena.Backend.DeletePreparedStatement(name, workGroup)

	return ignoreNotFound(err, athenabackend.ErrResourceNotFound)
}

// ---- AWS::Athena::CapacityReservation ----
// Ref returns the capacity reservation's ARN (docs); DeleteCapacityReservation
// is name-keyed (same class as the SageMaker ARN types above), and requires
// the reservation to be CANCELLED/CANCELLING first.

func (rc *ResourceCreator) createAthenaCapacityReservation(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Athena == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	err := rc.backends.Athena.Backend.CreateCapacityReservation(
		name, int32(intProp(props, "TargetDpus")), // #nosec G115 -- DPU count, not attacker-controlled range
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Athena capacity reservation %s: %w", name, err)
	}

	cr, err := rc.backends.Athena.Backend.GetCapacityReservation(name)
	if err != nil {
		return "", fmt.Errorf("get Athena capacity reservation %s after create: %w", name, err)
	}

	physicalIDs[logicalID+"/Name"] = cr.Name

	return rc.athenaCapacityReservationARN(name), nil
}

// athenaCapacityReservationARN mirrors athena.InMemoryBackend's own
// unexported capacityReservationARN helper (not accessible from this
// package: arn.Build("athena", region, accountID, "capacity-reservation/"+name)),
// so the Ref value round-trips through sagemakerNameFromARN's last-path-
// segment extraction on delete exactly like the SageMaker ARN types above.
func (rc *ResourceCreator) athenaCapacityReservationARN(name string) string {
	return arn.Build("athena", rc.backends.Region, rc.backends.AccountID, "capacity-reservation/"+name)
}

func (rc *ResourceCreator) deleteAthenaCapacityReservation(physicalID string) error {
	name := sagemakerNameFromARN(physicalID)

	b := rc.backends.Athena.Backend

	if err := b.CancelCapacityReservation(name); err != nil && !errors.Is(err, athenabackend.ErrNotFound) {
		return fmt.Errorf("cancel Athena capacity reservation %s: %w", name, err)
	}

	return ignoreNotFound(b.DeleteCapacityReservation(name), athenabackend.ErrNotFound)
}
