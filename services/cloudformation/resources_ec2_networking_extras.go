package cloudformation

import (
	"fmt"
	"strconv"

	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

const (
	resTypeEC2PlacementGroup     = "AWS::EC2::PlacementGroup"
	resTypeEC2NetIfacePermission = "AWS::EC2::NetworkInterfacePermission"
	resTypeEC2PrefixList         = "AWS::EC2::PrefixList"
	resTypeEC2VPCEndpointService = "AWS::EC2::VPCEndpointService"
	resTypeEC2VerifiedAccessInst = "AWS::EC2::VerifiedAccessInstance"
)

// createEC2NetworkingExtrasResource handles the standalone EC2 networking
// types listed above (no shared props). Returns handled=false otherwise.
func (rc *ResourceCreator) createEC2NetworkingExtrasResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeEC2PlacementGroup:
		id, err := rc.createEC2PlacementGroup(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEC2NetIfacePermission:
		id, err := rc.createEC2NetworkInterfacePermission(props, params, physicalIDs)

		return id, true, err
	case resTypeEC2PrefixList:
		id, err := rc.createEC2PrefixList(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEC2VPCEndpointService:
		id, err := rc.createEC2VPCEndpointService(props, params, physicalIDs)

		return id, true, err
	case resTypeEC2VerifiedAccessInst:
		id, err := rc.createEC2VerifiedAccessInstance(props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteEC2NetworkingExtrasResource handles deletion for the types created above.
func (rc *ResourceCreator) deleteEC2NetworkingExtrasResource(resourceType, physicalID string) (bool, error) {
	if rc.backends.EC2 == nil {
		switch resourceType {
		case resTypeEC2PlacementGroup, resTypeEC2NetIfacePermission, resTypeEC2PrefixList,
			resTypeEC2VPCEndpointService, resTypeEC2VerifiedAccessInst:
			return true, nil
		default:
			return false, nil
		}
	}

	switch resourceType {
	case resTypeEC2PlacementGroup:
		return true, ignoreNotFound(
			rc.backends.EC2.Backend.DeletePlacementGroup(physicalID), ec2backend.ErrPlacementGroupNotFound,
		)
	case resTypeEC2NetIfacePermission:
		return true, ignoreNotFound(
			rc.backends.EC2.Backend.DeleteNetworkInterfacePermission(physicalID),
			ec2backend.ErrNetworkInterfacePermissionNotFound,
		)
	case resTypeEC2PrefixList:
		_, err := rc.backends.EC2.Backend.DeleteManagedPrefixList(physicalID)

		return true, ignoreNotFound(err, ec2backend.ErrManagedPrefixListNotFound)
	case resTypeEC2VPCEndpointService:
		return true, rc.backends.EC2.Backend.DeleteVpcEndpointServiceConfigurations([]string{physicalID})
	case resTypeEC2VerifiedAccessInst:
		_, err := rc.backends.EC2.Backend.DeleteVerifiedAccessInstance(physicalID)

		return true, ignoreNotFound(err, ec2backend.ErrVerifiedAccessInstanceNotFound)
	default:
		return false, nil
	}
}

// ---- AWS::EC2::PlacementGroup ----
// Ref returns the placement group name (documented). GroupId has no
// separate identifier in this backend (PlacementGroup has no ID field
// distinct from its name), so Fn::GetAtt.GroupId falls back to the name too.

func (rc *ResourceCreator) createEC2PlacementGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "GroupName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	pg, err := rc.backends.EC2.Backend.CreatePlacementGroup(
		name, strProp(props, "Strategy", params, physicalIDs), tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create placement group %s: %w", name, err)
	}

	return pg.Name, nil
}

// ---- AWS::EC2::NetworkInterfacePermission ----
// Ref returns the permission's resource name (documented, undocumented
// attribute list -- no Fn::GetAtt section on the docs page).

func (rc *ResourceCreator) createEC2NetworkInterfacePermission(
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return "eni-perm-stub", nil
	}

	perm, err := rc.backends.EC2.Backend.CreateNetworkInterfacePermission(
		strProp(props, "NetworkInterfaceId", params, physicalIDs),
		strProp(props, "AwsAccountId", params, physicalIDs),
		strProp(props, "AwsService", params, physicalIDs),
		strProp(props, "Permission", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create network interface permission: %w", err)
	}

	return perm.PermissionID, nil
}

// ---- AWS::EC2::PrefixList ----
// Ref returns the prefix list ID (documented). Arn, OwnerId, and Version are
// stashed at create time and read back through resolveGetAtt's
// stack-props side channel (see getExtraResourceAttribute's prefix-list case).

func (rc *ResourceCreator) createEC2PrefixList(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "PrefixListName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	var entries []ec2backend.PrefixListEntry
	if raw, ok := props["Entries"].([]any); ok {
		for _, e := range raw {
			em, isMap := e.(map[string]any)
			if !isMap {
				continue
			}

			entries = append(entries, ec2backend.PrefixListEntry{
				Cidr:        strProp(em, "Cidr", params, physicalIDs),
				Description: strProp(em, "Description", params, physicalIDs),
			})
		}
	}

	pl, err := rc.backends.EC2.Backend.CreateManagedPrefixList(
		name, strProp(props, "AddressFamily", params, physicalIDs), intProp(props, "MaxEntries"), entries,
	)
	if err != nil {
		return "", fmt.Errorf("create managed prefix list %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = pl.PrefixListArn
	physicalIDs[logicalID+"/OwnerId"] = pl.OwnerID
	physicalIDs[logicalID+"/Version"] = strconv.FormatInt(pl.Version, 10)

	return pl.PrefixListID, nil
}

// ---- AWS::EC2::VPCEndpointService ----
// Ref returns the ID of the VPC endpoint service configuration (documented).
// PrivateDnsNameConfiguration is not modeled by this backend (private DNS
// name verification is not implemented), so those Fn::GetAtt attributes
// are left unimplemented rather than fabricated.

func (rc *ResourceCreator) createEC2VPCEndpointService(
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return "vpce-svc-stub", nil
	}

	cfg, err := rc.backends.EC2.Backend.CreateVpcEndpointServiceConfiguration(
		boolProp(props, "AcceptanceRequired"), strSliceProp(props["NetworkLoadBalancerArns"], params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create VPC endpoint service configuration: %w", err)
	}

	return cfg.ServiceID, nil
}

// ---- AWS::EC2::VerifiedAccessInstance ----
// Ref returns the ID of the Verified Access instance (documented).
// CreationTime, LastUpdatedTime, and CidrEndpointsCustomSubDomainNameServers
// are not tracked by this backend, so those attributes fall back to the
// instance ID rather than being fabricated.

func (rc *ResourceCreator) createEC2VerifiedAccessInstance(
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return "vai-stub", nil
	}

	inst, err := rc.backends.EC2.Backend.CreateVerifiedAccessInstance(
		strProp(props, "Description", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Verified Access instance: %w", err)
	}

	return inst.VerifiedAccessInstanceID, nil
}
