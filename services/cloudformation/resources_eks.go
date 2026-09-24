package cloudformation

import (
	"errors"
	"fmt"
	"strings"

	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
)

const resTypeEKSAccessEntry = "AWS::EKS::AccessEntry"

const resTypeEKSPodIdentityAssociation = "AWS::EKS::PodIdentityAssociation"

const resTypeEKSFargateProfile = "AWS::EKS::FargateProfile"

const resTypeEKSAddon = "AWS::EKS::Addon"

const resTypeEKSIdentityProviderConfig = "AWS::EKS::IdentityProviderConfig"

// eksNodegroupDefaultDesiredSize is the default desired node count for an EKS nodegroup.
const eksNodegroupDefaultDesiredSize int32 = 2

// eksNodegroupDefaultMaxSize is the default max node count for an EKS nodegroup.
const eksNodegroupDefaultMaxSize int32 = 5

func (rc *ResourceCreator) createEKSCluster(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EKS == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	version := strProp(props, "Version", params, physicalIDs)
	roleARN := strProp(props, "RoleArn", params, physicalIDs)

	_, err := rc.backends.EKS.Backend.CreateCluster(name, version, roleARN, nil, nil, nil)
	if err != nil {
		return "", fmt.Errorf("create EKS cluster %s: %w", name, err)
	}

	return name, nil
}

func (rc *ResourceCreator) deleteEKSCluster(physicalID string) error {
	if rc.backends.EKS == nil {
		return nil
	}

	_, err := rc.backends.EKS.Backend.DeleteCluster(physicalID)

	return err
}

func (rc *ResourceCreator) createEKSNodegroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EKS == nil {
		return logicalID + "-stub", nil
	}

	clusterName := strProp(props, "ClusterName", params, physicalIDs)
	nodegroupName := strProp(props, "NodegroupName", params, physicalIDs)
	if nodegroupName == "" {
		nodegroupName = logicalID
	}

	nodeRole := strProp(props, "NodeRole", params, physicalIDs)

	var instanceTypes []string
	if itRaw, ok := props["InstanceTypes"].([]any); ok {
		for _, v := range itRaw {
			if s, ok2 := v.(string); ok2 {
				instanceTypes = append(instanceTypes, s)
			}
		}
	}

	ng, err := rc.backends.EKS.Backend.CreateNodegroup(
		clusterName,
		nodegroupName,
		nodeRole,
		"AL2_x86_64",
		"ON_DEMAND",
		"",
		"",
		instanceTypes,
		eksNodegroupDefaultDesiredSize,
		1,
		eksNodegroupDefaultMaxSize,
		eksbackend.NodegroupInput{},
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("create EKS nodegroup %s: %w", nodegroupName, err)
	}

	return ng.ARN, nil
}

func (rc *ResourceCreator) deleteEKSNodegroup(arn string) error {
	if rc.backends.EKS == nil {
		return nil
	}

	// ARN format: arn:aws:eks:{region}:{account}:nodegroup/{cluster}/{nodegroup}/{uuid}
	parts := strings.Split(arn, "/")
	const eksNodegroupARNMinParts = 3
	if len(parts) < eksNodegroupARNMinParts {
		return nil
	}

	clusterName := parts[len(parts)-eksNodegroupARNMinParts]
	nodegroupName := parts[len(parts)-2]

	_, err := rc.backends.EKS.Backend.DeleteNodegroup(clusterName, nodegroupName)

	return err
}

// ---- EKS FargateProfile ----

func selectorsProp(props map[string]any) []eksbackend.FargateProfileSelector {
	raw, ok := props["Selectors"].([]any)
	if !ok {
		return nil
	}

	out := make([]eksbackend.FargateProfileSelector, 0, len(raw))

	for _, item := range raw {
		m, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		sel := eksbackend.FargateProfileSelector{
			Namespace: strProp(m, "Namespace", nil, nil),
		}

		if labels, hasLabels := m["Labels"].([]any); hasLabels {
			sel.Labels = tagListFromSlice(labels, nil, nil)
		}

		out = append(out, sel)
	}

	return out
}

func (rc *ResourceCreator) createEKSFargateProfile(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EKS == nil {
		return logicalID + "-stub", nil
	}

	clusterName := strProp(props, "ClusterName", params, physicalIDs)
	profileName := strProp(props, "FargateProfileName", params, physicalIDs)
	if profileName == "" {
		profileName = logicalID
	}

	roleARN := strProp(props, "PodExecutionRoleArn", params, physicalIDs)
	subnets := strSliceProp(props["Subnets"], params, physicalIDs)
	kv := tagListProp(props, params, physicalIDs)

	profile, err := rc.backends.EKS.Backend.CreateFargateProfile(
		clusterName, profileName, roleARN, selectorsProp(props), subnets, kv,
	)
	if err != nil {
		return "", fmt.Errorf("create EKS fargate profile %s: %w", profileName, err)
	}

	physicalIDs[logicalID+"/Arn"] = profile.ARN

	return clusterName + "/" + profileName, nil
}

func (rc *ResourceCreator) deleteEKSFargateProfile(physicalID string) error {
	if rc.backends.EKS == nil {
		return nil
	}

	clusterName, profileName, ok := strings.Cut(physicalID, "/")
	if !ok {
		return nil
	}

	_, err := rc.backends.EKS.Backend.DeleteFargateProfile(clusterName, profileName)
	if errors.Is(err, eksbackend.ErrNotFound) {
		return nil
	}

	return err
}

// ---- EKS Addon ----

func (rc *ResourceCreator) createEKSAddon(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EKS == nil {
		return logicalID + "-stub", nil
	}

	clusterName := strProp(props, "ClusterName", params, physicalIDs)
	addonName := strProp(props, "AddonName", params, physicalIDs)
	addonVersion := strProp(props, "AddonVersion", params, physicalIDs)
	roleARN := strProp(props, "ServiceAccountRoleArn", params, physicalIDs)
	resolveConflicts := strProp(props, "ResolveConflicts", params, physicalIDs)
	kv := tagListProp(props, params, physicalIDs)

	addon, err := rc.backends.EKS.Backend.CreateAddon(
		clusterName, addonName, addonVersion, roleARN, "", resolveConflicts, "", kv, nil,
	)
	if err != nil {
		return "", fmt.Errorf("create EKS addon %s: %w", addonName, err)
	}

	physicalIDs[logicalID+"/Arn"] = addon.ARN

	return clusterName + "|" + addonName, nil
}

func (rc *ResourceCreator) deleteEKSAddon(physicalID string) error {
	if rc.backends.EKS == nil {
		return nil
	}

	clusterName, addonName, ok := strings.Cut(physicalID, "|")
	if !ok {
		return nil
	}

	_, err := rc.backends.EKS.Backend.DeleteAddon(clusterName, addonName, false)
	if errors.Is(err, eksbackend.ErrNotFound) {
		return nil
	}

	return err
}

// ---- EKS AccessEntry ----

func (rc *ResourceCreator) createEKSAccessEntry(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EKS == nil {
		return logicalID + "-stub", nil
	}

	clusterName := strProp(props, "ClusterName", params, physicalIDs)
	principalARN := strProp(props, "PrincipalArn", params, physicalIDs)
	entryType := strProp(props, "Type", params, physicalIDs)
	username := strProp(props, "Username", params, physicalIDs)
	groups := strSliceProp(props["KubernetesGroups"], params, physicalIDs)
	kv := tagListProp(props, params, physicalIDs)

	entry, err := rc.backends.EKS.Backend.CreateAccessEntry(clusterName, principalARN, entryType, username, groups, kv)
	if err != nil {
		return "", fmt.Errorf("create EKS access entry %s: %w", principalARN, err)
	}

	physicalIDs[logicalID+"/AccessEntryArn"] = entry.ARN

	if policies, hasPolicies := props["AccessPolicies"].([]any); hasPolicies {
		for _, p := range policies {
			pm, isMap := p.(map[string]any)
			if !isMap {
				continue
			}

			policyARN := strProp(pm, "PolicyArn", params, physicalIDs)
			scope, _ := pm["AccessScope"].(map[string]any)

			if _, assocErr := rc.backends.EKS.Backend.AssociateAccessPolicy(
				clusterName, principalARN, policyARN, scope,
			); assocErr != nil {
				return "", fmt.Errorf("associate EKS access policy %s: %w", policyARN, assocErr)
			}
		}
	}

	return principalARN, nil
}

// deleteEKSAccessEntry deletes an access entry. ClusterName isn't recoverable from
// physicalID (Ref is PrincipalArn per the CFN docs), so it's read from props.
func (rc *ResourceCreator) deleteEKSAccessEntry(
	props map[string]any, stackPhysicalIDs map[string]string, physicalID string,
) error {
	if rc.backends.EKS == nil {
		return nil
	}

	clusterName := strProp(props, "ClusterName", nil, stackPhysicalIDs)

	err := rc.backends.EKS.Backend.DeleteAccessEntry(clusterName, physicalID)
	if errors.Is(err, eksbackend.ErrNotFound) {
		return nil
	}

	return err
}

// ---- EKS PodIdentityAssociation ----

func (rc *ResourceCreator) createEKSPodIdentityAssociation(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EKS == nil {
		return logicalID + "-stub", nil
	}

	clusterName := strProp(props, "ClusterName", params, physicalIDs)
	namespace := strProp(props, "Namespace", params, physicalIDs)
	serviceAccount := strProp(props, "ServiceAccount", params, physicalIDs)
	roleARN := strProp(props, "RoleArn", params, physicalIDs)
	kv := tagListProp(props, params, physicalIDs)

	opt := eksbackend.PodIdentityAssociationInput{
		Policy:             strProp(props, "Policy", params, physicalIDs),
		DisableSessionTags: boolProp(props, "DisableSessionTags"),
	}

	assoc, err := rc.backends.EKS.Backend.CreatePodIdentityAssociation(
		clusterName, namespace, serviceAccount, roleARN, kv, opt,
	)
	if err != nil {
		return "", fmt.Errorf("create EKS pod identity association in cluster %s: %w", clusterName, err)
	}

	physicalIDs[logicalID+"/AssociationArn"] = assoc.ARN
	physicalIDs[logicalID+"/ExternalId"] = assoc.ExternalID

	return assoc.AssociationID, nil
}

// deleteEKSPodIdentityAssociation deletes a pod identity association. ClusterName
// isn't recoverable from physicalID (Ref is the association ID), so it's read from props.
func (rc *ResourceCreator) deleteEKSPodIdentityAssociation(
	props map[string]any, stackPhysicalIDs map[string]string, physicalID string,
) error {
	if rc.backends.EKS == nil {
		return nil
	}

	clusterName := strProp(props, "ClusterName", nil, stackPhysicalIDs)

	_, err := rc.backends.EKS.Backend.DeletePodIdentityAssociation(clusterName, physicalID)
	if errors.Is(err, eksbackend.ErrNotFound) {
		return nil
	}

	return err
}

// ---- EKS IdentityProviderConfig ----

func (rc *ResourceCreator) createEKSIdentityProviderConfig(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EKS == nil {
		return logicalID + "-stub", nil
	}

	clusterName := strProp(props, "ClusterName", params, physicalIDs)
	configType := strProp(props, "Type", params, physicalIDs)
	name := strProp(props, "IdentityProviderConfigName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	oidcParams := map[string]string{}
	var requiredClaims map[string]string

	if oidc, isMap := props["Oidc"].(map[string]any); isMap {
		for _, key := range []string{
			"ClientId", "IssuerUrl", "GroupsClaim", "GroupsPrefix", "UsernameClaim", "UsernamePrefix",
		} {
			if v := strProp(oidc, key, params, physicalIDs); v != "" {
				oidcParams[key] = v
			}
		}

		if claims, hasClaims := oidc["RequiredClaims"].([]any); hasClaims {
			requiredClaims = tagListFromSlice(claims, params, physicalIDs)
		}
	}

	kv := tagListProp(props, params, physicalIDs)

	cfg, err := rc.backends.EKS.Backend.AssociateIdentityProviderConfig(
		clusterName, configType, name, oidcParams, requiredClaims, kv,
	)
	if err != nil {
		return "", fmt.Errorf("associate EKS identity provider config %s: %w", name, err)
	}

	physicalIDs[logicalID+"/IdentityProviderConfigArn"] = cfg.ARN

	return clusterName + "/oidc/" + name, nil
}

func (rc *ResourceCreator) deleteEKSIdentityProviderConfig(physicalID string) error {
	if rc.backends.EKS == nil {
		return nil
	}

	const idpConfigPhysIDParts = 3

	parts := strings.SplitN(physicalID, "/", idpConfigPhysIDParts)
	if len(parts) != idpConfigPhysIDParts {
		return nil
	}

	err := rc.backends.EKS.Backend.DisassociateIdentityProviderConfig(parts[0], parts[2])
	if errors.Is(err, eksbackend.ErrNotFound) {
		return nil
	}

	return err
}
