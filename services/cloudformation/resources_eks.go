package cloudformation

import (
	"fmt"
	"strings"

	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
)

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
