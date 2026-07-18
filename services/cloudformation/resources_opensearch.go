package cloudformation

import (
	"fmt"
	"strings"

	opensearchbackend "github.com/blackbirdworks/gopherstack/services/opensearch"
)

// ---- OpenSearch ----

func (rc *ResourceCreator) createOpenSearchDomain(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.OpenSearch == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "DomainName", params, physicalIDs)
	if name == "" {
		name = strings.ToLower(logicalID)
	}

	engineVersion := strProp(props, "EngineVersion", params, physicalIDs)

	var clusterConfig opensearchbackend.ClusterConfig
	if cc, ok := props["ClusterConfig"].(map[string]any); ok {
		clusterConfig.InstanceType = resolve(cc["InstanceType"], params, physicalIDs)

		if n, ok2 := cc["InstanceCount"].(float64); ok2 {
			clusterConfig.InstanceCount = int(n)
		}
	}

	domain, err := rc.backends.OpenSearch.Backend.CreateDomain(opensearchbackend.CreateDomainInput{
		Name:          name,
		EngineVersion: engineVersion,
		ClusterConfig: clusterConfig,
	})
	if err != nil {
		return "", fmt.Errorf("create OpenSearch domain %s: %w", name, err)
	}

	return domain.ARN, nil
}

func (rc *ResourceCreator) deleteOpenSearchDomain(arn string) error {
	if rc.backends.OpenSearch == nil {
		return nil
	}

	// OpenSearch domain name can be extracted from ARN: arn:aws:es:{region}:{account}:domain/{name}
	name := resourceNameFromARN(arn)

	_, err := rc.backends.OpenSearch.Backend.DeleteDomain(name)

	return err
}
