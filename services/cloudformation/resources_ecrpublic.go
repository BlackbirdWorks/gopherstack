package cloudformation

import (
	"fmt"

	ecrpublicbackend "github.com/blackbirdworks/gopherstack/services/ecrpublic"
)

const resTypeECRPublicRepository = "AWS::ECR::PublicRepository"

// createECRPublicResource handles the ECR Public resource type listed above.
func (rc *ResourceCreator) createECRPublicResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if resourceType != resTypeECRPublicRepository {
		return "", false, nil
	}

	id, err := rc.createECRPublicRepository(logicalID, props, params, physicalIDs)

	return id, true, err
}

// deleteECRPublicResource handles deletion for the type created above.
func (rc *ResourceCreator) deleteECRPublicResource(resourceType, physicalID string) (bool, error) {
	if resourceType != resTypeECRPublicRepository {
		return false, nil
	}

	if rc.backends.ECRPublic == nil {
		return true, nil
	}

	// No force: AWS::ECR::PublicRepository has no EmptyOnDelete property
	// (unlike AWS::ECR::Repository), so a non-empty repository fails to
	// delete here exactly as it does in real CloudFormation.
	_, err := rc.backends.ECRPublic.Backend.DeleteRepository(rc.backends.AccountID, physicalID, false)

	return true, ignoreNotFound(err, ecrpublicbackend.ErrRepositoryNotFound)
}

// ---- AWS::ECR::PublicRepository ----
// Ref returns the repository name (documented). Fn::GetAtt Arn returns the
// repository ARN (documented).

func (rc *ResourceCreator) createECRPublicRepository(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ECRPublic == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "RepositoryName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	catalogData, _ := props["RepositoryCatalogData"].(map[string]any)

	repo, err := rc.backends.ECRPublic.Backend.CreateRepository(
		name,
		&ecrpublicbackend.CatalogData{
			AboutText:        strProp(catalogData, "AboutText", params, physicalIDs),
			Description:      strProp(catalogData, "RepositoryDescription", params, physicalIDs),
			UsageText:        strProp(catalogData, "UsageText", params, physicalIDs),
			Architectures:    strSliceProp(catalogData["Architectures"], params, physicalIDs),
			OperatingSystems: strSliceProp(catalogData["OperatingSystems"], params, physicalIDs),
		},
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create ECR Public repository %s: %w", name, err)
	}

	if policyText := jsonProp(props, "RepositoryPolicyText"); policyText != "" {
		if _, setErr := rc.backends.ECRPublic.Backend.SetRepositoryPolicy(
			rc.backends.AccountID, repo.RepositoryName, policyText,
		); setErr != nil {
			return "", fmt.Errorf("set ECR Public repository policy %s: %w", name, setErr)
		}
	}

	physicalIDs[logicalID+"/Arn"] = repo.RepositoryArn

	return repo.RepositoryName, nil
}
