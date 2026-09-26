package cloudformation

import (
	"context"
	"fmt"

	codeartifactbackend "github.com/blackbirdworks/gopherstack/services/codeartifact"
)

const (
	resTypeCodeArtifactDomain       = "AWS::CodeArtifact::Domain"
	resTypeCodeArtifactRepository   = "AWS::CodeArtifact::Repository"
	resTypeCodeArtifactPackageGroup = "AWS::CodeArtifact::PackageGroup"
)

// createCodeArtifactResource handles the CodeArtifact resource types listed
// above. Returns handled=false when resourceType isn't one of them.
func (rc *ResourceCreator) createCodeArtifactResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeCodeArtifactDomain:
		id, err := rc.createCodeArtifactDomain(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeCodeArtifactRepository:
		id, err := rc.createCodeArtifactRepository(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeCodeArtifactPackageGroup:
		id, err := rc.createCodeArtifactPackageGroup(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteCodeArtifactResource handles AWS::CodeArtifact::Domain deletion only;
// Repository and PackageGroup delete through deletePropsBasedResource
// (resources.go) since their Delete* backend methods need the owning
// DomainName, a sibling property not embedded in the Ref/ARN value.
func (rc *ResourceCreator) deleteCodeArtifactResource(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	if resourceType != resTypeCodeArtifactDomain {
		return false, nil
	}

	if rc.backends.CodeArtifact == nil {
		return true, nil
	}

	name := sagemakerNameFromARN(physicalID)
	_, err := rc.backends.CodeArtifact.Backend.DeleteDomain(ctx, name)

	return true, ignoreNotFound(err, codeartifactbackend.ErrNotFound)
}

// ---- AWS::CodeArtifact::Domain ----
// Ref returns the resource ARN (docs); DeleteDomain is name-keyed, so
// physicalID is converted back to a name the same way resources_sagemaker.go
// does for its ARN-keyed types.

func (rc *ResourceCreator) createCodeArtifactDomain(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CodeArtifact == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "DomainName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	d, err := rc.backends.CodeArtifact.Backend.CreateDomain(
		ctx, name, strProp(props, "EncryptionKey", params, physicalIDs), tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create CodeArtifact domain %s: %w", name, err)
	}

	physicalIDs[logicalID+"/EncryptionKey"] = d.EncryptionKey
	physicalIDs[logicalID+"/Name"] = d.Name
	physicalIDs[logicalID+"/Owner"] = d.Owner

	return d.ARN, nil
}

// ---- AWS::CodeArtifact::Repository ----
// Ref returns the resource ARN (docs); delete needs the sibling DomainName
// property (DeleteRepository is (domainName, repoName)-keyed), so it's
// handled via deletePropsBasedResource.

func (rc *ResourceCreator) createCodeArtifactRepository(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CodeArtifact == nil {
		return logicalID + "-stub", nil
	}

	domainName := strProp(props, "DomainName", params, physicalIDs)
	repoName := strProp(props, "RepositoryName", params, physicalIDs)
	if repoName == "" {
		repoName = logicalID
	}

	r, err := rc.backends.CodeArtifact.Backend.CreateRepository(
		ctx,
		domainName,
		repoName,
		strProp(props, "Description", params, physicalIDs),
		tagListProp(props, params, physicalIDs),
		strSliceProp(props["Upstreams"], params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create CodeArtifact repository %s/%s: %w", domainName, repoName, err)
	}

	physicalIDs[logicalID+"/DomainName"] = r.DomainName
	physicalIDs[logicalID+"/DomainOwner"] = r.DomainOwner
	physicalIDs[logicalID+"/Name"] = r.Name

	return r.ARN, nil
}

func (rc *ResourceCreator) deleteCodeArtifactRepository(
	ctx context.Context, props map[string]any, stackPhysicalIDs map[string]string,
) error {
	if rc.backends.CodeArtifact == nil {
		return nil
	}

	domainName := strProp(props, "DomainName", nil, stackPhysicalIDs)
	repoName := strProp(props, "RepositoryName", nil, stackPhysicalIDs)

	_, err := rc.backends.CodeArtifact.Backend.DeleteRepository(ctx, domainName, repoName)

	return ignoreNotFound(err, codeartifactbackend.ErrNotFound)
}

// ---- AWS::CodeArtifact::PackageGroup ----
// Ref is undocumented (empty Ref section); the resource ARN is used as the
// physical ID, matching PackageGroup's documented Arn GetAtt attribute and
// its sibling types' ARN-shaped Ref above. Delete needs the sibling
// DomainName property (DeletePackageGroup is (domainName, pattern)-keyed),
// so it's handled via deletePropsBasedResource.

func (rc *ResourceCreator) createCodeArtifactPackageGroup(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CodeArtifact == nil {
		return logicalID + "-stub", nil
	}

	domainName := strProp(props, "DomainName", params, physicalIDs)
	pattern := strProp(props, "Pattern", params, physicalIDs)

	pg, err := rc.backends.CodeArtifact.Backend.CreatePackageGroup(
		ctx,
		domainName,
		pattern,
		strProp(props, "Description", params, physicalIDs),
		strProp(props, "ContactInfo", params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create CodeArtifact package group %s/%s: %w", domainName, pattern, err)
	}

	physicalIDs[logicalID+"/DomainName"] = pg.DomainName
	physicalIDs[logicalID+"/DomainOwner"] = pg.DomainOwner
	physicalIDs[logicalID+"/Pattern"] = pg.Pattern

	return pg.ARN, nil
}

func (rc *ResourceCreator) deleteCodeArtifactPackageGroup(
	ctx context.Context, props map[string]any, stackPhysicalIDs map[string]string,
) error {
	if rc.backends.CodeArtifact == nil {
		return nil
	}

	domainName := strProp(props, "DomainName", nil, stackPhysicalIDs)
	pattern := strProp(props, "Pattern", nil, stackPhysicalIDs)

	_, err := rc.backends.CodeArtifact.Backend.DeletePackageGroup(ctx, domainName, pattern)

	return ignoreNotFound(err, codeartifactbackend.ErrNotFound)
}
