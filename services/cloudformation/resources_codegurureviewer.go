package cloudformation

import (
	"context"
	"fmt"

	cgrbackend "github.com/blackbirdworks/gopherstack/services/codegurureviewer"
)

const resTypeCodeGuruRepositoryAssociation = "AWS::CodeGuruReviewer::RepositoryAssociation"

// createCodeGuruRepositoryAssociation provisions an AWS::CodeGuruReviewer::RepositoryAssociation.
// Ref and GetAtt AssociationArn return the association ARN.
func (rc *ResourceCreator) createCodeGuruRepositoryAssociation(
	ctx context.Context, logicalID string, props map[string]any, params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CodeGuruReviewer == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	repo, err := codeGuruRepository(
		strProp(props, "Type", params, physicalIDs),
		name,
		strProp(props, "Owner", params, physicalIDs),
		strProp(props, "BucketName", params, physicalIDs),
		strProp(props, "ConnectionArn", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create CodeGuru Reviewer association %s: %w", logicalID, err)
	}

	rec, err := rc.backends.CodeGuruReviewer.Backend.AssociateRepository(ctx, &cgrbackend.AssociateRepositoryInput{
		Repository: repo,
		Tags:       tagListProp(props, params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create CodeGuru Reviewer association %s: %w", name, err)
	}

	return rec.ARN, nil
}

func codeGuruRepository(repoType, name, owner, bucket, connection string) (*cgrbackend.Repository, error) {
	switch repoType {
	case "CodeCommit":
		return &cgrbackend.Repository{CodeCommit: &cgrbackend.CodeCommitRepository{Name: name}}, nil
	case "S3Bucket":
		return &cgrbackend.Repository{S3Bucket: &cgrbackend.S3Repository{Name: name, BucketName: bucket}}, nil
	case "Bitbucket":
		return &cgrbackend.Repository{Bitbucket: &cgrbackend.ThirdPartySourceRepository{
			Name: name, Owner: owner, ConnectionArn: connection,
		}}, nil
	case "GitHubEnterpriseServer":
		return &cgrbackend.Repository{GitHubEnterpriseServer: &cgrbackend.ThirdPartySourceRepository{
			Name: name, Owner: owner, ConnectionArn: connection,
		}}, nil
	default:
		return nil, fmt.Errorf("%w: Type %q", errUnsupportedCFNValue, repoType)
	}
}

func (rc *ResourceCreator) deleteCodeGuruRepositoryAssociation(physicalID string) error {
	if rc.backends.CodeGuruReviewer == nil {
		return nil
	}

	_, err := rc.backends.CodeGuruReviewer.Backend.DisassociateRepository(physicalID)

	return ignoreRESTNotFound(err)
}
