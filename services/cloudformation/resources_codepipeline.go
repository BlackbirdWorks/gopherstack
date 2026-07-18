package cloudformation

import (
	"context"
	"fmt"

	codepipelinebackend "github.com/blackbirdworks/gopherstack/services/codepipeline"
)

// ---- CodePipeline ----

func (rc *ResourceCreator) createCodePipelinePipeline(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CodePipeline == nil {
		return logicalID + "-stub", nil
	}

	name := logicalID

	var decl codepipelinebackend.PipelineDeclaration
	if p, ok := props["Pipeline"].(map[string]any); ok {
		if n := resolve(p["Name"], params, physicalIDs); n != "" {
			name = n
		}

		decl.Name = name
		decl.RoleArn = resolve(p["RoleArn"], params, physicalIDs)

		if as, ok2 := p["ArtifactStore"].(map[string]any); ok2 {
			decl.ArtifactStore = codepipelinebackend.ArtifactStore{
				Type:     resolve(as["Type"], params, physicalIDs),
				Location: resolve(as["Location"], params, physicalIDs),
			}
		}
	} else {
		decl.Name = name
	}

	pipeline, err := rc.backends.CodePipeline.Backend.CreatePipeline(
		ctx,
		decl,
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("create CodePipeline pipeline %s: %w", name, err)
	}

	return pipeline.Metadata.PipelineArn, nil
}

func (rc *ResourceCreator) deleteCodePipelinePipeline(ctx context.Context, arn string) error {
	if rc.backends.CodePipeline == nil {
		return nil
	}

	name := resourceNameFromARN(arn)

	return rc.backends.CodePipeline.Backend.DeletePipeline(ctx, name)
}
