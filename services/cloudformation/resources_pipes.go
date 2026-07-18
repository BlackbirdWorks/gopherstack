package cloudformation

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

// ---- Pipes ----

func (rc *ResourceCreator) createPipesPipe(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Pipes == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	roleARN := strProp(props, "RoleArn", params, physicalIDs)
	source := strProp(props, "Source", params, physicalIDs)
	target := strProp(props, "Target", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)

	pipe, err := rc.backends.Pipes.Backend.CreatePipe(ctx, pipes.CreatePipeInput{
		Name:        name,
		RoleARN:     roleARN,
		Source:      source,
		Target:      target,
		Description: description,
	})
	if err != nil {
		return "", fmt.Errorf("create Pipes pipe %s: %w", name, err)
	}

	return pipe.ARN, nil
}

func (rc *ResourceCreator) deletePipesPipe(ctx context.Context, arn string) error {
	if rc.backends.Pipes == nil {
		return nil
	}

	name := resourceNameFromARN(arn)

	_, err := rc.backends.Pipes.Backend.DeletePipe(ctx, name)

	return err
}
