package cloudformation

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
)

const (
	lambdaDefaultMemoryMB    = 128
	lambdaDefaultTimeoutSecs = 3
)

// UpdateInPlace applies changed properties to an existing resource whose update
// does not need replacement. It reports false for resource types that have no
// in-place updater, leaving the resource as deployed.
func (rc *ResourceCreator) UpdateInPlace(
	ctx context.Context,
	resourceType, physicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (bool, error) {
	if rc == nil || rc.backends == nil {
		return false, nil
	}

	ctx = rc.withStackRegion(ctx)

	switch resourceType {
	case resTypeCloudHSMCluster:
		return true, rc.updateCloudHSMCluster(ctx, physicalID, props, params, physicalIDs)
	case resTypeOAMSink, resTypeOAMLink:
		return true, rc.updateOAMResource(ctx, resourceType, physicalID, props, params, physicalIDs)
	case resTypeRUMAppMonitor:
		return true, rc.updateRUMAppMonitor(ctx, physicalID, props, params, physicalIDs)
	case resTypeLambdaFunction:
		return true, rc.updateLambdaFunction(physicalID, props, params, physicalIDs)
	case resTypeSQSQueue:
		return true, rc.updateSQSQueue(physicalID, props, params, physicalIDs)
	case resTypeStepFunctionsStateMachine:
		return true, rc.updateStepFunctionsStateMachine(physicalID, props, params, physicalIDs)
	case resTypeGAAccelerator, resTypeGAListener, resTypeGAEndpointGroup, resTypeGAAttachment:
		return true, rc.updateGlobalAcceleratorResource(resourceType, physicalID, props, params, physicalIDs)
	}

	return false, nil
}

func (rc *ResourceCreator) updateLambdaFunction(
	name string, props map[string]any, params, physicalIDs map[string]string,
) error {
	if rc.backends.Lambda == nil {
		return nil
	}

	cur, err := rc.backends.Lambda.Backend.GetFunction(name)
	if err != nil {
		return fmt.Errorf("update Lambda function: %w", err)
	}

	next := *cur
	next.Runtime = strProp(props, "Runtime", params, physicalIDs)
	next.Handler = strProp(props, "Handler", params, physicalIDs)
	next.Role = strProp(props, "Role", params, physicalIDs)
	next.Description = strProp(props, "Description", params, physicalIDs)
	next.Layers, next.Environment, next.TracingConfig, next.DeadLetterConfig = nil, nil, nil, nil
	applyLambdaTuning(&next, props, params, physicalIDs)

	if next.MemorySize == 0 {
		next.MemorySize = lambdaDefaultMemoryMB
	}

	if next.Timeout == 0 {
		next.Timeout = lambdaDefaultTimeoutSecs
	}

	if next.ZipData != nil {
		sum := sha256.Sum256(next.ZipData)
		next.CodeSha256 = base64.StdEncoding.EncodeToString(sum[:])
		next.CodeSize = int64(len(next.ZipData))
	}

	next.LastModified = time.Now().UTC().Format(time.RFC3339)
	next.RevisionID = uuid.NewString()

	if err = rc.backends.Lambda.Backend.UpdateFunction(&next); err != nil {
		return fmt.Errorf("update Lambda function: %w", err)
	}

	return nil
}

func sqsAttributesFromProps(props map[string]any, params, physicalIDs map[string]string) map[string]string {
	attrs := map[string]string{}

	for _, key := range []string{
		"VisibilityTimeout", "MessageRetentionPeriod", "DelaySeconds",
		"ReceiveMessageWaitTimeSeconds", "MaximumMessageSize",
	} {
		if v := strProp(props, key, params, physicalIDs); v != "" {
			attrs[key] = v
		}
	}

	if rp, ok := props["RedrivePolicy"]; ok {
		if b, err := json.Marshal(resolveDeep(rp, params, physicalIDs)); err == nil {
			attrs["RedrivePolicy"] = string(b)
		}
	}

	return attrs
}

func (rc *ResourceCreator) updateSQSQueue(
	queueURL string, props map[string]any, params, physicalIDs map[string]string,
) error {
	if rc.backends.SQS == nil {
		return nil
	}

	attrs := sqsAttributesFromProps(props, params, physicalIDs)
	if len(attrs) == 0 {
		return nil
	}

	err := rc.backends.SQS.Backend.SetQueueAttributes(&sqsbackend.SetQueueAttributesInput{
		QueueURL:   queueURL,
		Attributes: attrs,
		Region:     rc.backends.Region,
	})
	if err != nil {
		return fmt.Errorf("update SQS queue: %w", err)
	}

	return nil
}

func (rc *ResourceCreator) updateStepFunctionsStateMachine(
	arn string, props map[string]any, params, physicalIDs map[string]string,
) error {
	if rc.backends.StepFunctions == nil {
		return nil
	}

	definition := strProp(props, "DefinitionString", params, physicalIDs)
	roleArn := strProp(props, "RoleArn", params, physicalIDs)

	if _, _, err := rc.backends.StepFunctions.Backend.UpdateStateMachine(arn, definition, roleArn); err != nil {
		return fmt.Errorf("update StepFunctions state machine: %w", err)
	}

	return nil
}
