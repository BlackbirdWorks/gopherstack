package eventbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// arnParts is the field count of arn:partition:service:region:account:resource.
const arnParts = 6

var (
	errNoRunCommandParameters = errors.New("RunCommandParameters is required for a Systems Manager Run Command target")
	errNoGraphQLOperation     = errors.New("AppSyncParameters.GraphQLOperation is required for an AppSync target")
)

// SSMCommandSender calls SSM SendCommand for a Run Command target.
type SSMCommandSender interface {
	SendSSMCommand(ctx context.Context, req SSMCommandRequest) error
}

// SSMCommandRequest is the SendCommand call derived from a Run Command target.
type SSMCommandRequest struct {
	Parameters      map[string][]string
	DocumentName    string
	DocumentVersion string
	Region          string
	Targets         []RunCommandTarget
}

// SSMAutomationStarter calls SSM StartAutomationExecution for an Automation target.
type SSMAutomationStarter interface {
	StartSSMAutomation(ctx context.Context, req SSMAutomationRequest) error
}

// SSMAutomationRequest is the StartAutomationExecution call derived from an Automation target.
type SSMAutomationRequest struct {
	Parameters      map[string][]string
	DocumentName    string
	DocumentVersion string
	Region          string
}

// AppSyncExecutor runs a GraphQL operation against the AppSync API named by apiARN.
type AppSyncExecutor interface {
	ExecuteAppSyncOperation(ctx context.Context, apiARN, operation string, variables map[string]any) error
}

func isSSMDocumentARN(arn string) bool {
	return strings.HasPrefix(arn, "arn:aws:ssm:") && strings.Contains(arn, ":document/")
}

func isSSMAutomationARN(arn string) bool {
	return strings.HasPrefix(arn, "arn:aws:ssm:") && strings.Contains(arn, ":automation-definition/")
}

func isAppSyncARN(arn string) bool {
	return strings.HasPrefix(arn, "arn:aws:appsync:") && strings.Contains(arn, ":apis/")
}

func arnRegionOf(arn string) string {
	parts := strings.SplitN(arn, ":", arnParts)
	if len(parts) < arnParts {
		return ""
	}

	return parts[3]
}

// ssmInput is the JSON the target Input carries for Run Command targets.
type ssmInput struct {
	Parameters      map[string]any `json:"Parameters"`
	DocumentName    string         `json:"DocumentName"`
	DocumentVersion string         `json:"DocumentVersion"`
}

// ssmParameterMap keeps string and string-list values, the only shapes SSM parameters take.
func ssmParameterMap(in map[string]any) map[string][]string {
	out := make(map[string][]string, len(in))

	for k, v := range in {
		switch val := v.(type) {
		case string:
			out[k] = []string{val}
		case []any:
			list := make([]string, 0, len(val))

			for _, e := range val {
				if s, ok := e.(string); ok {
					list = append(list, s)
				}
			}

			out[k] = list
		}
	}

	return out
}

// BuildSSMCommandRequest maps a Run Command target and its payload to a SendCommand call.
// The document defaults to the one named by the target ARN; Input.DocumentName overrides it.
func BuildSSMCommandRequest(target *Target, payload string) (SSMCommandRequest, error) {
	if target.RunCommandParameters == nil {
		return SSMCommandRequest{}, errNoRunCommandParameters
	}

	var in ssmInput

	_ = json.Unmarshal([]byte(payload), &in)

	name := in.DocumentName
	if name == "" {
		name = target.Arn[strings.Index(target.Arn, ":document/")+len(":document/"):]
	}

	return SSMCommandRequest{
		DocumentName:    name,
		DocumentVersion: in.DocumentVersion,
		Parameters:      ssmParameterMap(in.Parameters),
		Targets:         target.RunCommandParameters.RunCommandTargets,
		Region:          arnRegionOf(target.Arn),
	}, nil
}

// BuildSSMAutomationRequest maps an Automation target (arn ...:automation-definition/Name:Version)
// and its payload to a StartAutomationExecution call. A payload with a Parameters object uses it;
// otherwise the payload object itself is the parameter map.
func BuildSSMAutomationRequest(target *Target, payload string) SSMAutomationRequest {
	def := target.Arn[strings.Index(target.Arn, ":automation-definition/")+len(":automation-definition/"):]
	name, version, _ := strings.Cut(def, ":")

	var raw map[string]any

	_ = json.Unmarshal([]byte(payload), &raw)

	params := raw
	if p, ok := raw["Parameters"].(map[string]any); ok {
		params = p
	}

	return SSMAutomationRequest{
		DocumentName:    name,
		DocumentVersion: version,
		Parameters:      ssmParameterMap(params),
		Region:          arnRegionOf(target.Arn),
	}
}

// deliverToOpsTarget routes SSM Run Command / Automation and AppSync ARNs; handled is false for any other ARN.
func deliverToOpsTarget(ctx context.Context, target *Target, dt DeliveryTargets, payload string) (bool, bool) {
	switch {
	case isSSMDocumentARN(target.Arn), isSSMAutomationARN(target.Arn):
		return deliverToSSM(ctx, target, dt, payload), true
	case isAppSyncARN(target.Arn):
		return deliverToAppSync(ctx, target, dt, payload), true
	}

	return false, false
}

func deliverToSSM(ctx context.Context, target *Target, dt DeliveryTargets, payload string) bool {
	if isSSMAutomationARN(target.Arn) {
		if dt.SSMAutomation == nil {
			return false
		}

		return logTargetErr(ctx, "SSM Automation",
			dt.SSMAutomation.StartSSMAutomation(ctx, BuildSSMAutomationRequest(target, payload)))
	}

	if dt.SSMCommand == nil {
		return false
	}

	req, err := BuildSSMCommandRequest(target, payload)
	if err != nil {
		return logTargetErr(ctx, "SSM Run Command", err)
	}

	return logTargetErr(ctx, "SSM Run Command", dt.SSMCommand.SendSSMCommand(ctx, req))
}

// deliverToAppSync runs AppSyncParameters.GraphQLOperation with the payload object as variables.
func deliverToAppSync(ctx context.Context, target *Target, dt DeliveryTargets, payload string) bool {
	if dt.AppSync == nil {
		return false
	}

	if target.AppSyncParameters == nil || target.AppSyncParameters.GraphQLOperation == "" {
		return logTargetErr(ctx, "AppSync", errNoGraphQLOperation)
	}

	var vars map[string]any

	if err := json.Unmarshal([]byte(payload), &vars); err != nil {
		return logTargetErr(ctx, "AppSync", fmt.Errorf("payload is not a JSON object: %w", err))
	}

	err := dt.AppSync.ExecuteAppSyncOperation(ctx, target.Arn, target.AppSyncParameters.GraphQLOperation, vars)

	return logTargetErr(ctx, "AppSync", err)
}
