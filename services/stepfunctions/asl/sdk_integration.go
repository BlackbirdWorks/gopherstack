package asl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
)

const (
	resourcePrefixSDK       = "arn:aws:states:::aws-sdk:"
	resourcePrefixOptimized = "arn:aws:states:::"
	lambdaInvokeAction      = "invoke"
	lambdaInvokeResource    = "arn:aws:states:::lambda:invoke"
	lambdaDefaultVersion    = "$LATEST"
	lambdaTypeRequestResp   = "RequestResponse"
	lambdaServiceException  = "ServiceException"
	lambdaErrUnknown        = "Lambda.Unknown"
	sdkServiceStates        = "sfn"
)

// SDKCall describes one AWS SDK or optimized service-integration call.
type SDKCall struct {
	Params    any
	Service   string
	Action    string
	Pattern   string
	Region    string
	AccountID string
	RoleArn   string
	Optimized bool
}

// SDKIntegration performs a Task state's service call in-process against the
// target service; failures are *FailError named <ServicePrefix>.<ErrorName>.
type SDKIntegration interface {
	SFNCallSDK(ctx context.Context, call SDKCall) (any, error)
}

// SetSDKIntegration configures the generic AWS SDK integration.
func (e *Executor) SetSDKIntegration(s SDKIntegration) { e.sdk = s }

// optimizedSDKService maps an optimized-integration name to its SDK service.
func optimizedSDKService(name string) (string, bool) {
	switch name {
	case "dynamodb", "sqs", "sns", "batch", "athena", "codebuild":
		return name, true
	case "events":
		return "eventbridge", true
	case "states":
		return sdkServiceStates, true
	case "bedrock":
		return "bedrockruntime", true
	default:
		return "", false
	}
}

// sdkCallFor maps a Task Resource to the SDK call it denotes, if any.
func sdkCallFor(resource string) (SDKCall, bool) {
	action, pattern := parseServiceIntegrationResource(resource)

	if rest, ok := strings.CutPrefix(resource, resourcePrefixSDK); ok {
		svc, _, found := strings.Cut(rest, ":")
		if !found {
			return SDKCall{}, false
		}

		return SDKCall{Service: svc, Action: action, Pattern: pattern}, true
	}

	rest, ok := strings.CutPrefix(resource, resourcePrefixOptimized)
	if !ok {
		return SDKCall{}, false
	}

	name, _, found := strings.Cut(rest, ":")
	if !found {
		return SDKCall{}, false
	}

	svc, ok := optimizedSDKService(name)
	if !ok {
		return SDKCall{}, false
	}

	return SDKCall{Service: svc, Action: action, Pattern: pattern, Optimized: true}, true
}

// regionAndAccount extracts region and account from the state machine ARN.
func (e *Executor) regionAndAccount() (string, string) {
	parts := strings.Split(e.execMeta.StateMachineArn, ":")
	const minARNParts = 5
	if len(parts) < minARNParts {
		return "", ""
	}

	return parts[3], parts[4]
}

func (e *Executor) invokeSDKTask(ctx context.Context, input any, call SDKCall) (any, error) {
	call.Params = input
	call.Region, call.AccountID = e.regionAndAccount()
	call.RoleArn = e.execMeta.RoleArn

	return e.sdk.SFNCallSDK(ctx, call)
}

func isLambdaInvokeResource(resource string) bool {
	return strings.HasPrefix(resource, lambdaInvokeResource) &&
		serviceAction(resource) == lambdaInvokeAction
}

// invokeLambdaOptimized runs arn:aws:states:::lambda:invoke through the
// Lambda invoker and shapes the result like the optimized integration.
func (e *Executor) invokeLambdaOptimized(ctx context.Context, input any) (any, error) {
	if e.lambda == nil {
		return nil, ErrLambdaNotConfigured
	}

	params, _ := input.(map[string]any)

	name, _ := params["FunctionName"].(string)
	if name == "" {
		return nil, &FailError{ErrCode: errCodeStatesRuntime, Cause: "lambda:invoke requires Parameters.FunctionName"}
	}

	qualifier, _ := params["Qualifier"].(string)
	if qualifier != "" {
		name += ":" + qualifier
	}

	invType, _ := params["InvocationType"].(string)
	if invType == "" {
		invType = lambdaTypeRequestResp
	}

	var payload []byte

	if raw, ok := params["Payload"]; ok {
		b, err := json.Marshal(raw)
		if err != nil {
			return nil, &FailError{ErrCode: errCodeStatesRuntime, Cause: err.Error()}
		}

		payload = b
	}

	fnARN := e.resourceARN("lambda", "function:", name)
	if authErr := e.authorizeRole("Lambda", "lambda:InvokeFunction", fnARN); authErr != nil {
		return nil, authErr
	}

	resp, status, err := e.lambda.InvokeFunction(ctx, name, invType, payload)
	if err != nil {
		return nil, lambdaInvokeError(err)
	}

	if ferr := lambdaFunctionError(resp); ferr != nil {
		return nil, ferr
	}

	out := map[string]any{"ExecutedVersion": lambdaExecutedVersion(name), "StatusCode": status}

	if len(bytes.TrimSpace(resp)) > 0 {
		var parsed any
		if json.Unmarshal(resp, &parsed) != nil {
			parsed = string(resp)
		}

		out["Payload"] = parsed
	}

	return out, nil
}

// lambdaExecutedVersion is the qualifier of name when it carries one, else $LATEST.
func lambdaExecutedVersion(name string) string {
	parts := strings.Split(name, ":")

	switch {
	case strings.HasPrefix(name, "arn:") && len(parts) > 7:
		return parts[7]
	case !strings.HasPrefix(name, "arn:") && len(parts) == 2:
		return parts[1]
	default:
		return lambdaDefaultVersion
	}
}

// lambdaInvokeError names an invoker failure Lambda.<ExceptionName>.
func lambdaInvokeError(err error) error {
	if _, ok := errors.AsType[*FailError](err); ok {
		return err
	}

	name := err.Error()
	if !strings.HasSuffix(name, "Exception") || strings.ContainsAny(name, " :") {
		name = lambdaServiceException
	}

	return &FailError{ErrCode: "Lambda." + name, Cause: err.Error()}
}

// lambdaFunctionError reports a Lambda-shaped error payload as a Task failure.
func lambdaFunctionError(resp []byte) error {
	trimmed := bytes.TrimSpace(resp)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil
	}

	var probe struct {
		ErrorMessage *json.RawMessage `json:"errorMessage"`
		ErrorType    *string          `json:"errorType"`
	}

	if json.Unmarshal(trimmed, &probe) == nil && (probe.ErrorMessage != nil || probe.ErrorType != nil) {
		code := lambdaErrUnknown
		if probe.ErrorType != nil && *probe.ErrorType != "" {
			code = *probe.ErrorType
		}

		return &FailError{ErrCode: code, Cause: string(trimmed)}
	}

	return nil
}
