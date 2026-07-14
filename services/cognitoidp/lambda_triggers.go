package cognitoidp

// lambda_triggers.go fires Amazon Cognito User Pool Lambda triggers for real: it
// builds the AWS-accurate event envelope for each trigger, invokes the function ARN
// configured in the pool's LambdaConfig via the pluggable LambdaTriggerInvoker, and
// applies the trigger's response back onto real backend state / the issued tokens.
//
// Event/response shapes here are verified against the aws-lambda-go events package
// (github.com/aws/aws-lambda-go/events, CognitoEventUserPools* types) and its
// testdata fixtures, which mirror the JSON Amazon Cognito actually sends to a
// trigger Lambda -- not just this package's own handler output.
//
// Trigger invocation happens while the caller already holds b.mu (Lock or RLock,
// matching the surrounding method), consistent with this backend's coarse
// per-backend RWMutex: Cognito itself blocks the originating API call until the
// synchronous Lambda invocation completes, so serializing other backend operations
// for that duration matches real behavior, not just an implementation shortcut.

import (
	"context"
	"fmt"
	"strings"
)

// LambdaTriggerInvoker invokes a Cognito Lambda trigger by function ARN with the
// given event payload (the standard Cognito trigger event envelope: version,
// triggerSource, region, userPoolId, userName, callerContext, request, response)
// and returns the event as returned by the function -- i.e. the same envelope with
// "response" populated/modified by the Lambda, exactly as AWS Cognito's real
// RequestResponse Lambda invocation contract works.
type LambdaTriggerInvoker interface {
	InvokeTrigger(ctx context.Context, functionARN string, event map[string]any) (map[string]any, error)
}

// SetLambdaTriggerInvoker wires the invoker used to fire Cognito User Pool Lambda
// triggers. Passing nil disables trigger invocation entirely: pools with a
// configured LambdaConfig behave exactly as before this feature existed (LambdaConfig
// is stored/returned but never invoked). This is the safe default -- tests and
// deployments that never call SetLambdaTriggerInvoker see no behavior change.
func (b *InMemoryBackend) SetLambdaTriggerInvoker(inv LambdaTriggerInvoker) {
	b.mu.Lock("SetLambdaTriggerInvoker")
	defer b.mu.Unlock()

	b.lambdaInvoker = inv
}

// Trigger source values, verified against aws-lambda-go/events testdata fixtures
// and the AWS Cognito developer guide's "Trigger source values" table.
const (
	triggerSourcePreSignUpSignUp          = "PreSignUp_SignUp"
	triggerSourcePreSignUpAdminCreateUser = "PreSignUp_AdminCreateUser"
	triggerSourcePostConfirmationSignUp   = "PostConfirmation_ConfirmSignUp"
	triggerSourceTokenGenAuthentication   = "TokenGeneration_Authentication"
	triggerSourceTokenGenNewPasswordFlow  = "TokenGeneration_NewPasswordChallenge"
	triggerSourceTokenGenRefreshTokens    = "TokenGeneration_RefreshTokens"
	triggerSourceCustomMessageSignUp      = "CustomMessage_SignUp"
	triggerSourceCustomMessageResendCode  = "CustomMessage_ResendCode"
	triggerSourceCustomMessageForgotPwd   = "CustomMessage_ForgotPassword"
)

// LambdaConfig key names, matching the JSON field names of AWS's LambdaConfigType
// exactly (LambdaConfig is stored as the raw client-supplied map, un-transformed --
// see UserPool.LambdaConfig).
const (
	triggerKeyPreSignUp          = "PreSignUp"
	triggerKeyPostConfirmation   = "PostConfirmation"
	triggerKeyPreTokenGeneration = "PreTokenGeneration"
	triggerKeyCustomMessage      = "CustomMessage"
)

// Trigger event request field names shared across every trigger's request
// object (userAttributes, clientMetadata), factored out because each is
// otherwise repeated as a literal at every one of this file's/callers' event
// call sites.
const (
	eventKeyUserAttributes = "userAttributes"
	eventKeyClientMetadata = "clientMetadata"
)

// customMessageCodeParameter is the literal placeholder AWS Cognito puts in
// request.codeParameter for CustomMessage triggers (verified against the
// aws-lambda-go/events CustomMessage testdata fixture: the field's real-world value
// is the bare token "####", not template-doc angle brackets like the fixture's other
// fields). A CustomMessage Lambda is expected to embed this token verbatim in the
// message it returns; gopherstack substitutes it with the real generated code so
// integration tests/harnesses that read the returned message see something useful,
// since (unlike real AWS) gopherstack has no downstream email/SMS delivery pipeline
// to perform that substitution during actual message dispatch.
const customMessageCodeParameter = "####"

// lambdaConfigARN returns the function ARN configured for triggerKey on cfg, or ""
// if unset. Most triggers store a bare ARN string under their own key (e.g.
// cfg["PreSignUp"]). PreTokenGeneration additionally supports the versioned
// "<Trigger>Config" object form ({"LambdaArn": "...", "LambdaVersion": "V2_0"})
// that modern callers (e.g. the Terraform AWS provider) send instead of the legacy
// bare-string field; other triggers do not have a "*Config" variant in the AWS API.
func lambdaConfigARN(cfg map[string]any, triggerKey string) string {
	if cfg == nil {
		return ""
	}

	if s, ok := cfg[triggerKey].(string); ok && s != "" {
		return s
	}

	if nested, nestedOK := cfg[triggerKey+"Config"].(map[string]any); nestedOK {
		if s, sOK := nested["LambdaArn"].(string); sOK {
			return s
		}
	}

	return ""
}

// invokeLambdaTrigger builds the standard Cognito trigger event envelope, invokes
// the Lambda configured for triggerKey on pool (if any), and returns the "response"
// sub-object from the (possibly modified) event the function returns.
//
// It returns (nil, nil) -- not an error -- when no invoker is wired or the pool has
// no Lambda configured for triggerKey, so every call site's existing behavior is
// preserved exactly for pools/deployments that never configure this feature.
func (b *InMemoryBackend) invokeLambdaTrigger(
	pool *UserPool,
	triggerKey, triggerSource, clientID, username string,
	request map[string]any,
	defaultResponse map[string]any,
) (map[string]any, error) {
	if b.lambdaInvoker == nil || pool == nil {
		return nil, nil //nolint:nilnil // sentinel "not configured" pair, documented above
	}

	functionARN := lambdaConfigARN(pool.LambdaConfig, triggerKey)
	if functionARN == "" {
		return nil, nil //nolint:nilnil // sentinel "not configured" pair, documented above
	}

	event := map[string]any{
		"version":       "1",
		"triggerSource": triggerSource,
		"region":        b.region,
		"userPoolId":    pool.ID,
		"userName":      username,
		"callerContext": map[string]any{
			"awsSdkVersion": "gopherstack",
			"clientId":      clientID,
		},
		"request":  request,
		"response": defaultResponse,
	}

	result, err := b.lambdaInvoker.InvokeTrigger(context.Background(), functionARN, event)
	if err != nil {
		return nil, fmt.Errorf("%w: %s trigger: %s", ErrUserLambdaValidation, triggerKey, err.Error())
	}

	respAny, ok := result["response"]
	if !ok {
		return nil, fmt.Errorf("%w: %s trigger response missing \"response\" object", ErrUnexpectedLambda, triggerKey)
	}

	resp, ok := respAny.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: %s trigger \"response\" was not an object", ErrUnexpectedLambda, triggerKey)
	}

	return resp, nil
}

// stringMapToAny converts a map[string]string to map[string]any for embedding into
// a trigger event's request object (Cognito trigger events are JSON, so all string
// maps marshal identically either way; map[string]any lets callers build the event
// with mixed-type siblings like clientMetadata/groupConfiguration in the same literal).
func stringMapToAny(m map[string]string) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}

	return out
}

// stringsToAny converts []string to []any for embedding into a trigger event.
func stringsToAny(s []string) []any {
	out := make([]any, len(s))
	for i, v := range s {
		out[i] = v
	}

	return out
}

// parseClaimsOverride extracts claimsOverrideDetails.claimsToAddOrOverride and
// claimsToSuppress from a PreTokenGeneration trigger response. Unset or
// malformed sub-fields are treated as empty (no override), never an error --
// a Lambda that only sets one of the two fields is valid.
func parseClaimsOverride(resp map[string]any) (map[string]string, []string) {
	if resp == nil {
		return nil, nil
	}

	cod, codOK := resp["claimsOverrideDetails"].(map[string]any)
	if !codOK {
		return nil, nil
	}

	var addOrOverride map[string]string

	if m, addOK := cod["claimsToAddOrOverride"].(map[string]any); addOK {
		addOrOverride = make(map[string]string, len(m))

		for k, v := range m {
			if s, sOK := v.(string); sOK {
				addOrOverride[k] = s
			}
		}
	}

	var suppress []string

	if s, suppressOK := cod["claimsToSuppress"].([]any); suppressOK {
		for _, v := range s {
			if str, strOK := v.(string); strOK {
				suppress = append(suppress, str)
			}
		}
	}

	return addOrOverride, suppress
}

// parsePreSignUpResponse extracts autoConfirmUser/autoVerifyEmail/autoVerifyPhone
// from a PreSignUp trigger response. A nil or malformed response yields all-false,
// matching the zero-value defaults Cognito uses when a trigger leaves a field unset.
func parsePreSignUpResponse(resp map[string]any) (bool, bool, bool) {
	if resp == nil {
		return false, false, false
	}

	autoConfirm, _ := resp["autoConfirmUser"].(bool)
	autoVerifyEmail, _ := resp["autoVerifyEmail"].(bool)
	autoVerifyPhone, _ := resp["autoVerifyPhone"].(bool)

	return autoConfirm, autoVerifyEmail, autoVerifyPhone
}

// InvokeCustomMessageTrigger fires the CustomMessage Lambda trigger (if configured)
// for a code-delivery flow (SignUp, ResendConfirmationCode, ForgotPassword) and
// returns any smsMessage/emailMessage/emailSubject override the Lambda supplied,
// with the "####" code placeholder (see customMessageCodeParameter) substituted for
// the real generated code. It is exported separately from the backend methods that
// generate the code (SignUpWithValidation, ResendConfirmationCode, ForgotPassword)
// so those methods' return signatures -- used across dozens of existing call sites
// -- do not need to change; callers request the override once they already have the
// code in hand.
//
// A missing client/pool/user is treated as "no override" rather than an error: by
// the time a caller has a code to pass in, the primary operation already succeeded
// (or, for PreventUserExistenceErrors masking, deliberately has no real user), so
// this best-effort lookup must never turn a successful SignUp/ForgotPassword/
// ResendConfirmationCode into a failure.
func (b *InMemoryBackend) InvokeCustomMessageTrigger(
	clientID, username, code, triggerSource string,
) (string, string, error) {
	b.mu.RLock("InvokeCustomMessageTrigger")
	defer b.mu.RUnlock()

	client, clientOK := b.clients.Get(clientID)
	if !clientOK {
		return "", "", nil
	}

	pool, poolOK := b.pools.Get(client.UserPoolID)
	if !poolOK {
		return "", "", nil
	}

	var attrs map[string]string
	if user, userOK := b.users.Get(userKey(client.UserPoolID, username)); userOK {
		attrs = user.Attributes
	}

	resp, err := b.invokeLambdaTrigger(pool, triggerKeyCustomMessage, triggerSource, clientID, username,
		map[string]any{
			eventKeyUserAttributes: stringMapToAny(attrs),
			"codeParameter":        customMessageCodeParameter,
			"usernameParameter":    username,
			eventKeyClientMetadata: map[string]any{},
		},
		map[string]any{"smsMessage": "", "emailMessage": "", "emailSubject": ""},
	)
	if err != nil {
		return "", "", err
	}

	if resp == nil {
		return "", "", nil
	}

	message, _ := resp["emailMessage"].(string)
	if message == "" {
		message, _ = resp["smsMessage"].(string)
	}

	subject, _ := resp["emailSubject"].(string)

	if code != "" {
		message = strings.ReplaceAll(message, customMessageCodeParameter, code)
	}

	return message, subject, nil
}

// preTokenGenerationOverride fires the PreTokenGeneration Lambda trigger (if
// configured) ahead of token issuance and returns the claimsToAddOrOverride /
// claimsToSuppress the Lambda requested, ready to feed into TokenParams. Caller
// must hold b.mu (issueTokensLocked and InitiateAuthRefreshToken both do).
func (b *InMemoryBackend) preTokenGenerationOverride(
	pool *UserPool, clientID string, user *User, groups []string, triggerSource string,
) (map[string]string, []string, error) {
	resp, err := b.invokeLambdaTrigger(pool, triggerKeyPreTokenGeneration, triggerSource, clientID, user.Username,
		map[string]any{
			eventKeyUserAttributes: stringMapToAny(user.Attributes),
			"groupConfiguration": map[string]any{
				"groupsToOverride":   stringsToAny(groups),
				"iamRolesToOverride": []any{},
				"preferredRole":      nil,
			},
			eventKeyClientMetadata: map[string]any{},
		},
		map[string]any{
			"claimsOverrideDetails": map[string]any{
				"claimsToAddOrOverride": map[string]any{},
				"claimsToSuppress":      []any{},
			},
		},
	)
	if err != nil {
		return nil, nil, err
	}

	claimsToAdd, claimsToSuppress := parseClaimsOverride(resp)

	return claimsToAdd, claimsToSuppress, nil
}
