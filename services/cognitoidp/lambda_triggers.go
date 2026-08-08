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
	triggerSourcePreAuthentication        = "PreAuthentication_Authentication"
	triggerSourcePostAuthentication       = "PostAuthentication_Authentication"
	triggerSourceDefineAuthChallenge      = "DefineAuthChallenge_Authentication"
	triggerSourceCreateAuthChallenge      = "CreateAuthChallenge_Authentication"
	triggerSourceVerifyAuthChallenge      = "VerifyAuthChallengeResponse_Authentication"
	triggerSourceUserMigrationAuth        = "UserMigration_Authentication"
	triggerSourceUserMigrationForgotPwd   = "UserMigration_ForgotPassword"
)

// LambdaConfig key names, matching the JSON field names of AWS's LambdaConfigType
// exactly (LambdaConfig is stored as the raw client-supplied map, un-transformed --
// see UserPool.LambdaConfig).
const (
	triggerKeyPreSignUp           = "PreSignUp"
	triggerKeyPostConfirmation    = "PostConfirmation"
	triggerKeyPreTokenGeneration  = "PreTokenGeneration"
	triggerKeyCustomMessage       = "CustomMessage"
	triggerKeyPreAuthentication   = "PreAuthentication"
	triggerKeyPostAuthentication  = "PostAuthentication"
	triggerKeyDefineAuthChallenge = "DefineAuthChallenge"
	triggerKeyCreateAuthChallenge = "CreateAuthChallenge"
	triggerKeyVerifyAuthChallenge = "VerifyAuthChallengeResponse"
	triggerKeyUserMigration       = "UserMigration"
)

// Trigger event request field names shared across every trigger's request
// object (userAttributes, clientMetadata), factored out because each is
// otherwise repeated as a literal at every one of this file's/callers' event
// call sites.
const (
	eventKeyUserAttributes = "userAttributes"
	eventKeyClientMetadata = "clientMetadata"
	eventKeyValidationData = "validationData"
	eventKeyChallengeName  = "challengeName"
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

// preAuthenticationCheck fires the PreAuthentication Lambda trigger (if configured)
// before credentials are validated, letting a Lambda reject a sign-in attempt early
// (e.g. based on validationData) by returning an error, which surfaces to the caller
// as UserLambdaValidationException -- matching AWS: "To prevent the user from signing
// in, throw an error in the Lambda function." The response object is always empty
// (CognitoEventUserPoolsPreAuthenticationResponse has no fields), so on success there
// is nothing to apply back onto state. Caller must hold b.mu (authenticate does).
func (b *InMemoryBackend) preAuthenticationCheck(pool *UserPool, clientID string, user *User) error {
	_, err := b.invokeLambdaTrigger(pool, triggerKeyPreAuthentication, triggerSourcePreAuthentication,
		clientID, user.Username,
		map[string]any{
			eventKeyUserAttributes: stringMapToAny(user.Attributes),
			eventKeyValidationData: map[string]any{},
		},
		map[string]any{},
	)

	return err
}

// postAuthenticationNotify fires the PostAuthentication Lambda trigger (if configured)
// after a successful sign-in, immediately before tokens are returned to the caller.
// Like PreAuthentication, throwing from the Lambda fails the authentication attempt
// (surfaced as UserLambdaValidationException); the response object is always empty.
// newDeviceUsed is always reported false: this backend does not track device keys on
// the authentication path, so there is no real signal to report here -- a
// simplification, not a masked stub, since AWS-side device tracking has no bearing on
// whether the trigger fires or what it is invoked with otherwise. Caller must hold
// b.mu (issueTokensLocked does).
func (b *InMemoryBackend) postAuthenticationNotify(pool *UserPool, clientID string, user *User) error {
	_, err := b.invokeLambdaTrigger(pool, triggerKeyPostAuthentication, triggerSourcePostAuthentication,
		clientID, user.Username,
		map[string]any{
			"newDeviceUsed":        false,
			eventKeyUserAttributes: stringMapToAny(user.Attributes),
			eventKeyClientMetadata: map[string]any{},
		},
		map[string]any{},
	)

	return err
}

// customAuthSessionToAny converts a CUSTOM_AUTH session history to the []any shape the
// trigger event envelope needs (aws-lambda-go events.CognitoEventUserPoolsChallengeResult).
func customAuthSessionToAny(session []customAuthChallengeResult) []any {
	out := make([]any, len(session))
	for i, r := range session {
		out[i] = map[string]any{
			eventKeyChallengeName: r.ChallengeName,
			"challengeResult":     r.ChallengeResult,
			"challengeMetadata":   r.ChallengeMetadata,
		}
	}

	return out
}

// defineAuthChallenge invokes the DefineAuthChallenge Lambda trigger, the entry point
// (and re-entry point, once per round) of the CUSTOM_AUTH state machine. The Lambda
// decides, from the round history in session, whether to issue tokens, fail the
// attempt outright, or present another challenge (identified by the returned
// challengeName, the Lambda's own bookkeeping name -- not the fixed "CUSTOM_CHALLENGE"
// ChallengeName Cognito always returns to the client). Returns an error both when the
// invoker/Lambda call itself fails and when CUSTOM_AUTH is requested but the pool has
// no DefineAuthChallenge trigger configured at all, since custom auth cannot function
// without one -- matching AWS, which refuses to start a CUSTOM_AUTH flow in that case.
// Caller must hold b.mu.
func (b *InMemoryBackend) defineAuthChallenge(
	pool *UserPool, clientID, username string, userAttrs map[string]string,
	session []customAuthChallengeResult, userNotFound bool,
) (string, bool, bool, error) {
	if lambdaConfigARN(pool.LambdaConfig, triggerKeyDefineAuthChallenge) == "" {
		return "", false, false, fmt.Errorf(
			"%w: CUSTOM_AUTH requires a DefineAuthChallenge Lambda trigger, "+
				"which is not configured for user pool %q", ErrInvalidUserPoolConfig, pool.ID)
	}

	resp, err := b.invokeLambdaTrigger(pool, triggerKeyDefineAuthChallenge, triggerSourceDefineAuthChallenge,
		clientID, username,
		map[string]any{
			eventKeyUserAttributes: stringMapToAny(userAttrs),
			"session":              customAuthSessionToAny(session),
			eventKeyClientMetadata: map[string]any{},
			"userNotFound":         userNotFound,
		},
		map[string]any{eventKeyChallengeName: "", "issueTokens": false, "failAuthentication": false},
	)
	if err != nil {
		return "", false, false, err
	}

	challengeName, _ := resp[eventKeyChallengeName].(string)
	issueTokens, _ := resp["issueTokens"].(bool)
	failAuthentication, _ := resp["failAuthentication"].(bool)

	return challengeName, issueTokens, failAuthentication, nil
}

// createAuthChallenge invokes the CreateAuthChallenge Lambda trigger to build the next
// CUSTOM_AUTH challenge's public parameters (sent to the client) and private parameters
// (kept server-side, used by verifyCustomAuthChallenge to judge the answer). Caller
// must hold b.mu.
func (b *InMemoryBackend) createAuthChallenge(
	pool *UserPool, clientID, username string, userAttrs map[string]string,
	challengeName string, session []customAuthChallengeResult,
) (map[string]string, map[string]string, string, error) {
	resp, err := b.invokeLambdaTrigger(pool, triggerKeyCreateAuthChallenge, triggerSourceCreateAuthChallenge,
		clientID, username,
		map[string]any{
			eventKeyUserAttributes: stringMapToAny(userAttrs),
			eventKeyChallengeName:  challengeName,
			"session":              customAuthSessionToAny(session),
			eventKeyClientMetadata: map[string]any{},
		},
		map[string]any{
			"publicChallengeParameters":  map[string]any{},
			"privateChallengeParameters": map[string]any{},
			"challengeMetadata":          "",
		},
	)
	if err != nil {
		return nil, nil, "", err
	}

	public := anyMapToStringMap(resp["publicChallengeParameters"])
	private := anyMapToStringMap(resp["privateChallengeParameters"])
	metadata, _ := resp["challengeMetadata"].(string)

	return public, private, metadata, nil
}

// verifyCustomAuthChallenge invokes the VerifyAuthChallengeResponse Lambda trigger to
// judge whether answer is correct for the challenge previously created by
// createAuthChallenge (private holds that round's privateChallengeParameters, never
// exposed to the client). Caller must hold b.mu.
func (b *InMemoryBackend) verifyCustomAuthChallenge(
	pool *UserPool, clientID, username string, userAttrs, private map[string]string, answer string,
) (bool, error) {
	resp, err := b.invokeLambdaTrigger(pool, triggerKeyVerifyAuthChallenge, triggerSourceVerifyAuthChallenge,
		clientID, username,
		map[string]any{
			eventKeyUserAttributes:       stringMapToAny(userAttrs),
			"privateChallengeParameters": stringMapToAny(private),
			"challengeAnswer":            answer,
			eventKeyClientMetadata:       map[string]any{},
		},
		map[string]any{"answerCorrect": false},
	)
	if err != nil {
		return false, err
	}

	answerCorrect, _ := resp["answerCorrect"].(bool)

	return answerCorrect, nil
}

// anyMapToStringMap converts a map[string]any (as decoded from a Lambda's JSON
// response) to map[string]string, dropping any non-string values. Returns an empty
// (non-nil) map for a nil/non-map input so callers can range over the result safely.
func anyMapToStringMap(v any) map[string]string {
	m, ok := v.(map[string]any)
	if !ok {
		return map[string]string{}
	}

	out := make(map[string]string, len(m))

	for k, val := range m {
		if s, sOK := val.(string); sOK {
			out[k] = s
		}
	}

	return out
}

// userMigrationResult is the parsed response from a UserMigration Lambda trigger
// (aws-lambda-go events.CognitoEventUserPoolsMigrateUserResponse); MessageAction,
// DesiredDeliveryMediums, and ForceAliasCreation are part of the real response shape
// but have no effect in this mock (no real email/SMS delivery pipeline or alias
// system reacts to them here), matching the documented simplification already made
// for CustomMessage trigger delivery.
type userMigrationResult struct {
	UserAttributes  map[string]string
	FinalUserStatus string
}

var defaultUserMigrationResponse = map[string]any{ //nolint:gochecknoglobals // static default-response envelope
	"userAttributes":         map[string]any{},
	"finalUserStatus":        "",
	"messageAction":          "",
	"desiredDeliveryMediums": []any{},
	"forceAliasCreation":     false,
}

// parseUserMigrationResponse extracts a userMigrationResult from a UserMigration
// Lambda response, or nil if the Lambda declined to migrate this user (no
// userAttributes) -- AWS treats a declined migration exactly like the trigger never
// having run, surfacing the caller's original "unknown user" handling.
func parseUserMigrationResponse(resp map[string]any) *userMigrationResult {
	attrs := anyMapToStringMap(resp["userAttributes"])
	if len(attrs) == 0 {
		return nil
	}

	finalStatus, _ := resp["finalUserStatus"].(string)

	return &userMigrationResult{UserAttributes: attrs, FinalUserStatus: finalStatus}
}

// invokeUserMigrationTrigger invokes the UserMigration Lambda trigger (if configured)
// with the plaintext password from the current sign-in attempt, letting the Lambda
// validate it against an external identity store and, on success, supply the
// attributes for a new Cognito user. Returns (nil, nil) -- not an error -- both when
// no UserMigration trigger is configured and when the Lambda declines to migrate this
// user (a response with no userAttributes), so the caller falls back to its normal
// "unknown user" handling in either case. Caller must hold b.mu.
func (b *InMemoryBackend) invokeUserMigrationTrigger(
	pool *UserPool, clientID, username, password string,
) (*userMigrationResult, error) {
	if lambdaConfigARN(pool.LambdaConfig, triggerKeyUserMigration) == "" {
		return nil, nil //nolint:nilnil // sentinel "not configured" pair, documented above
	}

	resp, err := b.invokeLambdaTrigger(pool, triggerKeyUserMigration, triggerSourceUserMigrationAuth,
		clientID, username,
		map[string]any{
			"password":             password,
			eventKeyValidationData: map[string]any{},
			eventKeyClientMetadata: map[string]any{},
		},
		defaultUserMigrationResponse,
	)
	if err != nil {
		return nil, err
	}

	return parseUserMigrationResponse(resp), nil
}

// invokeUserMigrationTriggerForgotPassword invokes the UserMigration Lambda trigger
// (if configured) for a ForgotPassword call naming a username that does not exist in
// the pool. Unlike sign-in migration, request.password is omitted entirely rather than
// sent empty: "Amazon Cognito doesn't send this value in a request that's initiated by
// a forgot-password flow" (Cognito developer guide, "Migrate user Lambda trigger
// parameters"). Returns (nil, nil) -- not an error -- both when no UserMigration
// trigger is configured and when the Lambda declines, so ForgotPassword falls back to
// its normal "unknown user" handling either way. Caller must hold b.mu.
func (b *InMemoryBackend) invokeUserMigrationTriggerForgotPassword(
	pool *UserPool, clientID, username string,
) (*userMigrationResult, error) {
	if lambdaConfigARN(pool.LambdaConfig, triggerKeyUserMigration) == "" {
		return nil, nil //nolint:nilnil // sentinel "not configured" pair, documented above
	}

	resp, err := b.invokeLambdaTrigger(pool, triggerKeyUserMigration, triggerSourceUserMigrationForgotPwd,
		clientID, username,
		map[string]any{
			eventKeyValidationData: map[string]any{},
			eventKeyClientMetadata: map[string]any{},
		},
		defaultUserMigrationResponse,
	)
	if err != nil {
		return nil, err
	}

	return parseUserMigrationResponse(resp), nil
}
