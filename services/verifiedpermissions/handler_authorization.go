package verifiedpermissions

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

type entityIdentifierJSON struct {
	EntityType string `json:"entityType"`
	EntityID   string `json:"entityId"`
}

type actionIdentifierJSON struct {
	ActionType string `json:"actionType"`
	ActionID   string `json:"actionId"`
}

type entityJSON struct {
	Identifier *entityIdentifierJSON      `json:"identifier,omitempty"`
	Attributes map[string]json.RawMessage `json:"attrs,omitempty"`
	Parents    []entityIdentifierJSON     `json:"parents,omitempty"`
}

type batchIsAuthorizedRequestItem struct {
	Principal *entityIdentifierJSON `json:"principal,omitempty"`
	Action    *actionIdentifierJSON `json:"action,omitempty"`
	Resource  *entityIdentifierJSON `json:"resource,omitempty"`
}

type batchIsAuthorizedInput struct {
	PolicyStoreID string                         `json:"policyStoreId"`
	Entities      []entityJSON                   `json:"entities,omitempty"`
	Requests      []batchIsAuthorizedRequestItem `json:"requests"`
}

// determiningPolicyItemJSON mirrors the real SDK's types.DeterminingPolicyItem:
// each determining policy is wire-encoded as an object {"policyId": "..."},
// NOT a bare string.
type determiningPolicyItemJSON struct {
	PolicyID string `json:"policyId"`
}

// evaluationErrorItemJSON mirrors the real SDK's types.EvaluationErrorItem:
// each evaluation error is wire-encoded as an object
// {"errorDescription": "..."}, NOT a bare string.
type evaluationErrorItemJSON struct {
	ErrorDescription string `json:"errorDescription"`
}

func toDeterminingPolicyItems(ids []string) []determiningPolicyItemJSON {
	out := make([]determiningPolicyItemJSON, 0, len(ids))
	for _, id := range ids {
		out = append(out, determiningPolicyItemJSON{PolicyID: id})
	}

	return out
}

func toEvaluationErrorItems(msgs []string) []evaluationErrorItemJSON {
	out := make([]evaluationErrorItemJSON, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, evaluationErrorItemJSON{ErrorDescription: m})
	}

	return out
}

// batchIsAuthorizedRequestEchoJSON mirrors the real SDK's
// BatchIsAuthorizedInputItem echoed back in each output item's "request"
// field: principal/action/resource are nested objects, not the flat
// principalEntityType/actionType/... fields AuthorizationRequest uses
// internally.
type batchIsAuthorizedRequestEchoJSON struct {
	Principal *entityIdentifierJSON `json:"principal,omitempty"`
	Action    *actionIdentifierJSON `json:"action,omitempty"`
	Resource  *entityIdentifierJSON `json:"resource,omitempty"`
}

func toBatchRequestEcho(req AuthorizationRequest) batchIsAuthorizedRequestEchoJSON {
	echo := batchIsAuthorizedRequestEchoJSON{}

	if req.PrincipalEntityType != "" {
		echo.Principal = &entityIdentifierJSON{EntityType: req.PrincipalEntityType, EntityID: req.PrincipalEntityID}
	}

	if req.ActionType != "" {
		echo.Action = &actionIdentifierJSON{ActionType: req.ActionType, ActionID: req.ActionID}
	}

	if req.ResourceEntityType != "" {
		echo.Resource = &entityIdentifierJSON{EntityType: req.ResourceEntityType, EntityID: req.ResourceEntityID}
	}

	return echo
}

type batchIsAuthorizedDecision struct {
	Request             batchIsAuthorizedRequestEchoJSON `json:"request"`
	Decision            string                           `json:"decision"`
	DeterminingPolicies []determiningPolicyItemJSON      `json:"determiningPolicies"`
	Errors              []evaluationErrorItemJSON        `json:"errors"`
}

type batchIsAuthorizedOutput struct {
	Results []batchIsAuthorizedDecision `json:"results"`
}

func toBatchDecisions(decisions []AuthDecision) []batchIsAuthorizedDecision {
	out := make([]batchIsAuthorizedDecision, 0, len(decisions))

	for _, d := range decisions {
		out = append(out, batchIsAuthorizedDecision{
			Request:             toBatchRequestEcho(d.Request),
			Decision:            d.Decision,
			DeterminingPolicies: toDeterminingPolicyItems(d.DeterminingPolicies),
			Errors:              toEvaluationErrorItems(d.Errors),
		})
	}

	return out
}

func (h *Handler) handleBatchIsAuthorized(
	_ context.Context,
	in *batchIsAuthorizedInput,
) (*batchIsAuthorizedOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if len(in.Requests) > maxBatchRequests {
		return nil, fmt.Errorf(
			"%w: batch size %d exceeds maximum of %d",
			errInvalidRequest, len(in.Requests), maxBatchRequests,
		)
	}

	resolvedID, err := h.resolvePolicyStoreID(in.PolicyStoreID)
	if err != nil {
		return nil, err
	}

	requests := make([]AuthorizationRequest, 0, len(in.Requests))

	for _, r := range in.Requests {
		req := AuthorizationRequest{}

		if r.Principal != nil {
			req.PrincipalEntityType = r.Principal.EntityType
			req.PrincipalEntityID = r.Principal.EntityID
		}

		if r.Action != nil {
			req.ActionType = r.Action.ActionType
			req.ActionID = r.Action.ActionID
		}

		if r.Resource != nil {
			req.ResourceEntityType = r.Resource.EntityType
			req.ResourceEntityID = r.Resource.EntityID
		}

		requests = append(requests, req)
	}

	decisions, err := h.Backend.BatchIsAuthorized(resolvedID, requests)
	if err != nil {
		return nil, err
	}

	return &batchIsAuthorizedOutput{Results: toBatchDecisions(decisions)}, nil
}

type batchIsAuthorizedWithTokenRequestItem struct {
	Action   *actionIdentifierJSON `json:"action,omitempty"`
	Resource *entityIdentifierJSON `json:"resource,omitempty"`
}

type batchIsAuthorizedWithTokenInput struct {
	PolicyStoreID string                                  `json:"policyStoreId"`
	AccessToken   string                                  `json:"accessToken,omitempty"`
	IdentityToken string                                  `json:"identityToken,omitempty"`
	Entities      []entityJSON                            `json:"entities,omitempty"`
	Requests      []batchIsAuthorizedWithTokenRequestItem `json:"requests"`
}

// batchIsAuthorizedWithTokenRequestEchoJSON mirrors the real SDK's
// BatchIsAuthorizedWithTokenInputItem echoed back in each output item's
// "request" field: unlike BatchIsAuthorizedInputItem (used by plain
// BatchIsAuthorized), it has no principal member -- the principal for every
// item comes from the token, and is echoed once at the top level instead
// (see batchIsAuthorizedWithTokenOutput.Principal).
type batchIsAuthorizedWithTokenRequestEchoJSON struct {
	Action   *actionIdentifierJSON `json:"action,omitempty"`
	Resource *entityIdentifierJSON `json:"resource,omitempty"`
}

type batchIsAuthorizedWithTokenDecision struct {
	Request             batchIsAuthorizedWithTokenRequestEchoJSON `json:"request"`
	Decision            string                                    `json:"decision"`
	DeterminingPolicies []determiningPolicyItemJSON               `json:"determiningPolicies"`
	Errors              []evaluationErrorItemJSON                 `json:"errors"`
}

type batchIsAuthorizedWithTokenOutput struct {
	Principal *entityIdentifierJSON                `json:"principal,omitempty"`
	Results   []batchIsAuthorizedWithTokenDecision `json:"results"`
}

func toBatchWithTokenDecisions(decisions []AuthDecision) []batchIsAuthorizedWithTokenDecision {
	out := make([]batchIsAuthorizedWithTokenDecision, 0, len(decisions))

	for _, d := range decisions {
		echo := batchIsAuthorizedWithTokenRequestEchoJSON{}

		if d.Request.ActionType != "" {
			echo.Action = &actionIdentifierJSON{ActionType: d.Request.ActionType, ActionID: d.Request.ActionID}
		}

		if d.Request.ResourceEntityType != "" {
			echo.Resource = &entityIdentifierJSON{
				EntityType: d.Request.ResourceEntityType,
				EntityID:   d.Request.ResourceEntityID,
			}
		}

		out = append(out, batchIsAuthorizedWithTokenDecision{
			Request:             echo,
			Decision:            d.Decision,
			DeterminingPolicies: toDeterminingPolicyItems(d.DeterminingPolicies),
			Errors:              toEvaluationErrorItems(d.Errors),
		})
	}

	return out
}

func (h *Handler) handleBatchIsAuthorizedWithToken(
	_ context.Context,
	in *batchIsAuthorizedWithTokenInput,
) (*batchIsAuthorizedWithTokenOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.AccessToken == "" && in.IdentityToken == "" {
		return nil, fmt.Errorf("%w: accessToken or identityToken is required", errInvalidRequest)
	}

	if len(in.Requests) > maxBatchRequests {
		return nil, fmt.Errorf(
			"%w: batch size %d exceeds maximum of %d",
			errInvalidRequest, len(in.Requests), maxBatchRequests,
		)
	}

	resolvedID, err := h.resolvePolicyStoreID(in.PolicyStoreID)
	if err != nil {
		return nil, err
	}

	isAccessToken := in.AccessToken != ""

	token := in.AccessToken
	if token == "" {
		token = in.IdentityToken
	}

	principalType, principalID := h.principalFromToken(resolvedID, isAccessToken, token)

	requests := make([]AuthorizationRequest, 0, len(in.Requests))

	for _, r := range in.Requests {
		req := AuthorizationRequest{
			PrincipalEntityType: principalType,
			PrincipalEntityID:   principalID,
		}

		if r.Action != nil {
			req.ActionType = r.Action.ActionType
			req.ActionID = r.Action.ActionID
		}

		if r.Resource != nil {
			req.ResourceEntityType = r.Resource.EntityType
			req.ResourceEntityID = r.Resource.EntityID
		}

		requests = append(requests, req)
	}

	decisions, err := h.Backend.BatchIsAuthorizedWithToken(resolvedID, requests)
	if err != nil {
		return nil, err
	}

	out := &batchIsAuthorizedWithTokenOutput{Results: toBatchWithTokenDecisions(decisions)}
	if principalType != "" {
		out.Principal = &entityIdentifierJSON{EntityType: principalType, EntityID: principalID}
	}

	return out, nil
}

type isAuthorizedInput struct {
	Principal     *entityIdentifierJSON `json:"principal,omitempty"`
	Action        *actionIdentifierJSON `json:"action,omitempty"`
	Resource      *entityIdentifierJSON `json:"resource,omitempty"`
	PolicyStoreID string                `json:"policyStoreId"`
}

type isAuthorizedOutput struct {
	Decision            string                      `json:"decision"`
	DeterminingPolicies []determiningPolicyItemJSON `json:"determiningPolicies"`
	Errors              []evaluationErrorItemJSON   `json:"errors"`
}

func (h *Handler) handleIsAuthorized(_ context.Context, in *isAuthorizedInput) (*isAuthorizedOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	resolvedID, err := h.resolvePolicyStoreID(in.PolicyStoreID)
	if err != nil {
		return nil, err
	}

	req := AuthorizationRequest{}

	if in.Principal != nil {
		req.PrincipalEntityType = in.Principal.EntityType
		req.PrincipalEntityID = in.Principal.EntityID
	}

	if in.Action != nil {
		req.ActionType = in.Action.ActionType
		req.ActionID = in.Action.ActionID
	}

	if in.Resource != nil {
		req.ResourceEntityType = in.Resource.EntityType
		req.ResourceEntityID = in.Resource.EntityID
	}

	decision, err := h.Backend.IsAuthorized(resolvedID, req)
	if err != nil {
		return nil, err
	}

	return &isAuthorizedOutput{
		Decision:            decision.Decision,
		DeterminingPolicies: toDeterminingPolicyItems(decision.DeterminingPolicies),
		Errors:              toEvaluationErrorItems(decision.Errors),
	}, nil
}

const jwtPartCount = 3

var errMalformedJWT = errors.New("malformed JWT")

// parseJWTClaims extracts claims from a JWT without verifying the signature.
func parseJWTClaims(token string) (map[string]any, error) {
	parts := strings.SplitN(token, ".", jwtPartCount)
	if len(parts) != jwtPartCount {
		return nil, fmt.Errorf("%w: expected 3 parts, got %d", errMalformedJWT, len(parts))
	}

	payload, decodeErr := base64.RawURLEncoding.DecodeString(parts[1])
	if decodeErr != nil {
		return nil, fmt.Errorf("%w: bad payload encoding: %w", errMalformedJWT, decodeErr)
	}

	var claims map[string]any
	if unmarshalErr := json.Unmarshal(payload, &claims); unmarshalErr != nil {
		return nil, fmt.Errorf("%w: bad payload JSON: %w", errMalformedJWT, unmarshalErr)
	}

	return claims, nil
}

// principalFromToken resolves PrincipalEntityType and PrincipalEntityID from
// a JWT token, using the identity source in the policy store whose issuer
// matches the token's "iss" claim (falling back to the first identity source
// when no "iss" claim is present or none match, since a policy store
// typically has exactly one identity source anyway), then validating the
// token's aud/client_id claim against that source's configured client
// IDs/audiences (see matchesConfiguredAudience). isAccessToken selects which
// claim real AWS checks: "client_id"/"aud" for access tokens vs "aud" for ID
// tokens. Returns ("", "") when no principal can be resolved -- the caller's
// response omits the (optional, per the real SDK) principal field and the
// request evaluates with no principal, i.e. fails closed to DENY.
func (h *Handler) principalFromToken(policyStoreID string, isAccessToken bool, token string) (string, string) {
	sources, _, err := h.Backend.ListIdentitySources(policyStoreID, "", 0, nil)
	if err != nil || len(sources) == 0 {
		return "", ""
	}

	claims, err := parseJWTClaims(token)
	if err != nil {
		return "", ""
	}

	iss, _ := claims["iss"].(string)

	is, ok := matchIdentitySource(sources, iss, claims, isAccessToken)
	if !ok {
		return "", ""
	}

	claimName := "sub"

	if is.OIDCTokenSelection != nil && is.OIDCTokenSelection.PrincipalIDClaim != "" {
		claimName = is.OIDCTokenSelection.PrincipalIDClaim
	}

	claimVal, _ := claims[claimName].(string)

	return is.PrincipalEntityType, claimVal
}

// matchIdentitySource returns the identity source in sources whose OIDC
// issuer (its own OpenIDIssuer, or the issuer AWS derives from a Cognito
// user pool's ARN) matches issuer, falling back to the first identity source
// when issuer is empty or no source matches (a policy store typically has
// exactly one identity source). ok is false when the matched source has a
// configured client ID/audience restriction that the token's claims don't
// satisfy -- see matchesConfiguredAudience.
func matchIdentitySource(
	sources []IdentitySource,
	issuer string,
	claims map[string]any,
	isAccessToken bool,
) (IdentitySource, bool) {
	if issuer != "" {
		for _, is := range sources {
			switch {
			case is.UserPoolArn != "" && cognitoIssuerFromUserPoolArn(is.UserPoolArn) == issuer:
				return is, matchesConfiguredAudience(is, claims, isAccessToken)
			case is.OpenIDIssuer != "" && is.OpenIDIssuer == issuer:
				return is, matchesConfiguredAudience(is, claims, isAccessToken)
			}
		}
	}

	is := sources[0]

	return is, matchesConfiguredAudience(is, claims, isAccessToken)
}

// matchesConfiguredAudience reports whether claims satisfies is's configured
// client-id/audience restriction, per real AWS's documented client and
// audience validation:
//
//   - Cognito (docs.aws.amazon.com/verifiedpermissions/latest/userguide/cognito-validation.html):
//     "Verified Permissions compares this list of app client IDs to the ID
//     token aud claim or the access token client_id claim."
//   - OIDC (docs.aws.amazon.com/verifiedpermissions/latest/userguide/oidc-validation.html):
//     ID tokens are checked via "aud"; access tokens are checked via "aud",
//     falling back to "cid"/"client_id" when "aud" is absent.
//
// A source with no client IDs/audiences configured has no restriction
// ("when you enter one or more values for Client application validation")
// and always matches.
func matchesConfiguredAudience(is IdentitySource, claims map[string]any, isAccessToken bool) bool {
	claimNames, configured := audienceClaimNames(is, isAccessToken)
	if len(configured) == 0 {
		return true
	}

	for _, name := range claimNames {
		v, present := claims[name]
		if !present {
			continue
		}

		return audienceClaimMatches(v, configured)
	}

	return false
}

// JWT claim names checked by matchesConfiguredAudience.
const (
	claimAud      = "aud"
	claimCid      = "cid"
	claimClientID = "client_id"
)

// audienceClaimNames returns the ordered JWT claim names to check (first
// present one wins) and the configured client IDs/audiences for is, per the
// docs cited on matchesConfiguredAudience.
func audienceClaimNames(is IdentitySource, isAccessToken bool) ([]string, []string) {
	switch {
	case is.UserPoolArn != "":
		if isAccessToken {
			return []string{claimClientID}, is.ClientIDs
		}

		return []string{claimAud}, is.ClientIDs
	case is.OIDCTokenSelection != nil:
		if is.OIDCTokenSelection.TokenType == tokenTypeAccess {
			return []string{claimAud, claimCid, claimClientID}, is.OIDCTokenSelection.Audiences
		}

		return []string{claimAud}, is.OIDCTokenSelection.Audiences
	default:
		return nil, nil
	}
}

// audienceClaimMatches reports whether v (a JWT claim value, either a single
// string or a JSON array of strings per the JWT spec's "aud" claim) contains
// any of the configured client IDs/audiences.
func audienceClaimMatches(v any, configured []string) bool {
	switch vv := v.(type) {
	case string:
		return slices.Contains(configured, vv)
	case []any:
		for _, item := range vv {
			if s, ok := item.(string); ok && slices.Contains(configured, s) {
				return true
			}
		}
	}

	return false
}

type isAuthorizedWithTokenInput struct {
	Action        *actionIdentifierJSON `json:"action,omitempty"`
	Resource      *entityIdentifierJSON `json:"resource,omitempty"`
	PolicyStoreID string                `json:"policyStoreId"`
	AccessToken   string                `json:"accessToken,omitempty"`
	IdentityToken string                `json:"identityToken,omitempty"`
}

// isAuthorizedWithTokenOutput mirrors the real SDK's
// IsAuthorizedWithTokenOutput: unlike plain IsAuthorized, it also echoes the
// principal resolved from the token (optional -- omitted when no principal
// could be resolved).
type isAuthorizedWithTokenOutput struct {
	Principal           *entityIdentifierJSON       `json:"principal,omitempty"`
	Decision            string                      `json:"decision"`
	DeterminingPolicies []determiningPolicyItemJSON `json:"determiningPolicies"`
	Errors              []evaluationErrorItemJSON   `json:"errors"`
}

func (h *Handler) handleIsAuthorizedWithToken(
	_ context.Context,
	in *isAuthorizedWithTokenInput,
) (*isAuthorizedWithTokenOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.AccessToken == "" && in.IdentityToken == "" {
		return nil, fmt.Errorf("%w: accessToken or identityToken is required", errInvalidRequest)
	}

	resolvedID, err := h.resolvePolicyStoreID(in.PolicyStoreID)
	if err != nil {
		return nil, err
	}

	isAccessToken := in.AccessToken != ""

	token := in.AccessToken
	if token == "" {
		token = in.IdentityToken
	}

	req := AuthorizationRequest{}

	if in.Action != nil {
		req.ActionType = in.Action.ActionType
		req.ActionID = in.Action.ActionID
	}

	if in.Resource != nil {
		req.ResourceEntityType = in.Resource.EntityType
		req.ResourceEntityID = in.Resource.EntityID
	}

	req.PrincipalEntityType, req.PrincipalEntityID = h.principalFromToken(resolvedID, isAccessToken, token)

	decision, err := h.Backend.IsAuthorizedWithToken(resolvedID, req)
	if err != nil {
		return nil, err
	}

	out := &isAuthorizedWithTokenOutput{
		Decision:            decision.Decision,
		DeterminingPolicies: toDeterminingPolicyItems(decision.DeterminingPolicies),
		Errors:              toEvaluationErrorItems(decision.Errors),
	}
	if req.PrincipalEntityType != "" {
		out.Principal = &entityIdentifierJSON{EntityType: req.PrincipalEntityType, EntityID: req.PrincipalEntityID}
	}

	return out, nil
}
