package verifiedpermissions

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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

	decisions, err := h.Backend.BatchIsAuthorized(in.PolicyStoreID, requests)
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

func (h *Handler) handleBatchIsAuthorizedWithToken(
	_ context.Context,
	in *batchIsAuthorizedWithTokenInput,
) (*batchIsAuthorizedOutput, error) {
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

	token := in.AccessToken
	if token == "" {
		token = in.IdentityToken
	}

	principalType, principalID := h.principalFromToken(in.PolicyStoreID, token)

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

	decisions, err := h.Backend.BatchIsAuthorizedWithToken(in.PolicyStoreID, requests)
	if err != nil {
		return nil, err
	}

	return &batchIsAuthorizedOutput{Results: toBatchDecisions(decisions)}, nil
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

	decision, err := h.Backend.IsAuthorized(in.PolicyStoreID, req)
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

// principalFromToken resolves PrincipalEntityType and PrincipalEntityID from a JWT
// token using the first matching identity source in the policy store.
func (h *Handler) principalFromToken(policyStoreID, token string) (string, string) {
	sources, _, err := h.Backend.ListIdentitySources(policyStoreID, "", 0, nil)
	if err != nil || len(sources) == 0 {
		return "", ""
	}

	claims, err := parseJWTClaims(token)
	if err != nil {
		return "", ""
	}

	is := sources[0]
	claimName := "sub"

	if is.OIDCTokenSelection != nil && is.OIDCTokenSelection.PrincipalIDClaim != "" {
		claimName = is.OIDCTokenSelection.PrincipalIDClaim
	}

	claimVal, _ := claims[claimName].(string)

	return is.PrincipalEntityType, claimVal
}

type isAuthorizedWithTokenInput struct {
	Action        *actionIdentifierJSON `json:"action,omitempty"`
	Resource      *entityIdentifierJSON `json:"resource,omitempty"`
	PolicyStoreID string                `json:"policyStoreId"`
	AccessToken   string                `json:"accessToken,omitempty"`
	IdentityToken string                `json:"identityToken,omitempty"`
}

func (h *Handler) handleIsAuthorizedWithToken(
	_ context.Context,
	in *isAuthorizedWithTokenInput,
) (*isAuthorizedOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.AccessToken == "" && in.IdentityToken == "" {
		return nil, fmt.Errorf("%w: accessToken or identityToken is required", errInvalidRequest)
	}

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

	req.PrincipalEntityType, req.PrincipalEntityID = h.principalFromToken(in.PolicyStoreID, token)

	decision, err := h.Backend.IsAuthorizedWithToken(in.PolicyStoreID, req)
	if err != nil {
		return nil, err
	}

	return &isAuthorizedOutput{
		Decision:            decision.Decision,
		DeterminingPolicies: toDeterminingPolicyItems(decision.DeterminingPolicies),
		Errors:              toEvaluationErrorItems(decision.Errors),
	}, nil
}
