package iot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// AuthInfo describes one action/resources pair to evaluate authorization for.
type AuthInfo struct {
	ActionType string   `json:"actionType,omitempty"`
	Resources  []string `json:"resources"`
}

// PolicyIdentifier identifies a policy referenced by an authorization result.
type PolicyIdentifier struct {
	PolicyName string `json:"policyName,omitempty"`
	PolicyARN  string `json:"policyArn,omitempty"`
}

// AuthDecisionPolicies is the set of policies backing an allow/deny decision.
type AuthDecisionPolicies struct {
	Policies []PolicyIdentifier `json:"policies,omitempty"`
}

// AuthResult is the per-authInfo evaluation result of TestAuthorization.
type AuthResult struct {
	AuthInfo     *AuthInfo             `json:"authInfo,omitempty"`
	Allowed      *AuthDecisionPolicies `json:"allowed,omitempty"`
	Denied       *AuthDecisionPolicies `json:"denied,omitempty"`
	AuthDecision string                `json:"authDecision"`
}

// TestAuthorizationInput is the input for TestAuthorization.
type TestAuthorizationInput struct {
	Principal             string
	CognitoIdentityPoolID string
	ClientID              string
	PolicyNamesToAdd      []string
	PolicyNamesToSkip     []string
	AuthInfos             []AuthInfo
}

// actionTypeToIoTAction maps an AWS IoT ActionType to the policy action name
// it corresponds to in an IoT policy document (e.g. "iot:Publish").
func actionTypeToIoTAction(actionType string) string {
	switch strings.ToUpper(actionType) {
	case "PUBLISH":
		return "iot:Publish"
	case "SUBSCRIBE":
		return "iot:Subscribe"
	case "RECEIVE":
		return "iot:Receive"
	case "CONNECT":
		return "iot:Connect"
	default:
		return ""
	}
}

// TestAuthorization evaluates the effective policy set for a principal (plus
// PolicyNamesToAdd, minus PolicyNamesToSkip) against each requested
// authInfo/resource pair using real, stored IoT policy documents.
func (b *InMemoryBackend) TestAuthorization(input *TestAuthorizationInput) ([]*AuthResult, error) {
	if input.Principal == "" {
		return nil, fmt.Errorf("%w: principal is required", ErrValidation)
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	policies, err := b.effectiveTestPolicies(input)
	if err != nil {
		return nil, err
	}

	statements := make([]iamStatementWithPolicy, 0, len(policies))

	for _, p := range policies {
		stmts, parseErr := parseIoTPolicyDocument(p.PolicyDocument)
		if parseErr != nil {
			continue
		}

		for _, s := range stmts {
			statements = append(statements, iamStatementWithPolicy{
				statement: s,
				policy:    PolicyIdentifier{PolicyName: p.PolicyName, PolicyARN: p.ARN},
			})
		}
	}

	out := make([]*AuthResult, 0, len(input.AuthInfos))

	for i := range input.AuthInfos {
		out = append(out, evaluateAuthInfo(&input.AuthInfos[i], statements))
	}

	return out, nil
}

// effectiveTestPolicies resolves the policy set to evaluate for
// TestAuthorization: those attached to the principal, plus PolicyNamesToAdd,
// minus PolicyNamesToSkip. Must be called with b.mu held (read or write).
func (b *InMemoryBackend) effectiveTestPolicies(input *TestAuthorizationInput) ([]*Policy, error) {
	seen := make(map[string]bool)

	var policies []*Policy

	for policyName, targets := range b.policyTargets {
		if !slices.Contains(targets, input.Principal) || seen[policyName] {
			continue
		}

		if p, ok := b.policies.Get(policyName); ok {
			cp := *p
			policies = append(policies, &cp)
			seen[policyName] = true
		}
	}

	for _, name := range input.PolicyNamesToAdd {
		if seen[name] {
			continue
		}

		p, ok := b.policies.Get(name)
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrPolicyNotFound, name)
		}

		cp := *p
		policies = append(policies, &cp)
		seen[name] = true
	}

	skip := make(map[string]bool, len(input.PolicyNamesToSkip))
	for _, name := range input.PolicyNamesToSkip {
		skip[name] = true
	}

	filtered := make([]*Policy, 0, len(policies))

	for _, p := range policies {
		if !skip[p.PolicyName] {
			filtered = append(filtered, p)
		}
	}

	return filtered, nil
}

// iamStatementWithPolicy pairs a parsed IAM-style statement with the policy
// identifier it came from, for reporting in AuthResult.
type iamStatementWithPolicy struct {
	policy    PolicyIdentifier
	statement iamStatement
}

// evaluateAuthInfo determines the AuthResult for one authInfo entry by
// evaluating every requested resource against the statement set. If any
// resource is denied/implicitly-denied, that is reflected in the aggregate
// per AWS's "explicit deny wins, else any implicit deny wins" semantics.
func evaluateAuthInfo(info *AuthInfo, statements []iamStatementWithPolicy) *AuthResult {
	action := actionTypeToIoTAction(info.ActionType)

	result := &AuthResult{AuthInfo: info}

	var (
		allowPolicies []PolicyIdentifier
		denyPolicies  []PolicyIdentifier
		anyAllowed    bool
	)

	seenAllow := map[string]bool{}
	seenDeny := map[string]bool{}

	resources := info.Resources
	if len(resources) == 0 {
		resources = []string{"*"}
	}

	for _, resource := range resources {
		matched := false

		for _, sw := range statements {
			if !statementMatches(sw.statement, action, resource) {
				continue
			}

			matched = true

			if strings.EqualFold(sw.statement.Effect, "Deny") {
				if !seenDeny[sw.policy.PolicyName] {
					denyPolicies = append(denyPolicies, sw.policy)
					seenDeny[sw.policy.PolicyName] = true
				}
			} else if !seenAllow[sw.policy.PolicyName] {
				allowPolicies = append(allowPolicies, sw.policy)
				seenAllow[sw.policy.PolicyName] = true
			}
		}

		if matched {
			anyAllowed = true
		}
	}

	switch {
	case len(denyPolicies) > 0:
		result.AuthDecision = "EXPLICIT_DENY"
		result.Denied = &AuthDecisionPolicies{Policies: denyPolicies}
	case anyAllowed && len(allowPolicies) > 0:
		result.AuthDecision = "ALLOWED"
		result.Allowed = &AuthDecisionPolicies{Policies: allowPolicies}
	default:
		result.AuthDecision = "IMPLICIT_DENY"
		result.Denied = &AuthDecisionPolicies{}
	}

	return result
}

// iamStatement is a minimal IAM-style policy statement as used in IoT policy
// documents: {"Effect":"Allow","Action":"iot:*","Resource":"*"}.
type iamStatement struct {
	Effect   string
	Action   []string
	Resource []string
}

// flexStringList unmarshals a JSON value that may be either a single string
// or an array of strings, as IAM-style Action/Resource fields allow.
type flexStringList []string

func (f *flexStringList) UnmarshalJSON(data []byte) error {
	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*f = []string{single}

		return nil
	}

	var list []string
	if err := json.Unmarshal(data, &list); err != nil {
		return err
	}

	*f = list

	return nil
}

// parseIoTPolicyDocument parses an IAM-style IoT policy document JSON string
// into a slice of statements.
func parseIoTPolicyDocument(doc string) ([]iamStatement, error) {
	var raw struct {
		Statement []struct {
			Effect   string         `json:"Effect"`
			Action   flexStringList `json:"Action"`
			Resource flexStringList `json:"Resource"`
		} `json:"Statement"`
	}

	if err := json.Unmarshal([]byte(doc), &raw); err != nil {
		return nil, err
	}

	out := make([]iamStatement, 0, len(raw.Statement))

	for _, s := range raw.Statement {
		out = append(out, iamStatement{
			Effect:   s.Effect,
			Action:   s.Action,
			Resource: s.Resource,
		})
	}

	return out, nil
}

// statementMatches reports whether stmt authorizes the given action on the
// given resource, supporting "*" wildcards as IAM/IoT policies do.
func statementMatches(stmt iamStatement, action, resource string) bool {
	actionOK := action == ""

	for _, a := range stmt.Action {
		if iamGlobMatch(a, action) {
			actionOK = true

			break
		}
	}

	if !actionOK {
		return false
	}

	for _, r := range stmt.Resource {
		if iamGlobMatch(r, resource) {
			return true
		}
	}

	return false
}

// iamGlobMatch reports whether value matches an IAM-style pattern containing
// "*" wildcards (e.g. "iot:*", "arn:aws:iot:*:*:topic/foo/*").
func iamGlobMatch(pattern, value string) bool {
	if pattern == "*" {
		return true
	}

	quoted := regexp.QuoteMeta(pattern)
	reStr := "^" + strings.ReplaceAll(quoted, `\*`, ".*") + "$"

	re, err := regexp.Compile(reStr)
	if err != nil {
		return pattern == value
	}

	return re.MatchString(value)
}

// TestInvokeAuthorizerInput is the input for TestInvokeAuthorizer.
type TestInvokeAuthorizerInput struct {
	Token          string
	TokenSignature string
	MQTTClientID   string
}

// TestInvokeAuthorizerOutput is the output for TestInvokeAuthorizer.
type TestInvokeAuthorizerOutput struct {
	PrincipalID              string
	PolicyDocuments          []string
	IsAuthenticated          bool
	DisconnectAfterInSeconds int32
	RefreshAfterInSeconds    int32
}

const (
	authorizerDisconnectAfterSeconds = 3600
	authorizerRefreshAfterSeconds    = 300
)

// TestInvokeAuthorizer evaluates a stored custom authorizer's configuration
// (status, signing requirement) against the supplied token/signature and
// returns a deterministic authentication result derived from that stored
// state.
func (b *InMemoryBackend) TestInvokeAuthorizer(
	authorizerName string,
	input *TestInvokeAuthorizerInput,
) (*TestInvokeAuthorizerOutput, error) {
	b.mu.RLock()
	a, ok := b.authorizers.Get(authorizerName)
	b.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("authorizer %q not found: %w", authorizerName, ErrResourceNotFound)
	}

	out := &TestInvokeAuthorizerOutput{}

	switch {
	case a.Status != "" && a.Status != statusActive:
		out.IsAuthenticated = false
	case !a.SigningDisabled && input.TokenSignature == "":
		out.IsAuthenticated = false
	default:
		out.IsAuthenticated = true

		principal := input.Token
		if principal == "" {
			principal = input.MQTTClientID
		}

		out.PrincipalID = authorizerPrincipalID(authorizerName, principal)
		out.PolicyDocuments = []string{defaultAuthorizerPolicyDocument()}
		out.DisconnectAfterInSeconds = authorizerDisconnectAfterSeconds
		out.RefreshAfterInSeconds = authorizerRefreshAfterSeconds
	}

	return out, nil
}

// authorizerPrincipalID deterministically derives a principal ID from the
// authorizer name and the caller-supplied token/clientId.
func authorizerPrincipalID(authorizerName, seed string) string {
	sum := sha256.Sum256([]byte(authorizerName + "/" + seed))

	return "authz-" + hex.EncodeToString(sum[:8])
}

// defaultAuthorizerPolicyDocument returns the (deterministic) IAM-style
// policy document a passing custom-authorizer invocation grants, absent a
// real Lambda to invoke.
func defaultAuthorizerPolicyDocument() string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}`
}
