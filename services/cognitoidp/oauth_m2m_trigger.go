package cognitoidp

import (
	"context"
	"slices"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

const (
	triggerSourceTokenGenClientCredentials = "TokenGeneration_ClientCredentials"
	preTokenLambdaVersionV3                = "V3_0"
	preTokenEventVersionV3                 = "3"
)

// m2mOverride is the accessTokenGeneration block of a V3_0 PreTokenGeneration response.
type m2mOverride struct {
	claims         map[string]any
	suppress       []string
	scopesToAdd    []string
	scopesToRemove []string
}

// preTokenLambdaVersion reads LambdaVersion from PreTokenGenerationConfig; the bare-ARN form means V1_0.
func preTokenLambdaVersion(cfg map[string]any) string {
	nested, _ := cfg[triggerKeyPreTokenGeneration+"Config"].(map[string]any)
	v, _ := nested["LambdaVersion"].(string)

	return v
}

// prepareM2MTrigger builds the client-credentials event; nil unless the pool is on V3_0
// (user-pool-lambda-pre-token-generation: M2M tokens only invoke the trigger for V3_0 events).
func (b *InMemoryBackend) prepareM2MTrigger(
	pool *UserPool, clientID string, scopes []string, metadata map[string]string,
) *triggerCall {
	if preTokenLambdaVersion(pool.LambdaConfig) != preTokenLambdaVersionV3 {
		return nil
	}

	call := b.prepareTrigger(pool, triggerKeyPreTokenGeneration, triggerSourceTokenGenClientCredentials, clientID, "",
		map[string]any{
			eventKeyUserAttributes: map[string]any{},
			"scopes":               stringsToAny(scopes),
			keyGroupConfiguration:  groupConfigEvent(nil, nil, nil),
			eventKeyClientMetadata: stringMapToAny(metadata),
		},
		map[string]any{"claimsAndScopeOverrideDetails": map[string]any{}},
	)
	if call != nil {
		call.event["version"] = preTokenEventVersionV3
	}

	return call
}

// runM2MTrigger invokes the prepared event (no lock held) and parses accessTokenGeneration.
func runM2MTrigger(call *triggerCall) (m2mOverride, error) {
	result, err := call.inv.InvokeTrigger(context.Background(), call.functionARN, call.event)

	resp, err := parseTriggerResult(triggerKeyPreTokenGeneration, result, err)
	if err != nil {
		return m2mOverride{}, err
	}

	details, _ := resp["claimsAndScopeOverrideDetails"].(map[string]any)
	access, _ := details["accessTokenGeneration"].(map[string]any)
	claims, _ := access["claimsToAddOrOverride"].(map[string]any)

	return m2mOverride{
		claims:         claims,
		suppress:       anyStrings(access["claimsToSuppress"]),
		scopesToAdd:    anyStrings(access["scopesToAdd"]),
		scopesToRemove: anyStrings(access["scopesToSuppress"]),
	}, nil
}

func anyStrings(v any) []string {
	list, _ := v.([]any)

	var out []string

	for _, e := range list {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}

	return out
}

// applyScopes adds then suppresses scopes; scopes containing whitespace cannot be added.
func (o m2mOverride) applyScopes(scopes []string) []string {
	out := slices.Clone(scopes)

	for _, s := range o.scopesToAdd {
		if s != "" && !strings.ContainsAny(s, " \t\r\n") && !slices.Contains(out, s) {
			out = append(out, s)
		}
	}

	return slices.DeleteFunc(out, func(s string) bool { return slices.Contains(o.scopesToRemove, s) })
}

// applyClaims adds, overrides and suppresses claims under the documented protected-claim rules.
func (o m2mOverride) applyClaims(claims jwt.MapClaims) {
	(&preTokenOverride{access: o}).applyAccess(claims)
}
