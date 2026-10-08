package cognitoidp

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

const (
	preTokenLambdaVersionV2 = "V2_0"
	preTokenEventVersionV2  = "2"
	claimCognitoRoles       = "cognito:roles"
	claimPreferredRole      = "cognito:preferred_role"
	claimPrefixCognito      = "cognito:"
	respKeyClaimsAndScope   = "claimsAndScopeOverrideDetails"
	attrEmailVerified       = "email_verified"
	keyGroupConfiguration   = "groupConfiguration"
	keyGroupsToOverride     = "groupsToOverride"
	keyIAMRolesToOverride   = "iamRolesToOverride"
	keyPreferredRole        = "preferredRole"
)

// preTokenOverride is the parsed V2_0/V3_0 claimsAndScopeOverrideDetails.
type preTokenOverride struct {
	groups *groupOverride
	idEdit claimEdits
	access m2mOverride
}

type claimEdits struct {
	claims   map[string]any
	suppress []string
}

// groupOverride is groupOverrideDetails; the zero value suppresses groups.
type groupOverride struct {
	preferredRole string
	groups        []string
	roles         []string
}

// tokenOverrides is whatever the PreTokenGeneration trigger returned, in V1 or V2 shape.
type tokenOverrides struct {
	v2         *preTokenOverride
	v1Claims   map[string]string
	v1Suppress []string
}

// preTokenEventVersion maps the pool's LambdaVersion to the event "version" field.
func preTokenEventVersion(cfg map[string]any) string {
	switch preTokenLambdaVersion(cfg) {
	case preTokenLambdaVersionV3:
		return preTokenEventVersionV3
	case preTokenLambdaVersionV2:
		return preTokenEventVersionV2
	default:
		return "1"
	}
}

func preTokenAccessCustomizable(cfg map[string]any) bool {
	return preTokenEventVersion(cfg) != "1"
}

// groupRolesLocked returns the IAM roles of groups and the first group's role as preferred
// (groups arrive precedence-sorted).
func (b *InMemoryBackend) groupRolesLocked(poolID string, groups []string) ([]string, string) {
	var (
		roles     []string
		preferred string
	)

	for _, name := range groups {
		g, ok := b.groups.Get(groupKey(poolID, name))
		if !ok || g.RoleArn == "" {
			continue
		}

		roles = append(roles, g.RoleArn)

		if preferred == "" {
			preferred = g.RoleArn
		}
	}

	return roles, preferred
}

// preTokenV2Request builds the version-2/3 request (user-pool-lambda-pre-token-generation).
func (b *InMemoryBackend) preTokenV2Request(
	pool *UserPool, user *User, groups, scopes []string, cm map[string]string,
) map[string]any {
	roles, preferred := b.groupRolesLocked(pool.ID, groups)

	var preferredAny any
	if preferred != "" {
		preferredAny = preferred
	}

	return map[string]any{
		eventKeyUserAttributes: stringMapToAny(user.Attributes),
		"scopes":               stringsToAny(scopes),
		keyGroupConfiguration:  groupConfigEvent(groups, roles, preferredAny),
		eventKeyClientMetadata: stringMapToAny(cm),
	}
}

func groupConfigEvent(groups, roles []string, preferred any) map[string]any {
	return map[string]any{
		keyGroupsToOverride:   stringsToAny(groups),
		keyIAMRolesToOverride: stringsToAny(roles),
		keyPreferredRole:      preferred,
	}
}

func malformedPreToken(field string) error {
	return fmt.Errorf("%w: PreTokenGeneration trigger response has an invalid %s", ErrUnexpectedLambda, field)
}

func strictStrings(v any, field string) ([]string, error) {
	if v == nil {
		return nil, nil
	}

	list, ok := v.([]any)
	if !ok {
		return nil, malformedPreToken(field)
	}

	out := make([]string, 0, len(list))

	for _, e := range list {
		s, isStr := e.(string)
		if !isStr {
			return nil, malformedPreToken(field)
		}

		out = append(out, s)
	}

	return out, nil
}

func parseGeneration(raw any, name string) (claimEdits, []string, []string, error) {
	if raw == nil {
		return claimEdits{}, nil, nil, nil
	}

	block, ok := raw.(map[string]any)
	if !ok {
		return claimEdits{}, nil, nil, malformedPreToken(name)
	}

	var edits claimEdits

	if c := block["claimsToAddOrOverride"]; c != nil {
		m, isMap := c.(map[string]any)
		if !isMap {
			return claimEdits{}, nil, nil, malformedPreToken(name + ".claimsToAddOrOverride")
		}

		edits.claims = m
	}

	var err error

	if edits.suppress, err = strictStrings(block["claimsToSuppress"], name+".claimsToSuppress"); err != nil {
		return claimEdits{}, nil, nil, err
	}

	add, err := strictStrings(block["scopesToAdd"], name+".scopesToAdd")
	if err != nil {
		return claimEdits{}, nil, nil, err
	}

	remove, err := strictStrings(block["scopesToSuppress"], name+".scopesToSuppress")
	if err != nil {
		return claimEdits{}, nil, nil, err
	}

	return edits, add, remove, nil
}

func parseGroupOverride(raw any) (*groupOverride, error) {
	if raw == nil {
		return &groupOverride{}, nil
	}

	m, ok := raw.(map[string]any)
	if !ok {
		return nil, malformedPreToken("groupOverrideDetails")
	}

	groups, err := strictStrings(m[keyGroupsToOverride], "groupOverrideDetails.groupsToOverride")
	if err != nil {
		return nil, err
	}

	roles, err := strictStrings(m[keyIAMRolesToOverride], "groupOverrideDetails.iamRolesToOverride")
	if err != nil {
		return nil, err
	}

	preferred, _ := m[keyPreferredRole].(string)

	return &groupOverride{groups: groups, roles: roles, preferredRole: preferred}, nil
}

// parseClaimsAndScopeOverride reads a V2_0/V3_0 response; a malformed block is UnexpectedLambdaException.
func parseClaimsAndScopeOverride(resp map[string]any) (*preTokenOverride, error) {
	out := &preTokenOverride{}

	raw := resp[respKeyClaimsAndScope]
	if raw == nil {
		return out, nil
	}

	details, ok := raw.(map[string]any)
	if !ok {
		return nil, malformedPreToken(respKeyClaimsAndScope)
	}

	var err error

	if out.idEdit, _, _, err = parseGeneration(details["idTokenGeneration"], "idTokenGeneration"); err != nil {
		return nil, err
	}

	edits, add, remove, err := parseGeneration(details["accessTokenGeneration"], "accessTokenGeneration")
	if err != nil {
		return nil, err
	}

	out.access = m2mOverride{claims: edits.claims, suppress: edits.suppress, scopesToAdd: add, scopesToRemove: remove}

	if rawGroups, hasGroups := details["groupOverrideDetails"]; hasGroups {
		if out.groups, err = parseGroupOverride(rawGroups); err != nil {
			return nil, err
		}
	}

	return out, nil
}

// claimWritable reports whether a trigger may add or override name.
func claimWritable(name string) bool {
	if _, protected := protectedTokenClaims[name]; protected {
		return false
	}

	return !strings.HasPrefix(name, claimPrefixCognito) && !strings.HasPrefix(name, "dev:")
}

// claimSuppressible reports whether a trigger may remove name; group claims can be suppressed.
func claimSuppressible(name string) bool {
	switch name {
	case claimCognitoGroups, claimCognitoRoles, claimPreferredRole:
		return true
	}

	if strings.HasPrefix(name, "dev:") {
		return true
	}

	_, protected := protectedTokenClaims[name]

	return !protected && !strings.HasPrefix(name, claimPrefixCognito)
}

func scalarClaim(v any) bool {
	switch v.(type) {
	case string, float64, bool:
		return true
	}

	return false
}

// claimValueAllowed accepts string, number, boolean, arrays of those, and JSON objects.
func claimValueAllowed(v any, complexOK bool) bool {
	if scalarClaim(v) {
		return true
	}

	if !complexOK {
		return false
	}

	switch t := v.(type) {
	case map[string]any:
		return true
	case []any:
		return slices.IndexFunc(t, func(e any) bool { return !scalarClaim(e) }) < 0
	}

	return false
}

// idComplexForbidden are ID-token claims that cannot hold objects or arrays.
func idComplexForbidden(name string) bool {
	switch name {
	case "phone_number_verified", attrEmailVerified, "updated_at", "address":
		return true
	}

	return false
}

func suppressClaims(claims jwt.MapClaims, names []string, idToken bool) {
	for _, k := range names {
		if !claimSuppressible(k) {
			continue
		}

		delete(claims, k)

		if idToken && k == claimCognitoGroups {
			delete(claims, claimCognitoRoles)
			delete(claims, claimPreferredRole)
		}
	}
}

// applyID applies idTokenGeneration to the ID token claims; suppress wins over add.
func (o *preTokenOverride) applyID(claims jwt.MapClaims) {
	if o == nil {
		return
	}

	for k, v := range o.idEdit.claims {
		if claimWritable(k) && claimValueAllowed(v, !idComplexForbidden(k)) {
			claims[k] = v
		}
	}

	suppressClaims(claims, o.idEdit.suppress, true)
}

// applyAccess applies accessTokenGeneration claim edits; aud is accepted only when it equals the client ID.
func (o *preTokenOverride) applyAccess(claims jwt.MapClaims) {
	if o == nil {
		return
	}

	for k, v := range o.access.claims {
		switch {
		case k == claimAud:
			if s, _ := v.(string); s != "" && s == claims[claimClientID] {
				claims[k] = v
			}
		case claimWritable(k) && claimValueAllowed(v, true):
			claims[k] = v
		}
	}

	suppressClaims(claims, o.access.suppress, false)
}

// accessScope applies scopesToAdd/scopesToSuppress to the resolved scope string.
func (o *preTokenOverride) accessScope(scope string) string {
	if o == nil {
		return scope
	}

	scopes := o.access.applyScopes(strings.Fields(scope))
	sort.Strings(scopes)

	return strings.Join(scopes, " ")
}

// useTo copies the trigger result into p: V1 claims as before, V2 edits plus group overrides.
func (o tokenOverrides) useTo(p *TokenParams) {
	p.ClaimsToAddOrOverride = o.v1Claims
	p.ClaimsToSuppress = o.v1Suppress
	p.Overrides = o.v2

	if o.v2 != nil && o.v2.groups != nil {
		p.Groups = o.v2.groups.groups
		p.Roles = o.v2.groups.roles
		p.PreferredRole = o.v2.groups.preferredRole
	}
}
