package azurearm

import (
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
)

const (
	msiMaxRestrictionProviders = 256
	msiFlexLanguageVersion     = 1
	msiSubClaim                = "sub"
	msiOpMatches               = "matches"
	msiOpEq                    = "eq"
	msiIssuerGitHub            = "token.actions.githubusercontent.com"
	msiFlexKey                 = "claimsMatchingExpression"
)

var (
	msiProviderRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]*(\.[A-Za-z][A-Za-z0-9]*)+(/[A-Za-z][A-Za-z0-9]*)?$`)
	msiFlexTermRe = regexp.MustCompile(`^claims\['([A-Za-z0-9_]+)'\] (matches|eq) '([^']*)'$`)
)

type storedFlexClaims struct {
	Value           string `json:"value"`
	LanguageVersion int    `json:"languageVersion"`
}

type storedRestrictions struct {
	Providers []string `json:"providers"`
}

// msiRestrictions parses properties.assignmentRestrictions (azure-rest-api-specs AssignmentRestrictions,
// 2025-05-31-preview).
func msiRestrictions(props map[string]any) (*storedRestrictions, error) {
	v, has := props["assignmentRestrictions"]
	if !has || v == nil {
		return nil, nil //nolint:nilnil // absent restrictions
	}

	m, ok := v.(map[string]any)
	if !ok {
		return nil, netErr(http.StatusBadRequest, msiCodeBad, "properties.assignmentRestrictions must be an object.")
	}

	raw, _ := m["providers"].([]any)
	if _, set := m["providers"]; set && m["providers"] != nil && raw == nil {
		return nil, netErr(http.StatusBadRequest, msiCodeBad,
			"properties.assignmentRestrictions.providers must be an array of strings.")
	}

	if len(raw) > msiMaxRestrictionProviders {
		return nil, netErr(http.StatusBadRequest, msiCodeBad,
			"properties.assignmentRestrictions.providers supports at most %d entries.", msiMaxRestrictionProviders)
	}

	out := &storedRestrictions{Providers: make([]string, 0, len(raw))}

	for _, e := range raw {
		s, isStr := e.(string)
		if !isStr || !msiProviderRe.MatchString(s) {
			return nil, netErr(http.StatusBadRequest, msiCodeBad,
				"The value '%v' of properties.assignmentRestrictions.providers is not a resource provider such as "+
					"'Microsoft.Compute' or a resource type such as 'Microsoft.Storage/Accounts'.", e)
		}

		if !slices.ContainsFunc(out.Providers, func(o string) bool { return strings.EqualFold(o, s) }) {
			out.Providers = append(out.Providers, s)
		}
	}

	return out, nil
}

func (r *storedRestrictions) body() map[string]any {
	return map[string]any{"providers": r.Providers}
}

// allows reports whether a resource of resourceType (Namespace/type) may carry the identity. An empty provider list
// restricts nothing.
func (r *storedRestrictions) allows(resourceType string) bool {
	if r == nil || len(r.Providers) == 0 {
		return true
	}

	ns, _, _ := strings.Cut(resourceType, "/")

	return slices.ContainsFunc(r.Providers, func(p string) bool {
		return strings.EqualFold(p, ns) || strings.EqualFold(p, resourceType) ||
			strings.HasPrefix(strings.ToLower(resourceType), strings.ToLower(p)+"/")
	})
}

// restrictionsOf returns the assignment restrictions of a user-assigned identity.
func (p *ManagedIdentityProvider) restrictionsOf(idStr string) *storedRestrictions {
	rid, err := ParseGenericResourcePath(idStr)
	if err != nil || len(rid.Names) != 1 {
		return nil
	}

	p.mu.RLock("restrictionsOf")
	defer p.mu.RUnlock()

	if i, ok := p.identities[msiKey(rid.SubscriptionID, rid.ResourceGroup, rid.Names[0])]; ok {
		return i.Restrictions
	}

	return nil
}

// flexClaims validates properties.claimsMatchingExpression against the documented flexible federated identity
// credential language (Learn: entra/workload-id/workload-identities-flexible-federated-identity-credentials).
func flexClaims(props map[string]any, issuer string) (*storedFlexClaims, error) {
	v, has := props[msiFlexKey]
	if !has || v == nil {
		return nil, nil //nolint:nilnil // absent expression
	}

	m, ok := v.(map[string]any)
	if !ok {
		return nil, netErr(http.StatusBadRequest, msiCodeBad, "properties.claimsMatchingExpression must be an object.")
	}

	value, _ := m[fieldValue].(string)
	lang, isNum := m["languageVersion"].(float64)

	switch {
	case value == "":
		return nil, netErr(http.StatusBadRequest, msiCodeBad, "properties.claimsMatchingExpression.value is required.")
	case !isNum || lang != msiFlexLanguageVersion:
		return nil, netErr(http.StatusBadRequest, msiCodeBad,
			"properties.claimsMatchingExpression.languageVersion must be %d.", msiFlexLanguageVersion)
	}

	if err := checkFlexExpression(value, issuer); err != nil {
		return nil, err
	}

	return &storedFlexClaims{Value: value, LanguageVersion: msiFlexLanguageVersion}, nil
}

func flexSupported(issuer string) (map[string][]string, []string, bool) {
	u, err := url.Parse(issuer)
	if err != nil {
		return nil, nil, false
	}

	host := strings.ToLower(u.Hostname())

	switch {
	case host == msiIssuerGitHub:
		return map[string][]string{
			msiSubClaim: {msiOpEq, msiOpMatches}, "job_workflow_ref": {msiOpEq, msiOpMatches},
			"repository_id": {msiOpEq}, "repository_owner_id": {msiOpEq},
		}, []string{"repository_id", "repository_owner_id"}, true
	case host == "gitlab.com" || strings.HasPrefix(host, "gitlab."):
		return map[string][]string{msiSubClaim: {msiOpEq, msiOpMatches}, "project_id": {msiOpEq}},
			[]string{"project_id"}, true
	case host == "app.terraform.io" || host == "app.eu.terraform.io":
		return map[string][]string{msiSubClaim: {msiOpEq, msiOpMatches}}, nil, true
	}

	return nil, nil, false
}

func checkFlexExpression(expr, issuer string) error {
	claims, immutable, ok := flexSupported(issuer)
	if !ok {
		return netErr(http.StatusBadRequest, msiCodeBad,
			"claimsMatchingExpression is supported only for GitHub, GitLab and Terraform Cloud issuers.")
	}

	seen := map[string]bool{}

	for term := range strings.SplitSeq(expr, " and ") {
		g := msiFlexTermRe.FindStringSubmatch(term)
		if g == nil {
			return netErr(http.StatusBadRequest, msiCodeBad,
				"The claimsMatchingExpression term '%s' is not of the form claims['<name>'] <matches|eq> '<value>'.",
				term)
		}

		if !slices.Contains(claims[g[1]], g[2]) {
			return netErr(http.StatusBadRequest, msiCodeBad,
				"The claim '%s' does not support the operator '%s' for this issuer.", g[1], g[2])
		}

		seen[g[1]] = true
	}

	if !seen[msiSubClaim] ||
		(len(immutable) > 0 && !slices.ContainsFunc(immutable, func(c string) bool { return seen[c] })) {
		return netErr(http.StatusBadRequest, msiCodeBad,
			"The claimsMatchingExpression must match the 'sub' claim%s.", immutableHint(immutable))
	}

	return nil
}

func immutableHint(immutable []string) string {
	if len(immutable) == 0 {
		return ""
	}

	return " and at least one of " + strings.Join(immutable, ", ")
}

// PatchResource implements the identity Update operation: tags and the mutable properties (isolationScope,
// assignmentRestrictions) that the request carries.
func (p *ManagedIdentityProvider) PatchResource(id ResourceID, body map[string]any) (map[string]any, error) {
	if ident, _ := msiKind(id); !ident {
		return nil, ErrResourceNotFound
	}

	props := propsOf(body)
	_, hasScope := props["isolationScope"]
	_, hasRestr := props["assignmentRestrictions"]

	scope, err := msiIsolationScope(props)
	if err != nil {
		return nil, err
	}

	restrictions, err := msiRestrictions(props)
	if err != nil {
		return nil, err
	}

	p.mu.Lock("PatchResource")
	defer p.mu.Unlock()

	i, ok := p.identities[msiKey(id.SubscriptionID, id.ResourceGroup, id.Names[0])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	if _, set := body[fieldTags]; set {
		i.Tags = stringTags(body[fieldTags])
	}

	if hasScope {
		i.IsolationScope = scope
	}

	if hasRestr {
		i.Restrictions = restrictions
	}

	return p.identityBody(id, i), nil
}

func parseFederated(name string, props map[string]any) (*storedFederated, error) {
	f := &storedFederated{
		Name:      name,
		Issuer:    strOf(props, "issuer"),
		Subject:   strOf(props, "subject"),
		Audiences: strList(props["audiences"]),
	}

	if u, err := url.Parse(f.Issuer); err != nil || u.Scheme != acaSchemeHTTPS || u.Host == "" {
		return nil, netErr(http.StatusBadRequest, msiCodeBad, "properties.issuer must be an https URL.")
	}

	claims, err := flexClaims(props, f.Issuer)
	if err != nil {
		return nil, err
	}

	f.Claims = claims

	switch {
	case claims != nil && f.Subject != "":
		return nil, netErr(http.StatusBadRequest, msiCodeBad,
			"properties.subject and properties.claimsMatchingExpression cannot both be set.")
	case claims == nil && f.Subject == "":
		return nil, netErr(http.StatusBadRequest, msiCodeBad,
			"Either properties.subject or properties.claimsMatchingExpression is required.")
	case len(f.Audiences) != 1:
		return nil, netErr(http.StatusBadRequest, msiCodeBad, "properties.audiences must contain exactly one value.")
	}

	return f, nil
}

func checkFederatedDuplicate(parent *storedIdentity, f *storedFederated) error {
	for _, o := range parent.Federated {
		if !strings.EqualFold(o.Name, f.Name) && o.Issuer == f.Issuer && o.Subject == f.Subject &&
			(o.Claims == nil) == (f.Claims == nil) && (f.Claims == nil || o.Claims.Value == f.Claims.Value) {
			return netErr(http.StatusConflict, "Conflict",
				"A federated identity credential with the same issuer and subject already exists on this identity.")
		}
	}

	return nil
}
