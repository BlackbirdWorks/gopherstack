package verifiedpermissions

import (
	"encoding/json"
	"regexp"

	cedar "github.com/cedar-policy/cedar-go"
)

// cedarScopeEntity mirrors the "entity" object nested inside a Cedar JSON
// policy scope clause (https://docs.cedarpolicy.com/policies/json-format.html):
// {"type": "...", "id": "..."}.
type cedarScopeEntity struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

// cedarScopeJSON mirrors one of a Cedar policy's principal/action/resource
// scope clauses in the Cedar JSON policy format, as emitted by cedar-go's
// Policy.MarshalJSON. Op is one of "All", "==", "in", or "is".
type cedarScopeJSON struct {
	Entity     *cedarScopeEntity  `json:"entity,omitempty"`
	Op         string             `json:"op"`
	EntityType string             `json:"entity_type,omitempty"`
	Entities   []cedarScopeEntity `json:"entities,omitempty"`
}

// cedarPolicyJSON is the subset of the Cedar JSON policy format needed to
// derive the effect/principal/action/resource fields Verified Permissions
// echoes at the top level of CreatePolicy/UpdatePolicy/GetPolicy/ListPolicies
// responses.
type cedarPolicyJSON struct {
	Effect    string         `json:"effect"`
	Principal cedarScopeJSON `json:"principal"`
	Action    cedarScopeJSON `json:"action"`
	Resource  cedarScopeJSON `json:"resource"`
}

// PolicyScopeResult holds the scope-derived fields the real SDK echoes at the
// top level of policy responses: effect, actions, principal, resource. A nil
// *PolicyScopeResult means the statement could not be resolved or parsed (e.g.
// a dangling template reference); callers should omit these fields, same as
// AWS omits them "when [the value] isn't present in the policy content".
type PolicyScopeResult struct {
	Principal *entityIdentifier
	Resource  *entityIdentifier
	Effect    string
	Actions   []actionIdentifierJSON
}

// templatePrincipalSlot and templateResourceSlot match the ?principal / ?resource
// placeholder tokens in a Cedar policy template's scope clause.
var (
	templatePrincipalSlot = regexp.MustCompile(`\?principal\b`)
	templateResourceSlot  = regexp.MustCompile(`\?resource\b`)
)

// instantiateTemplate substitutes the ?principal / ?resource slots in a
// policy template's Cedar statement with the concrete principal/resource
// entity literals bound on a TEMPLATE_LINKED policy, producing a complete,
// parseable Cedar statement. Slots left unbound (no PrincipalEntityType /
// ResourceEntityType on p) are left as-is, which will fail to parse -- the
// same outcome as a template reference that doesn't have all its slots
// filled in.
func instantiateTemplate(templateStatement string, p *Policy) string {
	out := templateStatement

	if p.PrincipalEntityType != "" {
		lit := cedarEntityLiteral(p.PrincipalEntityType, p.PrincipalEntityID)
		out = templatePrincipalSlot.ReplaceAllString(out, lit)
	}

	if p.ResourceEntityType != "" {
		lit := cedarEntityLiteral(p.ResourceEntityType, p.ResourceEntityID)
		out = templateResourceSlot.ReplaceAllString(out, lit)
	}

	return out
}

// cedarEntityLiteral renders entityType::"entityId" as a Cedar entity UID literal.
func cedarEntityLiteral(entityType, entityID string) string {
	b, _ := json.Marshal(entityID) // Cedar string-literal escaping matches JSON's for plain ASCII IDs.

	return entityType + "::" + string(b)
}

// parseCedarScope parses a single, fully-concrete (no template slots) Cedar
// policy statement and extracts its effect/principal/action/resource scope.
// Returns nil if the statement is empty, fails to parse, or contains no
// policies.
func parseCedarScope(statement string) *PolicyScopeResult {
	if statement == "" {
		return nil
	}

	list, err := cedar.NewPolicyListFromBytes("policy.cedar", []byte(statement))
	if err != nil || len(list) == 0 {
		return nil
	}

	raw, err := list[0].MarshalJSON()
	if err != nil {
		return nil
	}

	var pj cedarPolicyJSON
	if unmarshalErr := json.Unmarshal(raw, &pj); unmarshalErr != nil {
		return nil
	}

	scope := &PolicyScopeResult{Effect: cedarEffectToWire(pj.Effect)}
	scope.Principal = scopeSingleEntity(pj.Principal)
	scope.Resource = scopeSingleEntity(pj.Resource)
	scope.Actions = scopeActions(pj.Action)

	return scope
}

// cedarEffectToWire maps cedar-go's lowercase JSON effect string ("permit"/
// "forbid") to the real SDK's PolicyEffect enum ("Permit"/"Forbid").
func cedarEffectToWire(effect string) string {
	switch effect {
	case "permit":
		return "Permit"
	case "forbid":
		return "Forbid"
	default:
		return ""
	}
}

// scopeSingleEntity extracts a single concrete EntityIdentifier from a
// principal/resource scope clause. Only "==" and "in" (single-entity) scopes
// name one specific entity; "is" (entity-type-only) and "All" (unconstrained)
// scopes don't, so those return nil -- matching AWS's documented behavior
// that the field "isn't included in the response when [it] isn't present in
// the policy content".
func scopeSingleEntity(s cedarScopeJSON) *entityIdentifier {
	if s.Entity == nil {
		return nil
	}

	switch s.Op {
	case "==", "in":
		return &entityIdentifier{EntityType: s.Entity.Type, EntityID: s.Entity.ID}
	default:
		return nil
	}
}

// scopeActions extracts the list of concrete ActionIdentifiers from an
// action scope clause. "All" (unconstrained) yields no actions.
func scopeActions(s cedarScopeJSON) []actionIdentifierJSON {
	switch s.Op {
	case "==":
		if s.Entity == nil {
			return nil
		}

		return []actionIdentifierJSON{{ActionType: s.Entity.Type, ActionID: s.Entity.ID}}
	case "in":
		if s.Entity != nil {
			return []actionIdentifierJSON{{ActionType: s.Entity.Type, ActionID: s.Entity.ID}}
		}

		out := make([]actionIdentifierJSON, 0, len(s.Entities))
		for _, e := range s.Entities {
			out = append(out, actionIdentifierJSON{ActionType: e.Type, ActionID: e.ID})
		}

		return out
	default:
		return nil
	}
}

// PolicyScope returns the effect/actions/principal/resource derived from p's
// effective Cedar statement: p's own statement for STATIC policies, or the
// referenced policy template's statement (with ?principal/?resource
// substituted) for TEMPLATE_LINKED policies. Returns nil if the statement
// can't be resolved (e.g. a dangling template reference) or parsed.
func (b *InMemoryBackend) PolicyScope(p *Policy) *PolicyScopeResult {
	b.mu.RLock("PolicyScope")
	statement := b.resolveStatementLocked(p)
	b.mu.RUnlock()

	return parseCedarScope(statement)
}

// resolveStatementLocked returns the effective Cedar statement text for p.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) resolveStatementLocked(p *Policy) string {
	if p.PolicyType == policyTypeStatic {
		return p.Statement
	}

	tmpl, ok := b.policyTemplates.Get(policyTemplateKey(p.PolicyStoreID, p.PolicyTemplateID))
	if !ok {
		return ""
	}

	return instantiateTemplate(tmpl.Statement, p)
}
