package verifiedpermissions

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	cedar "github.com/cedar-policy/cedar-go"
)

var errSchemaMismatch = errors.New("policy validation failed")

type cedarSchemaNamespace struct {
	EntityTypes map[string]json.RawMessage `json:"entityTypes"`
	Actions     map[string]json.RawMessage `json:"actions"`
}

// splitQualified splits "NS::Sub::Name" into its namespace and local name.
func splitQualified(name string) (string, string) {
	before, after, found := strings.CutLast(name, "::")
	if !found {
		return "", name
	}

	return before, after
}

func (b *InMemoryBackend) loadSchemaNamespacesLocked(policyStoreID string) (map[string]cedarSchemaNamespace, bool) {
	s, ok := b.schemas.Get(policyStoreID)
	if !ok {
		return nil, false
	}

	var ns map[string]cedarSchemaNamespace
	if err := json.Unmarshal([]byte(s.Schema), &ns); err != nil {
		return nil, true
	}

	return ns, true
}

// validateStaticPolicyLocked enforces the store's STRICT validation mode: a
// schema must exist, and the policy's principal/resource entity types and
// action ids must be declared in it. Conditions are not type-checked. Caller
// holds b.mu.
func (b *InMemoryBackend) validateStaticPolicyLocked(policyStoreID, statement string) error {
	ps, ok := b.policyStores.Get(policyStoreID)
	if !ok || ps.ValidationMode != ValidationModeStrict {
		return nil
	}

	namespaces, hasSchema := b.loadSchemaNamespacesLocked(policyStoreID)
	if !hasSchema {
		return fmt.Errorf("%w: policy validation failed: the policy store has no schema and validation mode is STRICT",
			ErrValidation)
	}

	if namespaces == nil {
		return nil
	}

	list, parseErr := cedar.NewPolicyListFromBytes("policy.cedar", []byte(statement))
	if parseErr != nil {
		return nil //nolint:nilerr // syntax errors are reported by parseCedarStatement
	}

	for _, p := range list {
		raw, mErr := p.MarshalJSON()
		if mErr != nil {
			continue
		}

		var pj cedarPolicyJSON
		if json.Unmarshal(raw, &pj) != nil {
			continue
		}

		if vErr := validateScopeAgainstSchema(namespaces, pj); vErr != nil {
			return fmt.Errorf("%w: %w", ErrValidation, vErr)
		}
	}

	return nil
}

func validateScopeAgainstSchema(namespaces map[string]cedarSchemaNamespace, pj cedarPolicyJSON) error {
	for _, scope := range []cedarScopeJSON{pj.Principal, pj.Resource} {
		var types []string
		if scope.Entity != nil {
			types = append(types, scope.Entity.Type)
		}

		if scope.EntityType != "" {
			types = append(types, scope.EntityType)
		}

		for _, t := range types {
			nsName, local := splitQualified(t)
			if _, ok := namespaces[nsName].EntityTypes[local]; !ok {
				return fmt.Errorf("%w: unrecognized entity type %s", errSchemaMismatch, t)
			}
		}
	}

	var actions []cedarScopeEntity
	if pj.Action.Entity != nil {
		actions = append(actions, *pj.Action.Entity)
	}

	actions = append(actions, pj.Action.Entities...)

	for _, a := range actions {
		nsName, _ := splitQualified(a.Type)
		if _, ok := namespaces[nsName].Actions[a.ID]; !ok {
			return fmt.Errorf("%w: unrecognized action %s::%q", errSchemaMismatch, a.Type, a.ID)
		}
	}

	return nil
}

const maxListPageSize = 50

// validateListParams rejects a malformed continuation token or an
// out-of-range maxResults (1-50).
func validateListParams(nextToken string, maxResults int) error {
	if maxResults < 0 || maxResults > maxListPageSize {
		return fmt.Errorf("%w: maxResults must be between 1 and %d", ErrValidation, maxListPageSize)
	}

	if nextToken != "" {
		if _, err := base64.StdEncoding.DecodeString(nextToken); err != nil {
			return fmt.Errorf("%w: invalid nextToken", ErrValidation)
		}
	}

	return nil
}
