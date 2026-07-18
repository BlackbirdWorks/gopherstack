package verifiedpermissions

import (
	"context"
	"fmt"
)

type staticPolicyDefinition struct {
	Statement   string `json:"statement"`
	Description string `json:"description,omitempty"`
}

type templateLinkedPolicyDefinition struct {
	Principal        *entityIdentifier `json:"principal,omitempty"`
	Resource         *entityIdentifier `json:"resource,omitempty"`
	PolicyTemplateID string            `json:"policyTemplateId"`
}

type entityIdentifier struct {
	EntityType string `json:"entityType"`
	EntityID   string `json:"entityId"`
}

type policyDefinitionIn struct {
	Static         *staticPolicyDefinition         `json:"static,omitempty"`
	TemplateLinked *templateLinkedPolicyDefinition `json:"templateLinked,omitempty"`
}

type staticPolicyDefinitionOut struct {
	Statement   string `json:"statement"`
	Description string `json:"description,omitempty"`
}

type templateLinkedPolicyDefinitionOut struct {
	Principal        *entityIdentifier `json:"principal,omitempty"`
	Resource         *entityIdentifier `json:"resource,omitempty"`
	PolicyTemplateID string            `json:"policyTemplateId"`
}

type policyDefinitionOut struct {
	Static         *staticPolicyDefinitionOut         `json:"static,omitempty"`
	TemplateLinked *templateLinkedPolicyDefinitionOut `json:"templateLinked,omitempty"`
}

type createPolicyInput struct {
	Definition    policyDefinitionIn `json:"definition"`
	PolicyStoreID string             `json:"policyStoreId"`
}

type policyIDsOutput struct {
	PolicyStoreID   string `json:"policyStoreId"`
	PolicyID        string `json:"policyId"`
	PolicyType      string `json:"policyType"`
	CreatedDate     string `json:"createdDate"`
	LastUpdatedDate string `json:"lastUpdatedDate"`
}

//nolint:nestif // definition union type dispatch
func (h *Handler) handleCreatePolicy(_ context.Context, in *createPolicyInput) (*policyIDsOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if (in.Definition.Static == nil) == (in.Definition.TemplateLinked == nil) {
		return nil, fmt.Errorf("%w: definition must contain exactly one of static or templateLinked", errInvalidRequest)
	}

	var params CreatePolicyParams

	if in.Definition.Static != nil {
		if in.Definition.Static.Statement == "" {
			return nil, fmt.Errorf("%w: definition.static.statement is required", errInvalidRequest)
		}

		params.PolicyType = policyTypeStatic
		params.Statement = in.Definition.Static.Statement
		params.Description = in.Definition.Static.Description
	} else {
		tl := in.Definition.TemplateLinked
		if tl.PolicyTemplateID == "" {
			return nil, fmt.Errorf("%w: definition.templateLinked.policyTemplateId is required", errInvalidRequest)
		}

		params.PolicyType = policyTypeTemplateLinked
		params.PolicyTemplateID = tl.PolicyTemplateID

		if tl.Principal != nil {
			params.PrincipalEntityType = tl.Principal.EntityType
			params.PrincipalEntityID = tl.Principal.EntityID
		}

		if tl.Resource != nil {
			params.ResourceEntityType = tl.Resource.EntityType
			params.ResourceEntityID = tl.Resource.EntityID
		}
	}

	p, err := h.Backend.CreatePolicy(in.PolicyStoreID, params)
	if err != nil {
		return nil, err
	}

	return &policyIDsOutput{
		PolicyStoreID:   p.PolicyStoreID,
		PolicyID:        p.PolicyID,
		PolicyType:      p.PolicyType,
		CreatedDate:     p.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate: p.LastUpdated.UTC().Format(timeFormat),
	}, nil
}

type policyInput struct {
	PolicyStoreID string `json:"policyStoreId"`
	PolicyID      string `json:"policyId"`
}

type policyView struct {
	PolicyStoreID   string              `json:"policyStoreId"`
	PolicyID        string              `json:"policyId"`
	PolicyType      string              `json:"policyType"`
	Definition      policyDefinitionOut `json:"definition"`
	Principal       *entityIdentifier   `json:"principal,omitempty"`
	Resource        *entityIdentifier   `json:"resource,omitempty"`
	CreatedDate     string              `json:"createdDate"`
	LastUpdatedDate string              `json:"lastUpdatedDate"`
}

type getPolicyOutput struct {
	PolicyStoreID   string              `json:"policyStoreId"`
	PolicyID        string              `json:"policyId"`
	PolicyType      string              `json:"policyType"`
	Definition      policyDefinitionOut `json:"definition"`
	Principal       *entityIdentifier   `json:"principal,omitempty"`
	Resource        *entityIdentifier   `json:"resource,omitempty"`
	CreatedDate     string              `json:"createdDate"`
	LastUpdatedDate string              `json:"lastUpdatedDate"`
}

func policyToView(p *Policy) policyView {
	v := policyView{
		PolicyStoreID:   p.PolicyStoreID,
		PolicyID:        p.PolicyID,
		PolicyType:      p.PolicyType,
		CreatedDate:     p.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate: p.LastUpdated.UTC().Format(timeFormat),
	}

	switch p.PolicyType {
	case policyTypeStatic:
		v.Definition.Static = &staticPolicyDefinitionOut{
			Statement:   p.Statement,
			Description: p.Description,
		}
	case policyTypeTemplateLinked:
		v.Definition.TemplateLinked = &templateLinkedPolicyDefinitionOut{
			PolicyTemplateID: p.PolicyTemplateID,
		}

		if p.PrincipalEntityType != "" {
			v.Definition.TemplateLinked.Principal = &entityIdentifier{
				EntityType: p.PrincipalEntityType,
				EntityID:   p.PrincipalEntityID,
			}
			v.Principal = &entityIdentifier{
				EntityType: p.PrincipalEntityType,
				EntityID:   p.PrincipalEntityID,
			}
		}

		if p.ResourceEntityType != "" {
			v.Definition.TemplateLinked.Resource = &entityIdentifier{
				EntityType: p.ResourceEntityType,
				EntityID:   p.ResourceEntityID,
			}
			v.Resource = &entityIdentifier{
				EntityType: p.ResourceEntityType,
				EntityID:   p.ResourceEntityID,
			}
		}
	}

	return v
}

func (h *Handler) handleGetPolicy(_ context.Context, in *policyInput) (*getPolicyOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.PolicyID == "" {
		return nil, fmt.Errorf("%w: policyId is required", errInvalidRequest)
	}

	p, err := h.Backend.GetPolicy(in.PolicyStoreID, in.PolicyID)
	if err != nil {
		return nil, err
	}

	v := policyToView(p)

	return &getPolicyOutput{
		PolicyStoreID:   v.PolicyStoreID,
		PolicyID:        v.PolicyID,
		PolicyType:      v.PolicyType,
		Definition:      v.Definition,
		Principal:       v.Principal,
		Resource:        v.Resource,
		CreatedDate:     v.CreatedDate,
		LastUpdatedDate: v.LastUpdatedDate,
	}, nil
}

// entityReferenceJSON mirrors the real SDK's EntityReference union: a
// PolicyFilter's principal/resource is NOT a flat entityIdentifier, it's
// wrapped as {"identifier": {entityType, entityId}} or {"unspecified": true}
// (unlike GetPolicy's top-level principal/resource, which ARE flat).
type entityReferenceJSON struct {
	Identifier  *entityIdentifier `json:"identifier,omitempty"`
	Unspecified *bool             `json:"unspecified,omitempty"`
}

type listPoliciesFilterJSON struct {
	Principal                 *entityReferenceJSON `json:"principal,omitempty"`
	Resource                  *entityReferenceJSON `json:"resource,omitempty"`
	PolicyType                string               `json:"policyType,omitempty"`
	PolicyTemplateIDForFilter string               `json:"policyTemplateId,omitempty"`
}

type listPoliciesInput struct {
	PolicyStoreID string                  `json:"policyStoreId"`
	Filter        *listPoliciesFilterJSON `json:"filter,omitempty"`
	NextToken     string                  `json:"nextToken,omitempty"`
	MaxResults    int                     `json:"maxResults,omitempty"`
}

type listPoliciesOutput struct {
	NextToken string       `json:"nextToken,omitempty"`
	Policies  []policyView `json:"policies"`
}

// resolvedEntityReference holds the fields extracted from an EntityReference
// union (see entityReferenceJSON).
type resolvedEntityReference struct {
	entityType  string
	entityID    string
	unspecified bool
}

// resolveEntityReference extracts entityType/entityId/unspecified from an
// EntityReference union. unspecified is true only when the wire sent
// {"unspecified": true}.
func resolveEntityReference(ref *entityReferenceJSON) resolvedEntityReference {
	if ref == nil {
		return resolvedEntityReference{}
	}

	if ref.Identifier != nil {
		return resolvedEntityReference{entityType: ref.Identifier.EntityType, entityID: ref.Identifier.EntityID}
	}

	return resolvedEntityReference{unspecified: ref.Unspecified != nil && *ref.Unspecified}
}

// buildListPoliciesFilter converts the wire filter (nil-safe) into the
// backend's ListPoliciesFilter.
func buildListPoliciesFilter(in *listPoliciesFilterJSON) ListPoliciesFilter {
	if in == nil {
		return ListPoliciesFilter{}
	}

	filter := ListPoliciesFilter{
		PolicyType:       in.PolicyType,
		PolicyTemplateID: in.PolicyTemplateIDForFilter,
	}

	principal := resolveEntityReference(in.Principal)
	filter.PrincipalEntityType = principal.entityType
	filter.PrincipalEntityID = principal.entityID
	filter.PrincipalUnspecified = principal.unspecified

	resource := resolveEntityReference(in.Resource)
	filter.ResourceEntityType = resource.entityType
	filter.ResourceEntityID = resource.entityID
	filter.ResourceUnspecified = resource.unspecified

	return filter
}

func (h *Handler) handleListPolicies(_ context.Context, in *listPoliciesInput) (*listPoliciesOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	filter := buildListPoliciesFilter(in.Filter)

	policies, nextToken, err := h.Backend.ListPolicies(in.PolicyStoreID, filter, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	items := make([]policyView, 0, len(policies))

	for i := range policies {
		items = append(items, policyToView(&policies[i]))
	}

	return &listPoliciesOutput{Policies: items, NextToken: nextToken}, nil
}

type updatePolicyInput struct {
	Definition    policyDefinitionIn `json:"definition"`
	PolicyStoreID string             `json:"policyStoreId"`
	PolicyID      string             `json:"policyId"`
}

func (h *Handler) handleUpdatePolicy(_ context.Context, in *updatePolicyInput) (*policyIDsOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.PolicyID == "" {
		return nil, fmt.Errorf("%w: policyId is required", errInvalidRequest)
	}

	if (in.Definition.Static == nil) == (in.Definition.TemplateLinked == nil) {
		return nil, fmt.Errorf("%w: definition must contain exactly one of static or templateLinked", errInvalidRequest)
	}

	var params UpdatePolicyParams

	if in.Definition.Static != nil {
		if in.Definition.Static.Statement == "" {
			return nil, fmt.Errorf("%w: definition.static.statement is required", errInvalidRequest)
		}

		params.Statement = in.Definition.Static.Statement
		params.Description = in.Definition.Static.Description
	} else {
		tl := in.Definition.TemplateLinked
		if tl.Principal != nil {
			params.PrincipalEntityType = tl.Principal.EntityType
			params.PrincipalEntityID = tl.Principal.EntityID
		}

		if tl.Resource != nil {
			params.ResourceEntityType = tl.Resource.EntityType
			params.ResourceEntityID = tl.Resource.EntityID
		}
	}

	p, err := h.Backend.UpdatePolicy(in.PolicyStoreID, in.PolicyID, params)
	if err != nil {
		return nil, err
	}

	return &policyIDsOutput{
		PolicyStoreID:   p.PolicyStoreID,
		PolicyID:        p.PolicyID,
		PolicyType:      p.PolicyType,
		CreatedDate:     p.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate: p.LastUpdated.UTC().Format(timeFormat),
	}, nil
}

func (h *Handler) handleDeletePolicy(_ context.Context, in *policyInput) (*struct{}, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.PolicyID == "" {
		return nil, fmt.Errorf("%w: policyId is required", errInvalidRequest)
	}

	if err := h.Backend.DeletePolicy(in.PolicyStoreID, in.PolicyID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type batchGetPolicyRequest struct {
	Requests []struct {
		PolicyStoreID string `json:"policyStoreId"`
		PolicyID      string `json:"policyId"`
	} `json:"requests"`
}

type batchGetPolicyItemOut struct {
	Definition      policyDefinitionOut `json:"definition"`
	PolicyStoreID   string              `json:"policyStoreId"`
	PolicyID        string              `json:"policyId"`
	PolicyType      string              `json:"policyType"`
	CreatedDate     string              `json:"createdDate"`
	LastUpdatedDate string              `json:"lastUpdatedDate"`
}

type batchGetPolicyHandlerOutput struct {
	Results []batchGetPolicyItemOut   `json:"results"`
	Errors  []batchGetPolicyErrorItem `json:"errors"`
}

func (h *Handler) handleBatchGetPolicy(
	_ context.Context,
	in *batchGetPolicyRequest,
) (*batchGetPolicyHandlerOutput, error) {
	items := make([]BatchGetPolicyItem, 0, len(in.Requests))

	for _, r := range in.Requests {
		if r.PolicyStoreID == "" || r.PolicyID == "" {
			return nil, fmt.Errorf("%w: each request requires policyStoreId and policyId", errInvalidRequest)
		}

		items = append(items, BatchGetPolicyItem{
			PolicyStoreID: r.PolicyStoreID,
			PolicyID:      r.PolicyID,
		})
	}

	result := h.Backend.BatchGetPolicy(items)

	out := make([]batchGetPolicyItemOut, 0, len(result.Results))

	for i := range result.Results {
		v := policyToView(&result.Results[i])
		out = append(out, batchGetPolicyItemOut{
			Definition:      v.Definition,
			PolicyStoreID:   v.PolicyStoreID,
			PolicyID:        v.PolicyID,
			PolicyType:      v.PolicyType,
			CreatedDate:     v.CreatedDate,
			LastUpdatedDate: v.LastUpdatedDate,
		})
	}

	return &batchGetPolicyHandlerOutput{
		Results: out,
		Errors:  result.Errors,
	}, nil
}
