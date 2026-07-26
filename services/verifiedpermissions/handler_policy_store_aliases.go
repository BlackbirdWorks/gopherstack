package verifiedpermissions

import (
	"context"
	"fmt"
	"strings"
)

// resolvePolicyStoreID resolves a policyStoreId request field that may be
// either a literal policy store ID or an alias name (prefixed
// "policy-store-alias/") to the real underlying policy store ID. Real AWS
// accepts either form in the policyStoreId field of nearly every operation
// that has one -- verified against the API reference for GetPolicyStore,
// IsAuthorized, CreatePolicy, DeletePolicy, and PutSchema, all of which carry
// the identical documented text ("To specify a policy store, use its ID or
// alias name. When using an alias name, prefix it with
// policy-store-alias/."). The two documented exceptions are
// CreatePolicyStoreAlias.policyStoreId and DeletePolicyStore.policyStoreId,
// both of which explicitly require the literal ID ("the alias name cannot be
// used") -- those two call sites must NOT call this helper.
func (h *Handler) resolvePolicyStoreID(id string) (string, error) {
	if !strings.HasPrefix(id, policyStoreAliasPrefix) {
		return id, nil
	}

	return h.Backend.ResolvePolicyStoreAlias(id)
}

// policyStoreAliasItemOut mirrors the real SDK's PolicyStoreAliasItem (used
// by GetPolicyStoreAlias and ListPolicyStoreAliases): unlike
// CreatePolicyStoreAliasOutput, it additionally carries State.
type policyStoreAliasItemOut struct {
	AliasArn      string `json:"aliasArn"`
	AliasName     string `json:"aliasName"`
	CreatedAt     string `json:"createdAt"`
	PolicyStoreID string `json:"policyStoreId"`
	State         string `json:"state"`
}

func policyStoreAliasToItem(a *PolicyStoreAlias) policyStoreAliasItemOut {
	return policyStoreAliasItemOut{
		AliasArn:      a.Arn,
		AliasName:     a.AliasName,
		PolicyStoreID: a.PolicyStoreID,
		State:         a.State,
		CreatedAt:     a.CreatedAt.UTC().Format(timeFormat),
	}
}

type createPolicyStoreAliasInput struct {
	AliasName     string `json:"aliasName"`
	PolicyStoreID string `json:"policyStoreId"`
}

// createPolicyStoreAliasOutput mirrors the real SDK's
// CreatePolicyStoreAliasOutput: no State field (a freshly created alias is
// always Active), unlike GetPolicyStoreAlias/ListPolicyStoreAliases' item shape.
type createPolicyStoreAliasOutput struct {
	AliasArn      string `json:"aliasArn"`
	AliasName     string `json:"aliasName"`
	CreatedAt     string `json:"createdAt"`
	PolicyStoreID string `json:"policyStoreId"`
}

// handleCreatePolicyStoreAlias implements CreatePolicyStoreAlias.
// policyStoreId here is deliberately NOT passed through
// resolvePolicyStoreID: the real API requires the literal policy store ID
// ("The alias name cannot be used") -- see InMemoryBackend.CreatePolicyStoreAlias.
func (h *Handler) handleCreatePolicyStoreAlias(
	_ context.Context,
	in *createPolicyStoreAliasInput,
) (*createPolicyStoreAliasOutput, error) {
	if in.AliasName == "" {
		return nil, fmt.Errorf("%w: aliasName is required", errInvalidRequest)
	}

	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	a, err := h.Backend.CreatePolicyStoreAlias(in.AliasName, in.PolicyStoreID)
	if err != nil {
		return nil, err
	}

	return &createPolicyStoreAliasOutput{
		AliasArn:      a.Arn,
		AliasName:     a.AliasName,
		PolicyStoreID: a.PolicyStoreID,
		CreatedAt:     a.CreatedAt.UTC().Format(timeFormat),
	}, nil
}

type getPolicyStoreAliasInput struct {
	AliasName string `json:"aliasName"`
}

func (h *Handler) handleGetPolicyStoreAlias(
	_ context.Context,
	in *getPolicyStoreAliasInput,
) (*policyStoreAliasItemOut, error) {
	if in.AliasName == "" {
		return nil, fmt.Errorf("%w: aliasName is required", errInvalidRequest)
	}

	a, err := h.Backend.GetPolicyStoreAlias(in.AliasName)
	if err != nil {
		return nil, err
	}

	out := policyStoreAliasToItem(a)

	return &out, nil
}

type policyStoreAliasFilterJSON struct {
	PolicyStoreID string `json:"policyStoreId,omitempty"`
}

type listPolicyStoreAliasesInput struct {
	Filter     *policyStoreAliasFilterJSON `json:"filter,omitempty"`
	NextToken  string                      `json:"nextToken,omitempty"`
	MaxResults int                         `json:"maxResults,omitempty"`
}

type listPolicyStoreAliasesOutput struct {
	NextToken          string                    `json:"nextToken,omitempty"`
	PolicyStoreAliases []policyStoreAliasItemOut `json:"policyStoreAliases"`
}

func (h *Handler) handleListPolicyStoreAliases(
	_ context.Context,
	in *listPolicyStoreAliasesInput,
) (*listPolicyStoreAliasesOutput, error) {
	var policyStoreID string
	if in.Filter != nil {
		policyStoreID = in.Filter.PolicyStoreID
	}

	aliases, nextToken := h.Backend.ListPolicyStoreAliases(policyStoreID, in.NextToken, in.MaxResults)

	items := make([]policyStoreAliasItemOut, 0, len(aliases))
	for i := range aliases {
		items = append(items, policyStoreAliasToItem(&aliases[i]))
	}

	return &listPolicyStoreAliasesOutput{PolicyStoreAliases: items, NextToken: nextToken}, nil
}

type deletePolicyStoreAliasInput struct {
	AliasName    string `json:"aliasName"`
	DeletionMode string `json:"deletionMode,omitempty"`
}

func (h *Handler) handleDeletePolicyStoreAlias(
	_ context.Context,
	in *deletePolicyStoreAliasInput,
) (*struct{}, error) {
	if in.AliasName == "" {
		return nil, fmt.Errorf("%w: aliasName is required", errInvalidRequest)
	}

	if in.DeletionMode != "" && in.DeletionMode != DeletionModeSoftDelete && in.DeletionMode != DeletionModeHardDelete {
		return nil, fmt.Errorf(
			"%w: deletionMode must be %q or %q", errInvalidRequest, DeletionModeSoftDelete, DeletionModeHardDelete,
		)
	}

	if err := h.Backend.DeletePolicyStoreAlias(in.AliasName, in.DeletionMode == DeletionModeHardDelete); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}
