package verifiedpermissions

import (
	"context"
	"fmt"
	"strings"
)

// cedarVersion is the Cedar language version gopherstack's cedar-go
// evaluation engine implements (see GetPolicyStoreOutput.CedarVersion /
// Amazon Verified Permissions' Cedar v4 FAQ). Always CEDAR_4: gopherstack
// has no legacy CEDAR_2 policy stores to distinguish.
const cedarVersion = "CEDAR_4"

type validationSettingsJSON struct {
	Mode string `json:"mode"`
}

type kmsEncryptionJSON struct {
	EncryptionContext map[string]string `json:"encryptionContext,omitempty"`
	Key               string            `json:"key"`
}

type unitJSON struct{}

// encryptionSettingsJSON is the EncryptionSettings union (serializers.go:2509).
type encryptionSettingsJSON struct {
	Default               *unitJSON          `json:"default,omitempty"`
	KMSEncryptionSettings *kmsEncryptionJSON `json:"kmsEncryptionSettings,omitempty"`
}

// encryptionStateJSON is the EncryptionState union (deserializers.go:5955).
type encryptionStateJSON struct {
	Default            *unitJSON          `json:"default,omitempty"`
	KMSEncryptionState *kmsEncryptionJSON `json:"kmsEncryptionState,omitempty"`
}

func newEncryptionState(ps *PolicyStore) *encryptionStateJSON {
	if ps.KMSKeyArn == "" {
		return &encryptionStateJSON{Default: &unitJSON{}}
	}

	ctx := ps.KMSEncryptionCtx
	if ctx == nil {
		ctx = map[string]string{}
	}

	return &encryptionStateJSON{KMSEncryptionState: &kmsEncryptionJSON{Key: ps.KMSKeyArn, EncryptionContext: ctx}}
}

func parseEncryptionSettings(in *encryptionSettingsJSON) (*PolicyStoreEncryption, error) {
	switch {
	case in == nil:
		return nil, nil //nolint:nilnil // no encryption settings supplied
	case in.Default != nil && in.KMSEncryptionSettings != nil:
		return nil, fmt.Errorf("%w: encryptionSettings must set exactly one member", errInvalidRequest)
	case in.KMSEncryptionSettings == nil:
		return nil, nil //nolint:nilnil // default (AWS owned key)
	case in.KMSEncryptionSettings.Key == "":
		return nil, fmt.Errorf("%w: encryptionSettings.kmsEncryptionSettings.key is required", errInvalidRequest)
	}

	return &PolicyStoreEncryption{
		Key:     in.KMSEncryptionSettings.Key,
		Context: in.KMSEncryptionSettings.EncryptionContext,
	}, nil
}

type createPolicyStoreInput struct {
	EncryptionSettings *encryptionSettingsJSON `json:"encryptionSettings,omitempty"`
	Tags               map[string]string       `json:"tags"`
	Description        string                  `json:"description"`
	ValidationSettings validationSettingsJSON  `json:"validationSettings"`
	DeletionProtection string                  `json:"deletionProtection,omitempty"`
	ClientToken        string                  `json:"clientToken,omitempty"`
}

// createPolicyStoreOutput mirrors the real SDK's CreatePolicyStoreOutput:
// unlike GetPolicyStoreOutput, it does NOT echo validationSettings.
type createPolicyStoreOutput struct {
	PolicyStoreID   string `json:"policyStoreId"`
	Arn             string `json:"arn"`
	CreatedDate     string `json:"createdDate"`
	LastUpdatedDate string `json:"lastUpdatedDate"`
}

func (h *Handler) handleCreatePolicyStore(
	_ context.Context,
	in *createPolicyStoreInput,
) (*createPolicyStoreOutput, error) {
	if in.ValidationSettings.Mode == "" {
		return nil, fmt.Errorf("%w: validationSettings.mode is required", errInvalidRequest)
	}

	if in.ValidationSettings.Mode != ValidationModeOff && in.ValidationSettings.Mode != ValidationModeStrict {
		return nil, fmt.Errorf(
			"%w: validationSettings.mode must be %q or %q",
			errInvalidRequest, ValidationModeOff, ValidationModeStrict,
		)
	}

	// AWS bounds PolicyStoreDescription at 150 characters.
	if len(in.Description) > maxPolicyStoreDescriptionLen {
		return nil, fmt.Errorf(
			"%w: description must be %d characters or fewer",
			errInvalidRequest, maxPolicyStoreDescriptionLen,
		)
	}

	enc, err := parseEncryptionSettings(in.EncryptionSettings)
	if err != nil {
		return nil, err
	}

	ps, err := h.Backend.CreatePolicyStoreEncrypted(
		in.Description, in.Tags,
		in.ValidationSettings.Mode, in.DeletionProtection, in.ClientToken, enc,
	)
	if err != nil {
		return nil, err
	}

	return &createPolicyStoreOutput{
		PolicyStoreID:   ps.PolicyStoreID,
		Arn:             ps.Arn,
		CreatedDate:     ps.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate: ps.LastUpdated.UTC().Format(timeFormat),
	}, nil
}

type policyStoreIDInput struct {
	PolicyStoreID string `json:"policyStoreId"`
}

// policyStoreView mirrors the real SDK's PolicyStoreItem (ListPolicyStores):
// a leaner shape than GetPolicyStoreOutput -- no validationSettings,
// deletionProtection, or cedarVersion.
type policyStoreView struct {
	PolicyStoreID   string `json:"policyStoreId"`
	Arn             string `json:"arn"`
	Description     string `json:"description"`
	CreatedDate     string `json:"createdDate"`
	LastUpdatedDate string `json:"lastUpdatedDate"`
}

// getPolicyStoreOutput mirrors the real SDK's GetPolicyStoreOutput.
type getPolicyStoreOutput struct {
	EncryptionState    *encryptionStateJSON   `json:"encryptionState,omitempty"`
	Tags               map[string]string      `json:"tags,omitempty"`
	PolicyStoreID      string                 `json:"policyStoreId"`
	Arn                string                 `json:"arn"`
	Description        string                 `json:"description"`
	CreatedDate        string                 `json:"createdDate"`
	LastUpdatedDate    string                 `json:"lastUpdatedDate"`
	ValidationSettings validationSettingsJSON `json:"validationSettings"`
	CedarVersion       string                 `json:"cedarVersion,omitempty"`
	DeletionProtection string                 `json:"deletionProtection,omitempty"`
}

type getPolicyStoreInput struct {
	PolicyStoreID string `json:"policyStoreId"`
	Tags          bool   `json:"tags,omitempty"`
}

func (h *Handler) handleGetPolicyStore(_ context.Context, in *getPolicyStoreInput) (*getPolicyStoreOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	resolvedID, err := h.resolvePolicyStoreID(in.PolicyStoreID)
	if err != nil {
		return nil, err
	}

	ps, err := h.Backend.GetPolicyStore(resolvedID)
	if err != nil {
		return nil, err
	}

	var tags map[string]string
	if in.Tags {
		tags = ps.Tags
	}

	return &getPolicyStoreOutput{
		PolicyStoreID:      ps.PolicyStoreID,
		Arn:                ps.Arn,
		Description:        ps.Description,
		CreatedDate:        ps.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate:    ps.LastUpdated.UTC().Format(timeFormat),
		ValidationSettings: validationSettingsJSON{Mode: ps.ValidationMode},
		CedarVersion:       cedarVersion,
		DeletionProtection: ps.DeletionProtection,
		EncryptionState:    newEncryptionState(ps),
		Tags:               tags,
	}, nil
}

type listPolicyStoresInput struct {
	NextToken  string `json:"nextToken,omitempty"`
	MaxResults int    `json:"maxResults,omitempty"`
}

type listPolicyStoresOutput struct {
	NextToken    string            `json:"nextToken,omitempty"`
	PolicyStores []policyStoreView `json:"policyStores"`
}

func (h *Handler) handleListPolicyStores(
	_ context.Context,
	in *listPolicyStoresInput,
) (*listPolicyStoresOutput, error) {
	maxResults := in.MaxResults
	if maxResults <= 0 {
		maxResults = defaultListPageSize
	}

	stores, nextToken := h.Backend.ListPolicyStores(in.NextToken, maxResults)
	items := make([]policyStoreView, 0, len(stores))

	for i := range stores {
		ps := &stores[i]
		items = append(items, policyStoreView{
			PolicyStoreID:   ps.PolicyStoreID,
			Arn:             ps.Arn,
			Description:     ps.Description,
			CreatedDate:     ps.CreatedDate.UTC().Format(timeFormat),
			LastUpdatedDate: ps.LastUpdated.UTC().Format(timeFormat),
		})
	}

	return &listPolicyStoresOutput{PolicyStores: items, NextToken: nextToken}, nil
}

type updatePolicyStoreInput struct {
	PolicyStoreID      string                  `json:"policyStoreId"`
	Description        string                  `json:"description"`
	ValidationSettings *validationSettingsJSON `json:"validationSettings,omitempty"`
	DeletionProtection string                  `json:"deletionProtection,omitempty"`
}

// updatePolicyStoreOutput mirrors the real SDK's UpdatePolicyStoreOutput:
// unlike CreatePolicyStoreOutput's sibling shape, it requires CreatedDate
// too (since the store already existed), and -- like CreatePolicyStoreOutput
// -- does NOT echo validationSettings.
type updatePolicyStoreOutput struct {
	PolicyStoreID   string `json:"policyStoreId"`
	Arn             string `json:"arn"`
	CreatedDate     string `json:"createdDate"`
	LastUpdatedDate string `json:"lastUpdatedDate"`
}

func (h *Handler) handleUpdatePolicyStore(
	_ context.Context,
	in *updatePolicyStoreInput,
) (*updatePolicyStoreOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	resolvedID, err := h.resolvePolicyStoreID(in.PolicyStoreID)
	if err != nil {
		return nil, err
	}

	var validationMode string

	if in.ValidationSettings != nil {
		validationMode = in.ValidationSettings.Mode
	}

	ps, err := h.Backend.UpdatePolicyStore(resolvedID, in.Description, validationMode, in.DeletionProtection)
	if err != nil {
		return nil, err
	}

	return &updatePolicyStoreOutput{
		PolicyStoreID:   ps.PolicyStoreID,
		Arn:             ps.Arn,
		CreatedDate:     ps.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate: ps.LastUpdated.UTC().Format(timeFormat),
	}, nil
}

// handleDeletePolicyStore does not resolve policyStoreId through
// resolvePolicyStoreID: the real SDK's DeletePolicyStoreInput.PolicyStoreId
// doc is explicit that this operation is the exception to the usual
// ID-or-alias rule -- "the alias name cannot be used. Only the ID can be
// used." An alias-shaped value is rejected outright, distinct from
// DeletePolicyStore's own idempotent-on-missing-ID behavior.
func (h *Handler) handleDeletePolicyStore(_ context.Context, in *policyStoreIDInput) (*struct{}, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if strings.HasPrefix(in.PolicyStoreID, policyStoreAliasPrefix) {
		return nil, fmt.Errorf("%w: policyStoreId must be a policy store ID, not an alias name", errInvalidRequest)
	}

	if err := h.Backend.DeletePolicyStore(in.PolicyStoreID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}
