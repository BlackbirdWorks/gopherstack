package route53resolver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	r53rtypes "github.com/aws/aws-sdk-go-v2/service/route53resolver/types"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// dnsThreatProtectionRuleTypeConfigInput mirrors
// types.DnsThreatProtectionRuleTypeConfig (Value+ConfidenceThreshold,
// verified against serializers.go
// awsAwsjson11_serializeDocumentDnsThreatProtectionRuleTypeConfig). Value is
// typed types.DnsThreatProtection in the SDK -- the identical enum the
// top-level DnsThreatProtection field already uses -- so it validates and
// stores through the same path.
type dnsThreatProtectionRuleTypeConfigInput struct {
	Value               string `json:"Value"`
	ConfidenceThreshold string `json:"ConfidenceThreshold"`
}

// firewallRuleTypeInput mirrors types.FirewallRuleType, a tagged union with
// four members (verified against types.go and serializers.go
// awsAwsjson11_serializeDocumentFirewallRuleType). Only DnsThreatProtection
// maps onto backend state this service models -- it is mutually exclusive
// with, and translates into, the same DNSThreatProtection/ConfidenceThreshold
// fields the top-level DnsThreatProtection field already populates. The
// other three members are decoded only far enough to detect their presence
// and reject with a clear error, per this service's no-invented-values rule:
// FirewallAdvancedContentCategoryConfig.Category,
// FirewallAdvancedThreatCategoryConfig.Category, and
// PartnerThreatProtectionConfig.Partner are all untyped *string in the SDK
// with no backing enum, and the only way to learn valid values is to call
// ListFirewallRuleTypes itself (see PARITY.md gaps).
type firewallRuleTypeInput struct {
	DNSThreatProtection *dnsThreatProtectionRuleTypeConfigInput `json:"DnsThreatProtection,omitempty"`

	FirewallAdvancedContentCategory json.RawMessage `json:"FirewallAdvancedContentCategory,omitempty"`
	FirewallAdvancedThreatCategory  json.RawMessage `json:"FirewallAdvancedThreatCategory,omitempty"`
	PartnerThreatProtection         json.RawMessage `json:"PartnerThreatProtection,omitempty"`
}

// resolveFirewallRuleType validates a FirewallRuleType tagged union (exactly
// one member set) and, for its DnsThreatProtection variant, returns the
// Value/ConfidenceThreshold to merge into the existing top-level
// DNSThreatProtection/ConfidenceThreshold backend fields. rt == nil is a
// no-op. The other three variants are always rejected -- see the type's doc
// comment.
func resolveFirewallRuleType(rt *firewallRuleTypeInput) (string, string, error) {
	if rt == nil {
		return "", "", nil
	}

	set := 0
	if rt.DNSThreatProtection != nil {
		set++
	}
	if len(rt.FirewallAdvancedContentCategory) > 0 {
		set++
	}
	if len(rt.FirewallAdvancedThreatCategory) > 0 {
		set++
	}
	if len(rt.PartnerThreatProtection) > 0 {
		set++
	}
	if set != 1 {
		return "", "", fmt.Errorf(
			"%w: FirewallRuleType requires exactly one member set",
			ErrBatchValidation,
		)
	}

	switch {
	case len(rt.FirewallAdvancedContentCategory) > 0:
		return "", "", fmt.Errorf(
			"%w: FirewallRuleType.FirewallAdvancedContentCategory is not supported by this backend",
			ErrBatchValidation,
		)
	case len(rt.FirewallAdvancedThreatCategory) > 0:
		return "", "", fmt.Errorf(
			"%w: FirewallRuleType.FirewallAdvancedThreatCategory is not supported by this backend",
			ErrBatchValidation,
		)
	case len(rt.PartnerThreatProtection) > 0:
		return "", "", fmt.Errorf(
			"%w: FirewallRuleType.PartnerThreatProtection is not supported by this backend",
			ErrBatchValidation,
		)
	default:
		return rt.DNSThreatProtection.Value, rt.DNSThreatProtection.ConfidenceThreshold, nil
	}
}

// mergeFirewallRuleTypeDnsThreatProtection merges FirewallRuleType's
// DnsThreatProtection variant into the top-level dnsThreatProtection/
// confidenceThreshold values, enforcing the documented mutual exclusivity
// with the top-level FirewallDomainListId/DnsThreatProtection fields
// (verified: "This shape is mutually exclusive with the top-level
// FirewallDomainListId and DnsThreatProtection fields", types.go).
func mergeFirewallRuleTypeDNSThreatProtection(
	rt *firewallRuleTypeInput, firewallDomainListID, dnsThreatProtection, confidenceThreshold string,
) (string, string, error) {
	value, confidence, err := resolveFirewallRuleType(rt)
	if err != nil {
		return "", "", err
	}
	if value == "" {
		return dnsThreatProtection, confidenceThreshold, nil
	}
	if firewallDomainListID != "" || dnsThreatProtection != "" {
		return "", "", fmt.Errorf(
			"%w: FirewallRuleType is mutually exclusive with FirewallDomainListId and DnsThreatProtection",
			ErrBatchValidation,
		)
	}

	return value, confidence, nil
}

// firewallRuleOutput is the wire shape of types.FirewallRule. Note: the real
// SDK type has no Id or Arn member -- a firewall rule has no independent
// identity on the wire, it is addressed by the (FirewallRuleGroupId,
// FirewallDomainListId) pair (verified against types.FirewallRule and
// api_op_{Update,Delete,List}FirewallRule.go, none of which have an
// Id/Arn/FirewallRuleId member). An earlier revision of this handler invented
// Id/Arn fields here; they have been removed (see firewall_rules_test.go and
// PARITY.md for the fix note).
//
// BlockOverrideDnsType/BlockOverrideTtl json tags: verified against the real
// SDK's hand-rolled awsjson1.1 deserializer (deserializers.go,
// awsAwsjson11_deserializeDocumentFirewallRule), which does exact-case
// map-key matching, not case-insensitive struct-tag matching (same bug class
// as the OwnerID/OwnerId note elsewhere in this package). The wire keys are
// "BlockOverrideDnsType" and "BlockOverrideTtl" -- NOT "BlockOverrideDNSType"
// / "BlockOverrideTTL". A prior revision used the wrong casing here; real SDK
// clients would have silently never seen these two fields populated.
type firewallRuleOutput struct {
	FirewallRuleType                *firewallRuleTypeOutput `json:"FirewallRuleType,omitempty"`
	ConfidenceThreshold             string                  `json:"ConfidenceThreshold,omitempty"`
	CreationTime                    string                  `json:"CreationTime,omitempty"`
	Action                          string                  `json:"Action"`
	BlockResponse                   string                  `json:"BlockResponse,omitempty"`
	BlockOverrideDomain             string                  `json:"BlockOverrideDomain,omitempty"`
	BlockOverrideDNSType            string                  `json:"BlockOverrideDnsType,omitempty"`
	Qtype                           string                  `json:"Qtype,omitempty"`
	Name                            string                  `json:"Name"`
	FirewallDomainListID            string                  `json:"FirewallDomainListId,omitempty"`
	ModificationTime                string                  `json:"ModificationTime,omitempty"`
	CreatorRequestID                string                  `json:"CreatorRequestId,omitempty"`
	DNSThreatProtection             string                  `json:"DnsThreatProtection,omitempty"`
	FirewallThreatProtectionID      string                  `json:"FirewallThreatProtectionId,omitempty"`
	FirewallDomainRedirectionAction string                  `json:"FirewallDomainRedirectionAction,omitempty"`
	FirewallRuleGroupID             string                  `json:"FirewallRuleGroupId"`
	Priority                        int32                   `json:"Priority"`
	BlockOverrideTTL                int32                   `json:"BlockOverrideTtl,omitempty"`
}

// firewallRuleTypeOutput is the response-side counterpart of
// firewallRuleTypeInput. Only DnsThreatProtection is ever populated -- see
// firewallRuleTypeInput's doc comment for why the other three variants are
// not modeled.
type firewallRuleTypeOutput struct {
	DNSThreatProtection *dnsThreatProtectionRuleTypeConfigInput `json:"DnsThreatProtection,omitempty"`
}

type createFirewallRuleInput struct {
	FirewallRuleType                *firewallRuleTypeInput `json:"FirewallRuleType,omitempty"`
	Action                          string                 `json:"Action"`
	CreatorRequestID                string                 `json:"CreatorRequestId"`
	FirewallRuleGroupID             string                 `json:"FirewallRuleGroupId"`
	FirewallDomainListID            string                 `json:"FirewallDomainListId"`
	Name                            string                 `json:"Name"`
	BlockResponse                   string                 `json:"BlockResponse"`
	BlockOverrideDomain             string                 `json:"BlockOverrideDomain"`
	BlockOverrideDNSType            string                 `json:"BlockOverrideDnsType"`
	Qtype                           string                 `json:"Qtype"`
	ConfidenceThreshold             string                 `json:"ConfidenceThreshold"`
	DNSThreatProtection             string                 `json:"DnsThreatProtection"`
	FirewallDomainRedirectionAction string                 `json:"FirewallDomainRedirectionAction"`
	BlockOverrideTTL                int32                  `json:"BlockOverrideTtl"`
	Priority                        int32                  `json:"Priority"`
}

type createFirewallRuleOutput struct {
	FirewallRule firewallRuleOutput `json:"FirewallRule"`
}

func firewallRuleToOutput(r *FirewallRule) firewallRuleOutput {
	out := firewallRuleOutput{
		Name:                            r.Name,
		FirewallRuleGroupID:             r.FirewallRuleGroupID,
		FirewallDomainListID:            r.FirewallDomainListID,
		Action:                          r.Action,
		Priority:                        r.Priority,
		BlockResponse:                   r.BlockResponse,
		BlockOverrideDomain:             r.BlockOverrideDomain,
		BlockOverrideDNSType:            r.BlockOverrideDNSType,
		BlockOverrideTTL:                r.BlockOverrideTTL,
		Qtype:                           r.Qtype,
		ConfidenceThreshold:             r.ConfidenceThreshold,
		CreatorRequestID:                r.CreatorRequestID,
		CreationTime:                    r.CreationTime,
		ModificationTime:                r.ModificationTime,
		DNSThreatProtection:             r.DNSThreatProtection,
		FirewallThreatProtectionID:      r.FirewallThreatProtectionID,
		FirewallDomainRedirectionAction: r.FirewallDomainRedirectionAction,
	}
	if r.DNSThreatProtection != "" {
		out.FirewallRuleType = &firewallRuleTypeOutput{
			DNSThreatProtection: &dnsThreatProtectionRuleTypeConfigInput{
				Value:               r.DNSThreatProtection,
				ConfidenceThreshold: r.ConfidenceThreshold,
			},
		}
	}

	return out
}

func (h *Handler) handleCreateFirewallRule(
	ctx context.Context,
	in *createFirewallRuleInput,
) (*createFirewallRuleOutput, error) {
	if in.FirewallRuleGroupID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleGroupId is required", ErrBatchValidation)
	}

	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrBatchValidation)
	}

	switch in.Action {
	case firewallActionAllow, firewallActionBlock, firewallActionAlert:
		// valid
	default:
		return nil, fmt.Errorf(
			"%w: Action must be %s, %s, or %s",
			ErrBatchValidation,
			firewallActionAllow,
			firewallActionBlock,
			firewallActionAlert,
		)
	}

	dnsThreatProtection, confidenceThreshold, err := mergeFirewallRuleTypeDNSThreatProtection(
		in.FirewallRuleType, in.FirewallDomainListID, in.DNSThreatProtection, in.ConfidenceThreshold,
	)
	if err != nil {
		return nil, err
	}

	rule, err := h.Backend.CreateFirewallRule(ctx, CreateFirewallRuleParams{
		FirewallRuleGroupID:             in.FirewallRuleGroupID,
		Name:                            in.Name,
		Action:                          in.Action,
		BlockResponse:                   in.BlockResponse,
		BlockOverrideDomain:             in.BlockOverrideDomain,
		BlockOverrideDNSType:            in.BlockOverrideDNSType,
		BlockOverrideTTL:                in.BlockOverrideTTL,
		Qtype:                           in.Qtype,
		ConfidenceThreshold:             confidenceThreshold,
		CreatorRequestID:                in.CreatorRequestID,
		FirewallDomainListID:            in.FirewallDomainListID,
		DNSThreatProtection:             dnsThreatProtection,
		FirewallDomainRedirectionAction: in.FirewallDomainRedirectionAction,
		Priority:                        in.Priority,
	})
	if err != nil {
		return nil, err
	}

	return &createFirewallRuleOutput{FirewallRule: firewallRuleToOutput(rule)}, nil
}

// firewallRuleBatchErrorCode maps a backend error to the exception-type string
// carried in a batch operation's per-item Code field. This mirrors, one-to-one,
// the classification handleError applies to the same sentinels for the
// singular ops -- so a rule that fails inside a batch reports the identical
// Code a standalone call to Create/Update/DeleteFirewallRule would have
// surfaced as its top-level exception Type.
func firewallRuleBatchErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrNotFound):
		return "ResourceNotFoundException"
	case errors.Is(err, ErrAlreadyExists):
		return "ResourceExistsException"
	case errors.Is(err, ErrInvalidParameter):
		return "InvalidParameterException"
	case errors.Is(err, ErrBatchValidation):
		return "ValidationException"
	default:
		return "InternalServiceErrorException"
	}
}

// batchCreateFirewallRuleInput matches the real BatchCreateFirewallRuleInput
// shape: a single required list of entries, each with the exact field set of
// types.CreateFirewallRuleEntry (verified against serializers.go
// awsAwsjson11_serializeDocumentCreateFirewallRuleEntry for wire-key casing).
// createFirewallRuleInput's fields are already an exact match for that entry
// shape (both mirror CreateFirewallRuleInput/CreateFirewallRuleEntry, which
// carry the same members other than the request envelope), so entries are
// parsed directly into it and processed through handleCreateFirewallRule --
// the exact function CreateFirewallRule itself calls. This is what guarantees
// batch entries get identical validation, error codes, and backend state
// (the shared FirewallRule store) as the singular op; there is no separate
// batch-only code path to drift out of sync.
type batchCreateFirewallRuleInput struct {
	CreateFirewallRuleEntries []createFirewallRuleInput `json:"CreateFirewallRuleEntries"`
}

// batchCreateFirewallRuleError mirrors types.BatchCreateFirewallRuleError
// (Code/FirewallRule/Message, verified against deserializers.go
// awsAwsjson11_deserializeDocumentBatchCreateFirewallRuleError).
type batchCreateFirewallRuleError struct {
	FirewallRule *createFirewallRuleInput `json:"FirewallRule,omitempty"`
	Code         string                   `json:"Code,omitempty"`
	Message      string                   `json:"Message,omitempty"`
}

type batchCreateFirewallRuleOutput struct {
	CreatedFirewallRules []firewallRuleOutput           `json:"CreatedFirewallRules"`
	CreateErrors         []batchCreateFirewallRuleError `json:"CreateErrors"`
}

// handleBatchCreateFirewallRule creates multiple firewall rules in one call.
//
// Atomicity: this is partial-success, NOT all-or-nothing. The real
// BatchCreateFirewallRuleOutput carries both CreatedFirewallRules (the subset
// that succeeded) and CreateErrors (the subset that failed, each echoing back
// the offending entry plus a Code/Message) -- verified against
// api_op_BatchCreateFirewallRule.go and confirmed by AWS's own published
// example response (a 2-entry request with one benign entry returns
// CreateErrors: [] alongside 2 CreatedFirewallRules, i.e. success and failure
// are reported per-item within a single 200 response, not as a top-level
// all-or-nothing rejection). Each entry is therefore processed independently
// and in request order: a failing entry is recorded in CreateErrors and
// processing continues with the next entry: state changes from entries before
// it are NOT rolled back.
//
// Entries are not required to share a FirewallRuleGroupId -- each entry
// carries its own (verified: CreateFirewallRuleEntry.FirewallRuleGroupId has
// no batch-level counterpart in BatchCreateFirewallRuleInput, which holds only
// the entries list), so a single batch can target multiple rule groups.
// Group existence is validated per-entry by the reused CreateFirewallRule
// path, exactly as a standalone call would.
func (h *Handler) handleBatchCreateFirewallRule(
	ctx context.Context,
	in *batchCreateFirewallRuleInput,
) (*batchCreateFirewallRuleOutput, error) {
	if len(in.CreateFirewallRuleEntries) == 0 {
		return nil, fmt.Errorf("%w: CreateFirewallRuleEntries is required", ErrBatchValidation)
	}

	out := &batchCreateFirewallRuleOutput{
		CreatedFirewallRules: []firewallRuleOutput{},
		CreateErrors:         []batchCreateFirewallRuleError{},
	}
	for i := range in.CreateFirewallRuleEntries {
		entry := in.CreateFirewallRuleEntries[i]

		res, err := h.handleCreateFirewallRule(ctx, &entry)
		if err != nil {
			out.CreateErrors = append(out.CreateErrors, batchCreateFirewallRuleError{
				FirewallRule: &entry,
				Code:         firewallRuleBatchErrorCode(err),
				Message:      err.Error(),
			})

			continue
		}
		out.CreatedFirewallRules = append(out.CreatedFirewallRules, res.FirewallRule)
	}

	return out, nil
}

// --- CreateOutpostResolver ---

// deleteFirewallRuleInput matches the real DeleteFirewallRuleInput shape:
// there is no FirewallRuleId member. A rule is addressed by
// FirewallRuleGroupId plus EITHER FirewallDomainListId (the rule it was
// created with, for domain-list rules) OR FirewallThreatProtectionId (the
// DNS Firewall Advanced counterpart, for DnsThreatProtection rules) --
// verified against api_op_DeleteFirewallRule.go's doc comment ("Identify the
// rule using either FirewallDomainListId ... or
// FirewallThreatProtectionId ... together with FirewallRuleGroupId").
type deleteFirewallRuleInput struct {
	FirewallRuleGroupID        string `json:"FirewallRuleGroupId"`
	FirewallDomainListID       string `json:"FirewallDomainListId"`
	FirewallThreatProtectionID string `json:"FirewallThreatProtectionId"`
	Qtype                      string `json:"Qtype"`
}

type deleteFirewallRuleOutput struct {
	FirewallRule firewallRuleOutput `json:"FirewallRule"`
}

func (h *Handler) handleDeleteFirewallRule(
	ctx context.Context,
	in *deleteFirewallRuleInput,
) (*deleteFirewallRuleOutput, error) {
	if in.FirewallRuleGroupID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleGroupId is required", ErrBatchValidation)
	}
	if in.FirewallDomainListID == "" && in.FirewallThreatProtectionID == "" {
		return nil, fmt.Errorf(
			"%w: one of FirewallDomainListId or FirewallThreatProtectionId is required",
			ErrBatchValidation,
		)
	}
	rule, err := h.Backend.DeleteFirewallRule(
		ctx, in.FirewallRuleGroupID, in.FirewallDomainListID, in.FirewallThreatProtectionID, in.Qtype,
	)
	if err != nil {
		return nil, err
	}

	return &deleteFirewallRuleOutput{FirewallRule: firewallRuleToOutput(rule)}, nil
}

// batchDeleteFirewallRuleInput/batchDeleteFirewallRuleError/
// batchDeleteFirewallRuleOutput mirror the Batch{Create}FirewallRule shapes
// above, reusing deleteFirewallRuleInput (already an exact field match for
// types.DeleteFirewallRuleEntry within this pass's declared scope) and
// handleDeleteFirewallRule per entry.
type batchDeleteFirewallRuleInput struct {
	DeleteFirewallRuleEntries []deleteFirewallRuleInput `json:"DeleteFirewallRuleEntries"`
}

type batchDeleteFirewallRuleError struct {
	FirewallRule *deleteFirewallRuleInput `json:"FirewallRule,omitempty"`
	Code         string                   `json:"Code,omitempty"`
	Message      string                   `json:"Message,omitempty"`
}

type batchDeleteFirewallRuleOutput struct {
	DeletedFirewallRules []firewallRuleOutput           `json:"DeletedFirewallRules"`
	DeleteErrors         []batchDeleteFirewallRuleError `json:"DeleteErrors"`
}

// handleBatchDeleteFirewallRule deletes multiple firewall rules in one call.
// Same partial-success semantics as handleBatchCreateFirewallRule (see its
// doc comment): DeletedFirewallRules/DeleteErrors are both populated from a
// single 200 response, entries are processed independently in request order
// via the reused handleDeleteFirewallRule path, and a failing entry does not
// roll back deletions already applied earlier in the same batch.
func (h *Handler) handleBatchDeleteFirewallRule(
	ctx context.Context,
	in *batchDeleteFirewallRuleInput,
) (*batchDeleteFirewallRuleOutput, error) {
	if len(in.DeleteFirewallRuleEntries) == 0 {
		return nil, fmt.Errorf("%w: DeleteFirewallRuleEntries is required", ErrBatchValidation)
	}

	out := &batchDeleteFirewallRuleOutput{
		DeletedFirewallRules: []firewallRuleOutput{},
		DeleteErrors:         []batchDeleteFirewallRuleError{},
	}
	for i := range in.DeleteFirewallRuleEntries {
		entry := in.DeleteFirewallRuleEntries[i]

		res, err := h.handleDeleteFirewallRule(ctx, &entry)
		if err != nil {
			out.DeleteErrors = append(out.DeleteErrors, batchDeleteFirewallRuleError{
				FirewallRule: &entry,
				Code:         firewallRuleBatchErrorCode(err),
				Message:      err.Error(),
			})

			continue
		}
		out.DeletedFirewallRules = append(out.DeletedFirewallRules, res.FirewallRule)
	}

	return out, nil
}

// --- UpdateFirewallRule ---

// updateFirewallRuleInput matches the real UpdateFirewallRuleInput shape:
// there is no FirewallRuleId member. FirewallRuleGroupId+FirewallDomainListId
// identify which rule to update -- the domain list a rule targets is part of
// its identity, not a mutable property (verified against
// api_op_UpdateFirewallRule.go).
// updateFirewallRuleInput matches the real UpdateFirewallRuleInput shape:
// there is no FirewallRuleId member. FirewallRuleGroupId plus EITHER
// FirewallDomainListId (domain-list rules) OR FirewallThreatProtectionId
// (DnsThreatProtection rules) identify which rule to update -- a rule's
// match source is part of its identity, not a mutable property.
// FirewallDomainRedirectionAction IS a mutable property and is applied.
type updateFirewallRuleInput struct {
	FirewallRuleType                *firewallRuleTypeInput `json:"FirewallRuleType,omitempty"`
	FirewallRuleGroupID             string                 `json:"FirewallRuleGroupId"`
	FirewallDomainListID            string                 `json:"FirewallDomainListId"`
	FirewallThreatProtectionID      string                 `json:"FirewallThreatProtectionId"`
	Name                            string                 `json:"Name"`
	Action                          string                 `json:"Action"`
	BlockResponse                   string                 `json:"BlockResponse"`
	BlockOverrideDomain             string                 `json:"BlockOverrideDomain"`
	BlockOverrideDNSType            string                 `json:"BlockOverrideDnsType"`
	Qtype                           string                 `json:"Qtype"`
	ConfidenceThreshold             string                 `json:"ConfidenceThreshold"`
	DNSThreatProtection             string                 `json:"DnsThreatProtection"`
	FirewallDomainRedirectionAction string                 `json:"FirewallDomainRedirectionAction"`
	BlockOverrideTTL                int32                  `json:"BlockOverrideTtl"`
	Priority                        int32                  `json:"Priority"`
}

type updateFirewallRuleOutput struct {
	FirewallRule firewallRuleOutput `json:"FirewallRule"`
}

func (h *Handler) handleUpdateFirewallRule(
	ctx context.Context,
	in *updateFirewallRuleInput,
) (*updateFirewallRuleOutput, error) {
	if in.FirewallRuleGroupID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleGroupId is required", ErrBatchValidation)
	}
	if in.FirewallDomainListID == "" && in.FirewallThreatProtectionID == "" {
		return nil, fmt.Errorf(
			"%w: one of FirewallDomainListId or FirewallThreatProtectionId is required",
			ErrBatchValidation,
		)
	}

	dnsThreatProtection, confidenceThreshold, err := mergeFirewallRuleTypeDNSThreatProtection(
		in.FirewallRuleType, in.FirewallDomainListID, in.DNSThreatProtection, in.ConfidenceThreshold,
	)
	if err != nil {
		return nil, err
	}

	rule, err := h.Backend.UpdateFirewallRule(ctx, UpdateFirewallRuleParams{
		FirewallRuleGroupID:             in.FirewallRuleGroupID,
		FirewallDomainListID:            in.FirewallDomainListID,
		FirewallThreatProtectionID:      in.FirewallThreatProtectionID,
		Name:                            in.Name,
		Action:                          in.Action,
		BlockResponse:                   in.BlockResponse,
		BlockOverrideDomain:             in.BlockOverrideDomain,
		BlockOverrideDNSType:            in.BlockOverrideDNSType,
		BlockOverrideTTL:                in.BlockOverrideTTL,
		Qtype:                           in.Qtype,
		ConfidenceThreshold:             confidenceThreshold,
		DNSThreatProtection:             dnsThreatProtection,
		FirewallDomainRedirectionAction: in.FirewallDomainRedirectionAction,
		Priority:                        in.Priority,
	})
	if err != nil {
		return nil, err
	}

	return &updateFirewallRuleOutput{FirewallRule: firewallRuleToOutput(rule)}, nil
}

// batchUpdateFirewallRuleInput/batchUpdateFirewallRuleError/
// batchUpdateFirewallRuleOutput mirror the Batch{Create}FirewallRule shapes
// above, reusing updateFirewallRuleInput (already an exact field match for
// types.UpdateFirewallRuleEntry within this pass's declared scope) and
// handleUpdateFirewallRule per entry.
type batchUpdateFirewallRuleInput struct {
	UpdateFirewallRuleEntries []updateFirewallRuleInput `json:"UpdateFirewallRuleEntries"`
}

type batchUpdateFirewallRuleError struct {
	FirewallRule *updateFirewallRuleInput `json:"FirewallRule,omitempty"`
	Code         string                   `json:"Code,omitempty"`
	Message      string                   `json:"Message,omitempty"`
}

type batchUpdateFirewallRuleOutput struct {
	UpdatedFirewallRules []firewallRuleOutput           `json:"UpdatedFirewallRules"`
	UpdateErrors         []batchUpdateFirewallRuleError `json:"UpdateErrors"`
}

// handleBatchUpdateFirewallRule updates multiple firewall rules in one call.
// Same partial-success semantics as handleBatchCreateFirewallRule (see its
// doc comment): UpdatedFirewallRules/UpdateErrors are both populated from a
// single 200 response, entries are processed independently in request order
// via the reused handleUpdateFirewallRule path, and a failing entry does not
// roll back updates already applied earlier in the same batch.
func (h *Handler) handleBatchUpdateFirewallRule(
	ctx context.Context,
	in *batchUpdateFirewallRuleInput,
) (*batchUpdateFirewallRuleOutput, error) {
	if len(in.UpdateFirewallRuleEntries) == 0 {
		return nil, fmt.Errorf("%w: UpdateFirewallRuleEntries is required", ErrBatchValidation)
	}

	out := &batchUpdateFirewallRuleOutput{
		UpdatedFirewallRules: []firewallRuleOutput{},
		UpdateErrors:         []batchUpdateFirewallRuleError{},
	}
	for i := range in.UpdateFirewallRuleEntries {
		entry := in.UpdateFirewallRuleEntries[i]

		res, err := h.handleUpdateFirewallRule(ctx, &entry)
		if err != nil {
			out.UpdateErrors = append(out.UpdateErrors, batchUpdateFirewallRuleError{
				FirewallRule: &entry,
				Code:         firewallRuleBatchErrorCode(err),
				Message:      err.Error(),
			})

			continue
		}
		out.UpdatedFirewallRules = append(out.UpdatedFirewallRules, res.FirewallRule)
	}

	return out, nil
}

// --- ListFirewallRules ---

type listFirewallRulesInput struct {
	Priority            *int32 `json:"Priority,omitempty"`
	FirewallRuleGroupID string `json:"FirewallRuleGroupId"`
	NextToken           string `json:"NextToken"`
	Action              string `json:"Action"`
	MaxResults          int32  `json:"MaxResults"`
}

type listFirewallRulesOutput struct {
	NextToken     *string              `json:"NextToken,omitempty"`
	FirewallRules []firewallRuleOutput `json:"FirewallRules"`
}

func (h *Handler) handleListFirewallRules(
	ctx context.Context,
	in *listFirewallRulesInput,
) (*listFirewallRulesOutput, error) {
	rules := h.Backend.ListFirewallRules(ctx, in.FirewallRuleGroupID)
	items := make([]firewallRuleOutput, 0, len(rules))
	for _, r := range rules {
		if in.Action != "" && r.Action != in.Action {
			continue
		}
		if in.Priority != nil && r.Priority != *in.Priority {
			continue
		}
		items = append(items, firewallRuleToOutput(r))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	data, next := paginate(items, in.NextToken, in.MaxResults, defaultPageSizeLarge)

	return &listFirewallRulesOutput{FirewallRules: data, NextToken: next}, nil
}

// --- ListFirewallRuleTypes ---

// firewallRuleTypeDefinitionOutput mirrors types.FirewallRuleTypeDefinition
// (RuleType/Value/DisplayName/Description/SubscriptionInfo fields, verified
// against deserializers.go
// awsAwsjson11_deserializeDocumentFirewallRuleTypeDefinition for exact
// wire-key casing). SubscriptionInfo is omitted here: the real SDK only
// populates it for the PartnerThreatProtection variant, which this pass does
// not catalog (see firewallRuleTypeCatalog).
type firewallRuleTypeDefinitionOutput struct {
	RuleType    string `json:"RuleType,omitempty"`
	Value       string `json:"Value,omitempty"`
	DisplayName string `json:"DisplayName,omitempty"`
	Description string `json:"Description,omitempty"`
}

// firewallRuleTypeCatalog returns the rule-type definitions ListFirewallRuleTypes
// can back with real SDK data. Per types.FirewallRuleType (the tagged union
// used by the FirewallRuleType member on Create/UpdateFirewallRuleEntry, and
// on types.FirewallRule itself) there are exactly four RuleType variants:
// DnsThreatProtection, FirewallAdvancedContentCategory,
// FirewallAdvancedThreatCategory, and PartnerThreatProtection -- verified as
// the four member names of types.FirewallRuleType, which also match the four
// documented values of ListFirewallRuleTypesInput.RuleType's filter.
//
// Of those four, only DnsThreatProtection carries a genuine closed enum of
// concrete Values in the SDK: types.DnsThreatProtection
// (DGA/DNS_TUNNELING/DICTIONARY_DGA), reused directly below instead of
// re-typed by hand so the catalog can never drift from the real enum. The
// other three variants are AWS-managed, dynamically-updated catalogs with NO
// SDK-side enum of concrete identifiers to enumerate --
// FirewallAdvancedContentCategoryConfig.Category,
// FirewallAdvancedThreatCategoryConfig.Category, and
// PartnerThreatProtectionConfig.Partner are all untyped *string, and their
// doc comments explicitly say the only way to learn the valid values is to
// call ListFirewallRuleTypes itself (a chicken-and-egg the mock cannot
// resolve without inventing category/partner names the SDK does not define).
// Per this pass's no-invented-values rule, gopherstack does not fabricate
// FirewallAdvancedContentCategory/FirewallAdvancedThreatCategory/
// PartnerThreatProtection catalog entries; see PARITY.md gaps for this op.
func firewallRuleTypeCatalog() []firewallRuleTypeDefinitionOutput {
	dnsThreatProtectionTypes := []struct {
		value       r53rtypes.DnsThreatProtection
		displayName string
		description string
	}{
		{
			value:       r53rtypes.DnsThreatProtectionDga,
			displayName: "Domain Generation Algorithm (DGA) Detection",
			description: "Detects domains generated by domain generation algorithms, which " +
				"attackers use to generate large numbers of domains for malware attacks.",
		},
		{
			value:       r53rtypes.DnsThreatProtectionDnsTunneling,
			displayName: "DNS Tunneling Detection",
			description: "Detects DNS tunneling, which attackers use to exfiltrate data " +
				"through a DNS tunnel without making a direct network connection to the client.",
		},
		{
			value:       r53rtypes.DnsThreatProtectionDictionaryDga,
			displayName: "Dictionary DGA Detection",
			description: "Detects dictionary-based domain generation algorithms, which use " +
				"wordlists to generate domains that appear more legitimate than traditional DGAs.",
		},
	}

	out := make([]firewallRuleTypeDefinitionOutput, 0, len(dnsThreatProtectionTypes))
	for _, dt := range dnsThreatProtectionTypes {
		out = append(out, firewallRuleTypeDefinitionOutput{
			RuleType:    "DnsThreatProtection",
			Value:       string(dt.value),
			DisplayName: dt.displayName,
			Description: dt.description,
		})
	}

	return out
}

type listFirewallRuleTypesInput struct {
	RuleType   string `json:"RuleType"`
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listFirewallRuleTypesOutput struct {
	NextToken         *string                            `json:"NextToken,omitempty"`
	FirewallRuleTypes []firewallRuleTypeDefinitionOutput `json:"FirewallRuleTypes"`
}

func (h *Handler) handleListFirewallRuleTypes(
	_ context.Context,
	in *listFirewallRuleTypesInput,
) (*listFirewallRuleTypesOutput, error) {
	items := firewallRuleTypeCatalog()
	if in.RuleType != "" {
		filtered := make([]firewallRuleTypeDefinitionOutput, 0, len(items))
		for _, item := range items {
			if item.RuleType == in.RuleType {
				filtered = append(filtered, item)
			}
		}
		items = filtered
	}
	data, next := paginate(items, in.NextToken, in.MaxResults, defaultPageSizeLarge)

	return &listFirewallRuleTypesOutput{FirewallRuleTypes: data, NextToken: next}, nil
}

// --- DeleteFirewallRuleGroup ---

func (h *Handler) opsFirewallRules() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateFirewallRule":      service.WrapOp(h.handleCreateFirewallRule),
		"DeleteFirewallRule":      service.WrapOp(h.handleDeleteFirewallRule),
		"ListFirewallRules":       service.WrapOp(h.handleListFirewallRules),
		"UpdateFirewallRule":      service.WrapOp(h.handleUpdateFirewallRule),
		"BatchCreateFirewallRule": service.WrapOp(h.handleBatchCreateFirewallRule),
		"BatchDeleteFirewallRule": service.WrapOp(h.handleBatchDeleteFirewallRule),
		"BatchUpdateFirewallRule": service.WrapOp(h.handleBatchUpdateFirewallRule),
		"ListFirewallRuleTypes":   service.WrapOp(h.handleListFirewallRuleTypes),
	}
}
