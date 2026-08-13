package route53resolver

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// firewallDomainListOutput is the JSON representation of a FirewallDomainList.
type firewallDomainListOutput struct {
	ID               string `json:"Id"`
	Arn              string `json:"Arn"`
	Name             string `json:"Name"`
	CreatorRequestID string `json:"CreatorRequestId"`
	Status           string `json:"Status"`
	StatusMessage    string `json:"StatusMessage,omitempty"`
	ManagedOwnerName string `json:"ManagedOwnerName,omitempty"`
	CreationTime     string `json:"CreationTime,omitempty"`
	ModificationTime string `json:"ModificationTime,omitempty"`
	DomainCount      int32  `json:"DomainCount"`
}

type createFirewallDomainListInput struct {
	CreatorRequestID string       `json:"CreatorRequestId"`
	Name             string       `json:"Name"`
	Tags             []svcTags.KV `json:"Tags"`
}

type createFirewallDomainListOutput struct {
	FirewallDomainList firewallDomainListOutput `json:"FirewallDomainList"`
}

func firewallDomainListToOutput(dl *FirewallDomainList) firewallDomainListOutput {
	return firewallDomainListOutput{
		ID:               dl.ID,
		Arn:              dl.ARN,
		Name:             dl.Name,
		CreatorRequestID: dl.CreatorRequestID,
		Status:           dl.Status,
		StatusMessage:    dl.StatusMessage,
		DomainCount:      dl.DomainCount,
		ManagedOwnerName: dl.ManagedOwnerName,
		CreationTime:     dl.CreationTime,
		ModificationTime: dl.ModificationTime,
	}
}

// firewallDomainListMetadataOutput is the wire shape for
// ListFirewallDomainListsOutput.FirewallDomainLists (types.FirewallDomainListMetadata,
// route53resolver@v1.48.4 types/types.go:584, deserializer at
// deserializers.go:10568): only Arn/Category/CreatorRequestId/Id/
// ManagedListType/ManagedOwnerName/Name -- no Status/DomainCount/
// CreationTime/ModificationTime/StatusMessage. Category and ManagedListType
// are omitted here (rather than emitted always-empty): this backend never
// creates AWS-managed domain lists, so it has no source of truth for either.
type firewallDomainListMetadataOutput struct {
	ID               string `json:"Id"`
	Arn              string `json:"Arn"`
	Name             string `json:"Name"`
	CreatorRequestID string `json:"CreatorRequestId"`
	ManagedOwnerName string `json:"ManagedOwnerName,omitempty"`
}

func firewallDomainListToMetadataOutput(dl *FirewallDomainList) firewallDomainListMetadataOutput {
	return firewallDomainListMetadataOutput{
		ID:               dl.ID,
		Arn:              dl.ARN,
		Name:             dl.Name,
		CreatorRequestID: dl.CreatorRequestID,
		ManagedOwnerName: dl.ManagedOwnerName,
	}
}

func (h *Handler) handleCreateFirewallDomainList(
	ctx context.Context,
	in *createFirewallDomainListInput,
) (*createFirewallDomainListOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	dl, err := h.Backend.CreateFirewallDomainList(ctx, in.Name, in.CreatorRequestID)
	if err != nil {
		return nil, err
	}

	if len(in.Tags) > 0 {
		if tagErr := h.Backend.TagResource(ctx, dl.ARN, in.Tags); tagErr != nil {
			return nil, tagErr
		}
	}

	return &createFirewallDomainListOutput{FirewallDomainList: firewallDomainListToOutput(dl)}, nil
}

// --- DeleteFirewallDomainList ---

type deleteFirewallDomainListInput struct {
	FirewallDomainListID string `json:"FirewallDomainListId"`
}

type deleteFirewallDomainListOutput struct {
	FirewallDomainList firewallDomainListOutput `json:"FirewallDomainList"`
}

func (h *Handler) handleDeleteFirewallDomainList(
	ctx context.Context,
	in *deleteFirewallDomainListInput,
) (*deleteFirewallDomainListOutput, error) {
	if in.FirewallDomainListID == "" {
		return nil, fmt.Errorf("%w: FirewallDomainListId is required", ErrValidation)
	}

	dl, err := h.Backend.DeleteFirewallDomainList(ctx, in.FirewallDomainListID)
	if err != nil {
		return nil, err
	}

	return &deleteFirewallDomainListOutput{FirewallDomainList: firewallDomainListToOutput(dl)}, nil
}

// --- CreateFirewallRule ---

type getFirewallDomainListInput struct {
	FirewallDomainListID string `json:"FirewallDomainListId"`
}

type getFirewallDomainListOutput struct {
	FirewallDomainList firewallDomainListOutput `json:"FirewallDomainList"`
}

func (h *Handler) handleGetFirewallDomainList(
	ctx context.Context,
	in *getFirewallDomainListInput,
) (*getFirewallDomainListOutput, error) {
	if in.FirewallDomainListID == "" {
		return nil, fmt.Errorf("%w: FirewallDomainListId is required", ErrValidation)
	}
	dl, err := h.Backend.GetFirewallDomainList(ctx, in.FirewallDomainListID)
	if err != nil {
		return nil, err
	}

	return &getFirewallDomainListOutput{FirewallDomainList: firewallDomainListToOutput(dl)}, nil
}

// --- ListFirewallDomainLists ---

type listFirewallDomainListsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listFirewallDomainListsOutput struct {
	NextToken           *string                            `json:"NextToken,omitempty"`
	FirewallDomainLists []firewallDomainListMetadataOutput `json:"FirewallDomainLists"`
}

func (h *Handler) handleListFirewallDomainLists(
	ctx context.Context,
	in *listFirewallDomainListsInput,
) (*listFirewallDomainListsOutput, error) {
	lists := h.Backend.ListFirewallDomainLists(ctx)
	items := make([]firewallDomainListMetadataOutput, 0, len(lists))
	for _, dl := range lists {
		items = append(items, firewallDomainListToMetadataOutput(dl))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	data, next := paginate(items, in.NextToken, in.MaxResults, defaultPageSizeLarge)

	return &listFirewallDomainListsOutput{FirewallDomainLists: data, NextToken: next}, nil
}

// --- ListFirewallDomains ---

type listFirewallDomainsInput struct {
	NextToken            string `json:"NextToken"`
	FirewallDomainListID string `json:"FirewallDomainListId"`
	MaxResults           int32  `json:"MaxResults"`
}

type listFirewallDomainsOutput struct {
	NextToken *string  `json:"NextToken,omitempty"`
	Domains   []string `json:"Domains"`
}

func (h *Handler) handleListFirewallDomains(
	ctx context.Context,
	in *listFirewallDomainsInput,
) (*listFirewallDomainsOutput, error) {
	if in.FirewallDomainListID == "" {
		return nil, fmt.Errorf("%w: FirewallDomainListId is required", ErrValidation)
	}
	domains, err := h.Backend.ListFirewallDomains(ctx, in.FirewallDomainListID)
	if err != nil {
		return nil, err
	}
	data, next := paginate(domains, in.NextToken, in.MaxResults, defaultPageSizeLarge)

	return &listFirewallDomainsOutput{Domains: data, NextToken: next}, nil
}

// --- UpdateFirewallDomains ---

type updateFirewallDomainsInput struct {
	FirewallDomainListID string   `json:"FirewallDomainListId"`
	Operation            string   `json:"Operation"`
	Domains              []string `json:"Domains"`
}

type updateFirewallDomainsOutput struct {
	FirewallDomainList firewallDomainListOutput `json:"FirewallDomainList"`
}

func (h *Handler) handleUpdateFirewallDomains(
	ctx context.Context,
	in *updateFirewallDomainsInput,
) (*updateFirewallDomainsOutput, error) {
	if in.FirewallDomainListID == "" {
		return nil, fmt.Errorf("%w: FirewallDomainListId is required", ErrValidation)
	}
	if in.Operation == "" {
		return nil, fmt.Errorf("%w: Operation is required", ErrValidation)
	}
	dl, err := h.Backend.UpdateFirewallDomains(ctx, in.FirewallDomainListID, in.Operation, in.Domains)
	if err != nil {
		return nil, err
	}

	return &updateFirewallDomainsOutput{FirewallDomainList: firewallDomainListToOutput(dl)}, nil
}

// --- ImportFirewallDomains ---

type importFirewallDomainsInput struct {
	FirewallDomainListID string `json:"FirewallDomainListId"`
	DomainFileURL        string `json:"DomainFileUrl"`
	Operation            string `json:"Operation"`
}

type importFirewallDomainsOutput struct {
	FirewallDomainList firewallDomainListOutput `json:"FirewallDomainList"`
}

func (h *Handler) handleImportFirewallDomains(
	ctx context.Context,
	in *importFirewallDomainsInput,
) (*importFirewallDomainsOutput, error) {
	if in.FirewallDomainListID == "" {
		return nil, fmt.Errorf("%w: FirewallDomainListId is required", ErrValidation)
	}
	if in.DomainFileURL == "" {
		return nil, fmt.Errorf("%w: DomainFileUrl is required", ErrValidation)
	}
	if in.Operation == "" {
		return nil, fmt.Errorf("%w: Operation is required", ErrValidation)
	}
	dl, err := h.Backend.ImportFirewallDomains(
		ctx,
		in.FirewallDomainListID,
		in.Operation,
		in.DomainFileURL,
	)
	if err != nil {
		return nil, err
	}

	return &importFirewallDomainsOutput{FirewallDomainList: firewallDomainListToOutput(dl)}, nil
}

// --- GetFirewallConfig ---

func (h *Handler) opsFirewallDomainLists() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateFirewallDomainList": service.WrapOp(h.handleCreateFirewallDomainList),
		"DeleteFirewallDomainList": service.WrapOp(h.handleDeleteFirewallDomainList),
		"GetFirewallDomainList":    service.WrapOp(h.handleGetFirewallDomainList),
		"ImportFirewallDomains":    service.WrapOp(h.handleImportFirewallDomains),
		"ListFirewallDomainLists":  service.WrapOp(h.handleListFirewallDomainLists),
		"ListFirewallDomains":      service.WrapOp(h.handleListFirewallDomains),
		"UpdateFirewallDomains":    service.WrapOp(h.handleUpdateFirewallDomains),
	}
}
