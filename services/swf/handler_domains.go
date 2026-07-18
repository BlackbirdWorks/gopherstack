package swf

import (
	"context"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

// --- RegisterDomain ---

type registerDomainOutput struct{}

type resourceTagInput struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type handleRegisterDomainInput struct {
	Name                                   string             `json:"name"`
	Description                            string             `json:"description"`
	WorkflowExecutionRetentionPeriodInDays string             `json:"workflowExecutionRetentionPeriodInDays"`
	Tags                                   []resourceTagInput `json:"tags,omitempty"`
}

func (h *Handler) handleRegisterDomain(
	_ context.Context,
	in *handleRegisterDomainInput,
) (*registerDomainOutput, error) {
	retention := in.WorkflowExecutionRetentionPeriodInDays
	if err := h.Backend.RegisterDomain(in.Name, in.Description, retention); err != nil {
		return nil, err
	}
	// Apply tags via TagResource if provided.
	if len(in.Tags) > 0 {
		arn := domainARN(config.DefaultRegion, defaultAccountID, in.Name)
		tagMap := make(map[string]string, len(in.Tags))
		for _, t := range in.Tags {
			tagMap[t.Key] = t.Value
		}
		if err := h.Backend.TagResource(arn, tagMap); err != nil {
			return nil, err
		}
	}

	return &registerDomainOutput{}, nil
}

// --- DescribeDomain ---

type domainConfigOutput struct {
	WorkflowExecutionRetentionPeriodInDays string `json:"workflowExecutionRetentionPeriodInDays"`
}

type domainInfoOutput struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Status      string `json:"status"`
	Arn         string `json:"arn,omitempty"`
}

type describeDomainOutput struct {
	DomainInfo    *domainInfoOutput  `json:"domainInfo"`
	Configuration domainConfigOutput `json:"configuration"`
}

type handleDescribeDomainInput struct {
	Name string `json:"name"`
}

func (h *Handler) handleDescribeDomain(
	_ context.Context,
	in *handleDescribeDomainInput,
) (*describeDomainOutput, error) {
	d, err := h.Backend.DescribeDomain(in.Name)
	if err != nil {
		return nil, err
	}
	retention := d.WorkflowExecutionRetentionPeriodInDays
	if retention == "" {
		retention = retentionNone
	}

	return &describeDomainOutput{
		DomainInfo: &domainInfoOutput{
			Name:        d.Name,
			Description: d.Description,
			Status:      d.Status,
			Arn:         d.Arn,
		},
		Configuration: domainConfigOutput{WorkflowExecutionRetentionPeriodInDays: retention},
	}, nil
}

// --- ListDomains ---

type listDomainsOutput struct {
	NextPageToken string             `json:"nextPageToken,omitempty"`
	DomainInfos   []domainInfoOutput `json:"domainInfos"`
}

type handleListDomainsInput struct {
	RegistrationStatus string `json:"registrationStatus"`
	NextPageToken      string `json:"nextPageToken,omitempty"`
	MaximumPageSize    int    `json:"maximumPageSize,omitempty"`
}

func (h *Handler) handleListDomains(_ context.Context, in *handleListDomainsInput) (*listDomainsOutput, error) {
	domains, err := h.Backend.ListDomains(in.RegistrationStatus)
	if err != nil {
		return nil, err
	}
	sort.Slice(domains, func(i, j int) bool { return domains[i].Name < domains[j].Name })
	domains, nextPageToken := applyPageTokenSlice(domains, in.NextPageToken, in.MaximumPageSize)
	infos := make([]domainInfoOutput, len(domains))
	for i, d := range domains {
		infos[i] = domainInfoOutput{
			Name:        d.Name,
			Description: d.Description,
			Status:      d.Status,
			Arn:         d.Arn,
		}
	}

	return &listDomainsOutput{DomainInfos: infos, NextPageToken: nextPageToken}, nil
}

// --- DeprecateDomain ---

type deprecateDomainOutput struct{}

type handleDeprecateDomainInput struct {
	Name string `json:"name"`
}

func (h *Handler) handleDeprecateDomain(
	_ context.Context,
	in *handleDeprecateDomainInput,
) (*deprecateDomainOutput, error) {
	if err := h.Backend.DeprecateDomain(in.Name); err != nil {
		return nil, err
	}

	return &deprecateDomainOutput{}, nil
}

// --- UndeprecateDomain ---

type undeprecateDomainOutput struct{}

type handleUndeprecateDomainInput struct {
	Name string `json:"name"`
}

func (h *Handler) handleUndeprecateDomain(
	_ context.Context,
	in *handleUndeprecateDomainInput,
) (*undeprecateDomainOutput, error) {
	if err := h.Backend.UndeprecateDomain(in.Name); err != nil {
		return nil, err
	}

	return &undeprecateDomainOutput{}, nil
}
