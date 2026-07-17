package codeconnections

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type createHostInput struct {
	Name             string `json:"Name"`
	ProviderType     string `json:"ProviderType"`
	ProviderEndpoint string `json:"ProviderEndpoint"`
	Tags             []tag  `json:"Tags"`
}

type createHostOutput struct {
	HostArn string `json:"HostArn"`
	Tags    []tag  `json:"Tags,omitempty"`
}

func (h *Handler) handleCreateHost(
	ctx context.Context,
	in *createHostInput,
) (*createHostOutput, error) {
	host, err := h.Backend.CreateHost(
		ctx,
		in.Name,
		in.ProviderType,
		in.ProviderEndpoint,
		tagsFromArray(in.Tags),
	)
	if err != nil {
		return nil, err
	}

	return &createHostOutput{HostArn: host.HostArn, Tags: tagsToSortedArray(host.Tags)}, nil
}

type getHostInput struct {
	HostArn string `json:"HostArn"`
}

type getHostOutput struct {
	Name             string `json:"Name"`
	HostArn          string `json:"HostArn,omitempty"`
	ProviderEndpoint string `json:"ProviderEndpoint"`
	ProviderType     string `json:"ProviderType"`
	Status           string `json:"Status"`
	StatusMessage    string `json:"StatusMessage,omitempty"`
	Tags             []tag  `json:"Tags,omitempty"`
}

func (h *Handler) handleGetHost(ctx context.Context, in *getHostInput) (*getHostOutput, error) {
	if in.HostArn == "" {
		return nil, fmt.Errorf("%w: HostArn is required", ErrValidation)
	}

	host, err := h.Backend.GetHost(ctx, in.HostArn)
	if err != nil {
		return nil, err
	}

	return &getHostOutput{
		Name:             host.Name,
		HostArn:          host.HostArn,
		ProviderEndpoint: host.ProviderEndpoint,
		ProviderType:     host.ProviderType,
		Status:           host.Status,
		StatusMessage:    host.StatusMessage,
		Tags:             tagsToSortedArray(host.Tags),
	}, nil
}

type deleteHostInput struct {
	HostArn string `json:"HostArn"`
}

func (h *Handler) handleDeleteHost(ctx context.Context, in *deleteHostInput) (*emptyOutput, error) {
	if err := h.Backend.DeleteHost(ctx, in.HostArn); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type listHostsInput struct {
	NextToken  *string `json:"NextToken"`
	MaxResults *int32  `json:"MaxResults"`
}

type hostItem struct {
	HostArn          string `json:"HostArn"`
	Name             string `json:"Name"`
	ProviderEndpoint string `json:"ProviderEndpoint"`
	ProviderType     string `json:"ProviderType"`
	Status           string `json:"Status"`
	StatusMessage    string `json:"StatusMessage,omitempty"`
	Tags             []tag  `json:"Tags,omitempty"`
}

type listHostsOutput struct {
	NextToken *string    `json:"NextToken,omitempty"`
	Hosts     []hostItem `json:"Hosts"`
}

func (h *Handler) handleListHosts(
	ctx context.Context,
	in *listHostsInput,
) (*listHostsOutput, error) {
	hosts := h.Backend.ListHosts(ctx)
	items := make([]hostItem, len(hosts))

	for i, host := range hosts {
		items[i] = hostItem{
			HostArn:          host.HostArn,
			Name:             host.Name,
			ProviderEndpoint: host.ProviderEndpoint,
			ProviderType:     host.ProviderType,
			Status:           host.Status,
			StatusMessage:    host.StatusMessage,
			Tags:             tagsToSortedArray(host.Tags),
		}
	}

	var limit int
	if in.MaxResults != nil && *in.MaxResults > 0 {
		limit = int(*in.MaxResults)
	}

	token := ""
	if in.NextToken != nil {
		token = *in.NextToken
	}

	p := page.New(items, token, limit, ccDefaultPageSize)

	var nextToken *string
	if p.Next != "" {
		nextToken = &p.Next
	}

	return &listHostsOutput{Hosts: p.Data, NextToken: nextToken}, nil
}

type updateHostInput struct {
	HostArn          string `json:"HostArn"`
	ProviderEndpoint string `json:"ProviderEndpoint"`
}

func (h *Handler) handleUpdateHost(ctx context.Context, in *updateHostInput) (*emptyOutput, error) {
	if in.HostArn == "" {
		return nil, fmt.Errorf("%w: HostArn is required", ErrValidation)
	}

	if err := h.Backend.UpdateHost(ctx, in.HostArn, in.ProviderEndpoint); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}
