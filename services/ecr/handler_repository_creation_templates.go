package ecr

import (
	"context"
)

type repositoryCreationTemplateInput struct {
	EncryptionConfiguration            *encryptionConfigurationView   `json:"encryptionConfiguration,omitempty"`
	Prefix                             string                         `json:"prefix"`
	Description                        string                         `json:"description,omitempty"`
	ImageTagMutability                 string                         `json:"imageTagMutability,omitempty"`
	RepositoryPolicy                   string                         `json:"repositoryPolicy,omitempty"`
	LifecyclePolicy                    string                         `json:"lifecyclePolicy,omitempty"`
	CustomRoleArn                      string                         `json:"customRoleArn,omitempty"`
	ImageTagMutabilityExclusionFilters []imageTagMutabilityFilterView `json:"imageTagMutabilityExclusionFilters,omitempty"`
	AppliedFor                         []string                       `json:"appliedFor,omitempty"`
	ResourceTags                       []tagView                      `json:"resourceTags,omitempty"`
}

type createRepositoryCreationTemplateOutput struct {
	Template   *repositoryCreationTemplateView `json:"repositoryCreationTemplate"`
	RegistryID string                          `json:"registryId"`
}

type repositoryCreationTemplateView struct {
	EncryptionConfiguration            *encryptionConfigurationView   `json:"encryptionConfiguration,omitempty"`
	Prefix                             string                         `json:"prefix,omitempty"`
	Description                        string                         `json:"description,omitempty"`
	ImageTagMutability                 string                         `json:"imageTagMutability,omitempty"`
	RepositoryPolicy                   string                         `json:"repositoryPolicy,omitempty"`
	LifecyclePolicy                    string                         `json:"lifecyclePolicy,omitempty"`
	CustomRoleArn                      string                         `json:"customRoleArn,omitempty"`
	ImageTagMutabilityExclusionFilters []imageTagMutabilityFilterView `json:"imageTagMutabilityExclusionFilters,omitempty"`
	AppliedFor                         []string                       `json:"appliedFor,omitempty"`
	ResourceTags                       []tagView                      `json:"resourceTags,omitempty"`
	CreatedAt                          float64                        `json:"createdAt,omitempty"`
	UpdatedAt                          float64                        `json:"updatedAt,omitempty"`
}

func (h *Handler) handleCreateRepositoryCreationTemplate(
	ctx context.Context,
	in *repositoryCreationTemplateInput,
) (*createRepositoryCreationTemplateOutput, error) {
	req := repositoryCreationTemplateFromInput(in)

	tmpl, err := h.Backend.CreateRepositoryCreationTemplate(ctx, req)
	if err != nil {
		return nil, err
	}

	return &createRepositoryCreationTemplateOutput{
		Template: toRepositoryCreationTemplateView(tmpl),
	}, nil
}

type deleteRepositoryCreationTemplateInput struct {
	Prefix string `json:"prefix"`
}

func (h *Handler) handleDeleteRepositoryCreationTemplate(
	ctx context.Context,
	in *deleteRepositoryCreationTemplateInput,
) (*createRepositoryCreationTemplateOutput, error) {
	tmpl, err := h.Backend.DeleteRepositoryCreationTemplate(ctx, in.Prefix)
	if err != nil {
		return nil, err
	}

	return &createRepositoryCreationTemplateOutput{
		Template: toRepositoryCreationTemplateView(tmpl),
	}, nil
}

type describeRepositoryCreationTemplatesInput struct {
	Prefixes []string `json:"prefixes,omitempty"`
}

type describeRepositoryCreationTemplatesOutput struct {
	RegistryID                  string                           `json:"registryId"`
	RepositoryCreationTemplates []repositoryCreationTemplateView `json:"repositoryCreationTemplates"`
}

func (h *Handler) handleDescribeRepositoryCreationTemplates(
	ctx context.Context,
	in *describeRepositoryCreationTemplatesInput,
) (*describeRepositoryCreationTemplatesOutput, error) {
	tmpls, err := h.Backend.DescribeRepositoryCreationTemplates(ctx, in.Prefixes)
	if err != nil {
		return nil, err
	}

	out := make([]repositoryCreationTemplateView, 0, len(tmpls))
	for i := range tmpls {
		out = append(out, *toRepositoryCreationTemplateView(&tmpls[i]))
	}

	return &describeRepositoryCreationTemplatesOutput{
		RegistryID:                  h.Backend.AccountID(),
		RepositoryCreationTemplates: out,
	}, nil
}

func (h *Handler) handleUpdateRepositoryCreationTemplate(
	ctx context.Context,
	in *repositoryCreationTemplateInput,
) (*createRepositoryCreationTemplateOutput, error) {
	tmpl, err := h.Backend.UpdateRepositoryCreationTemplate(
		ctx,
		repositoryCreationTemplateFromInput(in),
	)
	if err != nil {
		return nil, err
	}

	return &createRepositoryCreationTemplateOutput{
		Template: toRepositoryCreationTemplateView(tmpl),
	}, nil
}

func toRepositoryCreationTemplateView(
	in *RepositoryCreationTemplate,
) *repositoryCreationTemplateView {
	if in == nil {
		return nil
	}

	filters := make([]imageTagMutabilityFilterView, 0, len(in.ImageTagMutabilityExclusionFilters))
	for _, filter := range in.ImageTagMutabilityExclusionFilters {
		filters = append(filters, imageTagMutabilityFilterView(filter))
	}

	out := &repositoryCreationTemplateView{
		AppliedFor:                         append([]string(nil), in.AppliedFor...),
		CreatedAt:                          float64(in.CreatedAt.Unix()),
		CustomRoleArn:                      in.CustomRoleArn,
		Description:                        in.Description,
		ImageTagMutability:                 in.ImageTagMutability,
		ImageTagMutabilityExclusionFilters: filters,
		LifecyclePolicy:                    in.LifecyclePolicy,
		Prefix:                             in.Prefix,
		RepositoryPolicy:                   in.RepositoryPolicy,
		ResourceTags:                       toTagViews(in.ResourceTags),
		UpdatedAt:                          float64(in.UpdatedAt.Unix()),
	}
	if in.EncryptionType != "" || in.KMSKey != "" {
		out.EncryptionConfiguration = &encryptionConfigurationView{
			EncryptionType: in.EncryptionType,
			KMSKey:         in.KMSKey,
		}
	}

	return out
}

func repositoryCreationTemplateFromInput(
	in *repositoryCreationTemplateInput,
) *RepositoryCreationTemplate {
	filters := make(
		[]ImageTagMutabilityExclusionFilter,
		0,
		len(in.ImageTagMutabilityExclusionFilters),
	)
	for _, filter := range in.ImageTagMutabilityExclusionFilters {
		filters = append(filters, ImageTagMutabilityExclusionFilter(filter))
	}

	tags := make(map[string]string, len(in.ResourceTags))
	for _, tag := range in.ResourceTags {
		tags[tag.Key] = tag.Value
	}

	req := &RepositoryCreationTemplate{
		Prefix:                             in.Prefix,
		Description:                        in.Description,
		ImageTagMutability:                 in.ImageTagMutability,
		ImageTagMutabilityExclusionFilters: filters,
		RepositoryPolicy:                   in.RepositoryPolicy,
		LifecyclePolicy:                    in.LifecyclePolicy,
		AppliedFor:                         in.AppliedFor,
		CustomRoleArn:                      in.CustomRoleArn,
		ResourceTags:                       tags,
	}
	if in.EncryptionConfiguration != nil {
		req.EncryptionType = in.EncryptionConfiguration.EncryptionType
		req.KMSKey = in.EncryptionConfiguration.KMSKey
	}

	return req
}
