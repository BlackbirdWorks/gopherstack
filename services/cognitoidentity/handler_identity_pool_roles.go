package cognitoidentity

import (
	"context"
	"fmt"
)

type roleMappingRuleInput struct {
	Claim     string `json:"Claim"`
	MatchType string `json:"MatchType"`
	Value     string `json:"Value"`
	RoleARN   string `json:"RoleARN"`
}

type rulesConfigurationInput struct {
	Rules []roleMappingRuleInput `json:"Rules"`
}

type roleMappingInput struct {
	RulesConfiguration      *rulesConfigurationInput `json:"RulesConfiguration,omitempty"`
	Type                    string                   `json:"Type"`
	AmbiguousRoleResolution string                   `json:"AmbiguousRoleResolution,omitempty"`
}

type setIdentityPoolRolesInput struct {
	Roles          map[string]string           `json:"Roles"`
	RoleMappings   map[string]roleMappingInput `json:"RoleMappings"`
	IdentityPoolID string                      `json:"IdentityPoolId"`
}

type setIdentityPoolRolesOutput struct{}

func (h *Handler) handleSetIdentityPoolRoles(
	ctx context.Context,
	in *setIdentityPoolRolesInput,
) (*setIdentityPoolRolesOutput, error) {
	if in.IdentityPoolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	if in.Roles == nil {
		return nil, fmt.Errorf(
			"%w: Roles is required",
			ErrInvalidParameter,
		)
	}

	var roleMappings map[string]RoleMapping
	if len(in.RoleMappings) > 0 {
		roleMappings = make(map[string]RoleMapping, len(in.RoleMappings))

		for provider, rm := range in.RoleMappings {
			backendRM := RoleMapping{
				Type:                    rm.Type,
				AmbiguousRoleResolution: rm.AmbiguousRoleResolution,
			}

			if rm.RulesConfiguration != nil {
				rules := make([]MappingRule, len(rm.RulesConfiguration.Rules))

				for i, r := range rm.RulesConfiguration.Rules {
					rules[i] = MappingRule(r)
				}

				backendRM.RulesConfiguration = &RulesConfiguration{Rules: rules}
			}

			roleMappings[provider] = backendRM
		}
	}

	if err := h.Backend.SetIdentityPoolRoles(
		ctx,
		in.IdentityPoolID,
		in.Roles["authenticated"],
		in.Roles["unauthenticated"],
		roleMappings,
	); err != nil {
		return nil, err
	}

	return &setIdentityPoolRolesOutput{}, nil
}

type getIdentityPoolRolesInput struct {
	IdentityPoolID string `json:"IdentityPoolId"`
}

type roleMappingRuleOutput struct {
	Claim     string `json:"Claim"`
	MatchType string `json:"MatchType"`
	Value     string `json:"Value"`
	RoleARN   string `json:"RoleARN"`
}

type rulesConfigurationOutput struct {
	Rules []roleMappingRuleOutput `json:"Rules"`
}

type roleMappingOutput struct {
	RulesConfiguration      *rulesConfigurationOutput `json:"RulesConfiguration,omitempty"`
	Type                    string                    `json:"Type"`
	AmbiguousRoleResolution string                    `json:"AmbiguousRoleResolution,omitempty"`
}

type getIdentityPoolRolesOutput struct {
	Roles          map[string]string            `json:"Roles"`
	RoleMappings   map[string]roleMappingOutput `json:"RoleMappings,omitempty"`
	IdentityPoolID string                       `json:"IdentityPoolId"`
}

func (h *Handler) handleGetIdentityPoolRoles(
	ctx context.Context,
	in *getIdentityPoolRolesInput,
) (*getIdentityPoolRolesOutput, error) {
	if in.IdentityPoolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	roles, err := h.Backend.GetIdentityPoolRoles(ctx, in.IdentityPoolID)
	if err != nil {
		return nil, err
	}

	// Only include roles that have been set (omit empty strings like AWS does).
	const maxRoleTypes = 2
	rolesMap := make(map[string]string, maxRoleTypes)
	if roles.AuthenticatedRoleARN != "" {
		rolesMap["authenticated"] = roles.AuthenticatedRoleARN
	}
	if roles.UnauthenticatedRoleARN != "" {
		rolesMap["unauthenticated"] = roles.UnauthenticatedRoleARN
	}

	var outMappings map[string]roleMappingOutput
	if len(roles.RoleMappings) > 0 {
		outMappings = make(map[string]roleMappingOutput, len(roles.RoleMappings))

		for provider, rm := range roles.RoleMappings {
			outRM := roleMappingOutput{
				Type:                    rm.Type,
				AmbiguousRoleResolution: rm.AmbiguousRoleResolution,
			}

			if rm.RulesConfiguration != nil {
				rules := make([]roleMappingRuleOutput, len(rm.RulesConfiguration.Rules))

				for i, r := range rm.RulesConfiguration.Rules {
					rules[i] = roleMappingRuleOutput(r)
				}

				outRM.RulesConfiguration = &rulesConfigurationOutput{Rules: rules}
			}

			outMappings[provider] = outRM
		}
	}

	return &getIdentityPoolRolesOutput{
		IdentityPoolID: in.IdentityPoolID,
		Roles:          rolesMap,
		RoleMappings:   outMappings,
	}, nil
}

type getPrincipalTagAttributeMapInput struct {
	IdentityPoolID       string `json:"IdentityPoolId"`
	IdentityProviderName string `json:"IdentityProviderName"`
}

type getPrincipalTagAttributeMapOutput struct {
	PrincipalTags        map[string]string `json:"PrincipalTags,omitempty"`
	IdentityPoolID       string            `json:"IdentityPoolId"`
	IdentityProviderName string            `json:"IdentityProviderName"`
	UseDefaults          bool              `json:"UseDefaults"`
}

func (h *Handler) handleGetPrincipalTagAttributeMap(
	ctx context.Context,
	in *getPrincipalTagAttributeMapInput,
) (*getPrincipalTagAttributeMapOutput, error) {
	mapping, err := h.Backend.GetPrincipalTagAttributeMap(ctx, in.IdentityPoolID, in.IdentityProviderName)
	if err != nil {
		return nil, err
	}

	return &getPrincipalTagAttributeMapOutput{
		IdentityPoolID:       in.IdentityPoolID,
		IdentityProviderName: in.IdentityProviderName,
		UseDefaults:          mapping.UseDefaults,
		PrincipalTags:        mapping.PrincipalTags,
	}, nil
}

type setPrincipalTagAttributeMapInput struct {
	PrincipalTags        map[string]string `json:"PrincipalTags"`
	IdentityPoolID       string            `json:"IdentityPoolId"`
	IdentityProviderName string            `json:"IdentityProviderName"`
	UseDefaults          bool              `json:"UseDefaults"`
}

type setPrincipalTagAttributeMapOutput struct {
	PrincipalTags        map[string]string `json:"PrincipalTags,omitempty"`
	IdentityPoolID       string            `json:"IdentityPoolId"`
	IdentityProviderName string            `json:"IdentityProviderName"`
	UseDefaults          bool              `json:"UseDefaults"`
}

func (h *Handler) handleSetPrincipalTagAttributeMap(
	ctx context.Context,
	in *setPrincipalTagAttributeMapInput,
) (*setPrincipalTagAttributeMapOutput, error) {
	mapping, err := h.Backend.SetPrincipalTagAttributeMap(
		ctx,
		in.IdentityPoolID,
		in.IdentityProviderName,
		in.UseDefaults,
		in.PrincipalTags,
	)
	if err != nil {
		return nil, err
	}

	return &setPrincipalTagAttributeMapOutput{
		IdentityPoolID:       in.IdentityPoolID,
		IdentityProviderName: in.IdentityProviderName,
		UseDefaults:          mapping.UseDefaults,
		PrincipalTags:        mapping.PrincipalTags,
	}, nil
}
