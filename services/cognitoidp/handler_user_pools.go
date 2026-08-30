package cognitoidp

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// poolToData converts a UserPool to the userPoolData wire format.
func poolToData(pool *UserPool) userPoolData {
	return userPoolData{
		ID:                 pool.ID,
		Name:               pool.Name,
		ARN:                pool.ARN,
		CreationDate:       float64(pool.CreatedAt.Unix()),
		LastModifiedDate:   float64(pool.CreatedAt.Unix()),
		DeletionProtection: "INACTIVE",
		MfaConfiguration:   mfaConfigOrDefault(pool.MfaConfiguration),
		SchemaAttributes:   sortedCustomAttributes(pool.CustomAttributes),
	}
}

func (h *Handler) handleListUserPools(
	_ context.Context,
	in *listUserPoolsInput,
) (*listUserPoolsOutput, error) {
	limit, err := validateCognitoMaxResults(in.MaxResults)
	if err != nil {
		return nil, err
	}

	// ListUserPools already returns pools sorted by Name, giving a stable
	// ordering for pagination tokens.
	pools := h.Backend.ListUserPools()

	// A miss (the pool the token named was deleted) defaults start to the
	// end of the collection: leaving it at 0 would resume a stale cursor at
	// page one, forever.
	start := 0
	if in.NextToken != "" {
		start = len(pools)

		for i, p := range pools {
			if p.ID == in.NextToken {
				start = i

				break
			}
		}
	}

	pools = pools[start:]

	nextToken := ""
	if len(pools) > limit {
		nextToken = pools[limit].ID
		pools = pools[:limit]
	}

	items := make([]userPoolData, 0, len(pools))
	for _, p := range pools {
		items = append(items, poolToData(p))
	}

	return &listUserPoolsOutput{UserPools: items, NextToken: nextToken}, nil
}

func (h *Handler) handleDeleteUserPool(_ context.Context, in *deleteUserPoolInput) (*deleteUserPoolOutput, error) {
	if err := h.Backend.DeleteUserPool(in.UserPoolID); err != nil {
		return nil, err
	}

	return &deleteUserPoolOutput{}, nil
}

const (
	defaultPasswordMinLength     = 8
	defaultTempPasswordValidDays = 7
)

// defaultPasswordPolicyData returns the AWS Cognito default password policy when none is configured.
func defaultPasswordPolicyData() *passwordPolicyData {
	return &passwordPolicyData{
		MinimumLength:                 defaultPasswordMinLength,
		RequireUppercase:              true,
		RequireLowercase:              true,
		RequireNumbers:                true,
		RequireSymbols:                true,
		TemporaryPasswordValidityDays: defaultTempPasswordValidDays,
	}
}

func poolToAccurateData(pool *UserPool) userPoolDataAccurate {
	lastModified := pool.CreatedAt
	if !pool.UpdatedAt.IsZero() {
		lastModified = pool.UpdatedAt
	}

	deletionProtection := pool.DeletionProtection
	if deletionProtection == "" {
		deletionProtection = "INACTIVE"
	}

	data := userPoolDataAccurate{
		ID:                     pool.ID,
		Name:                   pool.Name,
		ARN:                    pool.ARN,
		CreationDate:           float64(pool.CreatedAt.Unix()),
		LastModifiedDate:       float64(lastModified.Unix()),
		DeletionProtection:     deletionProtection,
		MfaConfiguration:       mfaConfigOrDefault(pool.MfaConfiguration),
		SchemaAttributes:       sortedCustomAttributes(pool.CustomAttributes),
		AutoVerifiedAttributes: pool.AutoVerifiedAttributes,
		LambdaConfig:           pool.LambdaConfig,
		EmailConfiguration:     pool.EmailConfiguration,
		AccountRecoverySetting: pool.AccountRecoverySetting,
	}

	if pool.PasswordPolicy != nil {
		data.Policies.PasswordPolicy = &passwordPolicyData{
			MinimumLength:                 pool.PasswordPolicy.MinimumLength,
			RequireUppercase:              pool.PasswordPolicy.RequireUppercase,
			RequireLowercase:              pool.PasswordPolicy.RequireLowercase,
			RequireNumbers:                pool.PasswordPolicy.RequireNumbers,
			RequireSymbols:                pool.PasswordPolicy.RequireSymbols,
			TemporaryPasswordValidityDays: pool.PasswordPolicy.TemporaryPasswordValidityDays,
		}
	} else {
		data.Policies.PasswordPolicy = defaultPasswordPolicyData()
	}

	return data
}

func (h *Handler) handleCreateUserPoolWithOpts(
	_ context.Context,
	in *createUserPoolWithOptsInput,
) (*createUserPoolWithOptsOutput, error) {
	opts := UserPoolOptions{
		AutoVerifiedAttributes: in.AutoVerifiedAttributes,
		LambdaConfig:           in.LambdaConfig,
		EmailConfiguration:     in.EmailConfiguration,
		AccountRecoverySetting: in.AccountRecoverySetting,
		DeletionProtection:     in.DeletionProtection,
		MfaConfiguration:       in.MfaConfiguration,
	}

	if in.Policies != nil && in.Policies.PasswordPolicy != nil {
		pp := in.Policies.PasswordPolicy
		opts.PasswordPolicy = &PasswordPolicy{
			MinimumLength:                 pp.MinimumLength,
			RequireUppercase:              pp.RequireUppercase,
			RequireLowercase:              pp.RequireLowercase,
			RequireNumbers:                pp.RequireNumbers,
			RequireSymbols:                pp.RequireSymbols,
			TemporaryPasswordValidityDays: pp.TemporaryPasswordValidityDays,
		}
		if err := validatePasswordPolicy(opts.PasswordPolicy); err != nil {
			return nil, err
		}
	}

	pool, err := h.Backend.CreateUserPoolWithOpts(in.PoolName, opts)
	if err != nil {
		return nil, err
	}

	if len(in.UserPoolTags) > 0 {
		h.Backend.TagResource(pool.ARN, in.UserPoolTags)
	}

	return &createUserPoolWithOptsOutput{UserPool: poolToAccurateData(pool)}, nil
}

func (h *Handler) handleUpdateUserPoolWithOpts(
	_ context.Context,
	in *updateUserPoolWithOptsInput,
) (*updateUserPoolWithOptsOutput, error) {
	opts := UserPoolOptions{
		AutoVerifiedAttributes: in.AutoVerifiedAttributes,
		LambdaConfig:           in.LambdaConfig,
		EmailConfiguration:     in.EmailConfiguration,
		AccountRecoverySetting: in.AccountRecoverySetting,
		DeletionProtection:     in.DeletionProtection,
	}

	if in.Policies != nil && in.Policies.PasswordPolicy != nil {
		pp := in.Policies.PasswordPolicy
		opts.PasswordPolicy = &PasswordPolicy{
			MinimumLength:                 pp.MinimumLength,
			RequireUppercase:              pp.RequireUppercase,
			RequireLowercase:              pp.RequireLowercase,
			RequireNumbers:                pp.RequireNumbers,
			RequireSymbols:                pp.RequireSymbols,
			TemporaryPasswordValidityDays: pp.TemporaryPasswordValidityDays,
		}
		if err := validatePasswordPolicy(opts.PasswordPolicy); err != nil {
			return nil, err
		}
	}

	if err := h.Backend.UpdateUserPoolWithOpts(in.UserPoolID, in.MfaConfiguration, opts); err != nil {
		return nil, err
	}

	return &updateUserPoolWithOptsOutput{}, nil
}

func (h *Handler) handleDescribeUserPoolAccurate(
	_ context.Context,
	in *describeUserPoolAccurateInput,
) (*describeUserPoolAccurateOutput, error) {
	pool, err := h.Backend.DescribeUserPool(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	return &describeUserPoolAccurateOutput{UserPool: poolToAccurateData(pool)}, nil
}

// Additional op name constants for batch-2.
const (
	opGetUserPoolMfaConfig       = "GetUserPoolMfaConfig"
	opSetUserPoolMfaConfig       = "SetUserPoolMfaConfig"
	opGetUICustomization         = "GetUICustomization"
	opSetUICustomization         = "SetUICustomization"
	opGetUserAttributeVerifCode  = "GetUserAttributeVerificationCode"
	opVerifyUserAttribute        = "VerifyUserAttribute"
	opCreateIdentityProvider     = "CreateIdentityProvider"
	opUpdateIdentityProvider     = "UpdateIdentityProvider"
	opDescribeIdentityProvider   = "DescribeIdentityProvider"
	opGetIdentityProviderByIdent = "GetIdentityProviderByIdentifier"
	opListIdentityProviders      = "ListIdentityProviders"
	opCreateUserPoolDomain       = "CreateUserPoolDomain"
	opUpdateUserPoolDomain       = "UpdateUserPoolDomain"
	opDescribeRiskConfiguration  = "DescribeRiskConfiguration"
	opSetRiskConfiguration       = "SetRiskConfiguration"
	opAdminCreateUser            = "AdminCreateUser"
	opAdminSetUserPassword       = "AdminSetUserPassword"
	opCreateGroup                = "CreateGroup"
	opUpdateGroup                = "UpdateGroup"
	opGetGroup                   = "GetGroup"
	opListGroups                 = "ListGroups"
	opListUsersInGroup           = "ListUsersInGroup"
)

func (h *Handler) handleGetUserPoolMfaConfigFull(
	_ context.Context,
	in *getUserPoolMfaConfigFullInput,
) (*getUserPoolMfaConfigFullOutput, error) {
	cfg, err := h.Backend.GetUserPoolMfaConfigFull(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	out := &getUserPoolMfaConfigFullOutput{MfaConfiguration: cfg.MfaConfiguration}
	if out.MfaConfiguration == "" {
		out.MfaConfiguration = mfaConfigOFF
	}

	if cfg.SmsMfaConfiguration != nil {
		out.SmsMfaConfiguration = &smsMfaConfigJSON{
			SmsAuthenticationMessage: cfg.SmsMfaConfiguration.SmsAuthenticationMessage,
		}

		if cfg.SmsMfaConfiguration.SmsConfiguration != nil {
			out.SmsMfaConfiguration.SmsConfiguration = &smsConfigJSON{
				SnsCallerArn: cfg.SmsMfaConfiguration.SmsConfiguration.SnsCallerArn,
				SnsRegion:    cfg.SmsMfaConfiguration.SmsConfiguration.SnsRegion,
				ExternalID:   cfg.SmsMfaConfiguration.SmsConfiguration.ExternalID,
			}
		}
	}

	if cfg.SoftwareTokenMfa != nil {
		out.SoftwareTokenMfaConfiguration = &softwareTokenMfaConfigJSON{Enabled: cfg.SoftwareTokenMfa.Enabled}
	}

	if cfg.EmailMfaConfiguration != nil {
		out.EmailMfaConfiguration = &emailMfaConfigJSON{
			Message: cfg.EmailMfaConfiguration.Message,
			Subject: cfg.EmailMfaConfiguration.Subject,
		}
	}

	return out, nil
}

func (h *Handler) handleSetUserPoolMfaConfigFull(
	_ context.Context,
	in *setUserPoolMfaConfigFullInput,
) (*setUserPoolMfaConfigFullOutput, error) {
	cfg := UserPoolMfaFullConfig{
		MfaConfiguration: in.MfaConfiguration,
	}

	if in.SmsMfaConfiguration != nil {
		smsCfg := &SmsMfaConfiguration{
			SmsAuthenticationMessage: in.SmsMfaConfiguration.SmsAuthenticationMessage,
		}

		if in.SmsMfaConfiguration.SmsConfiguration != nil {
			smsCfg.SmsConfiguration = &SmsConfiguration{
				SnsCallerArn: in.SmsMfaConfiguration.SmsConfiguration.SnsCallerArn,
				SnsRegion:    in.SmsMfaConfiguration.SmsConfiguration.SnsRegion,
				ExternalID:   in.SmsMfaConfiguration.SmsConfiguration.ExternalID,
			}
		}

		cfg.SmsMfaConfiguration = smsCfg
	}

	if in.SoftwareTokenMfaConfiguration != nil {
		cfg.SoftwareTokenMfa = &SoftwareTokenMfaConfiguration{Enabled: in.SoftwareTokenMfaConfiguration.Enabled}
	}

	if in.EmailMfaConfiguration != nil {
		cfg.EmailMfaConfiguration = &EmailMfaConfiguration{
			Message: in.EmailMfaConfiguration.Message,
			Subject: in.EmailMfaConfiguration.Subject,
		}
	}

	if err := h.Backend.SetUserPoolMfaConfigFull(in.UserPoolID, cfg); err != nil {
		return nil, err
	}

	out := &setUserPoolMfaConfigFullOutput{MfaConfiguration: in.MfaConfiguration}
	out.SmsMfaConfiguration = in.SmsMfaConfiguration
	out.SoftwareTokenMfaConfiguration = in.SoftwareTokenMfaConfiguration
	out.EmailMfaConfiguration = in.EmailMfaConfiguration

	return out, nil
}

// userPoolsOpsA registers the ops in this file that have no accurate twin in
// userPoolsOpsB/C (CreateUserPool, DescribeUserPool, UpdateUserPool,
// GetUserPoolMfaConfig, SetUserPoolMfaConfig all moved there only -- see
// de-stub-hygiene note in PARITY.md).
func (h *Handler) userPoolsOpsA() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"ListUserPools":  service.WrapOp(h.handleListUserPools),
		"DeleteUserPool": service.WrapOp(h.handleDeleteUserPool),
	}
}

func (h *Handler) userPoolsOpsB() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateUserPool:   wrapAccuracy(h.handleCreateUserPoolWithOpts),
		opUpdateUserPool:   wrapAccuracy(h.handleUpdateUserPoolWithOpts),
		opDescribeUserPool: wrapAccuracy(h.handleDescribeUserPoolAccurate),
	}
}

func (h *Handler) userPoolsOpsC() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opGetUserPoolMfaConfig: wrapAccuracy(h.handleGetUserPoolMfaConfigFull),
		opSetUserPoolMfaConfig: wrapAccuracy(h.handleSetUserPoolMfaConfigFull),
	}
}
