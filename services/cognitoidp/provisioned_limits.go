package cognitoidp

import "fmt"

// limitClassAPICategory is the only LimitClass Amazon Cognito user pools API
// rate limits currently use (see LimitDefinitionType.LimitClass in the SDK
// and the "Managing provisioned limits" section of the Cognito quotas guide).
const limitClassAPICategory = "API_CATEGORY"

// accountMaxMultiplier models the Service-Quotas "account-level max limit"
// tier that sits above the provisioned limit in AWS's real two-tier model
// ("Account-level max limit: The maximum rate that you can provision, set
// through Service Quotas.") -- that ceiling is account-specific and granted by
// AWS Support on request, so there is no universal published number to mirror
// exactly. This backend documents its own stand-in assumption instead of
// silently allowing an unbounded RequestedLimitValue: an adjustable
// category's account-level max is modeled as 10x its documented default (free)
// RPS. UpdateProvisionedLimit enforces this ceiling with
// ServiceQuotaExceededException, the real exception AWS returns when a
// requested value exceeds the account's approved max.
const accountMaxMultiplier = 10

// provisionedLimitCategory describes one Cognito user pools API rate-limit
// category's real, currently-documented default (free) quota in
// requests-per-second and whether AWS allows raising it via
// UpdateProvisionedLimit. Source: "Amazon Cognito user pools API operation
// categories and request rate quotas" in the Cognito quotas developer guide
// (fetched 2026-07 against the live docs) -- every value below is the real,
// documented AWS default, not an invented number.
type provisionedLimitCategory struct {
	defaultRPS int32
	adjustable bool
}

// Real, currently-documented AWS default (free) RPS values per API rate
// category -- named so the lookup table below carries no magic numbers. Named
// per-category (rather than deduplicated by value) so each constant's name
// documents which category it belongs to at its use site.
const (
	rpsUserAuthentication     int32 = 120
	rpsUserCreation           int32 = 50
	rpsUserFederation         int32 = 25
	rpsUserAccountRecovery    int32 = 30
	rpsUserRead               int32 = 120
	rpsUserUpdate             int32 = 25
	rpsUserToken              int32 = 120
	rpsUserResourceRead       int32 = 50
	rpsUserResourceUpdate     int32 = 25
	rpsUserList               int32 = 30
	rpsUserPoolRead           int32 = 15
	rpsUserPoolUpdate         int32 = 15
	rpsUserPoolResourceRead   int32 = 20
	rpsUserPoolResourceUpdate int32 = 15
	rpsUserPoolClientRead     int32 = 15
	rpsUserPoolClientUpdate   int32 = 15
	rpsClientAuthentication   int32 = 150
	rpsLimitManagement        int32 = 1
)

//nolint:gochecknoglobals // static lookup table of AWS-documented category defaults
var provisionedLimitCategories = map[string]provisionedLimitCategory{
	"UserAuthentication":     {defaultRPS: rpsUserAuthentication, adjustable: true},
	"UserCreation":           {defaultRPS: rpsUserCreation, adjustable: true},
	"UserFederation":         {defaultRPS: rpsUserFederation, adjustable: true},
	"UserAccountRecovery":    {defaultRPS: rpsUserAccountRecovery, adjustable: false},
	"UserRead":               {defaultRPS: rpsUserRead, adjustable: true},
	"UserUpdate":             {defaultRPS: rpsUserUpdate, adjustable: false},
	"UserToken":              {defaultRPS: rpsUserToken, adjustable: true},
	"UserResourceRead":       {defaultRPS: rpsUserResourceRead, adjustable: true},
	"UserResourceUpdate":     {defaultRPS: rpsUserResourceUpdate, adjustable: false},
	"UserList":               {defaultRPS: rpsUserList, adjustable: false},
	"UserPoolRead":           {defaultRPS: rpsUserPoolRead, adjustable: false},
	"UserPoolUpdate":         {defaultRPS: rpsUserPoolUpdate, adjustable: false},
	"UserPoolResourceRead":   {defaultRPS: rpsUserPoolResourceRead, adjustable: false},
	"UserPoolResourceUpdate": {defaultRPS: rpsUserPoolResourceUpdate, adjustable: false},
	"UserPoolClientRead":     {defaultRPS: rpsUserPoolClientRead, adjustable: false},
	"UserPoolClientUpdate":   {defaultRPS: rpsUserPoolClientUpdate, adjustable: false},
	"ClientAuthentication":   {defaultRPS: rpsClientAuthentication, adjustable: false},
	"LimitManagement":        {defaultRPS: rpsLimitManagement, adjustable: false},
}

// resolveProvisionedLimitCategory validates a LimitDefinition's LimitClass and
// Category attribute, returning the category's documented static defaults.
func resolveProvisionedLimitCategory(
	limitClass string, attributes map[string]string,
) (string, provisionedLimitCategory, error) {
	if limitClass != limitClassAPICategory {
		return "", provisionedLimitCategory{}, fmt.Errorf(
			"%w: LimitClass must be %q, got %q", ErrInvalidParameter, limitClassAPICategory, limitClass,
		)
	}

	category := attributes["Category"]

	def, ok := provisionedLimitCategories[category]
	if !ok {
		return "", provisionedLimitCategory{}, fmt.Errorf(
			"%w: unknown API_CATEGORY Category %q", ErrInvalidParameter, category,
		)
	}

	return category, def, nil
}

// ProvisionedLimit is the (LimitDefinition, current values) pair returned by
// GetProvisionedLimit/UpdateProvisionedLimit.
type ProvisionedLimit struct {
	LimitClass            string
	Category              string
	FreeLimitValue        int32
	ProvisionedLimitValue int32
}

// GetProvisionedLimit returns the current provisioned and default (free)
// values for a Cognito user pools API rate-limit category. Provisioned
// limits are account-level (and Region-level) resources, not per-user-pool --
// see "Managing provisioned limits" in the Cognito quotas guide:
// "Provisioned limits are account-level resources. They apply to the
// aggregate rate of all requests from all user pools in one AWS Region in
// your AWS account." This backend models exactly one account+Region, so no
// user pool ID is involved and no pool-existence check applies.
func (b *InMemoryBackend) GetProvisionedLimit(
	limitClass string, attributes map[string]string,
) (*ProvisionedLimit, error) {
	b.mu.RLock("GetProvisionedLimit")
	defer b.mu.RUnlock()

	category, def, err := resolveProvisionedLimitCategory(limitClass, attributes)
	if err != nil {
		return nil, err
	}

	provisioned, ok := b.provisionedLimits[category]
	if !ok {
		provisioned = def.defaultRPS
	}

	return &ProvisionedLimit{
		LimitClass:            limitClassAPICategory,
		Category:              category,
		FreeLimitValue:        def.defaultRPS,
		ProvisionedLimitValue: provisioned,
	}, nil
}

// UpdateProvisionedLimit sets the provisioned rate for an adjustable API rate
// category. Only categories AWS documents as "Adjustable: Yes" may be
// provisioned -- every other category always enforces its fixed default, and
// UpdateProvisionedLimit on it is rejected ("Only adjustable quota categories
// support provisioning."). requested must fall between 0 and the category's
// modeled account-level max (see accountMaxMultiplier). AWS's own real
// constraint (per the Cognito quotas guide) is that the requested value must
// fall between the default limit and the account's Service Quotas
// account-level max limit.
func (b *InMemoryBackend) UpdateProvisionedLimit(
	limitClass string, attributes map[string]string, requested int32,
) (*ProvisionedLimit, error) {
	b.mu.Lock("UpdateProvisionedLimit")
	defer b.mu.Unlock()

	category, def, err := resolveProvisionedLimitCategory(limitClass, attributes)
	if err != nil {
		return nil, err
	}

	if !def.adjustable {
		return nil, fmt.Errorf("%w: API_CATEGORY %q is not adjustable", ErrInvalidParameter, category)
	}

	if requested < 0 {
		return nil, fmt.Errorf("%w: RequestedLimitValue must be >= 0, got %d", ErrInvalidParameter, requested)
	}

	accountMax := def.defaultRPS * accountMaxMultiplier
	if requested > accountMax {
		return nil, fmt.Errorf(
			"%w: RequestedLimitValue %d exceeds the account-level max limit %d for %q",
			ErrServiceQuotaExceeded, requested, accountMax, category,
		)
	}

	b.provisionedLimits[category] = requested

	return &ProvisionedLimit{
		LimitClass:            limitClassAPICategory,
		Category:              category,
		FreeLimitValue:        def.defaultRPS,
		ProvisionedLimitValue: requested,
	}, nil
}
