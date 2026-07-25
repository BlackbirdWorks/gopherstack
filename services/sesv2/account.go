package sesv2

import "fmt"

// ---- account ----

// Real types.PricingPlan enum values (aws-sdk-go-v2/service/sesv2/types/enums.go).
const (
	pricingPlanNone       = "NONE"
	pricingPlanEssentials = "ESSENTIALS"
	pricingPlanPro        = "PRO"
	pricingPlanEnterprise = "ENTERPRISE"
)

// AccountDetails stores account-level SESv2 settings.
type AccountDetails struct {
	VdmAttributes   map[string]any `json:"vdmAttributes,omitempty"`
	MailType        string         `json:"mailType"`
	WebsiteURL      string         `json:"websiteURL"`
	ContactLanguage string         `json:"contactLanguage"`
	UseCaseName     string         `json:"useCaseName"`
	// PricingPlan is the account's currently active pricing plan (Plan of
	// PutAccountPricingAttributesInput / PricingAttributes.CurrentPlan of
	// GetAccountOutput). gopherstack has no billing-cycle concept, so a Put
	// takes effect immediately as the current plan -- there is no separate
	// "scheduled next plan" to track, unlike real SES v2.
	PricingPlan           string   `json:"pricingPlan,omitempty"`
	SuppressionAttributes []string `json:"suppressionAttributes,omitempty"`
	SendingEnabled        bool     `json:"sendingEnabled"`
	AutoWarmupEnabled     bool     `json:"autoWarmupEnabled,omitempty"`
}

// GetAccount returns the account details.
func (b *InMemoryBackend) GetAccount() (*AccountDetails, error) {
	b.mu.RLock("GetAccount")
	defer b.mu.RUnlock()

	if b.accountDetails == nil {
		return &AccountDetails{SendingEnabled: true}, nil
	}

	cp := *b.accountDetails

	return &cp, nil
}

// PutAccountDetails stores account details.
func (b *InMemoryBackend) PutAccountDetails(details *AccountDetails) error {
	b.mu.Lock("PutAccountDetails")
	defer b.mu.Unlock()

	cp := *details
	b.accountDetails = &cp

	return nil
}

// PutAccountSendingAttributes sets the sending enabled flag.
func (b *InMemoryBackend) PutAccountSendingAttributes(sendingEnabled bool) error {
	b.mu.Lock("PutAccountSendingAttributes")
	defer b.mu.Unlock()

	if b.accountDetails == nil {
		b.accountDetails = &AccountDetails{}
	}

	b.accountDetails.SendingEnabled = sendingEnabled

	return nil
}

func (b *InMemoryBackend) PutAccountSuppressionAttributes(suppressedReasons []string) error {
	b.mu.Lock("PutAccountSuppressionAttributes")
	defer b.mu.Unlock()

	if b.accountDetails == nil {
		b.accountDetails = &AccountDetails{}
	}

	reasons := make([]string, len(suppressedReasons))
	copy(reasons, suppressedReasons)
	b.accountDetails.SuppressionAttributes = reasons

	return nil
}

// PutAccountPricingAttributes sets the account's active pricing plan.
// Real SES v2 validates Plan against the PricingPlan enum (NONE/ESSENTIALS/
// PRO/ENTERPRISE); an unrecognized value is a BadRequestException.
func (b *InMemoryBackend) PutAccountPricingAttributes(plan string) error {
	switch plan {
	case pricingPlanNone, pricingPlanEssentials, pricingPlanPro, pricingPlanEnterprise:
	default:
		return fmt.Errorf(
			"%w: Plan must be one of %s, %s, %s, %s, got %q",
			ErrInvalidInput, pricingPlanNone, pricingPlanEssentials, pricingPlanPro, pricingPlanEnterprise, plan,
		)
	}

	b.mu.Lock("PutAccountPricingAttributes")
	defer b.mu.Unlock()

	if b.accountDetails == nil {
		b.accountDetails = &AccountDetails{}
	}

	b.accountDetails.PricingPlan = plan

	return nil
}

func (b *InMemoryBackend) PutAccountDedicatedIPWarmupAttributes(autoWarmupEnabled bool) error {
	b.mu.Lock("PutAccountDedicatedIPWarmupAttributes")
	defer b.mu.Unlock()

	if b.accountDetails == nil {
		b.accountDetails = &AccountDetails{}
	}

	b.accountDetails.AutoWarmupEnabled = autoWarmupEnabled

	return nil
}

// GetBlacklistReports returns empty blacklist reports.
func (b *InMemoryBackend) GetBlacklistReports() (map[string][]string, error) {
	return map[string][]string{}, nil
}
