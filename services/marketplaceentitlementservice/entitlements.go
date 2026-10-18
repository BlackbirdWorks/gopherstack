package marketplaceentitlementservice

import (
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// Filter names accepted by GetEntitlements (the SDK's GetEntitlementFilterName enum).
const (
	filterCustomerIdentifier = "CUSTOMER_IDENTIFIER"
	filterDimension          = "DIMENSION"
	filterCustomerAccount    = "CUSTOMER_AWS_ACCOUNT_ID"
	filterLicenseArn         = "LICENSE_ARN"

	maxProductCodeLen = 255
	maxFilterValues   = 100
)

// GrantEntitlement stores or replaces the entitlement for its product, customer and dimension.
// Entitlements only come from here: the Marketplace Metering backend derives them from customer
// subscriptions (ResolveCustomer), and tests may seed them directly.
func (b *InMemoryBackend) GrantEntitlement(e Entitlement) error {
	if e.ProductCode == "" || e.Dimension == "" || (e.CustomerIdentifier == "" && e.CustomerAWSAccountID == "") {
		return fail(ErrInvalidParameter, "ProductCode, Dimension and a customer are required")
	}

	if e.Value == nil {
		return fail(ErrInvalidParameter, "Value is required")
	}

	b.mu.Lock("GrantEntitlement")
	defer b.mu.Unlock()

	cp := e
	b.entitlements.Put(&cp)

	return nil
}

// RevokeEntitlements removes every entitlement of a product held by one customer.
func (b *InMemoryBackend) RevokeEntitlements(productCode, customerIdentifier string) {
	b.mu.Lock("RevokeEntitlements")
	defer b.mu.Unlock()

	for _, e := range b.entitlements.All() {
		if e.ProductCode == productCode && e.CustomerIdentifier == customerIdentifier {
			b.entitlements.Delete(entitlementKey(e))
		}
	}
}

// GetEntitlementsInput is the GetEntitlements request.
type GetEntitlementsInput struct {
	Filter      map[string][]string
	MaxResults  *int32
	ProductCode string
	NextToken   string
}

// GetEntitlementsOutput is the GetEntitlements response.
type GetEntitlementsOutput struct {
	NextToken    string `json:",omitempty"`
	Entitlements []Entitlement
}

func validateFilter(filter map[string][]string) error {
	for name, values := range filter {
		switch name {
		case filterCustomerIdentifier, filterDimension, filterCustomerAccount, filterLicenseArn:
		default:
			return fail(ErrInvalidParameter, "Filter key %q is not supported", name)
		}

		if len(values) == 0 || len(values) > maxFilterValues {
			return fail(ErrInvalidParameter, "Filter %s needs between 1 and %d values", name, maxFilterValues)
		}

		if slices.Contains(values, "") {
			return fail(ErrInvalidParameter, "Filter %s values must not be empty", name)
		}
	}

	_, hasID := filter[filterCustomerIdentifier]
	_, hasAcct := filter[filterCustomerAccount]

	if hasID && hasAcct {
		return fail(ErrInvalidParameter, "CUSTOMER_IDENTIFIER and CUSTOMER_AWS_ACCOUNT_ID are mutually exclusive")
	}

	return nil
}

func matches(e *Entitlement, filter map[string][]string) bool {
	fields := map[string]string{
		filterCustomerIdentifier: e.CustomerIdentifier,
		filterDimension:          e.Dimension,
		filterCustomerAccount:    e.CustomerAWSAccountID,
		filterLicenseArn:         e.LicenseArn,
	}

	for name, values := range filter {
		if !slices.Contains(values, fields[name]) {
			return false
		}
	}

	return true
}

// GetEntitlements lists a product's entitlements; values of one filter key are unioned, keys are intersected.
func (b *InMemoryBackend) GetEntitlements(in *GetEntitlementsInput) (*GetEntitlementsOutput, error) {
	if in.ProductCode == "" || len(in.ProductCode) > maxProductCodeLen {
		return nil, fail(ErrInvalidParameter, "ProductCode must be 1-%d characters", maxProductCodeLen)
	}

	if err := validateFilter(in.Filter); err != nil {
		return nil, err
	}

	limit := 0

	if in.MaxResults != nil {
		if *in.MaxResults < 1 || *in.MaxResults > maxPageSize {
			return nil, fail(ErrInvalidParameter, "MaxResults must be between 1 and %d", maxPageSize)
		}

		limit = int(*in.MaxResults)
	}

	if err := page.ValidateToken(in.NextToken); err != nil {
		return nil, fail(ErrInvalidParameter, "NextToken is not valid")
	}

	b.mu.RLock("GetEntitlements")

	var all []Entitlement

	for _, e := range b.entitlements.All() {
		if e.ProductCode == in.ProductCode && matches(e, in.Filter) {
			all = append(all, *e)
		}
	}

	b.mu.RUnlock()

	slices.SortFunc(all, func(a, c Entitlement) int {
		return strings.Compare(entitlementKey(&a), entitlementKey(&c))
	})

	p := page.New(all, in.NextToken, limit, defaultPageSize)

	return &GetEntitlementsOutput{Entitlements: p.Data, NextToken: p.Next}, nil
}
