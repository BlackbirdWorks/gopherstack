package marketplacemetering

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const licenseIDLen = 24

// LicenseSpec grants a license of a product to a customer account.
type LicenseSpec struct {
	ActivatedAt          *awstime.EpochTime `json:"activatedAt,omitempty"`
	ExpiresAt            *awstime.EpochTime `json:"expiresAt,omitempty"`
	ProductCode          string             `json:"productCode"`
	CustomerAWSAccountID string             `json:"customerAwsAccountId"`
}

// RegisterLicense grants a license and subscribes the customer to the product, which grants the entitlements.
func (b *InMemoryBackend) RegisterLicense(spec LicenseSpec) (*License, error) {
	if !validProductCode(spec.ProductCode) {
		return nil, fail(ErrInvalidProductCode, "ProductCode is not valid")
	}

	if !validAccountID(spec.CustomerAWSAccountID) {
		return nil, fail(ErrInvalidCustomerIdentifier, "customer account %q is not a 12-digit account ID",
			spec.CustomerAWSAccountID)
	}

	b.mu.Lock("RegisterLicense")
	defer b.mu.Unlock()

	activated := awstime.NewEpochTime(b.now().Add(-meterPastWindow))
	if spec.ActivatedAt != nil {
		activated = *spec.ActivatedAt
	}

	if spec.ExpiresAt != nil && !spec.ExpiresAt.After(activated.Time) {
		return nil, fail(ErrValidation, "expiresAt must be after activatedAt")
	}

	l := &License{
		Arn:         arn.Build("license-manager", "", b.accountID, "license:l-"+awsid.NewN("", licenseIDLen)),
		ProductCode: spec.ProductCode, CustomerAWSAccountID: spec.CustomerAWSAccountID,
		ActivatedAt: activated, ExpiresAt: spec.ExpiresAt,
	}
	b.licenses.Put(l)
	b.subscribeLocked(spec.ProductCode, spec.CustomerAWSAccountID)

	cp := *l

	return &cp, nil
}

// licenseForLocked resolves the license a record names and checks the customer and activation window.
func (b *InMemoryBackend) licenseForLocked(productCode string, r *UsageRecord) (*License, error) {
	l, ok := b.licenses.Get(r.LicenseArn)
	if !ok {
		return nil, fail(ErrInvalidLicense, "license %q was not found", r.LicenseArn)
	}

	if productCode != "" && productCode != l.ProductCode {
		return nil, fail(ErrInvalidLicense, "license %q does not belong to product %q", r.LicenseArn, productCode)
	}

	if r.CustomerAWSAccountID != "" && r.CustomerAWSAccountID != l.CustomerAWSAccountID ||
		r.CustomerIdentifier != "" && r.CustomerIdentifier != customerID(l.ProductCode, l.CustomerAWSAccountID) {
		return nil, fail(ErrInvalidLicense, "license %q does not match the customer", r.LicenseArn)
	}

	if r.Timestamp != nil && !r.Timestamp.IsZero() && !withinLicense(l, r.Timestamp.Time) {
		return nil, fail(ErrInvalidLicense,
			"the usage timestamp is outside the activation period of license %q", r.LicenseArn)
	}

	return l, nil
}

func withinLicense(l *License, t time.Time) bool {
	return !t.Before(l.ActivatedAt.Time) && (l.ExpiresAt == nil || t.Before(l.ExpiresAt.Time))
}

func hasLicenseRecords(records []UsageRecord) bool {
	for i := range records {
		if records[i].LicenseArn != "" {
			return true
		}
	}

	return false
}
