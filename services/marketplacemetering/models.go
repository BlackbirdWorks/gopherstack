package marketplacemetering

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	ent "github.com/blackbirdworks/gopherstack/services/marketplaceentitlementservice"
)

// Tag is a UsageAllocation tag.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// UsageAllocation splits a usage quantity across a unique tag set.
type UsageAllocation struct {
	Tags                   []Tag `json:"Tags,omitempty"`
	AllocatedUsageQuantity int32 `json:"AllocatedUsageQuantity"`
}

// UsageRecord is the BatchMeterUsage record shape; timestamps are epoch-seconds numbers.
type UsageRecord struct {
	Timestamp            *awstime.EpochTime `json:"Timestamp,omitempty"`
	Quantity             *int32             `json:"Quantity,omitempty"`
	CustomerAWSAccountID string             `json:"CustomerAWSAccountId,omitempty"`
	CustomerIdentifier   string             `json:"CustomerIdentifier,omitempty"`
	Dimension            string             `json:"Dimension,omitempty"`
	LicenseArn           string             `json:"LicenseArn,omitempty"`
	UsageAllocations     []UsageAllocation  `json:"UsageAllocations,omitempty"`
}

// UsageRecordResult is one BatchMeterUsage per-record outcome.
type UsageRecordResult struct {
	UsageRecord      *UsageRecord `json:"UsageRecord,omitempty"`
	MeteringRecordID string       `json:"MeteringRecordId,omitempty"`
	Status           string       `json:"Status"`
}

// Dimension is one metered or entitled dimension of a declared product.
type Dimension struct {
	ExpiresAt *awstime.EpochTime `json:"expiresAt,omitempty"`
	Value     *ent.Value         `json:"value,omitempty"`
	Name      string             `json:"name"`
}

// Product is a Marketplace product known to the emulator. Declared products enforce their dimensions and
// require a customer subscription; undeclared (implicit) products accept any dimension and customer.
type Product struct {
	RegisteredAt     awstime.EpochTime `json:"registeredAt"`
	Code             string            `json:"code"`
	Dimensions       []Dimension       `json:"dimensions,omitempty"`
	PublicKeyVersion int32             `json:"publicKeyVersion"`
	Declared         bool              `json:"declared"`
}

// Subscription ties a customer account to a product.
type Subscription struct {
	CreatedAt            awstime.EpochTime `json:"createdAt"`
	ProductCode          string            `json:"productCode"`
	CustomerAWSAccountID string            `json:"customerAwsAccountId"`
	CustomerIdentifier   string            `json:"customerIdentifier"`
}

// StoredUsage is a metering record accepted by MeterUsage or BatchMeterUsage.
type StoredUsage struct {
	Timestamp            awstime.EpochTime `json:"timestamp"`
	RecordedAt           awstime.EpochTime `json:"recordedAt"`
	ID                   string            `json:"id"`
	ProductCode          string            `json:"productCode"`
	CustomerIdentifier   string            `json:"customerIdentifier"`
	CustomerAWSAccountID string            `json:"customerAwsAccountId"`
	Dimension            string            `json:"dimension"`
	Operation            string            `json:"operation"`
	Allocations          []UsageAllocation `json:"allocations,omitempty"`
	Quantity             int32             `json:"quantity"`
}

// Registration is a RegisterUsage call.
type Registration struct {
	RegisteredAt     awstime.EpochTime `json:"registeredAt"`
	ID               string            `json:"id"`
	ProductCode      string            `json:"productCode"`
	Nonce            string            `json:"nonce,omitempty"`
	AccountID        string            `json:"accountId"`
	PublicKeyVersion int32             `json:"publicKeyVersion"`
}

func productKey(v *Product) string { return v.Code }

func subscriptionKey(v *Subscription) string {
	return v.ProductCode + "|" + v.CustomerAWSAccountID
}

func usageKey(v *StoredUsage) string {
	return usageDedupKey(v.ProductCode, v.CustomerIdentifier, v.Dimension, v.Timestamp.Unix())
}

func registrationKey(v *Registration) string { return v.ID }
