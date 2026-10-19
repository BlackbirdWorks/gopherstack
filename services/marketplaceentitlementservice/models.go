package marketplaceentitlementservice

import "github.com/blackbirdworks/gopherstack/pkgs/awstime"

// Value is the EntitlementValue union; exactly one member is set.
type Value struct {
	BooleanValue *bool    `json:"BooleanValue,omitempty"`
	DoubleValue  *float64 `json:"DoubleValue,omitempty"`
	IntegerValue *int32   `json:"IntegerValue,omitempty"`
	StringValue  *string  `json:"StringValue,omitempty"`
}

// Entitlement is one customer's capacity for one product dimension. Timestamps are epoch-seconds numbers.
type Entitlement struct {
	ExpirationDate       *awstime.EpochTime `json:"ExpirationDate,omitempty"`
	Value                *Value             `json:"Value,omitempty"`
	CustomerAWSAccountID string             `json:"CustomerAWSAccountId,omitempty"`
	CustomerIdentifier   string             `json:"CustomerIdentifier,omitempty"`
	Dimension            string             `json:"Dimension,omitempty"`
	LicenseArn           string             `json:"LicenseArn,omitempty"`
	ProductCode          string             `json:"ProductCode,omitempty"`
}

func entitlementKey(e *Entitlement) string {
	return e.ProductCode + "|" + e.CustomerIdentifier + "|" + e.CustomerAWSAccountID + "|" + e.Dimension
}
