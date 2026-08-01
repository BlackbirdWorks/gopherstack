// Package outposts emulates the AWS Outposts control-plane API
// (aws-sdk-go-v2/service/outposts, protocol restjson1).
package outposts

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// Address mirrors types.Address.
type Address struct {
	AddressLine1       string
	AddressLine2       string
	AddressLine3       string
	City               string
	ContactName        string
	ContactPhoneNumber string
	CountryCode        string
	DistrictOrCounty   string
	Municipality       string
	PostalCode         string
	StateOrRegion      string
}

// RackPhysicalProperties mirrors types.RackPhysicalProperties. Every member
// is a small closed enum on the real type; they are stored here as plain
// strings (the wire value) rather than re-declaring nine near-identical Go
// enum types.
type RackPhysicalProperties struct {
	FiberOpticCableType       string
	MaximumSupportedWeightLbs string
	OpticalStandard           string
	PowerConnector            string
	PowerDrawKva              string
	PowerFeedDrop             string
	PowerPhase                string
	UplinkCount               string
	UplinkGbps                string
}

// Subscription mirrors types.Subscription, one entry in an Outpost's
// billing/subscription history (see Outpost.Subscriptions).
type Subscription struct {
	BeginDate             time.Time
	EndDate               time.Time
	Currency              string
	SubscriptionID        string
	SubscriptionStatus    string
	SubscriptionType      string
	OrderIDs              []string
	MonthlyRecurringPrice float64
	UpfrontPrice          float64
}

// Outpost is the internal state of one Outpost, a superset of types.Outpost
// plus the billing/subscription sub-state that GetOutpostBillingInformation
// exposes but the Outpost type itself does not carry any fields for -- see
// PARITY.md's "Billing model" note for how this backend accumulates
// Subscriptions (order fulfillment and CreateRenewal), since types.Outpost
// has no CreatedDate/ModifiedDate members to hang bookkeeping off of.
type Outpost struct {
	ContractEndDate       time.Time
	Tags                  *tags.Tags
	AvailabilityZoneID    string
	LifeCycleStatus       string
	SiteARN               string
	Name                  string
	Description           string
	AvailabilityZone      string
	ARN                   string
	SiteID                string
	OwnerID               string
	SupportedHardwareType string
	PaymentOption         string
	PaymentTerm           string
	ID                    string
	Subscriptions         []Subscription
}

// Site is the internal state of one Outposts Site. ShippingAddress is
// write-only from the wire's perspective: types.Site (the GetSite/ListSites
// response shape) only exposes OperatingAddress, flattened to three fields
// (City/CountryCode/StateOrRegion) -- ShippingAddress only ever surfaces via
// GetSiteAddress(AddressType=SHIPPING_ADDRESS). See PARITY.md's ARN note for
// why SiteARN's resource-path segment is a documented, unconfirmed choice.
type Site struct {
	OperatingAddress       *Address
	ShippingAddress        *Address
	RackPhysicalProperties *RackPhysicalProperties
	Tags                   *tags.Tags
	ID                     string
	ARN                    string
	AccountID              string
	Name                   string
	Description            string
	Notes                  string
}

// InstanceTypeCapacity mirrors both types.InstanceTypeCapacity and
// types.AssetInstanceTypeCapacity, which are identically shaped
// (Count int32, InstanceType string) -- one internal type serves both wire
// shapes.
type InstanceTypeCapacity struct {
	InstanceType string
	Count        int32
}

// ComputeAttributes mirrors types.ComputeAttributes.
type ComputeAttributes struct {
	HostID                 string
	State                  string
	InstanceFamilies       []string
	InstanceTypeCapacities []InstanceTypeCapacity
	MaxVcpus               int32
}

// Asset is the internal state of one hardware asset on an Outpost. Real
// Outposts assets arrive via physical hardware installation, not a public
// CreateAsset API (there is none among these 43 operations) -- this backend
// seeds exactly one COMPUTE asset per created Outpost so capacity-task and
// asset-listing operations have real state to read and mutate. See
// PARITY.md's "Asset seeding" note.
type Asset struct {
	ComputeAttributes *ComputeAttributes
	ID                string
	OutpostID         string
	RackID            string
	AssetType         string
	RackElevation     float32
}

// CapacityTaskFailure mirrors types.CapacityTaskFailure.
type CapacityTaskFailure struct {
	Reason string
	Type   string
}

// InstancesToExclude mirrors types.InstancesToExclude.
type InstancesToExclude struct {
	AccountIDs []string
	Instances  []string
	Services   []string
}

// CapacityTask is the internal state of one capacity task.
type CapacityTask struct {
	CreationDate                  time.Time
	LastModifiedDate              time.Time
	CompletionDate                time.Time
	Failed                        *CapacityTaskFailure
	InstancesToExclude            *InstancesToExclude
	ID                            string
	OutpostID                     string
	AssetID                       string
	OrderID                       string
	Status                        string
	TaskActionOnBlockingInstances string
	RequestedInstancePools        []InstanceTypeCapacity
	DryRun                        bool
}

// LineItem mirrors types.LineItem. AssetInformationList and
// ShipmentInformation are always empty/nil in this backend -- see
// PARITY.md's "Order lifecycle" note for why (no real shipment/asset-install
// data source exists to populate them from).
type LineItem struct {
	LineItemID         string
	CatalogItemID      string
	Status             string
	PreviousLineItemID string
	PreviousOrderID    string
	Quantity           int32
}

// Order is the internal state of one order. OrderType is always "OUTPOST":
// CreateOrderInput has no OrderType member, so a "REPLACEMENT" order (the
// only other enum value) can never be produced by this API surface -- see
// PARITY.md.
type Order struct {
	OrderSubmissionDate   time.Time
	OrderFulfilledDate    time.Time
	ID                    string
	OutpostID             string
	OrderType             string
	Status                string
	PaymentOption         string
	PaymentTerm           string
	QuoteIdentifier       string
	QuoteOptionIdentifier string
	LineItems             []LineItem
}

// QuoteCapacity mirrors types.QuoteCapacity.
type QuoteCapacity struct {
	QuoteCapacityType string
	Unit              string
	Quantity          float32
}

// QuoteConstraint mirrors types.QuoteConstraint.
type QuoteConstraint struct {
	QuoteConstraintType string
	Value               string
}

// PricingOption mirrors a flattened types.PricingOption +
// types.SubscriptionPricingDetails (the only PricingType this SDK declares
// is SUBSCRIPTION, so the union is collapsed to one shape here).
type PricingOption struct {
	Currency              string
	PaymentOption         string
	PaymentTerm           string
	MonthlyRecurringPrice float32
	UpfrontPrice          float32
}

// OrderingRequirement mirrors types.OrderingRequirement.
type OrderingRequirement struct {
	OrderingRequirementType string
	Status                  string
	StatusMessage           string
}

// Quote is the internal state of one quote. This backend synthesizes a
// single QuoteOption per quote (QuoteOptionID/PricingOptions below) rather
// than the full N-option combinatorial shape real AWS could in principle
// return -- see PARITY.md's "Quote pricing model" note.
type Quote struct {
	CreatedDate      time.Time
	ExpirationDate   time.Time
	ID               string
	AccountID        string
	CountryCode      string
	Description      string
	OutpostID        string
	OutpostARN       string
	Status           string
	StatusMessage    string
	SubmittedOrderID string
	QuoteOptionID    string

	RequestedCapacities     []QuoteCapacity
	RequestedConstraints    []QuoteConstraint
	RequestedPaymentOptions []string
	RequestedPaymentTerms   []string
	OrderingRequirements    []OrderingRequirement
	PricingOptions          []PricingOption
}

// EC2Capacity mirrors types.EC2Capacity. Quantity and MaxSize are strings on
// the real wire type (not numeric), despite looking like they should be
// int32 -- confirmed from the SDK's types.go, preserved here deliberately.
type EC2Capacity struct {
	Family   string
	MaxSize  string
	Quantity string
}

// CatalogItem is a static reference-catalog entry (see seed_data.go).
// ItemClass is filterable (ListCatalogItemsInput.ItemClassFilter) but is not
// a field on the real types.CatalogItem output shape -- it is kept here for
// internal filtering only and is never serialized on the wire.
type CatalogItem struct {
	ID                  string
	ItemClass           string
	ItemStatus          string
	SupportedStorage    []string
	SupportedUplinkGbps []int32
	EC2Capacities       []EC2Capacity
	PowerKva            float32
	WeightLbs           int32
}

// OrderableInstanceType is a static reference-catalog entry (see
// seed_data.go) backing ListOrderableInstanceTypes and the VCPUs lookup used
// by GetOutpostInstanceTypes/GetOutpostSupportedInstanceTypes.
type OrderableInstanceType struct {
	InstanceType       string
	NetworkPerformance string
	FormFactor         string
	OutpostGeneration  string
	VCPUs              int32
	MemoryInMib        int32
}

// Connection is the internal state of one WireGuard-style installation
// tunnel (StartConnection/GetConnection). Every cryptographic-looking field
// is a synthetic placeholder, not real key material -- see PARITY.md.
type Connection struct {
	ID                  string
	AssetID             string
	ClientPublicKey     string
	ServerPublicKey     string
	ClientTunnelAddress string
	ServerTunnelAddress string
	ServerEndpoint      string
	UnderlayIPAddress   string
	AllowedIps          []string
}
