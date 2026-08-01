package outposts

// This file defines the JSON wire shapes for every operation's request and
// response bodies, verified field-by-field against
// aws-sdk-go-v2/service/outposts's serializers.go/deserializers.go (see
// PARITY.md for the verification method). Unlike services/grafana, this
// service's restjson1 document members serialize in PascalCase (matching the
// Go SDK's exported field names almost 1:1) rather than lowerCamel --
// confirmed by grepping every `object.Key("...")` call in serializers.go and
// every `case "..."` in deserializers.go, not assumed from the grafana
// precedent. Query-string parameters are ALSO PascalCase (e.g. "MaxResults",
// "NextToken", "OutpostIdentifierFilter") with exactly one exception:
// UntagResource's TagKeys list serializes as repeated lowerCamel "tagKeys"
// query parameters (serializers.go:3235) -- confirmed by direct grep, not a
// typo in this file.

// addressWire mirrors types.Address.
type addressWire struct {
	AddressLine1       string `json:"AddressLine1,omitempty"`
	AddressLine2       string `json:"AddressLine2,omitempty"`
	AddressLine3       string `json:"AddressLine3,omitempty"`
	City               string `json:"City,omitempty"`
	ContactName        string `json:"ContactName,omitempty"`
	ContactPhoneNumber string `json:"ContactPhoneNumber,omitempty"`
	CountryCode        string `json:"CountryCode,omitempty"`
	DistrictOrCounty   string `json:"DistrictOrCounty,omitempty"`
	Municipality       string `json:"Municipality,omitempty"`
	PostalCode         string `json:"PostalCode,omitempty"`
	StateOrRegion      string `json:"StateOrRegion,omitempty"`
}

// rackPhysicalPropertiesWire mirrors types.RackPhysicalProperties.
type rackPhysicalPropertiesWire struct {
	FiberOpticCableType       string `json:"FiberOpticCableType,omitempty"`
	MaximumSupportedWeightLbs string `json:"MaximumSupportedWeightLbs,omitempty"`
	OpticalStandard           string `json:"OpticalStandard,omitempty"`
	PowerConnector            string `json:"PowerConnector,omitempty"`
	PowerDrawKva              string `json:"PowerDrawKva,omitempty"`
	PowerFeedDrop             string `json:"PowerFeedDrop,omitempty"`
	PowerPhase                string `json:"PowerPhase,omitempty"`
	UplinkCount               string `json:"UplinkCount,omitempty"`
	UplinkGbps                string `json:"UplinkGbps,omitempty"`
}

// outpostWire mirrors types.Outpost.
type outpostWire struct {
	Tags                  map[string]string `json:"Tags,omitempty"`
	AvailabilityZone      string            `json:"AvailabilityZone,omitempty"`
	AvailabilityZoneId    string            `json:"AvailabilityZoneId,omitempty"` //nolint:revive,staticcheck // SDK name
	Description           string            `json:"Description,omitempty"`
	LifeCycleStatus       string            `json:"LifeCycleStatus,omitempty"`
	Name                  string            `json:"Name,omitempty"`
	OutpostArn            string            `json:"OutpostArn,omitempty"`
	OutpostId             string            `json:"OutpostId,omitempty"` //nolint:revive,staticcheck // SDK name
	OwnerId               string            `json:"OwnerId,omitempty"`   //nolint:revive,staticcheck // SDK name
	SiteArn               string            `json:"SiteArn,omitempty"`
	SiteId                string            `json:"SiteId,omitempty"` //nolint:revive,staticcheck // SDK name
	SupportedHardwareType string            `json:"SupportedHardwareType,omitempty"`
}

// outpostResponse wraps outpostWire under the "Outpost" key, the response
// envelope for CreateOutpost/GetOutpost/UpdateOutpost.
type outpostResponse struct {
	Outpost *outpostWire `json:"Outpost"`
}

// listOutpostsResponse mirrors ListOutpostsOutput.
type listOutpostsResponse struct {
	NextToken string        `json:"NextToken,omitempty"`
	Outposts  []outpostWire `json:"Outposts"`
}

// startOutpostDecommissionResponse mirrors StartOutpostDecommissionOutput.
type startOutpostDecommissionResponse struct {
	Status                string   `json:"Status,omitempty"`
	BlockingResourceTypes []string `json:"BlockingResourceTypes"`
}

// subscriptionWire mirrors types.Subscription.
type subscriptionWire struct {
	SubscriptionId        string   `json:"SubscriptionId,omitempty"` //nolint:revive,staticcheck // SDK name
	Currency              string   `json:"Currency,omitempty"`
	SubscriptionStatus    string   `json:"SubscriptionStatus,omitempty"`
	SubscriptionType      string   `json:"SubscriptionType,omitempty"`
	OrderIds              []string `json:"OrderIds,omitempty"` //nolint:revive // SDK name
	BeginDate             float64  `json:"BeginDate,omitempty"`
	EndDate               float64  `json:"EndDate,omitempty"`
	MonthlyRecurringPrice float64  `json:"MonthlyRecurringPrice,omitempty"`
	UpfrontPrice          float64  `json:"UpfrontPrice,omitempty"`
}

// getOutpostBillingInformationResponse mirrors
// GetOutpostBillingInformationOutput. ContractEndDate is *string on the real
// wire type (not a timestamp shape) -- confirmed from api_op_GetOutpostBillingInformation.go.
type getOutpostBillingInformationResponse struct {
	ContractEndDate string             `json:"ContractEndDate,omitempty"`
	NextToken       string             `json:"NextToken,omitempty"`
	PaymentOption   string             `json:"PaymentOption,omitempty"`
	PaymentTerm     string             `json:"PaymentTerm,omitempty"`
	Subscriptions   []subscriptionWire `json:"Subscriptions"`
}

// instanceTypeItemWire mirrors types.InstanceTypeItem.
type instanceTypeItemWire struct {
	VCPUs        *int32 `json:"VCPUs,omitempty"`
	InstanceType string `json:"InstanceType,omitempty"`
}

// getOutpostInstanceTypesResponse mirrors GetOutpostInstanceTypesOutput.
type getOutpostInstanceTypesResponse struct {
	NextToken     string                 `json:"NextToken,omitempty"`
	OutpostArn    string                 `json:"OutpostArn,omitempty"`
	OutpostId     string                 `json:"OutpostId,omitempty"` //nolint:revive,staticcheck // SDK name
	InstanceTypes []instanceTypeItemWire `json:"InstanceTypes"`
}

// getOutpostSupportedInstanceTypesResponse mirrors
// GetOutpostSupportedInstanceTypesOutput.
type getOutpostSupportedInstanceTypesResponse struct {
	NextToken     string                 `json:"NextToken,omitempty"`
	InstanceTypes []instanceTypeItemWire `json:"InstanceTypes"`
}

// subscriptionPricingDetailsWire mirrors types.SubscriptionPricingDetails.
type subscriptionPricingDetailsWire struct {
	Currency              string  `json:"Currency,omitempty"`
	PaymentOption         string  `json:"PaymentOption,omitempty"`
	PaymentTerm           string  `json:"PaymentTerm,omitempty"`
	MonthlyRecurringPrice float32 `json:"MonthlyRecurringPrice,omitempty"`
	UpfrontPrice          float32 `json:"UpfrontPrice,omitempty"`
}

// pricingOptionWire mirrors types.PricingOption.
type pricingOptionWire struct {
	SubscriptionPricingDetails *subscriptionPricingDetailsWire `json:"SubscriptionPricingDetails,omitempty"`
	PricingType                string                          `json:"PricingType,omitempty"`
}

// getRenewalPricingResponse mirrors GetRenewalPricingOutput.
type getRenewalPricingResponse struct {
	PricingResult  string              `json:"PricingResult,omitempty"`
	PricingOptions []pricingOptionWire `json:"PricingOptions"`
}

// createRenewalResponse mirrors CreateRenewalOutput.
type createRenewalResponse struct {
	Currency              string  `json:"Currency,omitempty"`
	OutpostId             string  `json:"OutpostId,omitempty"` //nolint:revive,staticcheck // SDK name
	PaymentOption         string  `json:"PaymentOption,omitempty"`
	PaymentTerm           string  `json:"PaymentTerm,omitempty"`
	MonthlyRecurringPrice float32 `json:"MonthlyRecurringPrice,omitempty"`
	UpfrontPrice          float32 `json:"UpfrontPrice,omitempty"`
}

// siteWire mirrors types.Site. OperatingAddress is flattened to three fields
// on this response shape; ShippingAddress is never present here at all (see
// models.go's Site doc comment).
type siteWire struct {
	Tags                   map[string]string           `json:"Tags,omitempty"`
	RackPhysicalProperties *rackPhysicalPropertiesWire `json:"RackPhysicalProperties,omitempty"`
	//nolint:revive,staticcheck // SDK name
	AccountId                     string `json:"AccountId,omitempty"`
	Description                   string `json:"Description,omitempty"`
	Name                          string `json:"Name,omitempty"`
	Notes                         string `json:"Notes,omitempty"`
	OperatingAddressCity          string `json:"OperatingAddressCity,omitempty"`
	OperatingAddressCountryCode   string `json:"OperatingAddressCountryCode,omitempty"`
	OperatingAddressStateOrRegion string `json:"OperatingAddressStateOrRegion,omitempty"`
	SiteArn                       string `json:"SiteArn,omitempty"`
	//nolint:revive,staticcheck // SDK name
	SiteId string `json:"SiteId,omitempty"`
}

// siteResponse wraps siteWire under the "Site" key.
type siteResponse struct {
	Site *siteWire `json:"Site"`
}

// listSitesResponse mirrors ListSitesOutput.
type listSitesResponse struct {
	NextToken string     `json:"NextToken,omitempty"`
	Sites     []siteWire `json:"Sites"`
}

// getSiteAddressResponse mirrors GetSiteAddressOutput.
type getSiteAddressResponse struct {
	Address     *addressWire `json:"Address,omitempty"`
	AddressType string       `json:"AddressType,omitempty"`
	SiteId      string       `json:"SiteId,omitempty"` //nolint:revive,staticcheck // SDK name
}

// updateSiteAddressResponse mirrors UpdateSiteAddressOutput.
type updateSiteAddressResponse struct {
	Address     *addressWire `json:"Address,omitempty"`
	AddressType string       `json:"AddressType,omitempty"`
}

// lineItemWire mirrors types.LineItem. AssetInformationList and
// ShipmentInformation are always absent -- see models.go's LineItem doc
// comment.
type lineItemWire struct {
	CatalogItemId      string `json:"CatalogItemId,omitempty"`      //nolint:revive,staticcheck // SDK name
	LineItemId         string `json:"LineItemId,omitempty"`         //nolint:revive,staticcheck // SDK name
	PreviousLineItemId string `json:"PreviousLineItemId,omitempty"` //nolint:revive,staticcheck // SDK name
	PreviousOrderId    string `json:"PreviousOrderId,omitempty"`    //nolint:revive,staticcheck // SDK name
	Status             string `json:"Status,omitempty"`
	Quantity           int32  `json:"Quantity,omitempty"`
}

// orderWire mirrors types.Order.
type orderWire struct {
	OrderId               string         `json:"OrderId,omitempty"` //nolint:revive,staticcheck // SDK name
	OrderType             string         `json:"OrderType,omitempty"`
	OutpostId             string         `json:"OutpostId,omitempty"` //nolint:revive,staticcheck // SDK name
	PaymentOption         string         `json:"PaymentOption,omitempty"`
	PaymentTerm           string         `json:"PaymentTerm,omitempty"`
	QuoteIdentifier       string         `json:"QuoteIdentifier,omitempty"`
	QuoteOptionIdentifier string         `json:"QuoteOptionIdentifier,omitempty"`
	Status                string         `json:"Status,omitempty"`
	LineItems             []lineItemWire `json:"LineItems"`
	OrderSubmissionDate   float64        `json:"OrderSubmissionDate,omitempty"`
	OrderFulfilledDate    float64        `json:"OrderFulfilledDate,omitempty"`
}

// orderResponse wraps orderWire under the "Order" key.
type orderResponse struct {
	Order *orderWire `json:"Order"`
}

// orderSummaryWire mirrors types.OrderSummary.
type orderSummaryWire struct {
	LineItemCountsByStatus map[string]int32 `json:"LineItemCountsByStatus,omitempty"`
	OrderId                string           `json:"OrderId,omitempty"` //nolint:revive,staticcheck // SDK name
	OrderType              string           `json:"OrderType,omitempty"`
	OutpostId              string           `json:"OutpostId,omitempty"` //nolint:revive,staticcheck // SDK name
	Status                 string           `json:"Status,omitempty"`
	OrderSubmissionDate    float64          `json:"OrderSubmissionDate,omitempty"`
	OrderFulfilledDate     float64          `json:"OrderFulfilledDate,omitempty"`
}

// listOrdersResponse mirrors ListOrdersOutput.
type listOrdersResponse struct {
	NextToken string             `json:"NextToken,omitempty"`
	Orders    []orderSummaryWire `json:"Orders"`
}

// quoteCapacityWire mirrors types.QuoteCapacity.
type quoteCapacityWire struct {
	QuoteCapacityType string  `json:"QuoteCapacityType,omitempty"`
	Unit              string  `json:"Unit,omitempty"`
	Quantity          float32 `json:"Quantity,omitempty"`
}

// quoteConstraintWire mirrors types.QuoteConstraint.
type quoteConstraintWire struct {
	QuoteConstraintType string `json:"QuoteConstraintType,omitempty"`
	Value               string `json:"Value,omitempty"`
}

// orderingRequirementWire mirrors types.OrderingRequirement.
type orderingRequirementWire struct {
	OrderingRequirementType string `json:"OrderingRequirementType,omitempty"`
	Status                  string `json:"Status,omitempty"`
	StatusMessage           string `json:"StatusMessage,omitempty"`
}

// capacitySummaryWire mirrors types.CapacitySummary. ExistingCapacities is
// always empty and Specifications on quoteOptionWire is always empty -- see
// PARITY.md's "Quote pricing model" note on what this backend does and does
// not model.
type capacitySummaryWire struct {
	CapacityChange     []quoteCapacityWire `json:"CapacityChange,omitempty"`
	ExistingCapacities []quoteCapacityWire `json:"ExistingCapacities,omitempty"`
	FinalCapacities    []quoteCapacityWire `json:"FinalCapacities,omitempty"`
}

// quoteOptionWire mirrors types.QuoteOption. Specifications is always empty
// -- this backend does not fabricate rack/server physical-specification
// numbers (RackDepthInches, ServerPowerDrawKva, etc.) with no real source
// data behind them. See PARITY.md.
type quoteOptionWire struct {
	CapacitySummary       *capacitySummaryWire `json:"CapacitySummary,omitempty"`
	QuoteOptionIdentifier string               `json:"QuoteOptionIdentifier,omitempty"`
	Capacities            []quoteCapacityWire  `json:"Capacities,omitempty"`
	PricingOptions        []pricingOptionWire  `json:"PricingOptions,omitempty"`
	Specifications        []struct{}           `json:"Specifications"`
}

// quoteWire mirrors types.Quote (and, minus OrderingRequirements, types.QuoteSummary --
// see toQuoteSummaryWire).
type quoteWire struct {
	AccountId     string `json:"AccountId,omitempty"` //nolint:revive,staticcheck // SDK name
	CountryCode   string `json:"CountryCode,omitempty"`
	Description   string `json:"Description,omitempty"`
	OutpostArn    string `json:"OutpostArn,omitempty"`
	QuoteId       string `json:"QuoteId,omitempty"` //nolint:revive,staticcheck // SDK name
	QuoteStatus   string `json:"QuoteStatus,omitempty"`
	StatusMessage string `json:"StatusMessage,omitempty"`
	//nolint:revive,staticcheck // SDK name
	SubmittedOrderId        string                    `json:"SubmittedOrderId,omitempty"`
	OrderingRequirements    []orderingRequirementWire `json:"OrderingRequirements,omitempty"`
	QuoteOptions            []quoteOptionWire         `json:"QuoteOptions,omitempty"`
	RequestedCapacities     []quoteCapacityWire       `json:"RequestedCapacities"`
	RequestedConstraints    []quoteConstraintWire     `json:"RequestedConstraints,omitempty"`
	RequestedPaymentOptions []string                  `json:"RequestedPaymentOptions,omitempty"`
	RequestedPaymentTerms   []string                  `json:"RequestedPaymentTerms,omitempty"`
	CreatedDate             float64                   `json:"CreatedDate,omitempty"`
	ExpirationDate          float64                   `json:"ExpirationDate,omitempty"`
}

// quoteResponse wraps quoteWire under the "Quote" key.
type quoteResponse struct {
	Quote *quoteWire `json:"Quote"`
}

// listQuotesResponse mirrors ListQuotesOutput.
type listQuotesResponse struct {
	NextToken string      `json:"NextToken,omitempty"`
	Quotes    []quoteWire `json:"Quotes"`
}

// instanceTypeCapacityWire mirrors both types.InstanceTypeCapacity and
// types.AssetInstanceTypeCapacity (identically shaped).
type instanceTypeCapacityWire struct {
	InstanceType string `json:"InstanceType,omitempty"`
	Count        int32  `json:"Count,omitempty"`
}

// instancesToExcludeWire mirrors types.InstancesToExclude.
type instancesToExcludeWire struct {
	AccountIds []string `json:"AccountIds,omitempty"` //nolint:revive // SDK name
	Instances  []string `json:"Instances,omitempty"`
	Services   []string `json:"Services,omitempty"`
}

// capacityTaskFailureWire mirrors types.CapacityTaskFailure.
type capacityTaskFailureWire struct {
	Reason string `json:"Reason,omitempty"`
	Type   string `json:"Type,omitempty"`
}

// capacityTaskResponse is the flat (no envelope key) response shape shared
// by GetCapacityTaskOutput and StartCapacityTaskOutput.
type capacityTaskResponse struct {
	Failed             *capacityTaskFailureWire `json:"Failed,omitempty"`
	InstancesToExclude *instancesToExcludeWire  `json:"InstancesToExclude,omitempty"`
	//nolint:revive,staticcheck // SDK name
	AssetId string `json:"AssetId,omitempty"`
	//nolint:revive,staticcheck // SDK name
	CapacityTaskId     string `json:"CapacityTaskId,omitempty"`
	CapacityTaskStatus string `json:"CapacityTaskStatus,omitempty"`
	//nolint:revive,staticcheck // SDK name
	OrderId string `json:"OrderId,omitempty"`
	//nolint:revive,staticcheck // SDK name
	OutpostId                     string                     `json:"OutpostId,omitempty"`
	TaskActionOnBlockingInstances string                     `json:"TaskActionOnBlockingInstances,omitempty"`
	RequestedInstancePools        []instanceTypeCapacityWire `json:"RequestedInstancePools,omitempty"`
	CompletionDate                float64                    `json:"CompletionDate,omitempty"`
	CreationDate                  float64                    `json:"CreationDate,omitempty"`
	LastModifiedDate              float64                    `json:"LastModifiedDate,omitempty"`
	DryRun                        bool                       `json:"DryRun,omitempty"`
}

// capacityTaskSummaryWire mirrors types.CapacityTaskSummary.
type capacityTaskSummaryWire struct {
	AssetId            string  `json:"AssetId,omitempty"`        //nolint:revive,staticcheck // SDK name
	CapacityTaskId     string  `json:"CapacityTaskId,omitempty"` //nolint:revive,staticcheck // SDK name
	CapacityTaskStatus string  `json:"CapacityTaskStatus,omitempty"`
	OrderId            string  `json:"OrderId,omitempty"`   //nolint:revive,staticcheck // SDK name
	OutpostId          string  `json:"OutpostId,omitempty"` //nolint:revive,staticcheck // SDK name
	CompletionDate     float64 `json:"CompletionDate,omitempty"`
	CreationDate       float64 `json:"CreationDate,omitempty"`
	LastModifiedDate   float64 `json:"LastModifiedDate,omitempty"`
}

// listCapacityTasksResponse mirrors ListCapacityTasksOutput.
type listCapacityTasksResponse struct {
	NextToken     string                    `json:"NextToken,omitempty"`
	CapacityTasks []capacityTaskSummaryWire `json:"CapacityTasks"`
}

// blockingInstanceWire mirrors types.BlockingInstance.
type blockingInstanceWire struct {
	AccountId      string `json:"AccountId,omitempty"` //nolint:revive,staticcheck // SDK name
	AwsServiceName string `json:"AwsServiceName,omitempty"`
	InstanceId     string `json:"InstanceId,omitempty"` //nolint:revive,staticcheck // SDK name
}

// listBlockingInstancesResponse mirrors
// ListBlockingInstancesForCapacityTaskOutput.
type listBlockingInstancesResponse struct {
	NextToken         string                 `json:"NextToken,omitempty"`
	BlockingInstances []blockingInstanceWire `json:"BlockingInstances"`
}

// assetLocationWire mirrors types.AssetLocation.
type assetLocationWire struct {
	RackElevation *float32 `json:"RackElevation,omitempty"`
}

// computeAttributesWire mirrors types.ComputeAttributes.
type computeAttributesWire struct {
	HostId                 string                     `json:"HostId,omitempty"` //nolint:revive,staticcheck // SDK name
	State                  string                     `json:"State,omitempty"`
	InstanceFamilies       []string                   `json:"InstanceFamilies,omitempty"`
	InstanceTypeCapacities []instanceTypeCapacityWire `json:"InstanceTypeCapacities,omitempty"`
	MaxVcpus               int32                      `json:"MaxVcpus,omitempty"`
}

// assetInfoWire mirrors types.AssetInfo.
type assetInfoWire struct {
	AssetLocation     *assetLocationWire     `json:"AssetLocation,omitempty"`
	ComputeAttributes *computeAttributesWire `json:"ComputeAttributes,omitempty"`
	AssetId           string                 `json:"AssetId,omitempty"` //nolint:revive,staticcheck // SDK name
	AssetType         string                 `json:"AssetType,omitempty"`
	RackId            string                 `json:"RackId,omitempty"` //nolint:revive,staticcheck // SDK name
}

// listAssetsResponse mirrors ListAssetsOutput.
type listAssetsResponse struct {
	NextToken string          `json:"NextToken,omitempty"`
	Assets    []assetInfoWire `json:"Assets"`
}

// assetInstanceWire mirrors types.AssetInstance.
type assetInstanceWire struct {
	AccountId      string `json:"AccountId,omitempty"` //nolint:revive,staticcheck // SDK name
	AssetId        string `json:"AssetId,omitempty"`   //nolint:revive,staticcheck // SDK name
	AwsServiceName string `json:"AwsServiceName,omitempty"`
	InstanceId     string `json:"InstanceId,omitempty"` //nolint:revive,staticcheck // SDK name
	InstanceType   string `json:"InstanceType,omitempty"`
}

// listAssetInstancesResponse mirrors ListAssetInstancesOutput.
type listAssetInstancesResponse struct {
	NextToken      string              `json:"NextToken,omitempty"`
	AssetInstances []assetInstanceWire `json:"AssetInstances"`
}

// ec2CapacityWire mirrors types.EC2Capacity. Quantity/MaxSize are strings on
// the real wire type -- see models.go's EC2Capacity doc comment.
type ec2CapacityWire struct {
	Family   string `json:"Family,omitempty"`
	MaxSize  string `json:"MaxSize,omitempty"`
	Quantity string `json:"Quantity,omitempty"`
}

// catalogItemWire mirrors types.CatalogItem.
type catalogItemWire struct {
	CatalogItemId       string            `json:"CatalogItemId,omitempty"` //nolint:revive,staticcheck // SDK name
	ItemStatus          string            `json:"ItemStatus,omitempty"`
	SupportedStorage    []string          `json:"SupportedStorage,omitempty"`
	SupportedUplinkGbps []int32           `json:"SupportedUplinkGbps,omitempty"`
	EC2Capacities       []ec2CapacityWire `json:"EC2Capacities,omitempty"`
	PowerKva            float32           `json:"PowerKva,omitempty"`
	WeightLbs           int32             `json:"WeightLbs,omitempty"`
}

// getCatalogItemResponse wraps catalogItemWire under the "CatalogItem" key.
type getCatalogItemResponse struct {
	CatalogItem *catalogItemWire `json:"CatalogItem"`
}

// listCatalogItemsResponse mirrors ListCatalogItemsOutput.
type listCatalogItemsResponse struct {
	NextToken    string            `json:"NextToken,omitempty"`
	CatalogItems []catalogItemWire `json:"CatalogItems"`
}

// formFactorConfigWire mirrors types.FormFactorConfig.
type formFactorConfigWire struct {
	FormFactor        string `json:"FormFactor,omitempty"`
	OutpostGeneration string `json:"OutpostGeneration,omitempty"`
}

// detailedInstanceTypeItemWire mirrors types.DetailedInstanceTypeItem.
type detailedInstanceTypeItemWire struct {
	VCPUs              *int32                 `json:"VCPUs,omitempty"`
	InstanceType       string                 `json:"InstanceType,omitempty"`
	NetworkPerformance string                 `json:"NetworkPerformance,omitempty"`
	FormFactorConfigs  []formFactorConfigWire `json:"FormFactorConfigs,omitempty"`
	MemoryInMib        int32                  `json:"MemoryInMib,omitempty"`
}

// listOrderableInstanceTypesResponse mirrors ListOrderableInstanceTypesOutput.
type listOrderableInstanceTypesResponse struct {
	NextToken     string                         `json:"NextToken,omitempty"`
	InstanceTypes []detailedInstanceTypeItemWire `json:"InstanceTypes"`
}

// connectionDetailsWire mirrors types.ConnectionDetails.
type connectionDetailsWire struct {
	ClientPublicKey     string   `json:"ClientPublicKey,omitempty"`
	ClientTunnelAddress string   `json:"ClientTunnelAddress,omitempty"`
	ServerEndpoint      string   `json:"ServerEndpoint,omitempty"`
	ServerPublicKey     string   `json:"ServerPublicKey,omitempty"`
	ServerTunnelAddress string   `json:"ServerTunnelAddress,omitempty"`
	AllowedIps          []string `json:"AllowedIps,omitempty"`
}

// getConnectionResponse mirrors GetConnectionOutput.
type getConnectionResponse struct {
	ConnectionDetails *connectionDetailsWire `json:"ConnectionDetails,omitempty"`
	ConnectionId      string                 `json:"ConnectionId,omitempty"` //nolint:revive,staticcheck // SDK name
}

// startConnectionResponse mirrors StartConnectionOutput.
type startConnectionResponse struct {
	ConnectionId      string `json:"ConnectionId,omitempty"`      //nolint:revive,staticcheck // SDK name
	UnderlayIpAddress string `json:"UnderlayIpAddress,omitempty"` //nolint:revive,staticcheck // SDK name
}

// listTagsForResourceResponse mirrors ListTagsForResourceOutput.
type listTagsForResourceResponse struct {
	Tags map[string]string `json:"Tags,omitempty"`
}

// ---- request bodies ----

// createOutpostRequest mirrors CreateOutpostInput's JSON body.
type createOutpostRequest struct {
	Tags                  map[string]string `json:"Tags,omitempty"`
	Name                  string            `json:"Name"`
	SiteId                string            `json:"SiteId"` //nolint:revive,staticcheck // SDK name
	AvailabilityZone      string            `json:"AvailabilityZone,omitempty"`
	AvailabilityZoneId    string            `json:"AvailabilityZoneId,omitempty"` //nolint:revive,staticcheck // SDK name
	Description           string            `json:"Description,omitempty"`
	SupportedHardwareType string            `json:"SupportedHardwareType,omitempty"`
}

// updateOutpostRequest mirrors UpdateOutpostInput's JSON body.
type updateOutpostRequest struct {
	Description           string `json:"Description,omitempty"`
	Name                  string `json:"Name,omitempty"`
	SupportedHardwareType string `json:"SupportedHardwareType,omitempty"`
}

// createSiteRequest mirrors CreateSiteInput's JSON body.
type createSiteRequest struct {
	Tags                   map[string]string           `json:"Tags,omitempty"`
	OperatingAddress       *addressWire                `json:"OperatingAddress,omitempty"`
	RackPhysicalProperties *rackPhysicalPropertiesWire `json:"RackPhysicalProperties,omitempty"`
	ShippingAddress        *addressWire                `json:"ShippingAddress,omitempty"`
	Description            string                      `json:"Description,omitempty"`
	Name                   string                      `json:"Name"`
	Notes                  string                      `json:"Notes,omitempty"`
}

// updateSiteRequest mirrors UpdateSiteInput's JSON body.
type updateSiteRequest struct {
	Description string `json:"Description,omitempty"`
	Name        string `json:"Name,omitempty"`
	Notes       string `json:"Notes,omitempty"`
}

// updateSiteAddressRequest mirrors UpdateSiteAddressInput's JSON body.
type updateSiteAddressRequest struct {
	Address     *addressWire `json:"Address"`
	AddressType string       `json:"AddressType"`
}

// lineItemRequestWire mirrors types.LineItemRequest.
type lineItemRequestWire struct {
	CatalogItemId string `json:"CatalogItemId,omitempty"` //nolint:revive,staticcheck // SDK name
	Quantity      int32  `json:"Quantity,omitempty"`
}

// createOrderRequest mirrors CreateOrderInput's JSON body.
type createOrderRequest struct {
	OutpostIdentifier     string                `json:"OutpostIdentifier"`
	PaymentOption         string                `json:"PaymentOption"`
	PaymentTerm           string                `json:"PaymentTerm,omitempty"`
	QuoteIdentifier       string                `json:"QuoteIdentifier,omitempty"`
	QuoteOptionIdentifier string                `json:"QuoteOptionIdentifier,omitempty"`
	LineItems             []lineItemRequestWire `json:"LineItems,omitempty"`
}

// createQuoteRequest mirrors CreateQuoteInput's JSON body.
type createQuoteRequest struct {
	RequestedCapacities     []quoteCapacityWire   `json:"RequestedCapacities"`
	RequestedConstraints    []quoteConstraintWire `json:"RequestedConstraints,omitempty"`
	CountryCode             string                `json:"CountryCode"`
	Description             string                `json:"Description,omitempty"`
	OutpostIdentifier       string                `json:"OutpostIdentifier,omitempty"`
	RequestedPaymentOptions []string              `json:"RequestedPaymentOptions,omitempty"`
	RequestedPaymentTerms   []string              `json:"RequestedPaymentTerms,omitempty"`
}

// updateQuoteRequest mirrors UpdateQuoteInput's JSON body. OutpostIdentifier
// is a pointer so the handler can distinguish "omitted" (nil, no change)
// from "explicit empty string" (non-nil, clears the association per the
// SDK's own doc comment on UpdateQuoteInput.OutpostIdentifier) -- see
// PARITY.md's trap #57.
type updateQuoteRequest struct {
	OutpostIdentifier       *string               `json:"OutpostIdentifier,omitempty"`
	RequestedCapacities     []quoteCapacityWire   `json:"RequestedCapacities,omitempty"`
	RequestedConstraints    []quoteConstraintWire `json:"RequestedConstraints,omitempty"`
	CountryCode             string                `json:"CountryCode,omitempty"`
	Description             string                `json:"Description,omitempty"`
	RequestedPaymentOptions []string              `json:"RequestedPaymentOptions,omitempty"`
	RequestedPaymentTerms   []string              `json:"RequestedPaymentTerms,omitempty"`
}

// createRenewalRequest mirrors CreateRenewalInput's JSON body (OutpostIdentifier
// is a body member here, NOT a path parameter -- the path is the fixed
// "/renewals").
type createRenewalRequest struct {
	ClientToken       string `json:"ClientToken,omitempty"`
	OutpostIdentifier string `json:"OutpostIdentifier"`
	PaymentOption     string `json:"PaymentOption"`
	PaymentTerm       string `json:"PaymentTerm"`
}

// startCapacityTaskRequest mirrors StartCapacityTaskInput's JSON body
// (OutpostIdentifier is a path parameter, not part of this body).
type startCapacityTaskRequest struct {
	InstancesToExclude *instancesToExcludeWire `json:"InstancesToExclude,omitempty"`
	//nolint:revive,staticcheck // SDK name
	AssetId string `json:"AssetId,omitempty"`
	//nolint:revive,staticcheck // SDK name
	OrderId                       string                     `json:"OrderId,omitempty"`
	TaskActionOnBlockingInstances string                     `json:"TaskActionOnBlockingInstances,omitempty"`
	InstancePools                 []instanceTypeCapacityWire `json:"InstancePools"`
	DryRun                        bool                       `json:"DryRun,omitempty"`
}

// startConnectionRequest mirrors StartConnectionInput's JSON body.
type startConnectionRequest struct {
	AssetId                     string `json:"AssetId"` //nolint:revive,staticcheck // SDK name
	ClientPublicKey             string `json:"ClientPublicKey"`
	DeviceSerialNumber          string `json:"DeviceSerialNumber,omitempty"`
	NetworkInterfaceDeviceIndex int32  `json:"NetworkInterfaceDeviceIndex"`
}

// tagResourceRequest mirrors TagResourceInput's JSON body (ResourceArn is a
// path parameter, not part of this body).
type tagResourceRequest struct {
	Tags map[string]string `json:"Tags"`
}
