package outposts

// defaultPageLimit is the page size used by every List operation when the
// caller omits MaxResults, matching services/grafana's convention.
const defaultPageLimit = 100

// maxSitesPerRegion/maxOutpostsPerSite are the real default account quotas
// AWS publishes for Outposts (docs.aws.amazon.com/outposts/latest/userguide/
// outposts-limits.html: "Outpost sites" = 100 per Region per account,
// "Outposts per site" = 10 per site) -- CreateSite/CreateOutpost enforce
// these, unlike the other 14 undocumented OrderingRequirementType checks
// this backend has no published quota to back (see PARITY.md).
const (
	maxSitesPerRegion  = 100
	maxOutpostsPerSite = 10
)

// LifeCycleStatus values. types.Outpost.LifeCycleStatus is a bare *string
// with NO enum type anywhere in this SDK module (confirmed: no LifeCycleStatus
// type exists in types/enums.go) -- these values are a documented, unconfirmed
// choice (general AWS documentation commonly references ACTIVE/CREATED/
// PENDING/DEGRADED, but that is not something this SDK checkout can back with
// a file:line). See PARITY.md's "Outpost lifecycle" note.
const (
	LifeCycleStatusActive              = "ACTIVE"
	LifeCycleStatusPendingDecommission = "PENDING_DECOMMISSION"
)

// ResourceType wire values (types.ResourceType) -- the ONLY two values this
// closed enum supports. ConflictException.ResourceType can therefore never
// legitimately carry "SITE" or "QUOTE"; conflicts on those resources omit
// ResourceType/ResourceId rather than fabricate an enum value the SDK does
// not declare. See errors.go.
const (
	ResourceTypeOutpost = "OUTPOST"
	ResourceTypeOrder   = "ORDER"
)

// OrderStatus wire values (types.OrderStatus) this backend produces:
// PREPARING -> IN_PROGRESS -> DELIVERED -> COMPLETED (async, see
// scheduleOrderCompletion), or CANCELLED. The deprecated values
// (RECEIVED/PENDING/PROCESSING/INSTALLING/FULFILLED) and ERROR are real,
// wire-accurate constants this emulator declares no transition path to --
// deprecated because AWS itself no longer produces them, ERROR because this
// backend has no failure trigger to base one on. See PARITY.md.
const (
	OrderStatusPreparing  = "PREPARING"
	OrderStatusInProgress = "IN_PROGRESS"
	OrderStatusDelivered  = "DELIVERED"
	OrderStatusCompleted  = "COMPLETED"
	OrderStatusCancelled  = "CANCELLED"
)

// LineItemStatus wire values (types.LineItemStatus) this backend produces,
// in lockstep with the owning Order's status transition (see
// scheduleOrderCompletion) -- an invented but documented rollup rule, since
// the SDK does not encode one (PARITY.md's hardest-things #1).
const (
	LineItemStatusPreparing = "PREPARING"
	LineItemStatusBuilding  = "BUILDING"
	LineItemStatusDelivered = "DELIVERED"
	LineItemStatusInstalled = "INSTALLED"
	LineItemStatusCancelled = "CANCELLED"
)

// QuoteStatus wire values (types.QuoteStatus).
const (
	QuoteStatusCreated        = "CREATED"
	QuoteStatusOrderSubmitted = "ORDER_SUBMITTED"
	QuoteStatusExpired        = "EXPIRED"
)

// OrderingRequirementType subset this backend evaluates from real state --
// see quotes.go's buildOrderingRequirements for the full 17-check bucketing
// and PARITY.md for why the remaining 5 (MAXIMUM_ALLOWED_ORDERS_CHECK_ERROR,
// OUTPOST_GENERATION_MISMATCH_ERROR, UNSUPPORTED, ENTERPRISE_SUPPORT_ERROR,
// OUTPOST_STATE_CHANGED_ERROR) are not.
const (
	OrderingRequirementTypeOutpostIDMissing                    = "OUTPOST_ID_MISSING_ON_QUOTE_ERROR"
	OrderingRequirementTypeOutpostActive                       = "OUTPOST_ACTIVE_CHECK_ERROR"
	OrderingRequirementTypeOutpostNotFound                     = "OUTPOST_NOT_FOUND_ERROR"
	OrderingRequirementTypeOutpostRenewalRequired              = "OUTPOST_RENEWAL_REQUIRED_ERROR"
	OrderingRequirementTypeOperatingAddressExistence           = "OPERATING_ADDRESS_EXISTENCE_CHECK_ERROR"
	OrderingRequirementTypeShippingAddressExistence            = "SHIPPING_ADDRESS_EXISTENCE_CHECK_ERROR"
	OrderingRequirementTypeCountryCodeMismatch                 = "COUNTRY_CODE_MISMATCH_CHECK_ERROR"
	OrderingRequirementTypeValidZipCode                        = "VALID_ZIP_CODE_CHECK_ERROR"
	OrderingRequirementTypeRackPhysicalProperties              = "RACK_PHYSICAL_PROPERTIES_CHECK_ERROR"
	OrderingRequirementTypeShippingAddressMissingContactName   = "SHIPPING_ADDRESS_MISSING_CONTACT_NAME_ERROR"
	OrderingRequirementTypeShippingAddressMissingContactNumber = "SHIPPING_ADDRESS_MISSING_CONTACT_NUMBER_ERROR"
	OrderingRequirementTypeShippingAddressMissingContactInfo   = "SHIPPING_ADDRESS_MISSING_CONTACT_INFO_ERROR"

	OrderingRequirementStatusPass   = "PASS"
	OrderingRequirementStatusFail   = "FAIL"
	OrderingRequirementStatusExempt = "EXEMPT"
)

// CapacityTaskStatus wire values (types.CapacityTaskStatus) this backend
// produces: REQUESTED -> IN_PROGRESS -> COMPLETED (async, see
// scheduleCapacityTaskCompletion), or REQUESTED/IN_PROGRESS ->
// CANCELLATION_IN_PROGRESS -> CANCELLED (see CancelCapacityTask).
// WAITING_FOR_EVACUATION never occurs: StartCapacityTask's model here is
// additive-only (mergeInstanceTypeCapacity never shrinks
// InstanceTypeCapacities), so no running instance can ever legitimately
// block a task -- see PARITY.md. FAILED never occurs: this backend has no
// failure trigger to base one on.
const (
	CapacityTaskStatusRequested              = "REQUESTED"
	CapacityTaskStatusInProgress             = "IN_PROGRESS"
	CapacityTaskStatusCompleted              = "COMPLETED"
	CapacityTaskStatusCancellationInProgress = "CANCELLATION_IN_PROGRESS"
	CapacityTaskStatusCancelled              = "CANCELLED"
)

// DecommissionRequestStatus wire values (types.DecommissionRequestStatus).
const (
	DecommissionStatusRequested = "REQUESTED"
	DecommissionStatusSkipped   = "SKIPPED"
	DecommissionStatusBlocked   = "BLOCKED"
)

// ComputeAssetState / AssetState wire values this backend produces for the
// single seeded asset per Outpost.
const (
	AssetStateActive = "ACTIVE"
)

// AssetType wire values (types.AssetType) this backend produces.
const (
	AssetTypeCompute = "COMPUTE"
)

// SubscriptionStatus/SubscriptionType wire values.
const (
	SubscriptionStatusActive = "ACTIVE"
	SubscriptionTypeOriginal = "ORIGINAL"
	SubscriptionTypeRenewal  = "RENEWAL"
)

// SupportedHardwareType wire values (types.SupportedHardwareType), also
// reused as CatalogItemClass values (types.CatalogItemClass shares the same
// two RACK/SERVER strings) and OrderableInstanceType.FormFactor.
const (
	HardwareTypeRack   = "RACK"
	HardwareTypeServer = "SERVER"
)

// PaymentOption wire values (types.PaymentOption).
const (
	PaymentOptionAllUpfront     = "ALL_UPFRONT"
	PaymentOptionPartialUpfront = "PARTIAL_UPFRONT"
	PaymentOptionNoUpfront      = "NO_UPFRONT"
)

// PaymentTerm wire values (types.PaymentTerm).
const (
	PaymentTermOneYear    = "ONE_YEAR"
	PaymentTermThreeYears = "THREE_YEARS"
	PaymentTermFiveYears  = "FIVE_YEARS"
)

// QuoteCapacityType wire values (types.QuoteCapacityType).
const (
	QuoteCapacityTypeEC2 = "EC2"
	QuoteCapacityTypeEBS = "EBS"
	QuoteCapacityTypeS3  = "S3"
)

// AddressType wire values (types.AddressType).
const (
	AddressTypeShipping  = "SHIPPING_ADDRESS"
	AddressTypeOperating = "OPERATING_ADDRESS"
)

// CatalogItemStatus wire values (types.CatalogItemStatus) this backend's
// static seed catalog uses.
const catalogItemStatusAvailable = "AVAILABLE"

// SupportedStorageEnum wire values (types.SupportedStorageEnum) this
// backend's static seed catalog uses.
const (
	supportedStorageEBS = "EBS"
	supportedStorageS3  = "S3"
)

// CurrencyCode -- the only value this SDK declares (types.CurrencyCode).
const currencyUSD = "USD"

// QuotePricingType -- the only value this SDK declares.
const quotePricingTypeSubscription = "SUBSCRIPTION"

// PricingResult wire values (types.PricingResult).
const (
	PricingResultPriced        = "PRICED"
	PricingResultUnableToPrice = "UNABLE_TO_PRICE"
)

// resourceType labels used in notFoundError/conflictError messages. These
// are message text only, distinct from the closed ResourceType wire enum
// above (which only Outpost/Order may use on the wire).
const (
	resourceOutpost      = "outpost"
	resourceSite         = "site"
	resourceOrder        = "order"
	resourceQuote        = "quote"
	resourceCapacityTask = "capacity task"
	resourceCatalogItem  = "catalog item"
	resourceConnection   = "connection"
	resourceAsset        = "asset"
)

// Path segment literals shared by handler.go's route matcher (goconst: each
// appears at 3+ call sites across the route tree).
const (
	segOutposts    = "outposts"
	segOutpost     = "outpost"
	segSites       = "sites"
	segQuotes      = "quotes"
	segOrders      = "orders"
	segCatalog     = "catalog"
	segCapacity    = "capacity"
	segConnections = "connections"
	segTags        = "tags"
)
