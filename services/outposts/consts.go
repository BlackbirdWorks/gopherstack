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

// OrderStatus wire values (types.OrderStatus). This backend only ever
// produces Preparing, Completed, Cancelled, and Error -- the deprecated
// (RECEIVED/PENDING/PROCESSING/INSTALLING/FULFILLED) and intermediate
// (IN_PROGRESS/DELIVERED) values are real, wire-accurate constants this
// emulator declares no transition path through. See PARITY.md's "Order
// lifecycle" note for the single-hop simplification this mirrors from
// services/grafana's workspace-transition pattern.
const (
	OrderStatusPreparing = "PREPARING"
	OrderStatusCompleted = "COMPLETED"
	OrderStatusCancelled = "CANCELLED"
)

// LineItemStatus wire values (types.LineItemStatus) this backend produces.
const (
	LineItemStatusPreparing = "PREPARING"
	LineItemStatusInstalled = "INSTALLED"
	LineItemStatusCancelled = "CANCELLED"
)

// QuoteStatus wire values (types.QuoteStatus).
const (
	QuoteStatusCreated        = "CREATED"
	QuoteStatusOrderSubmitted = "ORDER_SUBMITTED"
	QuoteStatusExpired        = "EXPIRED"
)

// OrderingRequirementType/OrderingRequirementStatus subset this backend
// actually evaluates -- see quotes.go's buildOrderingRequirements. The
// other 15 OrderingRequirementType enum values declared by the SDK
// (ENTERPRISE_SUPPORT_ERROR, VALID_ZIP_CODE_CHECK_ERROR, etc.) are real,
// wire-accurate constants this emulator has no backing state to evaluate --
// see PARITY.md.
const (
	OrderingRequirementTypeOutpostIDMissing = "OUTPOST_ID_MISSING_ON_QUOTE_ERROR"
	OrderingRequirementTypeOutpostActive    = "OUTPOST_ACTIVE_CHECK_ERROR"

	OrderingRequirementStatusPass   = "PASS"
	OrderingRequirementStatusFail   = "FAIL"
	OrderingRequirementStatusExempt = "EXEMPT"
)

// CapacityTaskStatus wire values (types.CapacityTaskStatus) this backend
// produces. WAITING_FOR_EVACUATION never occurs: it requires a live blocking
// EC2 instance, and this backend has no cross-service instance-placement
// data (see ListAssetInstances/ListBlockingInstancesForCapacityTask, always
// empty) -- see PARITY.md.
const (
	CapacityTaskStatusRequested = "REQUESTED"
	CapacityTaskStatusCompleted = "COMPLETED"
	CapacityTaskStatusCancelled = "CANCELLED"
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
