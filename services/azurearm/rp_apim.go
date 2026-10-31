package azurearm

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/mail"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	namespaceMicrosoftAPIM = "Microsoft.ApiManagement"
	apimServiceType        = "service"
	apimARMType            = namespaceMicrosoftAPIM + "/" + apimServiceType
	apimStateActivating    = "Activating"
	apimStateUpdating      = "Updating"
	apimStateCreated       = "Created"
	apimPlatformStv2       = "stv2"
	apimPlatformMtv1       = "mtv1"
	apimGatewaySuffix      = ".azure-api.net"
	apimDefaultNotifyMail  = "apimgmt-noreply@mail.windowsazure.com"
	apimActivateFast       = time.Second
	apimActivateClassic    = 5 * time.Second
	apimMaxServices        = 1000
	apimCodeValidation     = "ValidationError"
	apimCodeConflict       = "Conflict"
	apimCodePrecondition   = "PreconditionFailed"
	apimSKUConsumption     = "Consumption"
	apimPolicyID           = "policy"
	apimSKUPremium         = "Premium"
	apimBasicMax           = 2
	apimStandardMax        = 4
	apimPremiumMax         = 12
	apimVersionLatest      = "2024-05-01"
	apimDisabled           = "Disabled"
	apimReasonInvalid      = "Invalid"
	apimReasonExists       = "AlreadyExists"
	apimCapacity           = "capacity"
	apimMaxPublisher       = 100
	apimVNetNone           = "None"
)

func apimAPIVersions() []string { return []string{apimVersionLatest, "2022-08-01"} }

var (
	apimServiceNameRE = regexp.MustCompile(`^[A-Za-z]([A-Za-z0-9-]{0,48}[A-Za-z0-9])?$`)
	apimChildIDRE     = regexp.MustCompile(`^[^*#&+:<>?/]{1,256}$`)
)

// apimError is a control-plane failure with its ARM code and status.
type apimError struct {
	code   string
	msg    string
	status int
}

func (e *apimError) Error() string { return "azurearm: apim: " + e.code + ": " + e.msg }

func apimErr(status int, code, format string, args ...any) error {
	return &apimError{code: code, msg: fmt.Sprintf(format, args...), status: status}
}

func apimInvalid(format string, args ...any) error {
	return apimErr(http.StatusBadRequest, apimCodeValidation, format, args...)
}

func apimNotFound(what string) error {
	return apimErr(http.StatusNotFound, armErrorCodeResourceNotFound, "The %s was not found.", what)
}

func apimErrorDetails(err error) (errorEntry, bool) {
	if ae, ok := errors.AsType[*apimError](err); ok {
		return errorEntry{ae.code, ae.msg, ae.status}, true
	}

	return errorEntry{}, false
}

// apimEntity is one control-plane child (api, operation, product, subscription, named value, backend, policy);
// Props is the normalised wire "properties" object, secrets included.
type apimEntity struct {
	Props  map[string]any `json:"props"`
	ID     string         `json:"id"`
	Parent string         `json:"parent,omitempty"`
	Etag   uint64         `json:"etag"`
}

// apimService is the state of one API Management service instance.
type apimService struct {
	CreatedAt   time.Time              `json:"createdAt"`
	ReadyAt     time.Time              `json:"readyAt"`
	ProductAPIs map[string]bool        `json:"productApis,omitempty"`
	Identity    map[string]any         `json:"identity,omitempty"`
	Extra       map[string]any         `json:"extra,omitempty"`
	APIs        map[string]*apimEntity `json:"apis,omitempty"`
	Ops         map[string]*apimEntity `json:"operations,omitempty"`
	Products    map[string]*apimEntity `json:"products,omitempty"`
	Subs        map[string]*apimEntity `json:"subscriptions,omitempty"`
	Named       map[string]*apimEntity `json:"namedValues,omitempty"`
	Backends    map[string]*apimEntity `json:"backends,omitempty"`
	Policies    map[string]*apimEntity `json:"policies,omitempty"`
	Tags        map[string]string      `json:"tags,omitempty"`
	now         func() time.Time
	Transition  string `json:"transition,omitempty"`
	SubID       string `json:"subscriptionId"`
	Name        string `json:"name"`
	Location    string `json:"location"`
	SKU         string `json:"sku"`
	PublisherEm string `json:"publisherEmail"`
	PublisherNm string `json:"publisherName"`
	NotifyEmail string `json:"notificationSenderEmail"`
	PublicAcc   string `json:"publicNetworkAccess"`
	ResourceGrp string `json:"resourceGroup"`
	Zones       []any  `json:"zones,omitempty"`
	Capacity    int    `json:"capacity"`
	Etag        uint64 `json:"etag"`
}

// APIMProvider implements ResourceProvider for Microsoft.ApiManagement/service and serves the gateway data plane.
type APIMProvider struct {
	now           func() time.Time
	mu            *lockmetrics.RWMutex
	svcs          map[string]*apimService
	ops           map[string]*apimOp
	limiter       *apimLimiter
	client        *http.Client
	fastActivate  time.Duration
	classicActive time.Duration
	allowExternal atomic.Bool
}

// NewAPIMProvider creates an APIMProvider.
func NewAPIMProvider() *APIMProvider {
	p := &APIMProvider{
		now: time.Now, mu: lockmetrics.New("azurearm.apim"), svcs: map[string]*apimService{}, ops: map[string]*apimOp{},
		limiter: newAPIMLimiter(), fastActivate: apimActivateFast, classicActive: apimActivateClassic,
	}
	p.client = p.newBackendClient()

	return p
}

var _ ResourceProvider = (*APIMProvider)(nil)

// SetClock replaces the provider's clock; provisioning and rate limits follow it.
func (p *APIMProvider) SetClock(now func() time.Time) {
	p.mu.Lock("SetClock")
	p.now = now

	for _, s := range p.svcs {
		s.now = now
	}

	p.mu.Unlock()
}

// SetActivation sets how long Consumption and the other tiers stay in Activating or Updating.
func (p *APIMProvider) SetActivation(consumption, classic time.Duration) {
	p.mu.Lock("SetActivation")
	p.fastActivate, p.classicActive = consumption, classic
	p.mu.Unlock()
}

// SetAllowExternal lets gateway backends reach hosts outside loopback.
func (p *APIMProvider) SetAllowExternal(allow bool) { p.allowExternal.Store(allow) }

// Namespace implements ResourceProvider.
func (p *APIMProvider) Namespace() string { return namespaceMicrosoftAPIM }

// ResourceTypes implements ResourceProvider.
func (p *APIMProvider) ResourceTypes() []ResourceTypeDef {
	v := apimAPIVersions()
	out := []ResourceTypeDef{{Type: apimServiceType, APIVersions: v, HasChildren: true}}

	for _, k := range apimKinds() {
		out = append(out, ResourceTypeDef{Type: apimServiceType + "/" + k.typ, APIVersions: v})
	}

	return append(out, ResourceTypeDef{Type: acrActionCheckName, APIVersions: v})
}

func apimKey(sub, rg, name string) string {
	return strings.ToLower(ResourceID{
		SubscriptionID: sub, ResourceGroup: rg, Namespace: namespaceMicrosoftAPIM,
		Types: []string{apimServiceType}, Names: []string{name},
	}.ARMID())
}

func (s *apimService) armBase() string {
	return ResourceID{
		SubscriptionID: s.SubID, ResourceGroup: s.ResourceGrp, Namespace: namespaceMicrosoftAPIM,
		Types: []string{apimServiceType}, Names: []string{s.Name},
	}.ARMID()
}

func (s *apimService) state(now time.Time) string {
	if s.Transition != "" && now.Before(s.ReadyAt) {
		return s.Transition
	}

	return provisioningStateSucceeded
}

func apimEtag(n uint64) string {
	var b [8]byte

	binary.BigEndian.PutUint64(b[:], n)

	return `"` + base64.StdEncoding.EncodeToString(b[:]) + `"`
}

func (s *apimService) bump() { s.Etag++ }

func (p *APIMProvider) activation(sku string) time.Duration {
	if sku == apimSKUConsumption {
		return p.fastActivate
	}

	return p.classicActive
}

func apimSKURange(name string) (int, int, bool) {
	switch name {
	case apimSKUConsumption:
		return 0, 0, true
	case "Developer":
		return 1, 1, true
	case "Basic":
		return 1, apimBasicMax, true
	case skuTierStandard:
		return 1, apimStandardMax, true
	case apimSKUPremium:
		return 1, apimPremiumMax, true
	}

	return 0, 0, false
}

// apimLimits are the per-instance resource limits of one sku (azure-subscription-service-limits, "API Management
// limits"); zero means the table lists no limit.
type apimLimits struct {
	ops, named, products, subs, policyBytes int
}

const (
	apimMaxOps          = 75000
	limOpsLow           = 3000
	limOpsBasic         = 10000
	limOpsStd           = 50000
	limNamedLow         = 5000
	limNamedStd         = 10000
	limNamedPrem        = 18000
	limProdLow          = 100
	limProdBasic        = 200
	limProdStd          = 500
	limProdPrem         = 2000
	limSubsDev          = 10000
	limSubsBasic        = 15000
	limSubsStd          = 25000
	limSubsPrem         = 75000
	apimKiB             = 1024
	apimPolicyBytes     = 512 * apimKiB
	apimPolicyBytesCons = 16 * apimKiB
)

func apimLimitsFor(sku string) apimLimits {
	switch sku {
	case apimSKUConsumption:
		return apimLimits{ops: limOpsLow, named: limNamedLow, products: limProdLow, policyBytes: apimPolicyBytesCons}
	case "Developer":
		return apimLimits{
			ops: limOpsLow, named: limNamedLow, products: limProdLow, subs: limSubsDev, policyBytes: apimPolicyBytes,
		}
	case skuBasic:
		return apimLimits{
			ops:         limOpsBasic,
			named:       limNamedLow,
			products:    limProdBasic,
			subs:        limSubsBasic,
			policyBytes: apimPolicyBytes,
		}
	case skuTierStandard:
		return apimLimits{
			ops: limOpsStd, named: limNamedStd, products: limProdStd, subs: limSubsStd, policyBytes: apimPolicyBytes,
		}
	}

	return apimLimits{
		ops: apimMaxOps, named: limNamedPrem, products: limProdPrem, subs: limSubsPrem, policyBytes: apimPolicyBytes,
	}
}

func apimOverLimit(have, limit int, what string) error {
	if limit > 0 && have >= limit {
		return apimErr(http.StatusConflict, apimCodeConflict,
			"The service already has the maximum of %d %s for its sku.", limit, what)
	}

	return nil
}

func apimOwnedServiceProps() []string {
	return []string{
		apimCPublisherEmail, apimCPublisherName, apimCNotificationSenderEmail, sqlPropPublic, apimCProvisioningState,
		"targetProvisioningState", "createdAtUtc", "gatewayUrl", "gatewayRegionalUrl", "portalUrl",
		"developerPortalUrl", "managementApiUrl", "scmUrl", "platformVersion", "publicIPAddresses",
		"privateIPAddresses", "outboundPublicIPAddresses", apimCRestore,
	}
}

func (s *apimService) applyBody(body map[string]any, prev *apimService) error {
	if sku := mapOf(body[aksFieldSKU]); sku != nil {
		s.SKU, s.Capacity = strOf(sku, fieldName), intOf(sku[apimCapacity])
	}

	if err := s.checkSKU(prev); err != nil {
		return err
	}

	props := propsOf(body)

	if _, has := props[apimCRestore]; has {
		return apimInvalid("Restoring a soft-deleted service is not supported.")
	}

	if err := s.applyPublisher(props); err != nil {
		return err
	}

	if v, has := props[apimCNotificationSenderEmail].(string); has {
		s.NotifyEmail = v
	}

	if v, has := props[sqlPropPublic].(string); has {
		if v != sqlDefaultPublic && v != apimDisabled {
			return apimInvalid("The publicNetworkAccess must be Enabled or Disabled.")
		}

		s.PublicAcc = v
	}

	maps.Copy(s.Extra, takeProps(body, apimOwnedServiceProps()...))

	return nil
}

func (s *apimService) checkSKU(prev *apimService) error {
	lo, hi, ok := apimSKURange(s.SKU)
	if !ok {
		return apimInvalid("The sku name must be one of Consumption, Developer, Basic, Standard or Premium.")
	}

	if s.Capacity < lo || s.Capacity > hi {
		return apimInvalid("The capacity of the %s sku must be between %d and %d.", s.SKU, lo, hi)
	}

	if prev != nil && (prev.SKU == apimSKUConsumption) != (s.SKU == apimSKUConsumption) {
		return apimInvalid("The service cannot be converted to or from the Consumption sku.")
	}

	return nil
}

func (s *apimService) applyPublisher(props map[string]any) error {
	if v, has := props[apimCPublisherEmail].(string); has {
		s.PublisherEm = v
	}

	if v, has := props[apimCPublisherName].(string); has {
		s.PublisherNm = v
	}

	if _, err := mail.ParseAddress(s.PublisherEm); err != nil || len(s.PublisherEm) > apimMaxPublisher {
		return apimInvalid(
			"The publisherEmail is required and must be a valid email address of at most 100 characters.",
		)
	}

	if s.PublisherNm == "" || len(s.PublisherNm) > apimMaxPublisher {
		return apimInvalid("The publisherName is required and must be 1 to 100 characters long.")
	}

	return nil
}

func (s *apimService) applyEnvelope(body map[string]any) {
	if loc := strOf(body, fieldLocation); loc != "" {
		s.Location = loc
	}

	if _, has := body[fieldTags]; has {
		s.Tags = stringTags(body[fieldTags])
	}

	if z, ok := body[cmpFieldZones].([]any); ok {
		s.Zones = z
	}

	if id := mapOf(body[fieldIdentitySet]); id != nil {
		s.Identity = map[string]any{fieldType: strOf(id, fieldType)}
		if t := strOf(id, fieldType); strings.Contains(t, "SystemAssigned") {
			s.Identity[fieldPrincipalID] = uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.ToLower(s.armBase()))).
				String()
			s.Identity[kvFieldTenantID] = DefaultTenantID
		}

		if ua, has := id["userAssignedIdentities"]; has {
			s.Identity["userAssignedIdentities"] = ua
		}
	}
}

func (s *apimService) clone() *apimService {
	c := *s
	c.Tags, c.Extra = cloneStrMap(s.Tags), cloneAnyMap(s.Extra)
	c.ProductAPIs = maps.Clone(s.ProductAPIs)

	for _, m := range []struct {
		dst *map[string]*apimEntity
		src map[string]*apimEntity
	}{
		{&c.APIs, s.APIs}, {&c.Ops, s.Ops}, {&c.Products, s.Products}, {&c.Subs, s.Subs},
		{&c.Named, s.Named}, {&c.Backends, s.Backends}, {&c.Policies, s.Policies},
	} {
		*m.dst = maps.Clone(m.src)
	}

	return &c
}

func newAPIMService(sub, rg, name string, now time.Time) *apimService {
	return &apimService{
		Name: name, ResourceGrp: rg, SubID: sub, Location: DefaultLocation, CreatedAt: now.UTC(),
		NotifyEmail: apimDefaultNotifyMail, PublicAcc: sqlDefaultPublic, Extra: map[string]any{},
		APIs: map[string]*apimEntity{}, Ops: map[string]*apimEntity{}, Products: map[string]*apimEntity{},
		Subs: map[string]*apimEntity{}, Named: map[string]*apimEntity{}, Backends: map[string]*apimEntity{},
		Policies: map[string]*apimEntity{}, ProductAPIs: map[string]bool{},
	}
}

func gatewayHost(name string) string { return name + apimGatewaySuffix }

func (s *apimService) body(now time.Time) map[string]any {
	return s.bodyAs(now, "")
}

// createdBody is the 201 answer of the PUT that created the service: provisioningState Created heading for
// Activating.
func (s *apimService) createdBody(now time.Time) map[string]any {
	return s.bodyAs(now, apimStateCreated)
}

func (s *apimService) regionalURL() string {
	slug := strings.ToLower(strings.ReplaceAll(s.Location, " ", ""))

	return "https://" + s.Name + "-" + slug + "-01.regional.azure-api.net"
}

func (s *apimService) bodyAs(now time.Time, override string) map[string]any {
	state := s.state(now)
	props := cloneAnyMap(s.Extra)
	host := gatewayHost(s.Name)
	pending := state == apimStateActivating || override != ""

	props[apimCProvisioningState] = state
	props["targetProvisioningState"] = ""

	if override != "" {
		props[apimCProvisioningState], props["targetProvisioningState"] = override, apimStateActivating
	} else if state != provisioningStateSucceeded {
		props["targetProvisioningState"] = state
	}

	props[apimCPublisherEmail], props[apimCPublisherName] = s.PublisherEm, s.PublisherNm
	props[apimCNotificationSenderEmail], props[sqlPropPublic] = s.NotifyEmail, s.PublicAcc
	props["createdAtUtc"] = s.CreatedAt.UTC().Format(time.RFC3339)
	props["gatewayUrl"] = "https://" + host
	props["platformVersion"] = apimPlatformStv2

	if s.SKU == apimSKUConsumption {
		props["platformVersion"] = apimPlatformMtv1
	} else if !pending {
		props["gatewayRegionalUrl"] = s.regionalURL()
		props["portalUrl"] = "https://" + s.Name + ".portal.azure-api.net"
		props["developerPortalUrl"] = "https://" + s.Name + ".developer.azure-api.net"
		props["managementApiUrl"] = "https://" + s.Name + ".management.azure-api.net"
		props["scmUrl"] = "https://" + s.Name + ".scm.azure-api.net"
	}

	if _, has := props["virtualNetworkType"]; !has {
		props["virtualNetworkType"] = apimVNetNone
	}

	if _, has := props["hostnameConfigurations"]; !has {
		props["hostnameConfigurations"] = []any{map[string]any{
			fieldType: "Proxy", apimCHostName: host, "negotiateClientCertificate": false, "defaultSslBinding": true,
			"certificateSource": "BuiltIn",
		}}
	}

	out := map[string]any{
		cosmosFieldID: s.armBase(), fieldName: s.Name, fieldType: apimARMType, fieldLocation: s.Location,
		aksFieldSKU: map[string]any{fieldName: s.SKU, apimCapacity: s.Capacity},
		"etag":      apimEtag(s.Etag), fieldProperties: props,
	}

	if s.Tags != nil {
		out[fieldTags] = s.Tags
	}

	if s.Identity != nil {
		out[fieldIdentitySet] = s.Identity
	}

	if len(s.Zones) > 0 {
		out[cmpFieldZones] = s.Zones
	}

	return out
}

func (p *APIMProvider) nameTaken(sub, rg, name string) bool {
	for _, s := range p.svcs {
		if strings.EqualFold(s.Name, name) && apimKey(s.SubID, s.ResourceGrp, s.Name) != apimKey(sub, rg, name) {
			return true
		}
	}

	return false
}

func apimServiceNameError(name string) error {
	if !apimServiceNameRE.MatchString(name) {
		return apimInvalid(
			"The service name must be 1 to 50 characters: letters, digits and hyphens, " +
				"starting with a letter and ending with a letter or digit.",
		)
	}

	return nil
}

// writeService creates or updates a service from a PUT (patch false) or PATCH (patch true) body. The caller holds
// the write lock.
func (p *APIMProvider) writeService(
	sub, rg, name string, body map[string]any, patch bool,
) (*apimService, bool, error) {
	key := apimKey(sub, rg, name)
	prev := p.svcs[key]
	now := p.now()

	if prev == nil && patch {
		return nil, false, apimNotFound("API Management service")
	}

	if prev == nil {
		if err := apimServiceNameError(name); err != nil {
			return nil, false, err
		}

		if p.nameTaken(sub, rg, name) {
			return nil, false, apimErr(http.StatusConflict, apimCodeConflict,
				"The service name %q is already in use.", name)
		}

		if len(p.svcs) >= apimMaxServices {
			return nil, false, apimErr(http.StatusConflict, apimCodeConflict, "Too many API Management services.")
		}
	}

	if prev != nil && prev.state(now) != provisioningStateSucceeded {
		return nil, false, apimBusy()
	}

	next := newAPIMService(sub, rg, name, now)
	if prev != nil {
		next = prev.clone()
	}

	next.now = p.now

	oldSKU, oldCap := next.SKU, next.Capacity
	if err := next.applyBody(body, prev); err != nil {
		return nil, false, err
	}

	next.applyEnvelope(body)
	next.bump()

	switch {
	case prev == nil:
		next.Transition, next.ReadyAt = apimStateActivating, now.Add(p.activation(next.SKU))
	case oldSKU != next.SKU || oldCap != next.Capacity:
		next.Transition, next.ReadyAt = apimStateUpdating, now.Add(p.activation(next.SKU))
	}

	p.svcs[key] = next

	return next, prev == nil, nil
}

func apimBusy() error {
	return apimErr(http.StatusConflict, apimCodeConflict,
		"The service is being activated or updated; retry when its provisioningState is Succeeded.")
}

// Put implements ResourceProvider.
func (p *APIMProvider) Put(_ context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	p.mu.Lock(apimCPut)
	defer p.mu.Unlock()

	s, created, err := p.writeService(id.SubscriptionID, id.ResourceGroup, id.LeafName(), body, false)
	if err != nil {
		return nil, err
	}

	if created {
		return s.createdBody(p.now()), nil
	}

	return s.body(p.now()), nil
}

// Get implements ResourceProvider.
func (p *APIMProvider) Get(_ context.Context, id ResourceID) (map[string]any, error) {
	p.mu.RLock(apimCGet)
	defer p.mu.RUnlock()

	s, ok := p.svcs[apimKey(id.SubscriptionID, id.ResourceGroup, id.LeafName())]
	if !ok {
		return nil, apimNotFound("API Management service")
	}

	return s.body(p.now()), nil
}

// Delete implements ResourceProvider.
func (p *APIMProvider) Delete(_ context.Context, id ResourceID) error {
	p.mu.Lock(ehCleanupDelete)
	defer p.mu.Unlock()

	key := apimKey(id.SubscriptionID, id.ResourceGroup, id.LeafName())
	if _, ok := p.svcs[key]; !ok {
		return apimNotFound("API Management service")
	}

	delete(p.svcs, key)

	return nil
}

// List implements ResourceProvider.
func (p *APIMProvider) List(_ context.Context, id ResourceID) ([]map[string]any, error) {
	p.mu.RLock(apimCList)
	defer p.mu.RUnlock()

	var out []map[string]any

	for _, s := range p.svcs {
		if id.ResourceGroup != "" && !resourceGroupsEqual(s.ResourceGrp, id.ResourceGroup) {
			continue
		}

		if id.SubscriptionID != "" && !strings.EqualFold(s.SubID, id.SubscriptionID) {
			continue
		}

		out = append(out, s.body(p.now()))
	}

	sort.Slice(out, func(i, j int) bool { return nameField(out[i]) < nameField(out[j]) })

	return out, nil
}

// ListKeys implements ResourceProvider; services have no listKeys action (subscriptions use listSecrets).
func (p *APIMProvider) ListKeys(context.Context, ResourceID) (map[string]any, error) {
	return nil, ErrResourceNotFound
}

// Reset implements ResourceProvider.
func (p *APIMProvider) Reset() {
	p.mu.Lock(apimCReset)
	p.svcs = map[string]*apimService{}
	p.ops = map[string]*apimOp{}
	p.mu.Unlock()
	p.limiter.reset()
}

// DeleteResourcesInGroup implements ResourceProvider.
func (p *APIMProvider) DeleteResourcesInGroup(_ context.Context, resourceGroup string) {
	p.mu.Lock(apimCDeleteResourcesInGroup)
	defer p.mu.Unlock()

	for key, s := range p.svcs {
		if resourceGroupsEqual(s.ResourceGrp, resourceGroup) {
			delete(p.svcs, key)
		}
	}
}

// CheckName answers checkNameAvailability for a service name.
func (p *APIMProvider) CheckName(sub, name string) map[string]any {
	p.mu.RLock(apimCCheckName)
	defer p.mu.RUnlock()

	if err := apimServiceNameError(name); err != nil {
		return map[string]any{
			kvFieldNameAvailable: false,
			acFieldReason:        apimReasonInvalid,
			fieldMessage:         apimMessage(err),
		}
	}

	if p.nameTaken(sub, "", name) {
		return map[string]any{
			kvFieldNameAvailable: false, acFieldReason: apimReasonExists,
			fieldMessage: fmt.Sprintf("The service name %q is already in use.", name),
		}
	}

	return map[string]any{kvFieldNameAvailable: true, acFieldReason: "Valid", fieldMessage: ""}
}

func apimMessage(err error) string {
	if ae, ok := errors.AsType[*apimError](err); ok {
		return ae.msg
	}

	return err.Error()
}
