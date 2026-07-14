package vpclattice

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	arnService                     = "vpc-lattice"
	resourceService                = "service"
	resourceServiceNetwork         = "servicenetwork"
	resourceServiceNetworkSvcAssoc = "servicenetworkserviceassociation"
	resourceServiceNetworkVpcAssoc = "servicenetworkvpcassociation"
	resourceListener               = "listener"
	resourceRule                   = "rule"
	resourceTargetGroup            = "targetgroup"
	resourceAccessLogSubscription  = "accesslogsubscription"

	idPrefixService     = "svc-"
	idPrefixNetwork     = "sn-"
	idPrefixSNSA        = "snsa-"
	idPrefixSNVA        = "snva-"
	idPrefixListener    = "listener-"
	idPrefixRule        = "rule-"
	idPrefixTargetGroup = "tg-"
	idPrefixALS         = "als-"

	statusActive           = "ACTIVE"
	statusInactive         = "INACTIVE"
	statusCreateInProgress = "CREATE_IN_PROGRESS"
	statusDeleteInProgress = "DELETE_IN_PROGRESS"
	statusDeleted          = "DELETED"
	statusCreateFailed     = "CREATE_FAILED"

	authTypeNone  = "NONE"
	protocolHTTP  = "HTTP"
	protocolHTTPS = "HTTPS"

	tgStatusActive = "ACTIVE"

	targetStatusHealthy = "HEALTHY"

	authPolicyStateActive = "Active"

	defaultRulePriority = 100

	defaultMaxResults = 100
)

var (
	// ErrNotFound is returned when a resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists with the same name.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	// ErrInvalidParameter is returned for invalid input.
	ErrInvalidParameter = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

// storedService holds a service with all fields.
type storedService struct {
	CreatedAt        time.Time         `json:"createdAt"`
	LastUpdatedAt    time.Time         `json:"lastUpdatedAt"`
	Tags             map[string]string `json:"tags"`
	ARN              string            `json:"arn"`
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	AuthType         string            `json:"authType"`
	CertificateArn   string            `json:"certificateArn"`
	CustomDomainName string            `json:"customDomainName"`
	DNSName          string            `json:"dnsName"`
	Status           string            `json:"status"`
	Region           string            `json:"region"`
}

func (s *storedService) toService() *Service {
	return &Service{
		ARN:              s.ARN,
		ID:               s.ID,
		Name:             s.Name,
		AuthType:         s.AuthType,
		CertificateArn:   s.CertificateArn,
		CustomDomainName: s.CustomDomainName,
		DNSName:          s.DNSName,
		Status:           s.Status,
		CreatedAt:        s.CreatedAt,
		LastUpdatedAt:    s.LastUpdatedAt,
	}
}

func (s *storedService) toSummary() *ServiceSummary {
	return &ServiceSummary{
		ARN:              s.ARN,
		ID:               s.ID,
		Name:             s.Name,
		CustomDomainName: s.CustomDomainName,
		DNSName:          s.DNSName,
		Status:           s.Status,
		CreatedAt:        s.CreatedAt,
		LastUpdatedAt:    s.LastUpdatedAt,
	}
}

// storedServiceNetwork holds a service network.
type storedServiceNetwork struct {
	CreatedAt                  time.Time         `json:"createdAt"`
	LastUpdatedAt              time.Time         `json:"lastUpdatedAt"`
	Tags                       map[string]string `json:"tags"`
	ARN                        string            `json:"arn"`
	ID                         string            `json:"id"`
	Name                       string            `json:"name"`
	AuthType                   string            `json:"authType"`
	Region                     string            `json:"region"`
	NumberOfAssociatedServices int64             `json:"numberOfAssociatedServices"`
	NumberOfAssociatedVPCs     int64             `json:"numberOfAssociatedVpcs"`
}

func (s *storedServiceNetwork) toServiceNetwork() *ServiceNetwork {
	return &ServiceNetwork{
		ARN:                        s.ARN,
		ID:                         s.ID,
		Name:                       s.Name,
		AuthType:                   s.AuthType,
		NumberOfAssociatedServices: s.NumberOfAssociatedServices,
		NumberOfAssociatedVPCs:     s.NumberOfAssociatedVPCs,
		CreatedAt:                  s.CreatedAt,
		LastUpdatedAt:              s.LastUpdatedAt,
	}
}

func (s *storedServiceNetwork) toSummary() *ServiceNetworkSummary {
	return &ServiceNetworkSummary{
		ARN:                        s.ARN,
		ID:                         s.ID,
		Name:                       s.Name,
		NumberOfAssociatedServices: s.NumberOfAssociatedServices,
		NumberOfAssociatedVPCs:     s.NumberOfAssociatedVPCs,
		CreatedAt:                  s.CreatedAt,
	}
}

// storedSNSA holds a service network service association.
type storedSNSA struct {
	CreatedAt          time.Time         `json:"createdAt"`
	Tags               map[string]string `json:"tags"`
	ARN                string            `json:"arn"`
	ID                 string            `json:"id"`
	ServiceARN         string            `json:"serviceArn"`
	ServiceID          string            `json:"serviceId"`
	ServiceName        string            `json:"serviceName"`
	ServiceNetworkARN  string            `json:"serviceNetworkArn"`
	ServiceNetworkID   string            `json:"serviceNetworkId"`
	ServiceNetworkName string            `json:"serviceNetworkName"`
	Status             string            `json:"status"`
	CreatedBy          string            `json:"createdBy"`
	CustomDomainName   string            `json:"customDomainName"`
	DNSName            string            `json:"dnsName"`
	Region             string            `json:"region"`
}

func (s *storedSNSA) toAssociation() *ServiceNetworkServiceAssociation {
	return &ServiceNetworkServiceAssociation{
		ARN:                s.ARN,
		ID:                 s.ID,
		ServiceARN:         s.ServiceARN,
		ServiceID:          s.ServiceID,
		ServiceName:        s.ServiceName,
		ServiceNetworkARN:  s.ServiceNetworkARN,
		ServiceNetworkID:   s.ServiceNetworkID,
		ServiceNetworkName: s.ServiceNetworkName,
		Status:             s.Status,
		CreatedBy:          s.CreatedBy,
		CustomDomainName:   s.CustomDomainName,
		DNSName:            s.DNSName,
		CreatedAt:          s.CreatedAt,
	}
}

func (s *storedSNSA) toSummary() *ServiceNetworkServiceAssociationSummary {
	return &ServiceNetworkServiceAssociationSummary{
		ARN:                s.ARN,
		ID:                 s.ID,
		ServiceARN:         s.ServiceARN,
		ServiceID:          s.ServiceID,
		ServiceName:        s.ServiceName,
		ServiceNetworkARN:  s.ServiceNetworkARN,
		ServiceNetworkID:   s.ServiceNetworkID,
		ServiceNetworkName: s.ServiceNetworkName,
		Status:             s.Status,
		CustomDomainName:   s.CustomDomainName,
		DNSName:            s.DNSName,
		CreatedAt:          s.CreatedAt,
	}
}

// storedSNVA holds a service network VPC association.
type storedSNVA struct {
	CreatedAt          time.Time         `json:"createdAt"`
	LastUpdatedAt      time.Time         `json:"lastUpdatedAt"`
	Tags               map[string]string `json:"tags"`
	ServiceNetworkName string            `json:"serviceNetworkName"`
	ARN                string            `json:"arn"`
	ID                 string            `json:"id"`
	VpcID              string            `json:"vpcId"`
	ServiceNetworkARN  string            `json:"serviceNetworkArn"`
	ServiceNetworkID   string            `json:"serviceNetworkId"`
	Status             string            `json:"status"`
	CreatedBy          string            `json:"createdBy"`
	Region             string            `json:"region"`
	SecurityGroupIDs   []string          `json:"securityGroupIds"`
}

func (s *storedSNVA) toAssociation() *ServiceNetworkVpcAssociation {
	sgs := make([]string, len(s.SecurityGroupIDs))
	copy(sgs, s.SecurityGroupIDs)

	return &ServiceNetworkVpcAssociation{
		ARN:                s.ARN,
		ID:                 s.ID,
		VpcID:              s.VpcID,
		ServiceNetworkARN:  s.ServiceNetworkARN,
		ServiceNetworkID:   s.ServiceNetworkID,
		ServiceNetworkName: s.ServiceNetworkName,
		SecurityGroupIDs:   sgs,
		Status:             s.Status,
		CreatedBy:          s.CreatedBy,
		CreatedAt:          s.CreatedAt,
		LastUpdatedAt:      s.LastUpdatedAt,
	}
}

func (s *storedSNVA) toSummary() *ServiceNetworkVpcAssociationSummary {
	return &ServiceNetworkVpcAssociationSummary{
		ARN:                s.ARN,
		ID:                 s.ID,
		VpcID:              s.VpcID,
		ServiceNetworkARN:  s.ServiceNetworkARN,
		ServiceNetworkID:   s.ServiceNetworkID,
		ServiceNetworkName: s.ServiceNetworkName,
		Status:             s.Status,
		CreatedAt:          s.CreatedAt,
	}
}

// storedListener holds a listener.
type storedListener struct {
	Tags          map[string]string `json:"tags"`
	DefaultAction *RuleAction       `json:"defaultAction"`
	CreatedAt     time.Time         `json:"createdAt"`
	LastUpdatedAt time.Time         `json:"lastUpdatedAt"`
	ARN           string            `json:"arn"`
	ID            string            `json:"id"`
	ServiceARN    string            `json:"serviceArn"`
	ServiceID     string            `json:"serviceId"`
	Name          string            `json:"name"`
	Protocol      string            `json:"protocol"`
	Port          int32             `json:"port"`
}

func (l *storedListener) toListener() *Listener {
	return &Listener{
		ARN:           l.ARN,
		ID:            l.ID,
		ServiceARN:    l.ServiceARN,
		ServiceID:     l.ServiceID,
		Name:          l.Name,
		Protocol:      l.Protocol,
		Port:          l.Port,
		DefaultAction: l.DefaultAction,
		CreatedAt:     l.CreatedAt,
		LastUpdatedAt: l.LastUpdatedAt,
	}
}

func (l *storedListener) toSummary() *ListenerSummary {
	return &ListenerSummary{
		ARN:           l.ARN,
		ID:            l.ID,
		Name:          l.Name,
		Protocol:      l.Protocol,
		Port:          l.Port,
		CreatedAt:     l.CreatedAt,
		LastUpdatedAt: l.LastUpdatedAt,
	}
}

// storedRule holds a listener rule.
type storedRule struct {
	Tags          map[string]string `json:"tags"`
	Action        *RuleAction       `json:"action"`
	Match         *RuleMatch        `json:"match"`
	CreatedAt     time.Time         `json:"createdAt"`
	LastUpdatedAt time.Time         `json:"lastUpdatedAt"`
	ARN           string            `json:"arn"`
	ID            string            `json:"id"`
	ListenerID    string            `json:"listenerId"`
	ServiceID     string            `json:"serviceId"`
	Name          string            `json:"name"`
	Priority      int32             `json:"priority"`
	IsDefault     bool              `json:"isDefault"`
}

func (r *storedRule) toRule() *Rule {
	return &Rule{
		ARN:           r.ARN,
		ID:            r.ID,
		Name:          r.Name,
		Priority:      r.Priority,
		Action:        r.Action,
		Match:         r.Match,
		IsDefault:     r.IsDefault,
		CreatedAt:     r.CreatedAt,
		LastUpdatedAt: r.LastUpdatedAt,
	}
}

func (r *storedRule) toSummary() *RuleSummary {
	return &RuleSummary{
		ARN:           r.ARN,
		ID:            r.ID,
		Name:          r.Name,
		Priority:      r.Priority,
		IsDefault:     r.IsDefault,
		CreatedAt:     r.CreatedAt,
		LastUpdatedAt: r.LastUpdatedAt,
	}
}

// storedTargetGroup holds a target group.
type storedTargetGroup struct {
	CreatedAt     time.Time          `json:"createdAt"`
	LastUpdatedAt time.Time          `json:"lastUpdatedAt"`
	Tags          map[string]string  `json:"tags"`
	Config        *TargetGroupConfig `json:"config"`
	ARN           string             `json:"arn"`
	ID            string             `json:"id"`
	Name          string             `json:"name"`
	Type          string             `json:"type"`
	Status        string             `json:"status"`
	Region        string             `json:"region"`
	ServiceARNs   []string           `json:"serviceArns"`
}

func (tg *storedTargetGroup) toTargetGroup() *TargetGroup {
	arns := make([]string, len(tg.ServiceARNs))
	copy(arns, tg.ServiceARNs)

	return &TargetGroup{
		ARN:           tg.ARN,
		ID:            tg.ID,
		Name:          tg.Name,
		Type:          tg.Type,
		Status:        tg.Status,
		Config:        tg.Config,
		ServiceARNs:   arns,
		CreatedAt:     tg.CreatedAt,
		LastUpdatedAt: tg.LastUpdatedAt,
	}
}

func (tg *storedTargetGroup) toSummary() *TargetGroupSummary {
	s := &TargetGroupSummary{
		ARN:           tg.ARN,
		ID:            tg.ID,
		Name:          tg.Name,
		Type:          tg.Type,
		Status:        tg.Status,
		CreatedAt:     tg.CreatedAt,
		LastUpdatedAt: tg.LastUpdatedAt,
		ServiceARNs:   make([]string, len(tg.ServiceARNs)),
	}
	copy(s.ServiceARNs, tg.ServiceARNs)

	if tg.Config != nil {
		s.Port = tg.Config.Port
		s.Protocol = tg.Config.Protocol
		s.VpcID = tg.Config.VpcID
		s.IPAddressType = tg.Config.IPAddressType
		s.LambdaEventStructureVersion = tg.Config.LambdaEventStructureVersion
	}

	return s
}

// storedTarget holds a registered target.
type storedTarget struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Port   int32  `json:"port"`
}

// storedALS holds an access log subscription.
type storedALS struct {
	CreatedAt             time.Time         `json:"createdAt"`
	LastUpdatedAt         time.Time         `json:"lastUpdatedAt"`
	Tags                  map[string]string `json:"tags"`
	ARN                   string            `json:"arn"`
	ID                    string            `json:"id"`
	ResourceARN           string            `json:"resourceArn"`
	ResourceID            string            `json:"resourceId"`
	DestinationARN        string            `json:"destinationArn"`
	ServiceNetworkLogType string            `json:"serviceNetworkLogType"`
}

func (a *storedALS) toALS() *AccessLogSubscription {
	return &AccessLogSubscription{
		ARN:                   a.ARN,
		ID:                    a.ID,
		ResourceARN:           a.ResourceARN,
		ResourceID:            a.ResourceID,
		DestinationARN:        a.DestinationARN,
		ServiceNetworkLogType: a.ServiceNetworkLogType,
		CreatedAt:             a.CreatedAt,
		LastUpdatedAt:         a.LastUpdatedAt,
	}
}

func (a *storedALS) toSummary() *AccessLogSubscriptionSummary {
	return &AccessLogSubscriptionSummary{
		ARN:            a.ARN,
		ID:             a.ID,
		ResourceARN:    a.ResourceARN,
		ResourceID:     a.ResourceID,
		DestinationARN: a.DestinationARN,
		CreatedAt:      a.CreatedAt,
		LastUpdatedAt:  a.LastUpdatedAt,
	}
}

// InMemoryBackend is an in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	mu       *lockmetrics.RWMutex
	registry *store.Registry

	services        *store.Table[storedService]
	servicesByName  *store.Index[storedService]
	serviceNetworks *store.Table[storedServiceNetwork]
	networksByName  *store.Index[storedServiceNetwork]
	snsas           *store.Table[storedSNSA]
	snvas           *store.Table[storedSNVA]

	listeners          *store.Table[storedListener]
	listenersByService *store.Index[storedListener]

	rules           *store.Table[storedRule]
	rulesByListener *store.Index[storedRule]

	targetGroups *store.Table[storedTargetGroup]
	tgsByName    *store.Index[storedTargetGroup]
	targets      map[string][]*storedTarget

	alss             *store.Table[storedALS]
	authPolicies     map[string]string
	resourcePolicies map[string]string
	tags             map[string]map[string]string
	accountID        string
	region           string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:               lockmetrics.New("vpclattice"),
		registry:         store.NewRegistry(),
		targets:          make(map[string][]*storedTarget),
		authPolicies:     make(map[string]string),
		resourcePolicies: make(map[string]string),
		tags:             make(map[string]map[string]string),
		accountID:        accountID,
		region:           region,
	}
	registerAllTables(b)

	return b
}

// AccountID returns the configured account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the configured region.
func (b *InMemoryBackend) Region() string { return b.region }

func (b *InMemoryBackend) regionFor(ctx context.Context) string {
	if r := awsmeta.Region(ctx); r != "" {
		return r
	}

	return b.region
}

// Reset clears all stored data.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.targets = make(map[string][]*storedTarget)
	b.authPolicies = make(map[string]string)
	b.resourcePolicies = make(map[string]string)
	b.tags = make(map[string]map[string]string)
}

func (b *InMemoryBackend) buildListenerARN(serviceID, listenerID string) string {
	return arn.Build(arnService, b.region, b.accountID,
		fmt.Sprintf("%s/%s/%s/%s", resourceService, serviceID, resourceListener, listenerID))
}

func (b *InMemoryBackend) buildRuleARN(serviceID, listenerID, ruleID string) string {
	return arn.Build(
		arnService,
		b.region,
		b.accountID,
		fmt.Sprintf(
			"%s/%s/%s/%s/%s/%s",
			resourceService,
			serviceID,
			resourceListener,
			listenerID,
			resourceRule,
			ruleID,
		),
	)
}

func newID(prefix string) string {
	id := strings.ReplaceAll(uuid.NewString(), "-", "")[:17]

	return prefix + id
}

func copyTags(src map[string]string) map[string]string {
	if src == nil {
		return make(map[string]string)
	}

	dst := make(map[string]string, len(src))
	maps.Copy(dst, src)

	return dst
}

// resolveServiceID resolves a service identifier (ID or ARN) to an ID.
func (b *InMemoryBackend) resolveServiceID(identifier string) (string, bool) {
	if svc, ok := b.services.Get(identifier); ok {
		return svc.ID, true
	}
	// check if it's an ARN
	for _, svc := range b.services.All() {
		if svc.ARN == identifier {
			return svc.ID, true
		}
	}

	return "", false
}

// resolveServiceNetworkID resolves a service network identifier to an ID.
func (b *InMemoryBackend) resolveServiceNetworkID(identifier string) (string, bool) {
	if b.serviceNetworks.Has(identifier) {
		return identifier, true
	}
	for _, sn := range b.serviceNetworks.All() {
		if sn.ARN == identifier || sn.Name == identifier {
			return sn.ID, true
		}
	}

	return "", false
}

// resolveListenerID resolves a listener identifier to (serviceID, listenerID).
func (b *InMemoryBackend) resolveListenerID(serviceID, identifier string) (string, bool) {
	if l, ok := b.listeners.Get(identifier); ok && l.ServiceID == serviceID {
		return identifier, true
	}
	for _, l := range b.listenersByService.Get(serviceID) {
		if l.ARN == identifier {
			return l.ID, true
		}
	}

	return "", false
}

// resolveRuleID resolves a rule identifier within a listener to a rule ID.
func (b *InMemoryBackend) resolveRuleID(serviceID, listenerID, identifier string) (string, bool) {
	if r, ok := b.rules.Get(identifier); ok && r.ServiceID == serviceID && r.ListenerID == listenerID {
		return identifier, true
	}
	for _, r := range b.rulesByListener.Get(listenerID) {
		if r.ServiceID == serviceID && r.ARN == identifier {
			return r.ID, true
		}
	}

	return "", false
}

// resolveTargetGroupID resolves a target group identifier to an ID.
func (b *InMemoryBackend) resolveTargetGroupID(identifier string) (string, bool) {
	if b.targetGroups.Has(identifier) {
		return identifier, true
	}
	for _, tg := range b.targetGroups.All() {
		if tg.ARN == identifier {
			return tg.ID, true
		}
	}

	return "", false
}

// resolveALSID resolves an access log subscription identifier.
func (b *InMemoryBackend) resolveALSID(identifier string) (string, bool) {
	if b.alss.Has(identifier) {
		return identifier, true
	}
	for _, a := range b.alss.All() {
		if a.ARN == identifier {
			return a.ID, true
		}
	}

	return "", false
}

// resolveSNSAID resolves a SNSA identifier.
func (b *InMemoryBackend) resolveSNSAID(identifier string) (string, bool) {
	if b.snsas.Has(identifier) {
		return identifier, true
	}
	for _, s := range b.snsas.All() {
		if s.ARN == identifier {
			return s.ID, true
		}
	}

	return "", false
}

// resolveSNVAID resolves a SNVA identifier.
func (b *InMemoryBackend) resolveSNVAID(identifier string) (string, bool) {
	if b.snvas.Has(identifier) {
		return identifier, true
	}
	for _, s := range b.snvas.All() {
		if s.ARN == identifier {
			return s.ID, true
		}
	}

	return "", false
}

// ------- Service operations -------

// CreateService creates a new service.
func (b *InMemoryBackend) CreateService(
	ctx context.Context,
	name, authType, certificateArn, customDomainName string,
	tags map[string]string,
) (*Service, error) {
	if name == "" {
		return nil, ErrInvalidParameter
	}

	b.mu.Lock("CreateService")
	defer b.mu.Unlock()

	if len(b.servicesByName.Get(name)) > 0 {
		return nil, ErrAlreadyExists
	}

	now := time.Now().UTC()
	id := newID(idPrefixService)
	region := b.regionFor(ctx)
	svcARN := arn.Build(arnService, region, b.accountID, resourceService+"/"+id)

	if authType == "" {
		authType = authTypeNone
	}

	svc := &storedService{
		ARN:              svcARN,
		ID:               id,
		Name:             name,
		AuthType:         authType,
		CertificateArn:   certificateArn,
		CustomDomainName: customDomainName,
		DNSName:          id + ".vpc-lattice-svcs." + region + ".on.aws",
		Status:           statusActive,
		Tags:             copyTags(tags),
		CreatedAt:        now,
		LastUpdatedAt:    now,
		Region:           region,
	}

	b.services.Put(svc)
	b.tags[svcARN] = copyTags(tags)

	return svc.toService(), nil
}

// GetService returns a service by ID or ARN.
func (b *InMemoryBackend) GetService(serviceID string) (*Service, error) {
	b.mu.RLock("GetService")
	defer b.mu.RUnlock()

	id, ok := b.resolveServiceID(serviceID)
	if !ok {
		return nil, ErrNotFound
	}

	svc, _ := b.services.Get(id)

	return svc.toService(), nil
}

// UpdateService updates a service.
func (b *InMemoryBackend) UpdateService(
	serviceID, authType, certificateArn string,
) (*Service, error) {
	b.mu.Lock("UpdateService")
	defer b.mu.Unlock()

	id, ok := b.resolveServiceID(serviceID)
	if !ok {
		return nil, ErrNotFound
	}

	svc, _ := b.services.Get(id)
	if authType != "" {
		svc.AuthType = authType
	}

	svc.CertificateArn = certificateArn
	svc.LastUpdatedAt = time.Now().UTC()

	return svc.toService(), nil
}

// DeleteService deletes a service.
func (b *InMemoryBackend) DeleteService(serviceID string) (*Service, error) {
	b.mu.Lock("DeleteService")
	defer b.mu.Unlock()

	id, ok := b.resolveServiceID(serviceID)
	if !ok {
		return nil, ErrNotFound
	}

	svc, _ := b.services.Get(id)
	out := svc.toService()
	out.Status = statusDeleted

	b.services.Delete(id)
	delete(b.tags, svc.ARN)

	return out, nil
}

// ListServices returns a paginated list of services.
func (b *InMemoryBackend) ListServices(
	ctx context.Context,
	maxResults int32,
	nextToken string,
) ([]*ServiceSummary, string, error) {
	b.mu.RLock("ListServices")
	defer b.mu.RUnlock()

	region := b.regionFor(ctx)
	all := make([]*ServiceSummary, 0, b.services.Len())

	for _, svc := range b.services.All() {
		if svc.Region != region {
			continue
		}

		all = append(all, svc.toSummary())
	}

	slices.SortFunc(all, func(a, b *ServiceSummary) int { return strings.Compare(a.ID, b.ID) })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}

// ------- ServiceNetwork operations -------

// CreateServiceNetwork creates a new service network.
func (b *InMemoryBackend) CreateServiceNetwork(
	ctx context.Context,
	name, authType string,
	tags map[string]string,
) (*ServiceNetwork, error) {
	if name == "" {
		return nil, ErrInvalidParameter
	}

	b.mu.Lock("CreateServiceNetwork")
	defer b.mu.Unlock()

	if len(b.networksByName.Get(name)) > 0 {
		return nil, ErrAlreadyExists
	}

	now := time.Now().UTC()
	id := newID(idPrefixNetwork)
	region := b.regionFor(ctx)
	snARN := arn.Build(arnService, region, b.accountID, resourceServiceNetwork+"/"+id)

	if authType == "" {
		authType = authTypeNone
	}

	sn := &storedServiceNetwork{
		ARN:           snARN,
		ID:            id,
		Name:          name,
		AuthType:      authType,
		Tags:          copyTags(tags),
		CreatedAt:     now,
		LastUpdatedAt: now,
		Region:        region,
	}

	b.serviceNetworks.Put(sn)
	b.tags[snARN] = copyTags(tags)

	return sn.toServiceNetwork(), nil
}

// GetServiceNetwork returns a service network.
func (b *InMemoryBackend) GetServiceNetwork(snID string) (*ServiceNetwork, error) {
	b.mu.RLock("GetServiceNetwork")
	defer b.mu.RUnlock()

	id, ok := b.resolveServiceNetworkID(snID)
	if !ok {
		return nil, ErrNotFound
	}

	sn, _ := b.serviceNetworks.Get(id)

	// compute counts
	sn.NumberOfAssociatedServices = b.countSNSAs(id)
	sn.NumberOfAssociatedVPCs = b.countSNVAs(id)

	return sn.toServiceNetwork(), nil
}

func (b *InMemoryBackend) countSNSAs(snID string) int64 {
	var count int64
	for _, s := range b.snsas.All() {
		if s.ServiceNetworkID == snID {
			count++
		}
	}

	return count
}

func (b *InMemoryBackend) countSNVAs(snID string) int64 {
	var count int64
	for _, s := range b.snvas.All() {
		if s.ServiceNetworkID == snID {
			count++
		}
	}

	return count
}

// UpdateServiceNetwork updates a service network.
func (b *InMemoryBackend) UpdateServiceNetwork(snID, authType string) (*ServiceNetwork, error) {
	b.mu.Lock("UpdateServiceNetwork")
	defer b.mu.Unlock()

	id, ok := b.resolveServiceNetworkID(snID)
	if !ok {
		return nil, ErrNotFound
	}

	sn, _ := b.serviceNetworks.Get(id)
	if authType != "" {
		sn.AuthType = authType
	}

	sn.LastUpdatedAt = time.Now().UTC()

	return sn.toServiceNetwork(), nil
}

// DeleteServiceNetwork deletes a service network.
func (b *InMemoryBackend) DeleteServiceNetwork(snID string) error {
	b.mu.Lock("DeleteServiceNetwork")
	defer b.mu.Unlock()

	id, ok := b.resolveServiceNetworkID(snID)
	if !ok {
		return ErrNotFound
	}

	sn, _ := b.serviceNetworks.Get(id)
	b.serviceNetworks.Delete(id)
	delete(b.tags, sn.ARN)

	return nil
}

// ListServiceNetworks returns a paginated list of service networks.
func (b *InMemoryBackend) ListServiceNetworks(
	ctx context.Context,
	maxResults int32,
	nextToken string,
) ([]*ServiceNetworkSummary, string, error) {
	b.mu.RLock("ListServiceNetworks")
	defer b.mu.RUnlock()

	region := b.regionFor(ctx)
	all := make([]*ServiceNetworkSummary, 0, b.serviceNetworks.Len())

	for _, sn := range b.serviceNetworks.All() {
		if sn.Region != region {
			continue
		}

		all = append(all, sn.toSummary())
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}

// ------- ServiceNetworkServiceAssociation operations -------

// CreateServiceNetworkServiceAssociation creates a service-to-network association.
func (b *InMemoryBackend) CreateServiceNetworkServiceAssociation(
	ctx context.Context,
	serviceNetworkID, serviceID string,
	tags map[string]string,
) (*ServiceNetworkServiceAssociation, error) {
	b.mu.Lock("CreateServiceNetworkServiceAssociation")
	defer b.mu.Unlock()

	snID, ok := b.resolveServiceNetworkID(serviceNetworkID)
	if !ok {
		return nil, ErrNotFound
	}

	svcID, ok := b.resolveServiceID(serviceID)
	if !ok {
		return nil, ErrNotFound
	}

	// check for existing association
	for _, s := range b.snsas.All() {
		if s.ServiceNetworkID == snID && s.ServiceID == svcID {
			return nil, ErrAlreadyExists
		}
	}

	now := time.Now().UTC()
	id := newID(idPrefixSNSA)
	region := b.regionFor(ctx)
	assocARN := arn.Build(arnService, region, b.accountID, resourceServiceNetworkSvcAssoc+"/"+id)

	sn, _ := b.serviceNetworks.Get(snID)
	svc, _ := b.services.Get(svcID)

	snsa := &storedSNSA{
		ARN:                assocARN,
		ID:                 id,
		ServiceARN:         svc.ARN,
		ServiceID:          svcID,
		ServiceName:        svc.Name,
		ServiceNetworkARN:  sn.ARN,
		ServiceNetworkID:   snID,
		ServiceNetworkName: sn.Name,
		Status:             statusActive,
		CreatedBy:          b.accountID,
		CustomDomainName:   svc.CustomDomainName,
		DNSName:            svc.DNSName,
		Tags:               copyTags(tags),
		CreatedAt:          now,
		Region:             region,
	}

	b.snsas.Put(snsa)
	b.tags[assocARN] = copyTags(tags)

	return snsa.toAssociation(), nil
}

// GetServiceNetworkServiceAssociation returns a SNSA by ID or ARN.
func (b *InMemoryBackend) GetServiceNetworkServiceAssociation(
	snsaID string,
) (*ServiceNetworkServiceAssociation, error) {
	b.mu.RLock("GetServiceNetworkServiceAssociation")
	defer b.mu.RUnlock()

	id, ok := b.resolveSNSAID(snsaID)
	if !ok {
		return nil, ErrNotFound
	}

	s, _ := b.snsas.Get(id)

	return s.toAssociation(), nil
}

// DeleteServiceNetworkServiceAssociation deletes a SNSA.
func (b *InMemoryBackend) DeleteServiceNetworkServiceAssociation(snsaID string) error {
	b.mu.Lock("DeleteServiceNetworkServiceAssociation")
	defer b.mu.Unlock()

	id, ok := b.resolveSNSAID(snsaID)
	if !ok {
		return ErrNotFound
	}

	s, _ := b.snsas.Get(id)
	b.snsas.Delete(id)
	delete(b.tags, s.ARN)

	return nil
}

// ListServiceNetworkServiceAssociations lists SNSAs with optional filters.
func (b *InMemoryBackend) ListServiceNetworkServiceAssociations(
	ctx context.Context,
	serviceNetworkID, serviceID string,
	maxResults int32,
	nextToken string,
) ([]*ServiceNetworkServiceAssociationSummary, string, error) {
	b.mu.RLock("ListServiceNetworkServiceAssociations")
	defer b.mu.RUnlock()

	region := b.regionFor(ctx)
	all := make([]*ServiceNetworkServiceAssociationSummary, 0)

	for _, s := range b.snsas.All() {
		if s.Region != region {
			continue
		}

		if serviceNetworkID != "" && s.ServiceNetworkID != serviceNetworkID &&
			s.ServiceNetworkARN != serviceNetworkID {
			continue
		}

		if serviceID != "" && s.ServiceID != serviceID && s.ServiceARN != serviceID {
			continue
		}

		all = append(all, s.toSummary())
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}

// ------- ServiceNetworkVpcAssociation operations -------

// CreateServiceNetworkVpcAssociation creates a VPC-to-network association.
func (b *InMemoryBackend) CreateServiceNetworkVpcAssociation(
	ctx context.Context,
	serviceNetworkID, vpcID string,
	securityGroupIDs []string,
	tags map[string]string,
) (*ServiceNetworkVpcAssociation, error) {
	if vpcID == "" {
		return nil, ErrInvalidParameter
	}

	b.mu.Lock("CreateServiceNetworkVpcAssociation")
	defer b.mu.Unlock()

	snID, ok := b.resolveServiceNetworkID(serviceNetworkID)
	if !ok {
		return nil, ErrNotFound
	}

	// check for existing
	for _, s := range b.snvas.All() {
		if s.ServiceNetworkID == snID && s.VpcID == vpcID {
			return nil, ErrAlreadyExists
		}
	}

	now := time.Now().UTC()
	id := newID(idPrefixSNVA)
	region := b.regionFor(ctx)
	assocARN := arn.Build(arnService, region, b.accountID, resourceServiceNetworkVpcAssoc+"/"+id)

	sn, _ := b.serviceNetworks.Get(snID)
	sgs := make([]string, len(securityGroupIDs))
	copy(sgs, securityGroupIDs)

	snva := &storedSNVA{
		ARN:                assocARN,
		ID:                 id,
		VpcID:              vpcID,
		ServiceNetworkARN:  sn.ARN,
		ServiceNetworkID:   snID,
		ServiceNetworkName: sn.Name,
		SecurityGroupIDs:   sgs,
		Status:             statusActive,
		CreatedBy:          b.accountID,
		Tags:               copyTags(tags),
		CreatedAt:          now,
		LastUpdatedAt:      now,
		Region:             region,
	}

	b.snvas.Put(snva)
	b.tags[assocARN] = copyTags(tags)

	return snva.toAssociation(), nil
}

// GetServiceNetworkVpcAssociation returns a SNVA.
func (b *InMemoryBackend) GetServiceNetworkVpcAssociation(
	snvaID string,
) (*ServiceNetworkVpcAssociation, error) {
	b.mu.RLock("GetServiceNetworkVpcAssociation")
	defer b.mu.RUnlock()

	id, ok := b.resolveSNVAID(snvaID)
	if !ok {
		return nil, ErrNotFound
	}

	s, _ := b.snvas.Get(id)

	return s.toAssociation(), nil
}

// UpdateServiceNetworkVpcAssociation updates security groups on a SNVA.
func (b *InMemoryBackend) UpdateServiceNetworkVpcAssociation(
	snvaID string,
	securityGroupIDs []string,
) (*ServiceNetworkVpcAssociation, error) {
	b.mu.Lock("UpdateServiceNetworkVpcAssociation")
	defer b.mu.Unlock()

	id, ok := b.resolveSNVAID(snvaID)
	if !ok {
		return nil, ErrNotFound
	}

	snva, _ := b.snvas.Get(id)
	sgs := make([]string, len(securityGroupIDs))
	copy(sgs, securityGroupIDs)
	snva.SecurityGroupIDs = sgs
	snva.LastUpdatedAt = time.Now().UTC()

	return snva.toAssociation(), nil
}

// DeleteServiceNetworkVpcAssociation deletes a SNVA.
func (b *InMemoryBackend) DeleteServiceNetworkVpcAssociation(snvaID string) error {
	b.mu.Lock("DeleteServiceNetworkVpcAssociation")
	defer b.mu.Unlock()

	id, ok := b.resolveSNVAID(snvaID)
	if !ok {
		return ErrNotFound
	}

	s, _ := b.snvas.Get(id)
	b.snvas.Delete(id)
	delete(b.tags, s.ARN)

	return nil
}

// ListServiceNetworkVpcAssociations lists SNVAs with optional filters.
func (b *InMemoryBackend) ListServiceNetworkVpcAssociations(
	ctx context.Context,
	serviceNetworkID, vpcID string,
	maxResults int32,
	nextToken string,
) ([]*ServiceNetworkVpcAssociationSummary, string, error) {
	b.mu.RLock("ListServiceNetworkVpcAssociations")
	defer b.mu.RUnlock()

	region := b.regionFor(ctx)
	all := make([]*ServiceNetworkVpcAssociationSummary, 0)

	for _, s := range b.snvas.All() {
		if s.Region != region {
			continue
		}

		if serviceNetworkID != "" && s.ServiceNetworkID != serviceNetworkID &&
			s.ServiceNetworkARN != serviceNetworkID {
			continue
		}

		if vpcID != "" && s.VpcID != vpcID {
			continue
		}

		all = append(all, s.toSummary())
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}

// ------- Listener operations -------

// CreateListener creates a listener on a service.
func (b *InMemoryBackend) CreateListener(
	serviceID, name, protocol string,
	port int32,
	defaultAction *RuleAction,
	tags map[string]string,
) (*Listener, error) {
	if name == "" || protocol == "" {
		return nil, ErrInvalidParameter
	}

	b.mu.Lock("CreateListener")
	defer b.mu.Unlock()

	svcID, ok := b.resolveServiceID(serviceID)
	if !ok {
		return nil, ErrNotFound
	}

	// check duplicate name within service
	for _, l := range b.listenersByService.Get(svcID) {
		if l.Name == name {
			return nil, ErrAlreadyExists
		}
	}

	if port == 0 {
		if protocol == protocolHTTPS {
			port = 443
		} else {
			port = 80
		}
	}

	now := time.Now().UTC()
	id := newID(idPrefixListener)
	svc, _ := b.services.Get(svcID)
	listenerARN := b.buildListenerARN(svcID, id)

	l := &storedListener{
		ARN:           listenerARN,
		ID:            id,
		ServiceARN:    svc.ARN,
		ServiceID:     svcID,
		Name:          name,
		Protocol:      protocol,
		Port:          port,
		DefaultAction: defaultAction,
		Tags:          copyTags(tags),
		CreatedAt:     now,
		LastUpdatedAt: now,
	}

	b.listeners.Put(l)
	b.tags[listenerARN] = copyTags(tags)

	// create the default rule
	b.createDefaultRule(svcID, id, listenerARN, defaultAction, now)

	return l.toListener(), nil
}

func (b *InMemoryBackend) createDefaultRule(
	serviceID, listenerID, _ string,
	action *RuleAction,
	now time.Time,
) {
	id := newID(idPrefixRule)
	ruleARN := b.buildRuleARN(serviceID, listenerID, id)

	r := &storedRule{
		ARN:           ruleARN,
		ID:            id,
		ServiceID:     serviceID,
		ListenerID:    listenerID,
		Name:          "default",
		Priority:      defaultRulePriority,
		Action:        action,
		IsDefault:     true,
		Tags:          make(map[string]string),
		CreatedAt:     now,
		LastUpdatedAt: now,
	}

	b.rules.Put(r)
}

// GetListener returns a listener.
func (b *InMemoryBackend) GetListener(serviceID, listenerID string) (*Listener, error) {
	b.mu.RLock("GetListener")
	defer b.mu.RUnlock()

	svcID, ok := b.resolveServiceID(serviceID)
	if !ok {
		return nil, ErrNotFound
	}

	lID, ok := b.resolveListenerID(svcID, listenerID)
	if !ok {
		return nil, ErrNotFound
	}

	l, _ := b.listeners.Get(lID)

	return l.toListener(), nil
}

// UpdateListener updates the default action of a listener.
func (b *InMemoryBackend) UpdateListener(
	serviceID, listenerID string,
	defaultAction *RuleAction,
) (*Listener, error) {
	b.mu.Lock("UpdateListener")
	defer b.mu.Unlock()

	svcID, ok := b.resolveServiceID(serviceID)
	if !ok {
		return nil, ErrNotFound
	}

	lID, ok := b.resolveListenerID(svcID, listenerID)
	if !ok {
		return nil, ErrNotFound
	}

	l, _ := b.listeners.Get(lID)

	if defaultAction != nil {
		l.DefaultAction = defaultAction
	}

	l.LastUpdatedAt = time.Now().UTC()

	return l.toListener(), nil
}

// DeleteListener deletes a listener and its rules.
func (b *InMemoryBackend) DeleteListener(serviceID, listenerID string) error {
	b.mu.Lock("DeleteListener")
	defer b.mu.Unlock()

	svcID, ok := b.resolveServiceID(serviceID)
	if !ok {
		return ErrNotFound
	}

	lID, ok := b.resolveListenerID(svcID, listenerID)
	if !ok {
		return ErrNotFound
	}

	l, _ := b.listeners.Get(lID)
	b.listeners.Delete(lID)
	delete(b.tags, l.ARN)

	// delete all rules for this listener
	for _, r := range slices.Clone(b.rulesByListener.Get(lID)) {
		b.rules.Delete(r.ID)
		delete(b.tags, r.ARN)
	}

	return nil
}

// ListListeners lists listeners for a service.
func (b *InMemoryBackend) ListListeners(
	serviceID string,
	maxResults int32,
	nextToken string,
) ([]*ListenerSummary, string, error) {
	b.mu.RLock("ListListeners")
	defer b.mu.RUnlock()

	svcID, ok := b.resolveServiceID(serviceID)
	if !ok {
		return nil, "", ErrNotFound
	}

	all := make([]*ListenerSummary, 0)

	for _, l := range b.listenersByService.Get(svcID) {
		all = append(all, l.toSummary())
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}

// ------- Rule operations -------

// CreateRule creates a listener rule.
func (b *InMemoryBackend) CreateRule(
	serviceID, listenerID, name string,
	priority int32,
	action *RuleAction,
	match *RuleMatch,
	tags map[string]string,
) (*Rule, error) {
	if name == "" {
		return nil, ErrInvalidParameter
	}

	b.mu.Lock("CreateRule")
	defer b.mu.Unlock()

	svcID, ok := b.resolveServiceID(serviceID)
	if !ok {
		return nil, ErrNotFound
	}

	lID, ok := b.resolveListenerID(svcID, listenerID)
	if !ok {
		return nil, ErrNotFound
	}

	// check duplicate name within listener
	for _, r := range b.rulesByListener.Get(lID) {
		if r.Name == name {
			return nil, ErrAlreadyExists
		}
	}

	now := time.Now().UTC()
	id := newID(idPrefixRule)
	ruleARN := b.buildRuleARN(svcID, lID, id)

	r := &storedRule{
		ARN:           ruleARN,
		ID:            id,
		ServiceID:     svcID,
		ListenerID:    lID,
		Name:          name,
		Priority:      priority,
		Action:        action,
		Match:         match,
		Tags:          copyTags(tags),
		CreatedAt:     now,
		LastUpdatedAt: now,
	}

	b.rules.Put(r)
	b.tags[ruleARN] = copyTags(tags)

	return r.toRule(), nil
}

// GetRule returns a rule.
func (b *InMemoryBackend) GetRule(serviceID, listenerID, ruleID string) (*Rule, error) {
	b.mu.RLock("GetRule")
	defer b.mu.RUnlock()

	svcID, ok := b.resolveServiceID(serviceID)
	if !ok {
		return nil, ErrNotFound
	}

	lID, ok := b.resolveListenerID(svcID, listenerID)
	if !ok {
		return nil, ErrNotFound
	}

	rID, ok := b.resolveRuleID(svcID, lID, ruleID)
	if !ok {
		return nil, ErrNotFound
	}

	r, _ := b.rules.Get(rID)

	return r.toRule(), nil
}

// UpdateRule updates a rule.
func (b *InMemoryBackend) UpdateRule(
	serviceID, listenerID, ruleID string,
	priority int32,
	action *RuleAction,
	match *RuleMatch,
) (*Rule, error) {
	b.mu.Lock("UpdateRule")
	defer b.mu.Unlock()

	svcID, ok := b.resolveServiceID(serviceID)
	if !ok {
		return nil, ErrNotFound
	}

	lID, ok := b.resolveListenerID(svcID, listenerID)
	if !ok {
		return nil, ErrNotFound
	}

	rID, ok := b.resolveRuleID(svcID, lID, ruleID)
	if !ok {
		return nil, ErrNotFound
	}

	r, _ := b.rules.Get(rID)

	if priority != 0 {
		r.Priority = priority
	}

	if action != nil {
		r.Action = action
	}

	if match != nil {
		r.Match = match
	}

	r.LastUpdatedAt = time.Now().UTC()

	return r.toRule(), nil
}

// DeleteRule deletes a rule.
func (b *InMemoryBackend) DeleteRule(serviceID, listenerID, ruleID string) error {
	b.mu.Lock("DeleteRule")
	defer b.mu.Unlock()

	svcID, ok := b.resolveServiceID(serviceID)
	if !ok {
		return ErrNotFound
	}

	lID, ok := b.resolveListenerID(svcID, listenerID)
	if !ok {
		return ErrNotFound
	}

	rID, ok := b.resolveRuleID(svcID, lID, ruleID)
	if !ok {
		return ErrNotFound
	}

	r, _ := b.rules.Get(rID)

	if r.IsDefault {
		return ErrInvalidParameter
	}

	b.rules.Delete(rID)
	delete(b.tags, r.ARN)

	return nil
}

// ListRules lists rules for a listener.
func (b *InMemoryBackend) ListRules(
	serviceID, listenerID string,
	maxResults int32,
	nextToken string,
) ([]*RuleSummary, string, error) {
	b.mu.RLock("ListRules")
	defer b.mu.RUnlock()

	svcID, ok := b.resolveServiceID(serviceID)
	if !ok {
		return nil, "", ErrNotFound
	}

	lID, ok := b.resolveListenerID(svcID, listenerID)
	if !ok {
		return nil, "", ErrNotFound
	}

	all := make([]*RuleSummary, 0)

	for _, r := range b.rulesByListener.Get(lID) {
		if r.ServiceID == svcID {
			all = append(all, r.toSummary())
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Priority < all[j].Priority })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}

// BatchUpdateRule updates multiple rules atomically.
func (b *InMemoryBackend) BatchUpdateRule(
	serviceID, listenerID string,
	updates []*RuleUpdate,
) ([]*RuleUpdateSuccess, []*RuleUpdateFailure, error) {
	b.mu.Lock("BatchUpdateRule")
	defer b.mu.Unlock()

	svcID, ok := b.resolveServiceID(serviceID)
	if !ok {
		return nil, nil, ErrNotFound
	}

	lID, ok := b.resolveListenerID(svcID, listenerID)
	if !ok {
		return nil, nil, ErrNotFound
	}

	successes := make([]*RuleUpdateSuccess, 0, len(updates))
	failures := make([]*RuleUpdateFailure, 0)
	now := time.Now().UTC()

	for _, u := range updates {
		rID, found := b.resolveRuleID(svcID, lID, u.RuleIdentifier)
		if !found {
			failures = append(failures, &RuleUpdateFailure{
				RuleIdentifier: u.RuleIdentifier,
				Code:           "NOT_FOUND",
				Message:        "Rule not found",
			})

			continue
		}

		r, _ := b.rules.Get(rID)

		if u.Priority != 0 {
			r.Priority = u.Priority
		}

		if u.Action != nil {
			r.Action = u.Action
		}

		if u.Match != nil {
			r.Match = u.Match
		}

		r.LastUpdatedAt = now
		successes = append(successes, &RuleUpdateSuccess{
			ARN:       r.ARN,
			ID:        r.ID,
			Name:      r.Name,
			Priority:  r.Priority,
			IsDefault: r.IsDefault,
			Action:    r.Action,
			Match:     r.Match,
		})
	}

	return successes, failures, nil
}

// ------- TargetGroup operations -------

// CreateTargetGroup creates a target group.
func (b *InMemoryBackend) CreateTargetGroup(
	ctx context.Context,
	name, tgType string,
	config *TargetGroupConfig,
	tags map[string]string,
) (*TargetGroup, error) {
	if name == "" {
		return nil, ErrInvalidParameter
	}

	b.mu.Lock("CreateTargetGroup")
	defer b.mu.Unlock()

	if len(b.tgsByName.Get(name)) > 0 {
		return nil, ErrAlreadyExists
	}

	now := time.Now().UTC()
	id := newID(idPrefixTargetGroup)
	region := b.regionFor(ctx)
	tgARN := arn.Build(arnService, region, b.accountID, resourceTargetGroup+"/"+id)

	tg := &storedTargetGroup{
		ARN:           tgARN,
		ID:            id,
		Name:          name,
		Type:          tgType,
		Status:        tgStatusActive,
		Config:        config,
		Tags:          copyTags(tags),
		CreatedAt:     now,
		LastUpdatedAt: now,
		Region:        region,
	}

	b.targetGroups.Put(tg)
	b.targets[id] = make([]*storedTarget, 0)
	b.tags[tgARN] = copyTags(tags)

	return tg.toTargetGroup(), nil
}

// GetTargetGroup returns a target group.
func (b *InMemoryBackend) GetTargetGroup(tgID string) (*TargetGroup, error) {
	b.mu.RLock("GetTargetGroup")
	defer b.mu.RUnlock()

	id, ok := b.resolveTargetGroupID(tgID)
	if !ok {
		return nil, ErrNotFound
	}

	tg, _ := b.targetGroups.Get(id)

	return tg.toTargetGroup(), nil
}

// UpdateTargetGroup updates a target group's health check config.
func (b *InMemoryBackend) UpdateTargetGroup(
	tgID string,
	healthCheck *HealthCheckConfig,
) (*TargetGroup, error) {
	b.mu.Lock("UpdateTargetGroup")
	defer b.mu.Unlock()

	id, ok := b.resolveTargetGroupID(tgID)
	if !ok {
		return nil, ErrNotFound
	}

	tg, _ := b.targetGroups.Get(id)
	if tg.Config == nil {
		tg.Config = &TargetGroupConfig{}
	}

	if healthCheck != nil {
		tg.Config.HealthCheck = healthCheck
	}

	tg.LastUpdatedAt = time.Now().UTC()

	return tg.toTargetGroup(), nil
}

// DeleteTargetGroup deletes a target group.
func (b *InMemoryBackend) DeleteTargetGroup(tgID string) error {
	b.mu.Lock("DeleteTargetGroup")
	defer b.mu.Unlock()

	id, ok := b.resolveTargetGroupID(tgID)
	if !ok {
		return ErrNotFound
	}

	tg, _ := b.targetGroups.Get(id)
	b.targetGroups.Delete(id)
	delete(b.targets, id)
	delete(b.tags, tg.ARN)

	return nil
}

// ListTargetGroups lists target groups with optional filters.
func (b *InMemoryBackend) ListTargetGroups(
	ctx context.Context,
	tgType, serviceArn string,
	maxResults int32,
	nextToken string,
) ([]*TargetGroupSummary, string, error) {
	b.mu.RLock("ListTargetGroups")
	defer b.mu.RUnlock()

	region := b.regionFor(ctx)
	all := make([]*TargetGroupSummary, 0, b.targetGroups.Len())

	for _, tg := range b.targetGroups.All() {
		if tg.Region != region {
			continue
		}

		if tgType != "" && tg.Type != tgType {
			continue
		}

		if serviceArn != "" && !slices.Contains(tg.ServiceARNs, serviceArn) {
			continue
		}

		all = append(all, tg.toSummary())
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}

// RegisterTargets registers targets to a target group.
func (b *InMemoryBackend) RegisterTargets(
	tgID string,
	targets []*Target,
) ([]*TargetFailure, error) {
	b.mu.Lock("RegisterTargets")
	defer b.mu.Unlock()

	id, ok := b.resolveTargetGroupID(tgID)
	if !ok {
		return nil, ErrNotFound
	}

	failures := make([]*TargetFailure, 0)
	existing := b.targets[id]

	for _, t := range targets {
		// check for duplicate
		dup := false
		for _, e := range existing {
			if e.ID == t.ID && e.Port == t.Port {
				dup = true

				break
			}
		}

		if dup {
			failures = append(failures, &TargetFailure{
				ID:      t.ID,
				Port:    t.Port,
				Code:    "TARGET_ALREADY_REGISTERED",
				Message: "Target already registered",
			})

			continue
		}

		existing = append(existing, &storedTarget{
			ID:     t.ID,
			Port:   t.Port,
			Status: targetStatusHealthy,
		})
	}

	b.targets[id] = existing

	return failures, nil
}

// DeregisterTargets deregisters targets from a target group.
func (b *InMemoryBackend) DeregisterTargets( //nolint:gocognit // target deregistration logic is inherently complex
	tgID string,
	targets []*Target,
) ([]*TargetFailure, error) {
	b.mu.Lock("DeregisterTargets")
	defer b.mu.Unlock()

	id, ok := b.resolveTargetGroupID(tgID)
	if !ok {
		return nil, ErrNotFound
	}

	failures := make([]*TargetFailure, 0)
	existing := b.targets[id]

	for _, t := range targets {
		found := false

		for _, e := range existing {
			if e.ID == t.ID && (t.Port == 0 || e.Port == t.Port) {
				found = true

				break
			}
		}

		if !found {
			failures = append(failures, &TargetFailure{
				ID:      t.ID,
				Port:    t.Port,
				Code:    "TARGET_NOT_FOUND",
				Message: "Target not registered",
			})
		}
	}

	// rebuild remaining with non-deregistered targets
	remaining := make([]*storedTarget, 0, len(existing))

	for _, e := range existing {
		remove := false

		for _, t := range targets {
			if e.ID == t.ID && (t.Port == 0 || e.Port == t.Port) {
				remove = true

				break
			}
		}

		if !remove {
			remaining = append(remaining, e)
		}
	}

	b.targets[id] = remaining

	return failures, nil
}

// ListTargets lists registered targets for a target group.
func (b *InMemoryBackend) ListTargets(
	_ context.Context,
	tgID string,
	filters []Target,
	maxResults int32,
	nextToken string,
) ([]*TargetSummary, string, error) {
	b.mu.RLock("ListTargets")
	defer b.mu.RUnlock()

	id, ok := b.resolveTargetGroupID(tgID)
	if !ok {
		return nil, "", ErrNotFound
	}

	targets := b.targets[id]
	all := make([]*TargetSummary, 0, len(targets))

	for _, t := range targets {
		all = append(all, &TargetSummary{
			ID:     t.ID,
			Port:   t.Port,
			Status: t.Status,
		})
	}

	if len(filters) > 0 {
		filtered := all[:0]

		for _, t := range all {
			for _, f := range filters {
				if f.ID == t.ID && (f.Port == 0 || f.Port == t.Port) {
					filtered = append(filtered, t)

					break
				}
			}
		}

		all = filtered
	}

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}

// ------- AccessLogSubscription operations -------

// CreateAccessLogSubscription creates an access log subscription.
func (b *InMemoryBackend) CreateAccessLogSubscription(
	ctx context.Context,
	resourceID, destinationArn, logType string,
	tags map[string]string,
) (*AccessLogSubscription, error) {
	if destinationArn == "" {
		return nil, ErrInvalidParameter
	}

	b.mu.Lock("CreateAccessLogSubscription")
	defer b.mu.Unlock()

	// resolve resource ID (service or service network)
	resourceARN := b.resolveResourceARN(resourceID)

	now := time.Now().UTC()
	id := newID(idPrefixALS)
	region := b.regionFor(ctx)
	alsARN := arn.Build(arnService, region, b.accountID, resourceAccessLogSubscription+"/"+id)

	als := &storedALS{
		ARN:                   alsARN,
		ID:                    id,
		ResourceARN:           resourceARN,
		ResourceID:            resourceID,
		DestinationARN:        destinationArn,
		ServiceNetworkLogType: logType,
		Tags:                  copyTags(tags),
		CreatedAt:             now,
		LastUpdatedAt:         now,
	}

	b.alss.Put(als)
	b.tags[alsARN] = copyTags(tags)

	return als.toALS(), nil
}

func (b *InMemoryBackend) resolveResourceARN(resourceID string) string {
	if svc, ok := b.services.Get(resourceID); ok {
		return svc.ARN
	}

	for _, svc := range b.services.All() {
		if svc.ARN == resourceID {
			return svc.ARN
		}
	}

	if sn, ok := b.serviceNetworks.Get(resourceID); ok {
		return sn.ARN
	}

	for _, sn := range b.serviceNetworks.All() {
		if sn.ARN == resourceID {
			return sn.ARN
		}
	}

	return resourceID
}

// GetAccessLogSubscription returns an access log subscription.
func (b *InMemoryBackend) GetAccessLogSubscription(alsID string) (*AccessLogSubscription, error) {
	b.mu.RLock("GetAccessLogSubscription")
	defer b.mu.RUnlock()

	id, ok := b.resolveALSID(alsID)
	if !ok {
		return nil, ErrNotFound
	}

	als, _ := b.alss.Get(id)

	return als.toALS(), nil
}

// UpdateAccessLogSubscription updates the destination ARN.
func (b *InMemoryBackend) UpdateAccessLogSubscription(
	alsID, destinationArn string,
) (*AccessLogSubscription, error) {
	b.mu.Lock("UpdateAccessLogSubscription")
	defer b.mu.Unlock()

	id, ok := b.resolveALSID(alsID)
	if !ok {
		return nil, ErrNotFound
	}

	als, _ := b.alss.Get(id)
	als.DestinationARN = destinationArn
	als.LastUpdatedAt = time.Now().UTC()

	return als.toALS(), nil
}

// DeleteAccessLogSubscription deletes an access log subscription.
func (b *InMemoryBackend) DeleteAccessLogSubscription(alsID string) error {
	b.mu.Lock("DeleteAccessLogSubscription")
	defer b.mu.Unlock()

	id, ok := b.resolveALSID(alsID)
	if !ok {
		return ErrNotFound
	}

	a, _ := b.alss.Get(id)
	b.alss.Delete(id)
	delete(b.tags, a.ARN)

	return nil
}

// ListAccessLogSubscriptions lists access log subscriptions for a resource.
func (b *InMemoryBackend) ListAccessLogSubscriptions(
	_ context.Context,
	resourceID string,
	maxResults int32,
	nextToken string,
) ([]*AccessLogSubscriptionSummary, string, error) {
	b.mu.RLock("ListAccessLogSubscriptions")
	defer b.mu.RUnlock()

	all := make([]*AccessLogSubscriptionSummary, 0)

	for _, a := range b.alss.All() {
		if resourceID != "" && a.ResourceID != resourceID && a.ResourceARN != resourceID {
			continue
		}

		all = append(all, a.toSummary())
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}

// ------- Auth/Resource Policy operations -------

// PutAuthPolicy sets an auth policy on a resource.
func (b *InMemoryBackend) PutAuthPolicy(resourceID, policy string) (*AuthPolicy, error) {
	b.mu.Lock("PutAuthPolicy")
	defer b.mu.Unlock()

	b.authPolicies[resourceID] = policy

	return &AuthPolicy{Policy: policy, State: authPolicyStateActive}, nil
}

// GetAuthPolicy returns the auth policy for a resource.
func (b *InMemoryBackend) GetAuthPolicy(resourceID string) (*AuthPolicy, error) {
	b.mu.RLock("GetAuthPolicy")
	defer b.mu.RUnlock()

	policy, ok := b.authPolicies[resourceID]
	if !ok {
		return nil, ErrNotFound
	}

	return &AuthPolicy{Policy: policy, State: authPolicyStateActive}, nil
}

// DeleteAuthPolicy deletes the auth policy for a resource.
func (b *InMemoryBackend) DeleteAuthPolicy(resourceID string) error {
	b.mu.Lock("DeleteAuthPolicy")
	defer b.mu.Unlock()

	delete(b.authPolicies, resourceID)

	return nil
}

// PutResourcePolicy sets a resource policy.
func (b *InMemoryBackend) PutResourcePolicy(resourceArn, policy string) error {
	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	b.resourcePolicies[resourceArn] = policy

	return nil
}

// GetResourcePolicy returns a resource policy.
func (b *InMemoryBackend) GetResourcePolicy(resourceArn string) (string, error) {
	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	policy, ok := b.resourcePolicies[resourceArn]
	if !ok {
		return "", ErrNotFound
	}

	return policy, nil
}

// DeleteResourcePolicy deletes a resource policy.
func (b *InMemoryBackend) DeleteResourcePolicy(resourceArn string) error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	if _, ok := b.resourcePolicies[resourceArn]; !ok {
		return ErrNotFound
	}

	delete(b.resourcePolicies, resourceArn)

	return nil
}

// ------- Tagging operations -------

// TagResource adds tags to a resource.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if _, ok := b.tags[resourceArn]; !ok {
		b.tags[resourceArn] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceArn], tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceArn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if t, ok := b.tags[resourceArn]; ok {
		for _, k := range keys {
			delete(t, k)
		}
	}

	return nil
}

// ListTagsForResource returns tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t, ok := b.tags[resourceArn]
	if !ok {
		return make(map[string]string), nil
	}

	result := make(map[string]string, len(t))
	maps.Copy(result, t)

	return result, nil
}
