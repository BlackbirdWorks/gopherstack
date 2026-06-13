package vpclattice

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
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
	ARN                string            `json:"arn"`
	ID                 string            `json:"id"`
	VpcID              string            `json:"vpcId"`
	ServiceNetworkARN  string            `json:"serviceNetworkArn"`
	ServiceNetworkID   string            `json:"serviceNetworkId"`
	ServiceNetworkName string            `json:"serviceNetworkName"`
	Status             string            `json:"status"`
	CreatedBy          string            `json:"createdBy"`
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
		ARN:       r.ARN,
		ID:        r.ID,
		Name:      r.Name,
		Priority:  r.Priority,
		IsDefault: r.IsDefault,
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
		ARN:         tg.ARN,
		ID:          tg.ID,
		Name:        tg.Name,
		Type:        tg.Type,
		Status:      tg.Status,
		CreatedAt:   tg.CreatedAt,
		ServiceARNs: make([]string, len(tg.ServiceARNs)),
	}
	copy(s.ServiceARNs, tg.ServiceARNs)

	if tg.Config != nil {
		s.Port = tg.Config.Port
		s.Protocol = tg.Config.Protocol
		s.VpcID = tg.Config.VpcID
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

// snapshot is the serializable form of InMemoryBackend.
type snapshot struct {
	Services         map[string]*storedService        `json:"services"`
	ServiceNetworks  map[string]*storedServiceNetwork `json:"serviceNetworks"`
	SNSAs            map[string]*storedSNSA           `json:"snsas"`
	SNVAs            map[string]*storedSNVA           `json:"snvas"`
	Listeners        map[string]*storedListener       `json:"listeners"`
	Rules            map[string]*storedRule           `json:"rules"`
	TargetGroups     map[string]*storedTargetGroup    `json:"targetGroups"`
	Targets          map[string][]*storedTarget       `json:"targets"`
	ALSs             map[string]*storedALS            `json:"alss"`
	AuthPolicies     map[string]string                `json:"authPolicies"`
	ResourcePolicies map[string]string                `json:"resourcePolicies"`
	Tags             map[string]map[string]string     `json:"tags"`
}

// InMemoryBackend is an in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	mu               *lockmetrics.RWMutex
	services         map[string]*storedService
	servicesByName   map[string]string
	serviceNetworks  map[string]*storedServiceNetwork
	networksByName   map[string]string
	snsas            map[string]*storedSNSA
	snvas            map[string]*storedSNVA
	listeners        map[string]*storedListener
	rules            map[string]*storedRule
	targetGroups     map[string]*storedTargetGroup
	tgsByName        map[string]string
	targets          map[string][]*storedTarget
	alss             map[string]*storedALS
	authPolicies     map[string]string
	resourcePolicies map[string]string
	tags             map[string]map[string]string
	accountID        string
	region           string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:        lockmetrics.New("vpclattice"),
		accountID: accountID,
		region:    region,
	}
	b.initMaps()

	return b
}

func (b *InMemoryBackend) initMaps() {
	b.services = make(map[string]*storedService)
	b.servicesByName = make(map[string]string)
	b.serviceNetworks = make(map[string]*storedServiceNetwork)
	b.networksByName = make(map[string]string)
	b.snsas = make(map[string]*storedSNSA)
	b.snvas = make(map[string]*storedSNVA)
	b.listeners = make(map[string]*storedListener)
	b.rules = make(map[string]*storedRule)
	b.targetGroups = make(map[string]*storedTargetGroup)
	b.tgsByName = make(map[string]string)
	b.targets = make(map[string][]*storedTarget)
	b.alss = make(map[string]*storedALS)
	b.authPolicies = make(map[string]string)
	b.resourcePolicies = make(map[string]string)
	b.tags = make(map[string]map[string]string)
}

// AccountID returns the configured account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the configured region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all stored data.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()
	b.initMaps()
}

// Snapshot serializes the backend state.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	s := snapshot{
		Services:         b.services,
		ServiceNetworks:  b.serviceNetworks,
		SNSAs:            b.snsas,
		SNVAs:            b.snvas,
		Listeners:        b.listeners,
		Rules:            b.rules,
		TargetGroups:     b.targetGroups,
		Targets:          b.targets,
		ALSs:             b.alss,
		AuthPolicies:     b.authPolicies,
		ResourcePolicies: b.resourcePolicies,
		Tags:             b.tags,
	}

	data, _ := json.Marshal(s)

	return data
}

// Restore deserializes backend state.
func (b *InMemoryBackend) Restore(data []byte) error {
	var s snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.services = s.Services
	b.serviceNetworks = s.ServiceNetworks
	b.snsas = s.SNSAs
	b.snvas = s.SNVAs
	b.listeners = s.Listeners
	b.rules = s.Rules
	b.targetGroups = s.TargetGroups
	b.targets = s.Targets
	b.alss = s.ALSs
	b.authPolicies = s.AuthPolicies
	b.resourcePolicies = s.ResourcePolicies
	b.tags = s.Tags

	b.servicesByName = make(map[string]string)
	for id, svc := range b.services {
		b.servicesByName[svc.Name] = id
	}

	b.networksByName = make(map[string]string)
	for id, sn := range b.serviceNetworks {
		b.networksByName[sn.Name] = id
	}

	b.tgsByName = make(map[string]string)
	for id, tg := range b.targetGroups {
		b.tgsByName[tg.Name] = id
	}

	return nil
}

func (b *InMemoryBackend) buildARN(resourceType, resourceID string) string {
	return arn.Build(arnService, b.region, b.accountID, resourceType+"/"+resourceID)
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
	if svc, ok := b.services[identifier]; ok {
		return svc.ID, true
	}
	// check if it's an ARN
	for id, svc := range b.services {
		if svc.ARN == identifier {
			return id, true
		}
	}

	return "", false
}

// resolveServiceNetworkID resolves a service network identifier to an ID.
func (b *InMemoryBackend) resolveServiceNetworkID(identifier string) (string, bool) {
	if _, ok := b.serviceNetworks[identifier]; ok {
		return identifier, true
	}
	for id, sn := range b.serviceNetworks {
		if sn.ARN == identifier || sn.Name == identifier {
			return id, true
		}
	}

	return "", false
}

// resolveListenerID resolves a listener identifier to (serviceID, listenerID).
func (b *InMemoryBackend) resolveListenerID(serviceID, identifier string) (string, bool) {
	key := serviceID + "/" + identifier
	if _, ok := b.listeners[key]; ok {
		return identifier, true
	}
	for _, l := range b.listeners {
		if l.ServiceID == serviceID && (l.ARN == identifier) {
			return l.ID, true
		}
	}

	return "", false
}

// resolveRuleID resolves a rule identifier within a listener to a rule ID.
func (b *InMemoryBackend) resolveRuleID(serviceID, listenerID, identifier string) (string, bool) {
	key := serviceID + "/" + listenerID + "/" + identifier
	if _, ok := b.rules[key]; ok {
		return identifier, true
	}
	for _, r := range b.rules {
		if r.ServiceID == serviceID && r.ListenerID == listenerID && r.ARN == identifier {
			return r.ID, true
		}
	}

	return "", false
}

// resolveTargetGroupID resolves a target group identifier to an ID.
func (b *InMemoryBackend) resolveTargetGroupID(identifier string) (string, bool) {
	if _, ok := b.targetGroups[identifier]; ok {
		return identifier, true
	}
	for id, tg := range b.targetGroups {
		if tg.ARN == identifier {
			return id, true
		}
	}

	return "", false
}

// resolveALSID resolves an access log subscription identifier.
func (b *InMemoryBackend) resolveALSID(identifier string) (string, bool) {
	if _, ok := b.alss[identifier]; ok {
		return identifier, true
	}
	for id, a := range b.alss {
		if a.ARN == identifier {
			return id, true
		}
	}

	return "", false
}

// resolveSNSAID resolves a SNSA identifier.
func (b *InMemoryBackend) resolveSNSAID(identifier string) (string, bool) {
	if _, ok := b.snsas[identifier]; ok {
		return identifier, true
	}
	for id, s := range b.snsas {
		if s.ARN == identifier {
			return id, true
		}
	}

	return "", false
}

// resolveSNVAID resolves a SNVA identifier.
func (b *InMemoryBackend) resolveSNVAID(identifier string) (string, bool) {
	if _, ok := b.snvas[identifier]; ok {
		return identifier, true
	}
	for id, s := range b.snvas {
		if s.ARN == identifier {
			return id, true
		}
	}

	return "", false
}

// ------- Service operations -------

// CreateService creates a new service.
func (b *InMemoryBackend) CreateService(
	name, authType, certificateArn, customDomainName string,
	tags map[string]string,
) (*Service, error) {
	if name == "" {
		return nil, ErrInvalidParameter
	}

	b.mu.Lock("CreateService")
	defer b.mu.Unlock()

	if _, exists := b.servicesByName[name]; exists {
		return nil, ErrAlreadyExists
	}

	now := time.Now().UTC()
	id := newID(idPrefixService)
	svcARN := b.buildARN(resourceService, id)

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
		DNSName:          id + ".vpc-lattice-svcs." + b.region + ".on.aws",
		Status:           statusActive,
		Tags:             copyTags(tags),
		CreatedAt:        now,
		LastUpdatedAt:    now,
	}

	b.services[id] = svc
	b.servicesByName[name] = id
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

	return b.services[id].toService(), nil
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

	svc := b.services[id]
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

	svc := b.services[id]
	out := svc.toService()
	out.Status = statusDeleted

	delete(b.servicesByName, svc.Name)
	delete(b.services, id)
	delete(b.tags, svc.ARN)

	return out, nil
}

// ListServices returns a paginated list of services.
func (b *InMemoryBackend) ListServices(
	maxResults int32,
	nextToken string,
) ([]*ServiceSummary, string, error) {
	b.mu.RLock("ListServices")
	defer b.mu.RUnlock()

	all := make([]*ServiceSummary, 0, len(b.services))
	for _, svc := range b.services {
		all = append(all, svc.toSummary())
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}

// ------- ServiceNetwork operations -------

// CreateServiceNetwork creates a new service network.
func (b *InMemoryBackend) CreateServiceNetwork(
	name, authType string,
	tags map[string]string,
) (*ServiceNetwork, error) {
	if name == "" {
		return nil, ErrInvalidParameter
	}

	b.mu.Lock("CreateServiceNetwork")
	defer b.mu.Unlock()

	if _, exists := b.networksByName[name]; exists {
		return nil, ErrAlreadyExists
	}

	now := time.Now().UTC()
	id := newID(idPrefixNetwork)
	snARN := b.buildARN(resourceServiceNetwork, id)

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
	}

	b.serviceNetworks[id] = sn
	b.networksByName[name] = id
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

	sn := b.serviceNetworks[id]

	// compute counts
	sn.NumberOfAssociatedServices = b.countSNSAs(id)
	sn.NumberOfAssociatedVPCs = b.countSNVAs(id)

	return sn.toServiceNetwork(), nil
}

func (b *InMemoryBackend) countSNSAs(snID string) int64 {
	var count int64
	for _, s := range b.snsas {
		if s.ServiceNetworkID == snID {
			count++
		}
	}

	return count
}

func (b *InMemoryBackend) countSNVAs(snID string) int64 {
	var count int64
	for _, s := range b.snvas {
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

	sn := b.serviceNetworks[id]
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

	sn := b.serviceNetworks[id]
	delete(b.networksByName, sn.Name)
	delete(b.serviceNetworks, id)
	delete(b.tags, sn.ARN)

	return nil
}

// ListServiceNetworks returns a paginated list of service networks.
func (b *InMemoryBackend) ListServiceNetworks(
	maxResults int32,
	nextToken string,
) ([]*ServiceNetworkSummary, string, error) {
	b.mu.RLock("ListServiceNetworks")
	defer b.mu.RUnlock()

	all := make([]*ServiceNetworkSummary, 0, len(b.serviceNetworks))
	for _, sn := range b.serviceNetworks {
		all = append(all, sn.toSummary())
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}

// ------- ServiceNetworkServiceAssociation operations -------

// CreateServiceNetworkServiceAssociation creates a service-to-network association.
func (b *InMemoryBackend) CreateServiceNetworkServiceAssociation(
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
	for _, s := range b.snsas {
		if s.ServiceNetworkID == snID && s.ServiceID == svcID {
			return nil, ErrAlreadyExists
		}
	}

	now := time.Now().UTC()
	id := newID(idPrefixSNSA)
	assocARN := b.buildARN(resourceServiceNetworkSvcAssoc, id)

	sn := b.serviceNetworks[snID]
	svc := b.services[svcID]

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
	}

	b.snsas[id] = snsa
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

	return b.snsas[id].toAssociation(), nil
}

// DeleteServiceNetworkServiceAssociation deletes a SNSA.
func (b *InMemoryBackend) DeleteServiceNetworkServiceAssociation(snsaID string) error {
	b.mu.Lock("DeleteServiceNetworkServiceAssociation")
	defer b.mu.Unlock()

	id, ok := b.resolveSNSAID(snsaID)
	if !ok {
		return ErrNotFound
	}

	s := b.snsas[id]
	delete(b.snsas, id)
	delete(b.tags, s.ARN)

	return nil
}

// ListServiceNetworkServiceAssociations lists SNSAs with optional filters.
func (b *InMemoryBackend) ListServiceNetworkServiceAssociations(
	serviceNetworkID, serviceID string,
	maxResults int32,
	nextToken string,
) ([]*ServiceNetworkServiceAssociationSummary, string, error) {
	b.mu.RLock("ListServiceNetworkServiceAssociations")
	defer b.mu.RUnlock()

	all := make([]*ServiceNetworkServiceAssociationSummary, 0)

	for _, s := range b.snsas {
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
	for _, s := range b.snvas {
		if s.ServiceNetworkID == snID && s.VpcID == vpcID {
			return nil, ErrAlreadyExists
		}
	}

	now := time.Now().UTC()
	id := newID(idPrefixSNVA)
	assocARN := b.buildARN(resourceServiceNetworkVpcAssoc, id)

	sn := b.serviceNetworks[snID]
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
	}

	b.snvas[id] = snva
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

	return b.snvas[id].toAssociation(), nil
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

	snva := b.snvas[id]
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

	s := b.snvas[id]
	delete(b.snvas, id)
	delete(b.tags, s.ARN)

	return nil
}

// ListServiceNetworkVpcAssociations lists SNVAs with optional filters.
func (b *InMemoryBackend) ListServiceNetworkVpcAssociations(
	serviceNetworkID, vpcID string,
	maxResults int32,
	nextToken string,
) ([]*ServiceNetworkVpcAssociationSummary, string, error) {
	b.mu.RLock("ListServiceNetworkVpcAssociations")
	defer b.mu.RUnlock()

	all := make([]*ServiceNetworkVpcAssociationSummary, 0)

	for _, s := range b.snvas {
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
	for _, l := range b.listeners {
		if l.ServiceID == svcID && l.Name == name {
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
	svc := b.services[svcID]
	listenerARN := b.buildListenerARN(svcID, id)
	key := svcID + "/" + id

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

	b.listeners[key] = l
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
	key := serviceID + "/" + listenerID + "/" + id

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

	b.rules[key] = r
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

	key := svcID + "/" + lID

	return b.listeners[key].toListener(), nil
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

	key := svcID + "/" + lID
	l := b.listeners[key]

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

	key := svcID + "/" + lID
	l := b.listeners[key]
	delete(b.listeners, key)
	delete(b.tags, l.ARN)

	// delete all rules for this listener
	prefix := svcID + "/" + lID + "/"
	for k, r := range b.rules {
		if strings.HasPrefix(k, prefix) {
			delete(b.rules, k)
			delete(b.tags, r.ARN)
		}
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

	for _, l := range b.listeners {
		if l.ServiceID == svcID {
			all = append(all, l.toSummary())
		}
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
	for _, r := range b.rules {
		if r.ServiceID == svcID && r.ListenerID == lID && r.Name == name {
			return nil, ErrAlreadyExists
		}
	}

	now := time.Now().UTC()
	id := newID(idPrefixRule)
	ruleARN := b.buildRuleARN(svcID, lID, id)
	key := svcID + "/" + lID + "/" + id

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

	b.rules[key] = r
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

	key := svcID + "/" + lID + "/" + rID

	return b.rules[key].toRule(), nil
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

	key := svcID + "/" + lID + "/" + rID
	r := b.rules[key]

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

	key := svcID + "/" + lID + "/" + rID
	r := b.rules[key]

	if r.IsDefault {
		return ErrInvalidParameter
	}

	delete(b.rules, key)
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

	for _, r := range b.rules {
		if r.ServiceID == svcID && r.ListenerID == lID {
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

		key := svcID + "/" + lID + "/" + rID
		r := b.rules[key]

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
	name, tgType string,
	config *TargetGroupConfig,
	tags map[string]string,
) (*TargetGroup, error) {
	if name == "" {
		return nil, ErrInvalidParameter
	}

	b.mu.Lock("CreateTargetGroup")
	defer b.mu.Unlock()

	if _, exists := b.tgsByName[name]; exists {
		return nil, ErrAlreadyExists
	}

	now := time.Now().UTC()
	id := newID(idPrefixTargetGroup)
	tgARN := b.buildARN(resourceTargetGroup, id)

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
	}

	b.targetGroups[id] = tg
	b.tgsByName[name] = id
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

	return b.targetGroups[id].toTargetGroup(), nil
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

	tg := b.targetGroups[id]
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

	tg := b.targetGroups[id]
	delete(b.targetGroups, id)
	delete(b.tgsByName, tg.Name)
	delete(b.targets, id)
	delete(b.tags, tg.ARN)

	return nil
}

// ListTargetGroups lists target groups with optional filters.
func (b *InMemoryBackend) ListTargetGroups(
	tgType, serviceArn string,
	maxResults int32,
	nextToken string,
) ([]*TargetGroupSummary, string, error) {
	b.mu.RLock("ListTargetGroups")
	defer b.mu.RUnlock()

	all := make([]*TargetGroupSummary, 0, len(b.targetGroups))

	for _, tg := range b.targetGroups {
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
	tgID string,
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

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}

// ------- AccessLogSubscription operations -------

// CreateAccessLogSubscription creates an access log subscription.
func (b *InMemoryBackend) CreateAccessLogSubscription(
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
	alsARN := b.buildARN(resourceAccessLogSubscription, id)

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

	b.alss[id] = als
	b.tags[alsARN] = copyTags(tags)

	return als.toALS(), nil
}

func (b *InMemoryBackend) resolveResourceARN(resourceID string) string {
	if svc, ok := b.services[resourceID]; ok {
		return svc.ARN
	}

	for _, svc := range b.services {
		if svc.ARN == resourceID {
			return svc.ARN
		}
	}

	if sn, ok := b.serviceNetworks[resourceID]; ok {
		return sn.ARN
	}

	for _, sn := range b.serviceNetworks {
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

	return b.alss[id].toALS(), nil
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

	als := b.alss[id]
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

	a := b.alss[id]
	delete(b.alss, id)
	delete(b.tags, a.ARN)

	return nil
}

// ListAccessLogSubscriptions lists access log subscriptions for a resource.
func (b *InMemoryBackend) ListAccessLogSubscriptions(
	resourceID string,
	maxResults int32,
	nextToken string,
) ([]*AccessLogSubscriptionSummary, string, error) {
	b.mu.RLock("ListAccessLogSubscriptions")
	defer b.mu.RUnlock()

	all := make([]*AccessLogSubscriptionSummary, 0)

	for _, a := range b.alss {
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
		return &AuthPolicy{Policy: "", State: "Active"}, nil
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
