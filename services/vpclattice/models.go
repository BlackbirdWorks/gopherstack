package vpclattice

import (
	"time"
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
