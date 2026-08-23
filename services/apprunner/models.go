package apprunner

import "time"

const (
	statusRunning = "RUNNING"
	statusPaused  = "PAUSED"
	statusDeleted = "DELETED"

	opTypeCreate = "CREATE_SERVICE"
	opTypePause  = "PAUSE_SERVICE"
	opTypeResume = "RESUME_SERVICE"
	opTypeDelete = "DELETE_SERVICE"
	opTypeDeploy = "START_DEPLOYMENT"
	opTypeUpdate = "UPDATE_SERVICE"

	opStatusSucceeded = "SUCCEEDED"

	// maxOperationsPerService bounds the per-service operation history so a
	// long-lived service that is repeatedly updated/paused/resumed cannot grow
	// svc.Operations without limit. ListOperations returns the most recent ones.
	maxOperationsPerService = 200

	defaultMaxResults = 20
	defaultCPU        = "1 vCPU"
	defaultMemory     = "2 GB"

	asgStatusActive   = "ACTIVE"
	asgStatusInactive = "INACTIVE"

	connStatusAvailable = "AVAILABLE"
	connStatusDeleted   = "DELETED"

	obsStatusActive   = "ACTIVE"
	obsStatusInactive = "INACTIVE"

	vpcConnStatusActive   = "ACTIVE"
	vpcConnStatusInactive = "INACTIVE"

	vicStatusAvailable = "AVAILABLE"
	vicStatusDeleted   = "DELETED"

	customDomainStatusActive = "ACTIVE"

	defaultMaxConcurrency int32 = 100
	defaultMaxSize        int32 = 25
	defaultMinSize        int32 = 1

	// defaultASGConfigName is the name of the system-managed default auto
	// scaling configuration every account has, matching real App Runner's
	// "DefaultConfiguration" that's associated with a service when
	// CreateServiceInput.AutoScalingConfigurationArn is omitted.
	defaultASGConfigName = "DefaultConfiguration"

	egressTypeDefault = "DEFAULT"
	egressTypeVPC     = "VPC"

	ipAddressTypeIPv4      = "IPV4"
	ipAddressTypeDualStack = "DUAL_STACK"

	healthCheckProtocolTCP = "TCP"

	defaultHealthCheckPath           = "/"
	defaultHealthCheckInterval int32 = 5
	defaultHealthCheckTimeout  int32 = 2
	defaultHealthyThreshold    int32 = 1
	defaultUnhealthyThreshold  int32 = 5

	imageRepositoryTypeECRPublic = "ECR_PUBLIC"
)

// storedService holds a service with all fields.
// CreatedAt is first so its non-pointer prefix (wall, ext) reduces GC pointer bytes.
type storedService struct {
	Source                      SourceConfig         `json:"source"`
	UpdatedAt                   time.Time            `json:"updatedAt"`
	CreatedAt                   time.Time            `json:"createdAt"`
	DeletedAt                   time.Time            `json:"deletedAt"`
	Tags                        map[string]string    `json:"tags"`
	Network                     NetworkConfig        `json:"network"`
	Instance                    InstanceConfig       `json:"instance"`
	Observability               ServiceObservability `json:"observability"`
	ServiceID                   string               `json:"serviceId"`
	Status                      string               `json:"status"`
	ServiceURL                  string               `json:"serviceUrl"`
	AutoScalingConfigurationArn string               `json:"autoScalingConfigurationArn"`
	ServiceName                 string               `json:"serviceName"`
	EncryptionKmsKey            string               `json:"encryptionKmsKey"`
	ServiceArn                  string               `json:"serviceArn"`
	Operations                  []*storedOperation   `json:"operations"`
	HealthCheck                 HealthCheckConfig    `json:"healthCheck"`
}

func (s *storedService) toService() Service {
	return Service{
		ServiceArn:                  s.ServiceArn,
		ServiceID:                   s.ServiceID,
		ServiceName:                 s.ServiceName,
		ServiceURL:                  s.ServiceURL,
		Status:                      s.Status,
		Instance:                    s.Instance,
		Source:                      s.Source,
		AutoScalingConfigurationArn: s.AutoScalingConfigurationArn,
		Network:                     s.Network,
		HealthCheck:                 s.HealthCheck,
		EncryptionKmsKey:            s.EncryptionKmsKey,
		Observability:               s.Observability,
		CreatedAt:                   s.CreatedAt,
		UpdatedAt:                   s.UpdatedAt,
		DeletedAt:                   s.DeletedAt,
	}
}

func (s *storedService) toSummary() ServiceSummary {
	return ServiceSummary{
		ServiceArn:  s.ServiceArn,
		ServiceID:   s.ServiceID,
		ServiceName: s.ServiceName,
		ServiceURL:  s.ServiceURL,
		Status:      s.Status,
		CreatedAt:   s.CreatedAt,
		UpdatedAt:   s.UpdatedAt,
	}
}

// storedOperation holds an operation record.
// StartedAt is first so its non-pointer prefix (wall, ext) reduces GC pointer bytes.
type storedOperation struct {
	StartedAt time.Time `json:"startedAt"`
	EndedAt   time.Time `json:"endedAt"`
	UpdatedAt time.Time `json:"updatedAt"`
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Status    string    `json:"status"`
	TargetArn string    `json:"targetArn"`
}

func (o *storedOperation) toSummary() OperationSummary {
	return OperationSummary{
		ID:        o.ID,
		Type:      o.Type,
		Status:    o.Status,
		TargetArn: o.TargetArn,
		StartedAt: o.StartedAt,
		EndedAt:   o.EndedAt,
		UpdatedAt: o.UpdatedAt,
	}
}

// storedAutoScalingConfiguration holds an ASG config with all fields.
// CreatedAt is first to reduce GC pointer bytes.
type storedAutoScalingConfiguration struct {
	CreatedAt                        time.Time `json:"createdAt"`
	DeletedAt                        time.Time `json:"deletedAt"`
	AutoScalingConfigurationArn      string    `json:"arn"`
	AutoScalingConfigurationName     string    `json:"name"`
	Status                           string    `json:"status"`
	AutoScalingConfigurationRevision int32     `json:"revision"`
	MaxConcurrency                   int32     `json:"maxConcurrency"`
	MaxSize                          int32     `json:"maxSize"`
	MinSize                          int32     `json:"minSize"`
	IsDefault                        bool      `json:"isDefault"`
	HasAssociatedService             bool      `json:"hasAssociatedService"`
	Latest                           bool      `json:"latest"`
}

func (a *storedAutoScalingConfiguration) toASG() AutoScalingConfiguration {
	return AutoScalingConfiguration{
		AutoScalingConfigurationArn:      a.AutoScalingConfigurationArn,
		AutoScalingConfigurationName:     a.AutoScalingConfigurationName,
		AutoScalingConfigurationRevision: a.AutoScalingConfigurationRevision,
		Status:                           a.Status,
		MaxConcurrency:                   a.MaxConcurrency,
		MaxSize:                          a.MaxSize,
		MinSize:                          a.MinSize,
		IsDefault:                        a.IsDefault,
		HasAssociatedService:             a.HasAssociatedService,
		CreatedAt:                        a.CreatedAt,
		DeletedAt:                        a.DeletedAt,
		Latest:                           a.Latest,
	}
}

func (a *storedAutoScalingConfiguration) toSummary() AutoScalingConfigurationSummary {
	return AutoScalingConfigurationSummary{
		AutoScalingConfigurationArn:      a.AutoScalingConfigurationArn,
		AutoScalingConfigurationName:     a.AutoScalingConfigurationName,
		AutoScalingConfigurationRevision: a.AutoScalingConfigurationRevision,
		Status:                           a.Status,
		IsDefault:                        a.IsDefault,
		HasAssociatedService:             a.HasAssociatedService,
		CreatedAt:                        a.CreatedAt,
	}
}

// storedConnection holds a connection resource.
// CreatedAt is first to reduce GC pointer bytes.
type storedConnection struct {
	CreatedAt      time.Time `json:"createdAt"`
	ConnectionArn  string    `json:"arn"`
	ConnectionName string    `json:"name"`
	ProviderType   string    `json:"providerType"`
	Status         string    `json:"status"`
}

func (c *storedConnection) toConnection() Connection {
	return Connection{
		ConnectionArn:  c.ConnectionArn,
		ConnectionName: c.ConnectionName,
		ProviderType:   c.ProviderType,
		Status:         c.Status,
		CreatedAt:      c.CreatedAt,
	}
}

func (c *storedConnection) toSummary() ConnectionSummary {
	return ConnectionSummary{
		ConnectionArn:  c.ConnectionArn,
		ConnectionName: c.ConnectionName,
		ProviderType:   c.ProviderType,
		Status:         c.Status,
		CreatedAt:      c.CreatedAt,
	}
}

// storedObservabilityConfiguration holds an observability config.
// CreatedAt is first to reduce GC pointer bytes.
type storedObservabilityConfiguration struct {
	CreatedAt                          time.Time `json:"createdAt"`
	DeletedAt                          time.Time `json:"deletedAt"`
	ObservabilityConfigurationArn      string    `json:"arn"`
	ObservabilityConfigurationName     string    `json:"name"`
	TracingVendor                      string    `json:"tracingVendor"`
	Status                             string    `json:"status"`
	ObservabilityConfigurationRevision int32     `json:"revision"`
	Latest                             bool      `json:"latest"`
}

func (o *storedObservabilityConfiguration) toObs() ObservabilityConfiguration {
	return ObservabilityConfiguration{
		ObservabilityConfigurationArn:      o.ObservabilityConfigurationArn,
		ObservabilityConfigurationName:     o.ObservabilityConfigurationName,
		ObservabilityConfigurationRevision: o.ObservabilityConfigurationRevision,
		TracingVendor:                      o.TracingVendor,
		Status:                             o.Status,
		Latest:                             o.Latest,
		CreatedAt:                          o.CreatedAt,
		DeletedAt:                          o.DeletedAt,
	}
}

func (o *storedObservabilityConfiguration) toSummary() ObservabilityConfigurationSummary {
	return ObservabilityConfigurationSummary{
		ObservabilityConfigurationArn:      o.ObservabilityConfigurationArn,
		ObservabilityConfigurationName:     o.ObservabilityConfigurationName,
		ObservabilityConfigurationRevision: o.ObservabilityConfigurationRevision,
		Status:                             o.Status,
		Latest:                             o.Latest,
		CreatedAt:                          o.CreatedAt,
	}
}

// storedVpcConnector holds a VPC connector resource.
// CreatedAt is first to reduce GC pointer bytes.
type storedVpcConnector struct {
	CreatedAt            time.Time `json:"createdAt"`
	DeletedAt            time.Time `json:"deletedAt"`
	VpcConnectorArn      string    `json:"arn"`
	VpcConnectorName     string    `json:"name"`
	Status               string    `json:"status"`
	SecurityGroups       []string  `json:"securityGroups"`
	Subnets              []string  `json:"subnets"`
	VpcConnectorRevision int32     `json:"revision"`
}

func (v *storedVpcConnector) toVpcConnector() VpcConnector {
	sg := make([]string, len(v.SecurityGroups))
	copy(sg, v.SecurityGroups)
	sn := make([]string, len(v.Subnets))
	copy(sn, v.Subnets)

	return VpcConnector{
		VpcConnectorArn:      v.VpcConnectorArn,
		VpcConnectorName:     v.VpcConnectorName,
		VpcConnectorRevision: v.VpcConnectorRevision,
		Status:               v.Status,
		Subnets:              sn,
		SecurityGroups:       sg,
		CreatedAt:            v.CreatedAt,
		DeletedAt:            v.DeletedAt,
	}
}

// storedVpcIngressConnection holds a VPC ingress connection resource.
// CreatedAt is first to reduce GC pointer bytes.
type storedVpcIngressConnection struct {
	CreatedAt                time.Time `json:"createdAt"`
	DeletedAt                time.Time `json:"deletedAt"`
	VpcIngressConnectionArn  string    `json:"arn"`
	VpcIngressConnectionName string    `json:"name"`
	ServiceArn               string    `json:"serviceArn"`
	AccountID                string    `json:"accountId"`
	DomainName               string    `json:"domainName"`
	VpcID                    string    `json:"vpcId"`
	VpcEndpointID            string    `json:"vpcEndpointId"`
	Status                   string    `json:"status"`
}

func (v *storedVpcIngressConnection) toVIC() VpcIngressConnection {
	return VpcIngressConnection{
		VpcIngressConnectionArn:  v.VpcIngressConnectionArn,
		VpcIngressConnectionName: v.VpcIngressConnectionName,
		ServiceArn:               v.ServiceArn,
		AccountID:                v.AccountID,
		DomainName:               v.DomainName,
		VpcID:                    v.VpcID,
		VpcEndpointID:            v.VpcEndpointID,
		Status:                   v.Status,
		CreatedAt:                v.CreatedAt,
		DeletedAt:                v.DeletedAt,
	}
}

func (v *storedVpcIngressConnection) toSummary() VpcIngressConnectionSummary {
	return VpcIngressConnectionSummary{
		VpcIngressConnectionArn: v.VpcIngressConnectionArn,
		ServiceArn:              v.ServiceArn,
	}
}

// storedCustomDomain holds a custom domain association.
type storedCustomDomain struct {
	DomainName         string `json:"domainName"`
	Status             string `json:"status"`
	EnableWWWSubdomain bool   `json:"enableWwwSubdomain"`
}

func (d *storedCustomDomain) toCustomDomain() CustomDomain {
	return CustomDomain{
		DomainName:         d.DomainName,
		Status:             d.Status,
		EnableWWWSubdomain: d.EnableWWWSubdomain,
	}
}
