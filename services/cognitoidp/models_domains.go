package cognitoidp

import "fmt"

// UserPoolDomain holds the custom domain configuration for a user pool.
type UserPoolDomain struct {
	Routing                *DomainRouting `json:"routing,omitempty"`
	Domain                 string         `json:"domain,omitempty"`
	UserPoolID             string         `json:"userPoolID,omitempty"`
	CloudFrontDistribution string         `json:"cloudFrontDistribution,omitempty"`
	CertificateArn         string         `json:"certificateArn,omitempty"`
	Status                 string         `json:"status,omitempty"`
	S3Bucket               string         `json:"s3Bucket,omitempty"`
	AWSAccountID           string         `json:"awsAccountID,omitempty"`
	ManagedLoginVersion    int32          `json:"managedLoginVersion,omitempty"`
}

// DomainRouting mirrors types.RoutingType.
type DomainRouting struct {
	Failover *DomainFailover `json:"failover,omitempty"`
}

// DomainFailover mirrors types.FailoverType; both members are required.
type DomainFailover struct {
	PrimaryRoute53HealthCheckID string `json:"primaryRoute53HealthCheckId"`
	SecondaryRegion             string `json:"secondaryRegion"`
}

type routingJSON struct {
	Failover *struct {
		PrimaryRoute53HealthCheckID string `json:"PrimaryRoute53HealthCheckId,omitempty"`
		SecondaryRegion             string `json:"SecondaryRegion,omitempty"`
	} `json:"Failover,omitempty"`
}

func routingFromJSON(in *routingJSON) (*DomainRouting, error) {
	if in == nil {
		return nil, nil //nolint:nilnil // routing omitted
	}

	if in.Failover == nil || in.Failover.PrimaryRoute53HealthCheckID == "" || in.Failover.SecondaryRegion == "" {
		return nil, fmt.Errorf(
			"%w: Routing.Failover requires PrimaryRoute53HealthCheckId and SecondaryRegion", ErrInvalidParameter,
		)
	}

	return &DomainRouting{Failover: &DomainFailover{
		PrimaryRoute53HealthCheckID: in.Failover.PrimaryRoute53HealthCheckID,
		SecondaryRegion:             in.Failover.SecondaryRegion,
	}}, nil
}

func routingToJSON(r *DomainRouting) *routingJSON {
	if r == nil || r.Failover == nil {
		return nil
	}

	out := &routingJSON{Failover: &struct {
		PrimaryRoute53HealthCheckID string `json:"PrimaryRoute53HealthCheckId,omitempty"`
		SecondaryRegion             string `json:"SecondaryRegion,omitempty"`
	}{
		PrimaryRoute53HealthCheckID: r.Failover.PrimaryRoute53HealthCheckID,
		SecondaryRegion:             r.Failover.SecondaryRegion,
	}}

	return out
}

type customDomainConfigJSON struct {
	CertificateArn string `json:"CertificateArn,omitempty"`
}

type createUserPoolDomainFullInput struct {
	CustomDomainConfig  *customDomainConfigJSON `json:"CustomDomainConfig,omitempty"`
	ManagedLoginVersion *int32                  `json:"ManagedLoginVersion,omitempty"`
	Routing             *routingJSON            `json:"Routing,omitempty"`
	UserPoolID          string                  `json:"UserPoolId,omitempty"`
	Domain              string                  `json:"Domain,omitempty"`
}

type createUserPoolDomainFullOutput struct {
	Routing             *routingJSON `json:"Routing,omitempty"`
	ManagedLoginVersion *int32       `json:"ManagedLoginVersion,omitempty"`
	CloudFrontDomain    string       `json:"CloudFrontDomain,omitempty"`
}

type updateUserPoolDomainFullInput struct {
	CustomDomainConfig  *customDomainConfigJSON `json:"CustomDomainConfig,omitempty"`
	ManagedLoginVersion *int32                  `json:"ManagedLoginVersion,omitempty"`
	Routing             *routingJSON            `json:"Routing,omitempty"`
	UserPoolID          string                  `json:"UserPoolId,omitempty"`
	Domain              string                  `json:"Domain,omitempty"`
}

type updateUserPoolDomainFullOutput struct {
	Routing             *routingJSON `json:"Routing,omitempty"`
	ManagedLoginVersion *int32       `json:"ManagedLoginVersion,omitempty"`
	CloudFrontDomain    string       `json:"CloudFrontDomain,omitempty"`
}

type deleteUserPoolDomainInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Domain     string `json:"Domain,omitempty"`
}

type deleteUserPoolDomainOutput struct{}

type describeUserPoolDomainInput struct {
	Domain string `json:"Domain,omitempty"`
}

type userPoolDomainDescription struct {
	Routing                *routingJSON            `json:"Routing,omitempty"`
	CustomDomainConfig     *customDomainConfigJSON `json:"CustomDomainConfig,omitempty"`
	ManagedLoginVersion    *int32                  `json:"ManagedLoginVersion,omitempty"`
	Domain                 string                  `json:"Domain,omitempty"`
	UserPoolID             string                  `json:"UserPoolId,omitempty"`
	Status                 string                  `json:"Status,omitempty"`
	CloudFrontDistribution string                  `json:"CloudFrontDistribution,omitempty"`
	AWSAccountID           string                  `json:"AWSAccountId,omitempty"`
	S3Bucket               string                  `json:"S3Bucket,omitempty"`
}

type describeUserPoolDomainOutput struct {
	DomainDescription *userPoolDomainDescription `json:"DomainDescription,omitempty"`
}
