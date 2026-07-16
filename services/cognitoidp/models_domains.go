package cognitoidp

// UserPoolDomain holds the custom domain configuration for a user pool.
type UserPoolDomain struct {
	Domain                 string `json:"domain,omitempty"`
	UserPoolID             string `json:"userPoolID,omitempty"`
	CloudFrontDistribution string `json:"cloudFrontDistribution,omitempty"`
	CertificateArn         string `json:"certificateArn,omitempty"`
	Status                 string `json:"status,omitempty"`
}

type customDomainConfigJSON struct {
	CertificateArn string `json:"CertificateArn,omitempty"`
}

type createUserPoolDomainFullInput struct {
	CustomDomainConfig *customDomainConfigJSON `json:"CustomDomainConfig,omitempty"`
	UserPoolID         string                  `json:"UserPoolId,omitempty"`
	Domain             string                  `json:"Domain,omitempty"`
}

type createUserPoolDomainFullOutput struct {
	CloudFrontDomain string `json:"CloudFrontDomain,omitempty"`
}

type updateUserPoolDomainFullInput struct {
	CustomDomainConfig *customDomainConfigJSON `json:"CustomDomainConfig,omitempty"`
	UserPoolID         string                  `json:"UserPoolId,omitempty"`
	Domain             string                  `json:"Domain,omitempty"`
}

type updateUserPoolDomainFullOutput struct {
	CloudFrontDomain string `json:"CloudFrontDomain,omitempty"`
}

type createUserPoolDomainInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Domain     string `json:"Domain,omitempty"`
}

type createUserPoolDomainOutput struct {
	CloudFrontDomain string `json:"CloudFrontDomain,omitempty"`
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
	Domain                 string `json:"Domain,omitempty"`
	UserPoolID             string `json:"UserPoolId,omitempty"`
	Status                 string `json:"Status,omitempty"`
	CloudFrontDistribution string `json:"CloudFrontDistribution,omitempty"`
}

type describeUserPoolDomainOutput struct {
	DomainDescription *userPoolDomainDescription `json:"DomainDescription,omitempty"`
}

type updateUserPoolDomainInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Domain     string `json:"Domain,omitempty"`
}

type updateUserPoolDomainOutput struct {
	CloudFrontDomain string `json:"CloudFrontDomain,omitempty"`
}
