package cognitoidp

// UserPoolDomain holds the custom domain configuration for a user pool.
type UserPoolDomain struct {
	Domain                 string `json:"domain,omitempty"`
	UserPoolID             string `json:"userPoolID,omitempty"`
	CloudFrontDistribution string `json:"cloudFrontDistribution,omitempty"`
	CertificateArn         string `json:"certificateArn,omitempty"`
	Status                 string `json:"status,omitempty"`
	S3Bucket               string `json:"s3Bucket,omitempty"`
	AWSAccountID           string `json:"awsAccountID,omitempty"`
	ManagedLoginVersion    int32  `json:"managedLoginVersion,omitempty"`
}

type customDomainConfigJSON struct {
	CertificateArn string `json:"CertificateArn,omitempty"`
}

type createUserPoolDomainFullInput struct {
	CustomDomainConfig  *customDomainConfigJSON `json:"CustomDomainConfig,omitempty"`
	ManagedLoginVersion *int32                  `json:"ManagedLoginVersion,omitempty"`
	UserPoolID          string                  `json:"UserPoolId,omitempty"`
	Domain              string                  `json:"Domain,omitempty"`
}

type createUserPoolDomainFullOutput struct {
	ManagedLoginVersion *int32 `json:"ManagedLoginVersion,omitempty"`
	CloudFrontDomain    string `json:"CloudFrontDomain,omitempty"`
}

type updateUserPoolDomainFullInput struct {
	CustomDomainConfig  *customDomainConfigJSON `json:"CustomDomainConfig,omitempty"`
	ManagedLoginVersion *int32                  `json:"ManagedLoginVersion,omitempty"`
	UserPoolID          string                  `json:"UserPoolId,omitempty"`
	Domain              string                  `json:"Domain,omitempty"`
}

type updateUserPoolDomainFullOutput struct {
	ManagedLoginVersion *int32 `json:"ManagedLoginVersion,omitempty"`
	CloudFrontDomain    string `json:"CloudFrontDomain,omitempty"`
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

type updateUserPoolDomainInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Domain     string `json:"Domain,omitempty"`
}

type updateUserPoolDomainOutput struct {
	CloudFrontDomain string `json:"CloudFrontDomain,omitempty"`
}
