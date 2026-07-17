package ssoadmin

import "strings"

// awsApplicationProviderCatalog is the static catalog of AWS-managed SSO application providers.
//
//nolint:gochecknoglobals // read-only constant table
var awsApplicationProviderCatalog = []*ApplicationProvider{
	{
		ApplicationProviderArn: "arn:aws:sso::aws:applicationProvider/custom",
		FederationProtocol:     federationProtocolSAML,
		DisplayData: ApplicationProviderDisplayData{
			DisplayName: "Custom SAML 2.0 application",
			Description: "Connect any SAML 2.0 compatible application",
		},
	},
	{
		ApplicationProviderArn: "arn:aws:sso::aws:applicationProvider/salesforce",
		FederationProtocol:     federationProtocolSAML,
		DisplayData: ApplicationProviderDisplayData{
			DisplayName: "Salesforce",
			Description: "Customer relationship management platform",
		},
	},
	{
		ApplicationProviderArn: "arn:aws:sso::aws:applicationProvider/jira-cloud",
		FederationProtocol:     federationProtocolSAML,
		DisplayData: ApplicationProviderDisplayData{
			DisplayName: "Jira Cloud",
			Description: "Project and issue tracking by Atlassian",
		},
	},
	{
		ApplicationProviderArn: "arn:aws:sso::aws:applicationProvider/slack",
		FederationProtocol:     federationProtocolSAML,
		DisplayData: ApplicationProviderDisplayData{
			DisplayName: "Slack",
			Description: "Team messaging and collaboration",
		},
	},
	{
		ApplicationProviderArn: "arn:aws:sso::aws:applicationProvider/github",
		FederationProtocol:     federationProtocolSAML,
		DisplayData: ApplicationProviderDisplayData{
			DisplayName: "GitHub",
			Description: "Code hosting and version control",
		},
	},
	{
		ApplicationProviderArn: "arn:aws:sso::aws:applicationProvider/datadog",
		FederationProtocol:     federationProtocolSAML,
		DisplayData: ApplicationProviderDisplayData{
			DisplayName: "Datadog",
			Description: "Monitoring and analytics platform",
		},
	},
	{
		ApplicationProviderArn: "arn:aws:sso::aws:applicationProvider/pagerduty",
		FederationProtocol:     federationProtocolSAML,
		DisplayData: ApplicationProviderDisplayData{
			DisplayName: "PagerDuty",
			Description: "Incident management platform",
		},
	},
}

// awsProvidersByARN provides O(1) lookup into the catalog.
//
//nolint:gochecknoglobals // read-only constant table, built from awsApplicationProviderCatalog
var awsProvidersByARN = func() map[string]*ApplicationProvider {
	m := make(map[string]*ApplicationProvider, len(awsApplicationProviderCatalog))
	for _, p := range awsApplicationProviderCatalog {
		m[p.ApplicationProviderArn] = p
	}

	return m
}()

// ListApplicationProviders returns the static AWS-managed provider catalog.
func (b *InMemoryBackend) ListApplicationProviders() []*ApplicationProvider {
	result := make([]*ApplicationProvider, len(awsApplicationProviderCatalog))
	for i, p := range awsApplicationProviderCatalog {
		cp := *p
		result[i] = &cp
	}

	return result
}

// DescribeApplicationProvider returns details for an application provider.
// Account-scoped custom provider ARNs (arn:aws:sso::<accountId>:applicationProvider/custom)
// are resolved to the AWS-managed custom provider entry.
func (b *InMemoryBackend) DescribeApplicationProvider(
	applicationProviderArn string,
) (*ApplicationProvider, error) {
	if p, ok := awsProvidersByARN[applicationProviderArn]; ok {
		cp := *p

		return &cp, nil
	}
	// Account-scoped ARN: try matching by provider path suffix.
	if idx := strings.LastIndex(applicationProviderArn, ":applicationProvider/"); idx >= 0 {
		suffix := applicationProviderArn[idx+len(":applicationProvider/"):]
		canonicalArn := "arn:aws:sso::aws:applicationProvider/" + suffix
		if p, ok := awsProvidersByARN[canonicalArn]; ok {
			cp := *p

			return &cp, nil
		}
	}

	return nil, ErrRequestNotFound
}
