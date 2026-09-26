package cloudformation

// These are the real CloudFormation type names for two families whose
// legacy (undocumented) names are already wired in resources.go:
// AWS::ACM::Certificate has no such entry in the CFN resource
// specification -- the real name is AWS::CertificateManager::Certificate --
// and AWS::OpenSearch::Domain's real name is
// AWS::OpenSearchService::Domain. Both aliases dispatch to the same
// creators/deleters as their legacy counterparts (see createMiscLegacyResource,
// deleteComputeStorageResource, and deleteAppNetworkResource in resources.go).
const (
	resTypeCertificateManagerCertificate = "AWS::CertificateManager::Certificate"
	resTypeOpenSearchServiceDomain       = "AWS::OpenSearchService::Domain"
)
