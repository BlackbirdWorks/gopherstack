package redshift

import (
	"encoding/xml"
	"net/url"
)

// ----- Namespace Registration -----
//
// RegisterNamespaceInput/DeregisterNamespaceInput both carry required
// ConsumerIdentifiers ([]string) and NamespaceIdentifier
// (NamespaceIdentifierUnion: ProvisionedIdentifier{ClusterIdentifier} or
// ServerlessIdentifier{NamespaceIdentifier, WorkgroupIdentifier}) -- redshift
// is the awsAwsquery_* (Query) protocol, so these arrive as form-encoded
// dotted/numbered keys, confirmed against
// awsAwsquery_serializeOpDocumentRegisterNamespaceInput and
// awsAwsquery_serializeDocumentNamespaceIdentifierUnion in serializers.go:
//   ConsumerIdentifiers.member.1, .2, ...
//   NamespaceIdentifier.ProvisionedIdentifier.ClusterIdentifier
//   NamespaceIdentifier.ServerlessIdentifier.NamespaceIdentifier
//   NamespaceIdentifier.ServerlessIdentifier.WorkgroupIdentifier

type namespaceRegistrationResult struct {
	Status string `xml:"Status"`
}

type deregisterNamespaceResponse struct {
	XMLName xml.Name                    `xml:"DeregisterNamespaceResponse"`
	Xmlns   string                      `xml:"xmlns,attr"`
	Result  namespaceRegistrationResult `xml:"DeregisterNamespaceResult"`
}

func (h *Handler) handleDeregisterNamespace(vals url.Values) (any, error) {
	reg, err := h.Backend.DeregisterNamespace(namespaceIdentifierArgs(vals))
	if err != nil {
		return nil, err
	}

	return &deregisterNamespaceResponse{
		Xmlns:  redshiftXMLNS,
		Result: namespaceRegistrationResult{Status: reg.Status},
	}, nil
}

type registerNamespaceResponse struct {
	XMLName xml.Name                    `xml:"RegisterNamespaceResponse"`
	Xmlns   string                      `xml:"xmlns,attr"`
	Result  namespaceRegistrationResult `xml:"RegisterNamespaceResult"`
}

func (h *Handler) handleRegisterNamespace(vals url.Values) (any, error) {
	reg, err := h.Backend.RegisterNamespace(namespaceIdentifierArgs(vals))
	if err != nil {
		return nil, err
	}

	return &registerNamespaceResponse{
		Xmlns:  redshiftXMLNS,
		Result: namespaceRegistrationResult{Status: reg.Status},
	}, nil
}

// namespaceIdentifierArgs extracts the shared RegisterNamespace/
// DeregisterNamespace request fields from the query-protocol form values.
func namespaceIdentifierArgs(vals url.Values) ([]string, string, string, string) {
	consumerIdentifiers := parseStringList(vals, "ConsumerIdentifiers.member.")
	clusterIdentifier := vals.Get("NamespaceIdentifier.ProvisionedIdentifier.ClusterIdentifier")
	serverlessNamespace := vals.Get("NamespaceIdentifier.ServerlessIdentifier.NamespaceIdentifier")
	serverlessWorkgroup := vals.Get("NamespaceIdentifier.ServerlessIdentifier.WorkgroupIdentifier")

	return consumerIdentifiers, clusterIdentifier, serverlessNamespace, serverlessWorkgroup
}
