package apigateway

// handler_stubs.go provides stub action handlers for API Gateway SDK operations
// not yet fully implemented.  Each stub returns a minimal valid response so the
// operation is visible in GetSupportedOperations and the SDK completeness test passes.

import "net/http"

const (
	// vpcLinkStatusAvailable is the status for an available VPC Link.
	vpcLinkStatusAvailable = "AVAILABLE"
	// stubClientCertIDKey is the JSON key for client certificate ID responses.
	stubClientCertIDKey = "clientCertificateId"
	// stubDefaultGWResponseType is the default gateway response type for stubs.
	stubDefaultGWResponseType = "DEFAULT_4XX"
	// stubImportedAPIName is the placeholder name for imported REST APIs.
	stubImportedAPIName = "imported-api"
	// stubImportedAPIID is the placeholder ID for imported REST APIs.
	stubImportedAPIID = "stub0000"
	// keyAPIName is the JSON key for API name in stub responses.
	keyAPIName = "name"
)

const (
	opCreateVpcLink                     = "CreateVpcLink"
	opDeleteDomainNameAccessAssociation = "DeleteDomainNameAccessAssociation"
	opDeleteVpcLink                     = "DeleteVpcLink"
	opGetDomainNameAccessAssociations   = "GetDomainNameAccessAssociations"
	opGetExport                         = "GetExport"
	opGetSdk                            = "GetSdk"
	opGetSdkType                        = "GetSdkType"
	opGetSdkTypes                       = "GetSdkTypes"
	opGetVpcLink                        = "GetVpcLink"
	opGetVpcLinks                       = "GetVpcLinks"
	opImportAPIKeys                     = "ImportApiKeys"
	opImportDocumentationParts          = "ImportDocumentationParts"
	opImportRestAPI                     = "ImportRestApi"
	opPutRestAPI                        = "PutRestApi"
	opRejectDomainNameAccessAssociation = "RejectDomainNameAccessAssociation"
	opUpdateClientCertificate           = "UpdateClientCertificate"
	opUpdateGatewayResponse             = "UpdateGatewayResponse"
	opUpdateUsage                       = "UpdateUsage"
	opUpdateVpcLink                     = "UpdateVpcLink"
)

// vpcLinkStub is a minimal VPC Link representation.
type vpcLinkStub struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
}

// vpcLinksStub is a minimal list of VPC Links.
type vpcLinksStub struct {
	Items []vpcLinkStub `json:"item"`
}

// domainNameAccessAssociationStub is a minimal domain name access association.
type domainNameAccessAssociationStub struct {
	DomainNameAccessAssociationArn string `json:"domainNameAccessAssociationArn"`
	DomainNameArn                  string `json:"domainNameArn"`
	AccessAssociationSourceType    string `json:"accessAssociationSourceType"`
	Status                         string `json:"status,omitempty"`
}

// domainNameAccessAssociationsStub is a list of access associations.
type domainNameAccessAssociationsStub struct {
	Items []domainNameAccessAssociationStub `json:"item"`
}

// sdkTypeStub is a minimal SDK type.
type sdkTypeStub struct {
	ID           string `json:"id"`
	FriendlyName string `json:"friendlyName,omitempty"`
}

// sdkTypesStub is a minimal list of SDK types.
type sdkTypesStub struct {
	Items []sdkTypeStub `json:"item"`
}

// apiKeysImportStub is the response for ImportApiKeys.
type apiKeysImportStub struct {
	IDs      []string `json:"ids"`
	Warnings []string `json:"warnings"`
}

// documentationPartsImportStub is the response for ImportDocumentationParts.
type documentationPartsImportStub struct {
	IDs      []string `json:"ids"`
	Warnings []string `json:"warnings"`
}

// stubActions returns the actionFn map for stub operations.
func (h *Handler) stubActions() map[string]actionFn {
	return map[string]actionFn{
		// VPC Links
		opCreateVpcLink: func(_ []byte) (int, any, error) {
			return http.StatusCreated, &vpcLinkStub{Status: vpcLinkStatusAvailable}, nil
		},
		opDeleteVpcLink: func(_ []byte) (int, any, error) {
			return http.StatusAccepted, nil, nil
		},
		opGetVpcLink: func(_ []byte) (int, any, error) {
			return http.StatusOK, &vpcLinkStub{Status: vpcLinkStatusAvailable}, nil
		},
		opGetVpcLinks: func(_ []byte) (int, any, error) {
			return http.StatusOK, &vpcLinksStub{Items: []vpcLinkStub{}}, nil
		},
		opUpdateVpcLink: func(_ []byte) (int, any, error) {
			return http.StatusOK, &vpcLinkStub{Status: vpcLinkStatusAvailable}, nil
		},
		// Domain name access associations
		opDeleteDomainNameAccessAssociation: func(_ []byte) (int, any, error) {
			return http.StatusAccepted, nil, nil
		},
		opGetDomainNameAccessAssociations: func(_ []byte) (int, any, error) {
			return http.StatusOK, &domainNameAccessAssociationsStub{Items: []domainNameAccessAssociationStub{}}, nil
		},
		opRejectDomainNameAccessAssociation: func(_ []byte) (int, any, error) {
			return http.StatusAccepted, nil, nil
		},
		// SDK export/generate
		opGetExport: func(_ []byte) (int, any, error) {
			return http.StatusOK, map[string]any{"contentType": "application/json", "body": "{}"}, nil
		},
		opGetSdk: func(_ []byte) (int, any, error) {
			return http.StatusOK, map[string]any{"contentType": "application/zip", "body": ""}, nil
		},
		opGetSdkType: func(_ []byte) (int, any, error) {
			return http.StatusOK, &sdkTypeStub{ID: "javascript", FriendlyName: "JavaScript"}, nil
		},
		opGetSdkTypes: func(_ []byte) (int, any, error) {
			return http.StatusOK, &sdkTypesStub{Items: []sdkTypeStub{
				{ID: "javascript", FriendlyName: "JavaScript"},
				{ID: "android", FriendlyName: "Android"},
				{ID: "swift", FriendlyName: "Swift (iOS)"},
			}}, nil
		},
		// Import operations
		opImportAPIKeys: func(_ []byte) (int, any, error) {
			return http.StatusCreated, &apiKeysImportStub{IDs: []string{}, Warnings: []string{}}, nil
		},
		opImportDocumentationParts: func(_ []byte) (int, any, error) {
			return http.StatusOK, &documentationPartsImportStub{IDs: []string{}, Warnings: []string{}}, nil
		},
		opImportRestAPI: func(_ []byte) (int, any, error) {
			return http.StatusCreated, map[string]any{"id": stubImportedAPIID, keyAPIName: stubImportedAPIName}, nil
		},
		opPutRestAPI: func(_ []byte) (int, any, error) {
			return http.StatusOK, map[string]any{"id": stubImportedAPIID, keyAPIName: stubImportedAPIName}, nil
		},
		// Client certificate update
		opUpdateClientCertificate: func(_ []byte) (int, any, error) {
			return http.StatusOK, map[string]any{stubClientCertIDKey: "stub", "description": ""}, nil
		},
		// Gateway response update
		opUpdateGatewayResponse: func(_ []byte) (int, any, error) {
			return http.StatusOK, map[string]any{"responseType": stubDefaultGWResponseType, "statusCode": "400"}, nil
		},
		// Usage update
		opUpdateUsage: func(_ []byte) (int, any, error) {
			return http.StatusOK, map[string]any{"usagePlanId": "", "items": map[string]any{}}, nil
		},
	}
}
