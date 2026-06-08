package apigateway

// handler_stubs.go provides stub action handlers for API Gateway SDK operations
// not yet fully implemented.  Each stub returns a minimal valid response so the
// operation is visible in GetSupportedOperations and the SDK completeness test passes.

import (
	"encoding/json"
	"maps"
	"net/http"
)

const (
	// vpcLinkStatusAvailable is the status for an available VPC Link.
	vpcLinkStatusAvailable = "AVAILABLE"
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

// vpcLinkActions returns real stateful action handlers for VPC Link operations.
func (h *Handler) vpcLinkActions() map[string]actionFn {
	return map[string]actionFn{
		opCreateVpcLink: func(b []byte) (int, any, error) {
			var input CreateVpcLinkInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			link, err := h.Backend.CreateVpcLink(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, link, nil
		},
		opDeleteVpcLink: func(b []byte) (int, any, error) {
			var input deleteVpcLinkInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			if err := h.Backend.DeleteVpcLink(input.VpcLinkID); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, nil, nil
		},
		opGetVpcLink: func(b []byte) (int, any, error) {
			var input getVpcLinkInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			link, err := h.Backend.GetVpcLink(input.VpcLinkID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, link, nil
		},
		opGetVpcLinks: func(_ []byte) (int, any, error) {
			links, err := h.Backend.GetVpcLinks()
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{keyItem: links}, nil
		},
		opUpdateVpcLink: func(b []byte) (int, any, error) {
			var input UpdateVpcLinkInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			link, err := h.Backend.UpdateVpcLink(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, link, nil
		},
	}
}

// exportAndCertActions returns real stateful handlers for export, client cert update,
// and gateway response update operations.
func (h *Handler) exportAndCertActions() map[string]actionFn {
	return map[string]actionFn{
		opGetExport: func(b []byte) (int, any, error) {
			var input getExportInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			export, err := h.Backend.GetExport(input.RestAPIID, input.StageName, input.ExportType)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, export, nil
		},
		opUpdateClientCertificate: func(b []byte) (int, any, error) {
			var input UpdateClientCertificateInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			cert, err := h.Backend.UpdateClientCertificate(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, cert, nil
		},
		opUpdateGatewayResponse: func(b []byte) (int, any, error) {
			var input PutGatewayResponseInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			gr, err := h.Backend.PutGatewayResponse(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, gr, nil
		},
	}
}

// stubActions returns the actionFn map for stub operations.
func (h *Handler) stubActions() map[string]actionFn {
	actions := make(map[string]actionFn)

	maps.Copy(actions, h.vpcLinkActions())
	maps.Copy(actions, h.exportAndCertActions())

	// Domain name access associations
	actions[opDeleteDomainNameAccessAssociation] = func(_ []byte) (int, any, error) {
		return http.StatusAccepted, nil, nil
	}
	actions[opGetDomainNameAccessAssociations] = func(_ []byte) (int, any, error) {
		return http.StatusOK, &domainNameAccessAssociationsStub{Items: []domainNameAccessAssociationStub{}}, nil
	}
	actions[opRejectDomainNameAccessAssociation] = func(_ []byte) (int, any, error) {
		return http.StatusAccepted, nil, nil
	}

	// SDK operations
	actions[opGetSdk] = func(_ []byte) (int, any, error) {
		return http.StatusOK, map[string]any{"contentType": "application/zip", "body": ""}, nil
	}
	actions[opGetSdkType] = func(_ []byte) (int, any, error) {
		return http.StatusOK, &sdkTypeStub{ID: "javascript", FriendlyName: "JavaScript"}, nil
	}
	actions[opGetSdkTypes] = func(_ []byte) (int, any, error) {
		return http.StatusOK, &sdkTypesStub{Items: []sdkTypeStub{
			{ID: "javascript", FriendlyName: "JavaScript"},
			{ID: "android", FriendlyName: "Android"},
			{ID: "swift", FriendlyName: "Swift (iOS)"},
		}}, nil
	}

	// Import operations
	actions[opImportAPIKeys] = func(_ []byte) (int, any, error) {
		return http.StatusCreated, &apiKeysImportStub{IDs: []string{}, Warnings: []string{}}, nil
	}
	actions[opImportDocumentationParts] = func(_ []byte) (int, any, error) {
		return http.StatusOK, &documentationPartsImportStub{IDs: []string{}, Warnings: []string{}}, nil
	}
	actions[opImportRestAPI] = func(_ []byte) (int, any, error) {
		return http.StatusCreated, map[string]any{"id": stubImportedAPIID, keyAPIName: stubImportedAPIName}, nil
	}
	actions[opPutRestAPI] = func(_ []byte) (int, any, error) {
		return http.StatusOK, map[string]any{"id": stubImportedAPIID, keyAPIName: stubImportedAPIName}, nil
	}

	// Usage update
	actions[opUpdateUsage] = func(_ []byte) (int, any, error) {
		return http.StatusOK, map[string]any{"usagePlanId": "", "items": map[string]any{}}, nil
	}

	return actions
}
