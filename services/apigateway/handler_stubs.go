package apigateway

// handler_stubs.go wires the previously-stubbed API Gateway operations
// (VPC Links, GetExport/UpdateClientCertificate/UpdateGatewayResponse, domain
// name access associations, SDK generation, API key/documentation-part bulk
// import, and usage updates) into the action dispatch table, backed by real
// state in backend.go / backend_destub.go.

import (
	"encoding/json"
	"maps"
	"net/http"
)

const (
	// vpcLinkStatusAvailable is the status for an available VPC Link.
	vpcLinkStatusAvailable = "AVAILABLE"
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

// domainNameAccessAssociationsView is the response for GetDomainNameAccessAssociations.
type domainNameAccessAssociationsView struct {
	Items []DomainNameAccessAssociation `json:"item"`
}

// sdkTypeView is the response for GetSdkType, and one entry of GetSdkTypes.
type sdkTypeView struct {
	ID           string `json:"id"`
	FriendlyName string `json:"friendlyName,omitempty"`
}

// sdkTypesView is the response for GetSdkTypes.
type sdkTypesView struct {
	Items []sdkTypeView `json:"item"`
}

// apiKeysImportView is the response for ImportApiKeys.
type apiKeysImportView struct {
	IDs      []string `json:"ids"`
	Warnings []string `json:"warnings"`
}

// documentationPartsImportView is the response for ImportDocumentationParts.
type documentationPartsImportView struct {
	IDs      []string `json:"ids"`
	Warnings []string `json:"warnings"`
}

// importAPIKeysInput is the input for ImportApiKeys. The raw payload document is
// carried in Body; Format is the query parameter (csv or json).
type importAPIKeysInput struct {
	Format string `json:"format"`
	Body   []byte `json:"body"`
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

// domainNameAccessAssociationActions returns real stateful handlers for the
// Get/Delete/Reject domain name access association operations. Create is
// handled separately in backend.go's newResourceActions (it was already real).
func (h *Handler) domainNameAccessAssociationActions() map[string]actionFn {
	return map[string]actionFn{
		opGetDomainNameAccessAssociations: func(b []byte) (int, any, error) {
			var input struct {
				ResourceOwner string `json:"resourceOwner,omitempty"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			assocs, err := h.Backend.GetDomainNameAccessAssociations(input.ResourceOwner)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, &domainNameAccessAssociationsView{Items: assocs}, nil
		},
		opDeleteDomainNameAccessAssociation: func(b []byte) (int, any, error) {
			var input struct {
				Arn string `json:"domainNameAccessAssociationArn"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			if err := h.Backend.DeleteDomainNameAccessAssociation(input.Arn); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, nil, nil
		},
		opRejectDomainNameAccessAssociation: func(b []byte) (int, any, error) {
			var input struct {
				Arn           string `json:"domainNameAccessAssociationArn"`
				DomainNameArn string `json:"domainNameArn"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			if err := h.Backend.RejectDomainNameAccessAssociation(input.Arn, input.DomainNameArn); err != nil {
				return 0, nil, err
			}

			return http.StatusAccepted, nil, nil
		},
	}
}

// sdkActions returns real handlers for the SDK generation operations, backed
// by the fixed SDK type catalog and a real per-API/stage generated package.
func (h *Handler) sdkActions() map[string]actionFn {
	return map[string]actionFn{
		opGetSdk: func(b []byte) (int, any, error) {
			var input struct {
				RestAPIID string `json:"restApiId"`
				StageName string `json:"stageName"`
				SdkType   string `json:"sdkType"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			sdk, err := h.Backend.GetSdk(input.RestAPIID, input.StageName, input.SdkType)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{
				"contentType":        sdk.ContentType,
				"contentDisposition": sdk.ContentDisposition,
				"body":               sdk.Body,
			}, nil
		},
		opGetSdkType: func(b []byte) (int, any, error) {
			var input struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			t, err := h.Backend.GetSdkType(input.ID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, &sdkTypeView{ID: t.ID, FriendlyName: t.FriendlyName}, nil
		},
		opGetSdkTypes: func(_ []byte) (int, any, error) {
			types := h.Backend.GetSdkTypes()
			items := make([]sdkTypeView, 0, len(types))

			for _, t := range types {
				items = append(items, sdkTypeView(t))
			}

			return http.StatusOK, &sdkTypesView{Items: items}, nil
		},
	}
}

// importActions returns real handlers for the bulk-import operations.
func (h *Handler) importActions() map[string]actionFn {
	return map[string]actionFn{
		opImportAPIKeys: func(b []byte) (int, any, error) {
			specBody, env := decodeRestAPISpecPayload(b)

			ids, warnings, err := h.Backend.ImportAPIKeys(specBody, env.Parameters["format"], env.FailOnWarnings)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, &apiKeysImportView{IDs: ids, Warnings: warnings}, nil
		},
		opImportDocumentationParts: func(b []byte) (int, any, error) {
			specBody, env := decodeRestAPISpecPayload(b)

			ids, warnings, err := h.Backend.ImportDocumentationParts(
				env.RestAPIID,
				specBody,
				env.Mode,
				env.FailOnWarnings,
			)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, &documentationPartsImportView{IDs: ids, Warnings: warnings}, nil
		},
	}
}

// usageActions returns the real handler for UpdateUsage.
func (h *Handler) usageActions() map[string]actionFn {
	return map[string]actionFn{
		opUpdateUsage: func(b []byte) (int, any, error) {
			var raw map[string]string
			if err := json.Unmarshal(b, &raw); err != nil {
				return 0, nil, err
			}

			usagePlanID := raw[keyUsagePlanID]
			keyID := raw[keyKeyID]
			delete(raw, keyUsagePlanID)
			delete(raw, keyKeyID)

			usage, err := h.Backend.UpdateUsage(usagePlanID, keyID, raw)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, usage, nil
		},
	}
}

// stubActions returns the actionFn map for the previously-stubbed operations.
func (h *Handler) stubActions() map[string]actionFn {
	actions := make(map[string]actionFn)

	maps.Copy(actions, h.vpcLinkActions())
	maps.Copy(actions, h.exportAndCertActions())
	maps.Copy(actions, h.domainNameAccessAssociationActions())
	maps.Copy(actions, h.sdkActions())
	maps.Copy(actions, h.importActions())
	maps.Copy(actions, h.usageActions())

	// ImportRestApi and PutRestApi are real, stateful operations implemented
	// in import_export.go / handler_import_export.go, wired in via
	// restAPISpecActions() in dispatchTable.

	return actions
}
