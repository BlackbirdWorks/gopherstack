package apigateway

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
)

// handler_router.go maps an HTTP method + URL path to an API Gateway
// operation name and its path parameters. It is the pure "URL -> operation"
// resolution layer that sits in front of dispatch (see buildDispatchTable in
// handler.go); the actual per-operation handlers live in the matching
// handler_<family>.go files.

const (
	keyRestAPIID                      = "restApiId"
	keyResourceArn                    = "resourceArn"
	keyAPIKeyID                       = "apiKeyId"
	keyDomainName                     = "domainName"
	keyBasePath                       = "basePath"
	keyUsagePlanID                    = "usagePlanId"
	keyResourceID                     = "resourceId"
	keyDeploymentID                   = "deploymentId"
	keyStageName                      = "stageName"
	keyAuthorizerID                   = "authorizerId"
	keyRequestValidatorID             = "requestValidatorId"
	keyModelName                      = "modelName"
	keyDocPartID                      = "docPartId"
	keyDocumentationVersion           = "documentationVersion"
	keyResponseType                   = "responseType"
	keyHTTPMethod                     = "httpMethod"
	keyStatusCode                     = "statusCode"
	keyVpcLinkID                      = "vpcLinkId"
	keyClientCertificateID            = "clientCertificateId"
	keySdkTypeID                      = "id"
	keyExportType                     = "exportType"
	keySdkType                        = "sdkType"
	keyDomainNameAccessAssociationArn = "domainNameAccessAssociationArn"
	keyKeyID                          = "keyId"
	// keyAPIName is the JSON key for API name in stub responses.
	keyAPIName = "name"
	// keyItem is the response collection key used by AWS API Gateway list
	// operations. The AWS Go SDK v2 deserializer expects the singular "item"
	// for every list response (it is the wire name in the smithy model).
	keyItem = "item"
)

const (
	opUpdateRequestValidator = "UpdateRequestValidator"
	opUpdateStage            = "UpdateStage"
	opUpdateRestAPI          = "UpdateRestApi"
	opUpdateResource         = "UpdateResource"
	opUpdateUsagePlan        = "UpdateUsagePlan"
)

const (
	opGetDocumentationParts      = "GetDocumentationParts"
	opGetDocumentationVersion    = "GetDocumentationVersion"
	opGetDocumentationVersions   = "GetDocumentationVersions"
	opGetDomainName              = "GetDomainName"
	opGetDomainNames             = "GetDomainNames"
	opGetGatewayResponse         = "GetGatewayResponse"
	opGetGatewayResponses        = "GetGatewayResponses"
	opGetIntegration             = "GetIntegration"
	opGetIntegrationResponse     = "GetIntegrationResponse"
	opGetMethod                  = "GetMethod"
	opGetMethodResponse          = "GetMethodResponse"
	opGetModel                   = "GetModel"
	opGetModelTemplate           = "GetModelTemplate"
	opGetModels                  = "GetModels"
	opGetRequestValidator        = "GetRequestValidator"
	opGetRequestValidators       = "GetRequestValidators"
	opGetResource                = "GetResource"
	opGetResources               = "GetResources"
	opGetRestAPI                 = "GetRestApi"
	opGetRestApis                = "GetRestApis"
	opGetStage                   = "GetStage"
	opGetStages                  = "GetStages"
	opGetTags                    = "GetTags"
	opGetUsage                   = "GetUsage"
	opGetUsagePlan               = "GetUsagePlan"
	opGetUsagePlanKey            = "GetUsagePlanKey"
	opGetUsagePlanKeys           = "GetUsagePlanKeys"
	opGetUsagePlans              = "GetUsagePlans"
	opPutGatewayResponse         = "PutGatewayResponse"
	opPutIntegration             = "PutIntegration"
	opPutIntegrationResponse     = "PutIntegrationResponse"
	opPutMethod                  = "PutMethod"
	opPutMethodResponse          = "PutMethodResponse"
	opTagResource                = "TagResource"
	opTestInvokeAuthorizer       = "TestInvokeAuthorizer"
	opTestInvokeMethod           = "TestInvokeMethod"
	opUntagResource              = "UntagResource"
	opUpdateAccount              = "UpdateAccount"
	opUpdateAPIKey               = "UpdateApiKey"
	opUpdateAuthorizer           = "UpdateAuthorizer"
	opUpdateBasePathMapping      = "UpdateBasePathMapping"
	opUpdateDeployment           = "UpdateDeployment"
	opUpdateDocumentationPart    = "UpdateDocumentationPart"
	opUpdateDocumentationVersion = "UpdateDocumentationVersion"
	opUpdateDomainName           = "UpdateDomainName"
	opUpdateIntegration          = "UpdateIntegration"
	opUpdateIntegrationResponse  = "UpdateIntegrationResponse"
	opUpdateMethod               = "UpdateMethod"
	opUpdateMethodResponse       = "UpdateMethodResponse"
	opUpdateModel                = "UpdateModel"

	opCreateAPIKey                      = "CreateApiKey"
	opCreateAuthorizer                  = "CreateAuthorizer"
	opCreateBasePathMapping             = "CreateBasePathMapping"
	opCreateDeployment                  = "CreateDeployment"
	opCreateDocumentationPart           = "CreateDocumentationPart"
	opCreateDocumentationVersion        = "CreateDocumentationVersion"
	opCreateDomainName                  = "CreateDomainName"
	opCreateDomainNameAccessAssociation = "CreateDomainNameAccessAssociation"
	opCreateModel                       = "CreateModel"
	opCreateRequestValidator            = "CreateRequestValidator"
	opCreateResource                    = "CreateResource"
	opCreateRestAPI                     = "CreateRestApi"
	opCreateStage                       = "CreateStage"
	opCreateUsagePlan                   = "CreateUsagePlan"
	opCreateUsagePlanKey                = "CreateUsagePlanKey"
	opDeleteAPIKey                      = "DeleteApiKey"
	opDeleteAuthorizer                  = "DeleteAuthorizer"
	opDeleteBasePathMapping             = "DeleteBasePathMapping"
	opDeleteClientCertificate           = "DeleteClientCertificate"
	opDeleteDeployment                  = "DeleteDeployment"
	opDeleteDocumentationPart           = "DeleteDocumentationPart"
	opDeleteDocumentationVersion        = "DeleteDocumentationVersion"
	opDeleteDomainName                  = "DeleteDomainName"
	opDeleteGatewayResponse             = "DeleteGatewayResponse"
	opDeleteIntegration                 = "DeleteIntegration"
	opDeleteIntegrationResponse         = "DeleteIntegrationResponse"
	opDeleteMethod                      = "DeleteMethod"
	opDeleteMethodResponse              = "DeleteMethodResponse"
	opDeleteModel                       = "DeleteModel"
	opDeleteRequestValidator            = "DeleteRequestValidator"
	opDeleteResource                    = "DeleteResource"
	opDeleteRestAPI                     = "DeleteRestApi"
	opDeleteStage                       = "DeleteStage"
	opDeleteUsagePlan                   = "DeleteUsagePlan"
	opDeleteUsagePlanKey                = "DeleteUsagePlanKey"
	opFlushStageAuthorizersCache        = "FlushStageAuthorizersCache"
	opFlushStageCache                   = "FlushStageCache"
	opGenerateClientCertificate         = "GenerateClientCertificate"
	opGetAccount                        = "GetAccount"
	opGetAPIKey                         = "GetApiKey"
	opGetAPIKeys                        = "GetApiKeys"
	opGetAuthorizer                     = "GetAuthorizer"
	opGetAuthorizers                    = "GetAuthorizers"
	opGetBasePathMapping                = "GetBasePathMapping"
	opGetBasePathMappings               = "GetBasePathMappings"
	opGetClientCertificate              = "GetClientCertificate"
	opGetClientCertificates             = "GetClientCertificates"
	opGetDeployment                     = "GetDeployment"
	opGetDeployments                    = "GetDeployments"
	opGetDocumentationPart              = "GetDocumentationPart"
)

var errUnknownOperation = errors.New("UnknownOperationException")

// path depth constants for URL segment counting.
const (
	pathDepth1 = 1
	pathDepth2 = 2
	pathDepth3 = 3
	pathDepth4 = 4
	pathDepth5 = 5
)

// path segment constants used in REST route matching.
const (
	apiGWUnknownOp                             = "Unknown"
	apiGWSegRestAPIs                           = "restapis"
	apiGWSegResources                          = "resources"
	apiGWSegDeployment                         = "deployments"
	apiGWSegStages                             = "stages"
	apiGWSegMethods                            = "methods"
	apiGWSegInteg                              = "integration"
	apiGWSegResponses                          = "responses"
	apiGWSegAuthorizers                        = "authorizers"
	apiGWSegValidators                         = "requestvalidators"
	apiGWSegAPIKeys                            = "apikeys"
	apiGWSegDomainNames                        = "domainnames"
	apiGWSegBasePathMappings                   = "basepathmappings"
	apiGWSegDocumentation                      = "documentation"
	apiGWSegDocParts                           = "parts"
	apiGWSegDocVersions                        = "versions"
	apiGWSegModels                             = "models"
	apiGWSegUsagePlans                         = "usageplans"
	apiGWSegUsagePlanKeys                      = "keys"
	apiGWSegGatewayResponses                   = "gatewayresponses"
	apiGWSegClientCerts                        = "clientcertificates"
	apiGWSegVpcLinks                           = "vpclinks"
	apiGWSegSdkTypes                           = "sdktypes"
	apiGWSegExports                            = "exports"
	apiGWSegSdks                               = "sdks"
	apiGWSegDomainNameAccessAssociations       = "domainnameaccessassociations"
	apiGWSegRejectDomainNameAccessAssociations = "rejectdomainnameaccessassociations"
	apiGWSegUsage                              = "usage"

	// apiGWMinTagPathSegs is the minimum number of path segments for a /tags/{arn} path.
	apiGWMinTagPathSegs = 2

	// pathDepth5 is declared above; pathDepth6 covers paths like
	// /restapis/{id}/stages/{name}/exports/{type}.
	pathDepth6 = 6
)

// parseAPIGWRESTPath maps an HTTP method + URL path to an API Gateway operation name
// and extracts path parameters. Returns ("Unknown", nil, false) when no pattern matches.
// query carries the request's query string parameters; it is only consulted for
// the handful of routes (e.g. POST /restapis?mode=import) where AWS
// distinguishes operations by query parameter rather than path shape.
func parseAPIGWRESTPath(method, path string, query url.Values) (string, map[string]string, bool) {
	// Strip leading "/" and split into path segments.
	segs := strings.Split(strings.TrimPrefix(path, "/"), "/")
	n := len(segs)

	if n == 0 {
		return apiGWUnknownOp, nil, false
	}

	switch segs[0] {
	case apiGWSegRestAPIs:
		return parseAPIGWRestAPIsPath(method, segs, n, query)
	case apiGWSegAPIKeys:
		return parseAPIGWAPIKeysPath(method, segs, n, query)
	case apiGWSegDomainNames:
		return parseAPIGWDomainNamesPath(method, segs, n)
	case apiGWSegUsagePlans:
		return parseAPIGWUsagePlansPath(method, segs, n)
	case "account":
		return parseAPIGWAccountPath(method, segs, n)
	case "tags":
		return parseAPIGWTagsPath(method, segs, n)
	case apiGWSegClientCerts:
		return parseAPIGWClientCertificatesPath(method, segs, n)
	case apiGWSegVpcLinks:
		return parseAPIGWVpcLinksPath(method, segs, n)
	case apiGWSegSdkTypes:
		return parseAPIGWSdkTypesPath(method, segs, n)
	case apiGWSegDomainNameAccessAssociations:
		return parseAPIGWDomainNameAccessAssociationsPath(method, segs, n)
	case apiGWSegRejectDomainNameAccessAssociations:
		if n == pathDepth1 && method == http.MethodPost {
			return opRejectDomainNameAccessAssociation, nil, true
		}
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWDomainNamesPath handles /domainnames/... paths.
//

// parseAPIGWRestAPIsPath handles /restapis/... paths.
func parseAPIGWRestAPIsPath(method string, segs []string, n int, query url.Values) (string, map[string]string, bool) {
	apiID := ""
	if n >= pathDepth2 {
		apiID = segs[1]
	}

	switch n {
	case pathDepth1:
		switch method {
		case http.MethodPost:
			// POST /restapis?mode=import → ImportRestApi. POST /restapis
			// (no mode) → CreateRestApi. Real AWS distinguishes the two
			// solely by this query parameter (both use the same path).
			if query.Get("mode") == modeImport {
				return opImportRestAPI, nil, true
			}

			return opCreateRestAPI, nil, true
		case http.MethodGet:
			return opGetRestApis, nil, true
		}
	case pathDepth2:
		return parseAPIGWRestAPIsDepth2(method, apiID)
	case pathDepth3:
		return parseAPIGWRestAPIsDepth3(method, segs, apiID)
	case pathDepth4:
		return parseAPIGWRestAPIsDepth4(method, segs, apiID)
	default:
		return parseAPIGWRestAPIsDeepPath(method, segs, n, apiID)
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWRestAPIsDepth2 handles /restapis/{id} paths.
func parseAPIGWRestAPIsDepth2(method, apiID string) (string, map[string]string, bool) {
	params := map[string]string{keyRestAPIID: apiID}
	switch method {
	case http.MethodGet:
		return opGetRestAPI, params, true
	case http.MethodDelete:
		return opDeleteRestAPI, params, true
	case http.MethodPatch:
		return opUpdateRestAPI, params, true
	case http.MethodPut:
		// PUT /restapis/{id} is PutRestApi (OpenAPI import into an existing
		// API). The body is the raw spec; detectImportRESTAPI handles it.
		return opPutRestAPI, params, true
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWRestAPIsDepth3 handles /restapis/{id}/{subresource} paths.
func parseAPIGWRestAPIsDepth3(method string, segs []string, apiID string) (string, map[string]string, bool) {
	apiParam := map[string]string{keyRestAPIID: apiID}

	if op, params, ok := parseAPIGWRestAPIsDepth3Core(method, segs[2], apiParam); ok {
		return op, params, ok
	}

	return parseAPIGWRestAPIsDepth3Extended(method, segs[2], apiParam)
}

// parseAPIGWRestAPIsDepth3Core handles resources, deployments, and stages depth-3 paths.
func parseAPIGWRestAPIsDepth3Core(method, sub string, apiParam map[string]string) (string, map[string]string, bool) {
	switch sub {
	case apiGWSegResources:
		if method == http.MethodGet {
			return opGetResources, apiParam, true
		}
	case apiGWSegDeployment:
		switch method {
		case http.MethodPost:
			return opCreateDeployment, apiParam, true
		case http.MethodGet:
			return opGetDeployments, apiParam, true
		}
	case apiGWSegStages:
		switch method {
		case http.MethodGet:
			return opGetStages, apiParam, true
		case http.MethodPost:
			return opCreateStage, apiParam, true
		}
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWRestAPIsDepth3Extended handles authorizers, validators, models, and gateway responses depth-3 paths.
func parseAPIGWRestAPIsDepth3Extended(
	method, sub string,
	apiParam map[string]string,
) (string, map[string]string, bool) {
	switch sub {
	case apiGWSegAuthorizers:
		switch method {
		case http.MethodPost:
			return opCreateAuthorizer, apiParam, true
		case http.MethodGet:
			return opGetAuthorizers, apiParam, true
		}
	case apiGWSegValidators:
		switch method {
		case http.MethodPost:
			return opCreateRequestValidator, apiParam, true
		case http.MethodGet:
			return opGetRequestValidators, apiParam, true
		}
	case apiGWSegModels:
		switch method {
		case http.MethodPost:
			return opCreateModel, apiParam, true
		case http.MethodGet:
			return opGetModels, apiParam, true
		}
	case apiGWSegGatewayResponses:
		if method == http.MethodGet {
			return opGetGatewayResponses, apiParam, true
		}
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWRestAPIsDepth4 handles /restapis/{id}/{subresource}/{item} paths.
func parseAPIGWRestAPIsDepth4(method string, segs []string, apiID string) (string, map[string]string, bool) {
	if op, params, ok := parseAPIGWRestAPIsDepth4Core(method, segs, apiID); ok {
		return op, params, ok
	}

	return parseAPIGWRestAPIsDepth4Extended(method, segs, apiID)
}

// parseAPIGWRestAPIsDepth4Core handles resources, deployments, and stages depth-4 paths.
func parseAPIGWRestAPIsDepth4Core(method string, segs []string, apiID string) (string, map[string]string, bool) {
	switch segs[2] {
	case apiGWSegResources:
		params := map[string]string{keyRestAPIID: apiID, keyResourceID: segs[3]}
		switch method {
		case http.MethodGet:
			return opGetResource, params, true
		case http.MethodPost:
			return opCreateResource, map[string]string{keyRestAPIID: apiID, "parentId": segs[3]}, true
		case http.MethodPatch:
			return opUpdateResource, params, true
		case http.MethodDelete:
			return opDeleteResource, params, true
		}
	case apiGWSegDeployment:
		params := map[string]string{keyRestAPIID: apiID, keyDeploymentID: segs[3]}
		switch method {
		case http.MethodGet:
			return opGetDeployment, params, true
		case http.MethodPatch:
			return opUpdateDeployment, params, true
		case http.MethodDelete:
			return opDeleteDeployment, params, true
		}
	case apiGWSegStages:
		params := map[string]string{keyRestAPIID: apiID, keyStageName: segs[3]}
		switch method {
		case http.MethodGet:
			return opGetStage, params, true
		case http.MethodDelete:
			return opDeleteStage, params, true
		case http.MethodPatch:
			return opUpdateStage, params, true
		}
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWRestAPIsDepth4Extended handles authorizers, validators, models, gateway responses, and doc depth-4 paths.
func parseAPIGWRestAPIsDepth4Extended(method string, segs []string, apiID string) (string, map[string]string, bool) {
	if op, params, ok := parseAPIGWRestAPIsDepth4AuthVal(method, segs, apiID); ok {
		return op, params, ok
	}

	return parseAPIGWRestAPIsDepth4ModelGW(method, segs, apiID)
}

// parseAPIGWRestAPIsDepth4AuthVal handles authorizer and validator depth-4 paths.
func parseAPIGWRestAPIsDepth4AuthVal(method string, segs []string, apiID string) (string, map[string]string, bool) {
	switch segs[2] {
	case apiGWSegAuthorizers:
		params := map[string]string{keyRestAPIID: apiID, keyAuthorizerID: segs[3]}
		switch method {
		case http.MethodGet:
			return opGetAuthorizer, params, true
		case http.MethodPatch:
			return opUpdateAuthorizer, params, true
		case http.MethodDelete:
			return opDeleteAuthorizer, params, true
		}
	case apiGWSegValidators:
		params := map[string]string{keyRestAPIID: apiID, keyRequestValidatorID: segs[3]}
		switch method {
		case http.MethodGet:
			return opGetRequestValidator, params, true
		case http.MethodPatch:
			return opUpdateRequestValidator, params, true
		case http.MethodDelete:
			return opDeleteRequestValidator, params, true
		}
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWRestAPIsDepth4ModelGW handles model, gateway response, and documentation depth-4 paths.
func parseAPIGWRestAPIsDepth4ModelGW(method string, segs []string, apiID string) (string, map[string]string, bool) {
	switch segs[2] {
	case apiGWSegModels:
		params := map[string]string{keyRestAPIID: apiID, keyModelName: segs[3]}
		switch method {
		case http.MethodGet:
			return opGetModel, params, true
		case http.MethodDelete:
			return opDeleteModel, params, true
		case http.MethodPatch:
			return opUpdateModel, params, true
		}
	case apiGWSegGatewayResponses:
		params := map[string]string{keyRestAPIID: apiID, keyResponseType: segs[3]}
		switch method {
		case http.MethodGet:
			return opGetGatewayResponse, params, true
		case http.MethodPut:
			return opPutGatewayResponse, params, true
		case http.MethodPatch:
			return opUpdateGatewayResponse, params, true
		case http.MethodDelete:
			return opDeleteGatewayResponse, params, true
		}
	case apiGWSegDocumentation:
		return parseAPIGWRestAPIsDocDepth4(method, segs, apiID)
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWRestAPIsDeepPath handles /restapis/{id}/... paths with n≥5.
func parseAPIGWRestAPIsDeepPath(method string, segs []string, n int, apiID string) (string, map[string]string, bool) {
	if n >= 5 && segs[2] == apiGWSegResources && segs[4] == apiGWSegMethods {
		return parseAPIGWMethodPath(method, segs)
	}

	return parseAPIGWRestAPIsDepth5Plus(method, segs, n, apiID)
}

// parseAPIGWRestAPIsDepth5Plus handles depth-5+ paths (documentation, stage cache, model template).
func parseAPIGWRestAPIsDepth5Plus(method string, segs []string, n int, apiID string) (string, map[string]string, bool) {
	switch segs[2] {
	case apiGWSegStages:
		return parseAPIGWRestAPIsStageDeep(method, segs, n, apiID)
	case apiGWSegDocumentation:
		return parseAPIGWRestAPIsDocDeep(method, segs, n, apiID)
	case apiGWSegModels:
		if n == 5 && segs[4] == "default_template" && method == http.MethodGet {
			return opGetModelTemplate, map[string]string{keyRestAPIID: apiID, keyModelName: segs[3]}, true
		}
	case apiGWSegAuthorizers:
		if n == 5 && segs[4] == "invocations" && method == http.MethodPost {
			return opTestInvokeAuthorizer, map[string]string{keyRestAPIID: apiID, keyAuthorizerID: segs[3]}, true
		}
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWRestAPIsStageDeep handles depth-5+ stage paths (cache flush).
func parseAPIGWRestAPIsStageDeep(method string, segs []string, n int, apiID string) (string, map[string]string, bool) {
	stageParams := map[string]string{keyRestAPIID: apiID, keyStageName: segs[3]}

	if n == 5 && segs[4] == "cache" && method == http.MethodDelete {
		return opFlushStageCache, stageParams, true
	}

	if n == 6 && segs[4] == "cache" && segs[5] == "authorizers" && method == http.MethodDelete {
		return opFlushStageAuthorizersCache, stageParams, true
	}

	// GET /restapis/{id}/stages/{name}/exports/{exportType} → GetExport
	if n == pathDepth6 && segs[4] == apiGWSegExports && method == http.MethodGet {
		stageParams[keyExportType] = segs[5]

		return opGetExport, stageParams, true
	}

	// GET /restapis/{id}/stages/{name}/sdks/{sdkType} → GetSdk
	if n == pathDepth6 && segs[4] == apiGWSegSdks && method == http.MethodGet {
		stageParams[keySdkType] = segs[5]

		return opGetSdk, stageParams, true
	}

	return apiGWUnknownOp, nil, false
}
