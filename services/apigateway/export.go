package apigateway

import (
	"fmt"
	"strings"
)

// Constants for OpenAPI export document construction.
const (
	exportKeyAPIKey      = "api_key"
	exportKeyType        = "type"
	exportKeyDescription = "description"
	exportKeySchema      = "schema"
	exportKeyObject      = "object"
	exportKeyBody        = "body"
)

const (
	paramLocationHeader = "header"
	paramLocationPath   = "path"
	paramLocationQuery  = "querystring"
)

// GetExport generates an OpenAPI 2.0 (Swagger) or OAS 3.0 export of the REST API.
// exportType "oas30" produces OpenAPI 3.0.1; any other value produces Swagger 2.0.
func (b *InMemoryBackend) GetExport(restAPIID, stageName, exportType string) (map[string]any, error) {
	b.mu.RLock("GetExport")
	defer b.mu.RUnlock()

	api, ok := b.restApis.Get(restAPIID)
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	ctx := exportContext{b: b, restAPIID: restAPIID, apiName: api.Name}

	if exportType == "oas30" {
		return buildOAS30Export(ctx, stageName), nil
	}

	return buildSwagger20Export(ctx, stageName), nil
}

// exportContext carries the read-only context buildSwagger20Export/buildOAS30Export
// and their helpers need to look up a REST API's resources/models from the
// backend's flat store.Table collections. It replaces the old *apiData
// parameter that these free functions took before the pkgs/store conversion
// flattened per-API nested maps into backend-level tables keyed by
// "<restAPIID>#<childID>".
type exportContext struct {
	b         *InMemoryBackend
	restAPIID string
	apiName   string
}

// buildSwagger20Export constructs a Swagger 2.0 export document.
func buildSwagger20Export(ctx exportContext, stageName string) map[string]any {
	paths := buildExportPaths(ctx, false)

	secDefs := map[string]any{
		exportKeyAPIKey: map[string]any{
			exportKeyType: "apiKey",
			keyAPIName:    "x-api-key",
			"in":          paramLocationHeader,
		},
	}

	return map[string]any{
		"swagger":             "2.0",
		"info":                map[string]any{"title": ctx.apiName, "version": "1.0"},
		"basePath":            "/" + stageName,
		"paths":               paths,
		"securityDefinitions": secDefs,
	}
}

// buildOAS30Export constructs an OpenAPI 3.0.1 export document.
func buildOAS30Export(ctx exportContext, stageName string) map[string]any {
	paths := buildExportPaths(ctx, true)

	components := map[string]any{
		"securitySchemes": map[string]any{
			exportKeyAPIKey: map[string]any{
				exportKeyType: "apiKey",
				keyAPIName:    "x-api-key",
				"in":          paramLocationHeader,
			},
		},
	}

	// Include model schemas in components.
	models := ctx.b.modelsByAPI.Get(ctx.restAPIID)
	if len(models) > 0 {
		schemas := make(map[string]any, len(models))
		for _, m := range models {
			schemas[m.Name] = map[string]any{
				exportKeyDescription: m.Description,
				exportKeyType:        exportKeyObject,
			}
		}
		components["schemas"] = schemas
	}

	return map[string]any{
		"openapi":    "3.0.1",
		"info":       map[string]any{"title": ctx.apiName, "version": "1.0"},
		"servers":    []map[string]any{{"url": "/" + stageName}},
		"paths":      paths,
		"components": components,
	}
}

// buildExportPaths constructs the paths object for an OpenAPI export.
// oas30=true emits OAS 3.0 operation objects; false emits Swagger 2.0.
func buildExportPaths(ctx exportContext, oas30 bool) map[string]any {
	paths := make(map[string]any)

	for _, res := range ctx.b.resourcesByAPI.Get(ctx.restAPIID) {
		if res.Path == "/" || len(res.ResourceMethods) == 0 {
			continue
		}

		pathItem := make(map[string]any)

		for httpMethod, method := range res.ResourceMethods {
			if method == nil {
				continue
			}

			op := buildExportOperation(ctx, method, oas30)
			pathItem[strings.ToLower(httpMethod)] = op
		}

		if len(pathItem) > 0 {
			paths[res.Path] = pathItem
		}
	}

	return paths
}

// buildExportOperation constructs a single OAS operation object for a method.
func buildExportOperation(ctx exportContext, method *Method, oas30 bool) map[string]any {
	op := make(map[string]any)
	op["responses"] = buildExportResponses(ctx, method, oas30)
	buildExportRequestBody(op, ctx, method, oas30)
	buildExportSecurity(op, method)

	if method.OperationName != "" {
		op["operationId"] = method.OperationName
	}

	if method.MethodIntegration != nil {
		op["x-amazon-apigateway-integration"] = buildExportIntegration(method.MethodIntegration)
	}

	if !oas30 {
		op["produces"] = []string{contentTypeJSON}
	}

	return op
}

// buildExportResponses constructs the responses map for an OAS operation.
func buildExportResponses(ctx exportContext, method *Method, oas30 bool) map[string]any {
	responses := make(map[string]any)

	for statusCode, mr := range method.MethodResponses {
		rsp := map[string]any{exportKeyDescription: statusCode + " response"}

		if len(mr.ResponseModels) > 0 {
			if oas30 {
				content := make(map[string]any)
				for ct, modelName := range mr.ResponseModels {
					content[ct] = map[string]any{exportKeySchema: buildModelRef(ctx, modelName, oas30)}
				}
				rsp["content"] = content
			} else {
				for _, modelName := range mr.ResponseModels {
					rsp[exportKeySchema] = buildModelRef(ctx, modelName, oas30)

					break
				}
			}
		}

		responses[statusCode] = rsp
	}

	if len(responses) == 0 {
		responses["200"] = map[string]any{exportKeyDescription: "200 response"}
	}

	return responses
}

// buildExportRequestBody adds request body / request model entries to an operation map.
func buildExportRequestBody(op map[string]any, ctx exportContext, method *Method, oas30 bool) {
	if len(method.RequestModels) == 0 {
		return
	}

	if oas30 {
		content := make(map[string]any)
		for ct, modelName := range method.RequestModels {
			content[ct] = map[string]any{exportKeySchema: buildModelRef(ctx, modelName, oas30)}
		}
		op["requestBody"] = map[string]any{"content": content}
	} else {
		for _, modelName := range method.RequestModels {
			op["consumes"] = []string{contentTypeJSON}
			op["parameters"] = []map[string]any{
				{
					"in":            exportKeyBody,
					"name":          exportKeyBody,
					exportKeySchema: buildModelRef(ctx, modelName, oas30),
				},
			}

			break
		}
	}
}

// buildExportSecurity adds the security requirement to an operation when API key or authorizer is configured.
func buildExportSecurity(op map[string]any, method *Method) {
	if method.AuthorizerID != "" {
		scheme := "lambda_authorizer"
		if method.AuthorizationType == AuthTypeCognitoUserPool {
			scheme = "cognito"
		}
		op["security"] = []map[string]any{{scheme: []string{}}}

		return
	}

	if method.APIKeyRequired {
		op["security"] = []map[string]any{{exportKeyAPIKey: []string{}}}
	}
}

// buildExportIntegration constructs the x-amazon-apigateway-integration extension.
func buildExportIntegration(integ *Integration) map[string]any {
	xInteg := map[string]any{
		exportKeyType:         integ.Type,
		"httpMethod":          integ.HTTPMethod,
		"uri":                 integ.URI,
		"passthroughBehavior": integ.PassthroughBehavior,
	}

	if len(integ.RequestTemplates) > 0 {
		xInteg["requestTemplates"] = integ.RequestTemplates
	}

	if len(integ.IntegrationResponses) > 0 {
		xResponses := make(map[string]any, len(integ.IntegrationResponses))
		for sc, ir := range integ.IntegrationResponses {
			xIR := map[string]any{"statusCode": ir.StatusCode}
			if ir.SelectionPattern != "" {
				xIR["selectionPattern"] = ir.SelectionPattern
			}

			if len(ir.ResponseTemplates) > 0 {
				xIR["responseTemplates"] = ir.ResponseTemplates
			}

			if ir.ContentHandling != "" {
				xIR["contentHandling"] = ir.ContentHandling
			}

			xResponses[sc] = xIR
		}

		xInteg["responses"] = xResponses
	}

	return xInteg
}

// buildModelRef returns a schema reference or inline schema for a model name.
//
// NOTE: this preserves a pre-existing quirk carried over mechanically from the
// map[string]*apiData days: the backend's models table is keyed by model ID
// (see modelKeyFn), but modelName here is the model's *Name*, not its ID -- so
// this lookup only succeeds when a model's Name happens to equal its ID. This
// was already the case before the store.Table conversion (the old
// data.models[modelName] indexed an ID-keyed map by name) and is preserved
// byte-for-byte rather than "fixed" as part of a storage-layer swap.
func buildModelRef(ctx exportContext, modelName string, oas30 bool) map[string]any {
	m, ok := ctx.b.models.Get(modelKey(ctx.restAPIID, modelName))
	if !ok {
		return map[string]any{exportKeyType: exportKeyObject}
	}

	if m.Schema != "" {
		if oas30 {
			return map[string]any{"$ref": "#/components/schemas/" + modelName}
		}

		return map[string]any{"$ref": "#/definitions/" + modelName}
	}

	return map[string]any{exportKeyType: exportKeyObject, exportKeyDescription: m.Description}
}
