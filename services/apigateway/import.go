package apigateway

// import.go implements real OpenAPI/Swagger import for the ImportRestApi and
// PutRestApi operations. Unlike a canned stub, this parses the supplied spec
// (Swagger 2.0 or OpenAPI 3.0) and materialises the full resource tree,
// methods, integrations (from the x-amazon-apigateway-integration extension),
// method/integration responses and models — matching how real AWS API Gateway
// converts an imported document into the resource/method/integration model.

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	// importModeMerge merges the spec into an existing API (PutRestApi).
	importModeMerge = "merge"
	// importModeOverwrite replaces the existing API definition (PutRestApi).
	importModeOverwrite = "overwrite"
	// importRefDefault is the AWS integration-responses map key for the
	// catch-all response that carries no selection pattern.
	importRefDefault = "default"
)

// PutRestAPIInput is the input for the PutRestApi operation.
type PutRestAPIInput struct {
	RestAPIID      string `json:"restApiId"`
	Mode           string `json:"mode,omitempty"`
	Body           []byte `json:"body,omitempty"`
	FailOnWarnings bool   `json:"failOnWarnings,omitempty"`
}

// openAPIDoc is the subset of an OpenAPI/Swagger document we interpret.
type openAPIDoc struct {
	Info             openAPIInfo                           `json:"info"`
	Paths            map[string]map[string]json.RawMessage `json:"paths"`
	Definitions      map[string]json.RawMessage            `json:"definitions"`
	Components       *openAPIComponents                    `json:"components"`
	Swagger          string                                `json:"swagger"`
	OpenAPI          string                                `json:"openapi"`
	APIKeySourceExt  string                                `json:"x-amazon-apigateway-api-key-source"`
	BinaryMediaTypes []string                              `json:"x-amazon-apigateway-binary-media-types"`
}

type openAPIInfo struct {
	Title       string `json:"title"`
	Description string `json:"description"`
}

type openAPIComponents struct {
	Schemas map[string]json.RawMessage `json:"schemas"`
}

// openAPIOperation is the subset of an OpenAPI operation object we interpret.
type openAPIOperation struct {
	Responses   map[string]openAPIResponse `json:"responses"`
	Integration *openAPIIntegration        `json:"x-amazon-apigateway-integration"`
	RequestBody *openAPIRequestBody        `json:"requestBody"`
	OperationID string                     `json:"operationId"`
	Security    []map[string][]string      `json:"security"`
	Parameters  []openAPIParameter         `json:"parameters"`
}

type openAPIParameter struct {
	Name     string          `json:"name"`
	In       string          `json:"in"`
	Schema   json.RawMessage `json:"schema"`
	Required bool            `json:"required"`
}

type openAPIResponse struct {
	Content     map[string]openAPIMediaTyp `json:"content"`
	Description string                     `json:"description"`
	Schema      json.RawMessage            `json:"schema"`
}

type openAPIMediaTyp struct {
	Schema json.RawMessage `json:"schema"`
}

type openAPIRequestBody struct {
	Content map[string]openAPIMediaTyp `json:"content"`
}

// openAPIIntegration mirrors the x-amazon-apigateway-integration extension.
type openAPIIntegration struct {
	RequestParameters   map[string]string                     `json:"requestParameters"`
	RequestTemplates    map[string]string                     `json:"requestTemplates"`
	Responses           map[string]openAPIIntegrationResponse `json:"responses"`
	Type                string                                `json:"type"`
	HTTPMethod          string                                `json:"httpMethod"`
	URI                 string                                `json:"uri"`
	PassthroughBehavior string                                `json:"passthroughBehavior"`
	ConnectionType      string                                `json:"connectionType"`
	ConnectionID        string                                `json:"connectionId"`
	ContentHandling     string                                `json:"contentHandling"`
	Credentials         string                                `json:"credentials"`
	CacheNamespace      string                                `json:"cacheNamespace"`
	CacheKeyParameters  []string                              `json:"cacheKeyParameters"`
	TimeoutInMillis     int                                   `json:"timeoutInMillis"`
}

type openAPIIntegrationResponse struct {
	ResponseTemplates  map[string]string `json:"responseTemplates"`
	ResponseParameters map[string]string `json:"responseParameters"`
	StatusCode         string            `json:"statusCode"`
	SelectionPattern   string            `json:"selectionPattern"`
	ContentHandling    string            `json:"contentHandling"`
}

// parseOpenAPI decodes the import body. A non-JSON or structurally invalid
// document yields a BadRequestException, matching AWS.
func parseOpenAPI(body []byte) (*openAPIDoc, error) {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return nil, fmt.Errorf("%w: import body is empty", ErrInvalidParameter)
	}
	if trimmed[0] != '{' && trimmed[0] != '[' {
		// AWS accepts YAML too, but the SDK/JSON path is the canonical one and
		// the only one exercised here; reject non-JSON with the AWS error code.
		return nil, fmt.Errorf("%w: unable to parse OpenAPI document (expected JSON)", ErrInvalidParameter)
	}

	var doc openAPIDoc
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("%w: invalid OpenAPI document: %w", ErrInvalidParameter, err)
	}
	if doc.Swagger == "" && doc.OpenAPI == "" {
		return nil, fmt.Errorf("%w: document is not a valid Swagger 2.0 or OpenAPI 3.0 spec", ErrInvalidParameter)
	}
	if doc.Info.Title == "" {
		return nil, fmt.Errorf("%w: info.title is required", ErrInvalidParameter)
	}

	return &doc, nil
}

// schemaDefinitions returns the named schemas regardless of spec version.
func (d *openAPIDoc) schemaDefinitions() map[string]json.RawMessage {
	if len(d.Definitions) > 0 {
		return d.Definitions
	}
	if d.Components != nil {
		return d.Components.Schemas
	}

	return nil
}

// ImportRestAPI creates a brand-new REST API from an OpenAPI/Swagger document,
// materialising resources, methods, integrations, responses and models.
func (b *InMemoryBackend) ImportRestAPI(input ImportRestAPIInput) (*RestAPI, error) {
	doc, err := parseOpenAPI(input.Body)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("ImportRestAPI")
	defer b.mu.Unlock()

	id := randomID(apiIDLength)
	rootID := randomID(resourceIDLength)

	api := RestAPI{
		ID:             id,
		Name:           doc.Info.Title,
		Description:    doc.Info.Description,
		CreatedDate:    unixEpochTime{time.Now()},
		Tags:           initTagsFromInput("apigw.api."+id+".tags", nil),
		RootResourceID: rootID,
		APIKeySource:   doc.APIKeySourceExt,
	}
	if len(doc.BinaryMediaTypes) > 0 {
		api.BinaryMediaTypes = doc.BinaryMediaTypes
	}

	root := &Resource{
		ID:              rootID,
		Path:            "/",
		RestAPIID:       id,
		ResourceMethods: make(map[string]*Method),
	}

	data := &apiData{
		api:                   api,
		resources:             map[string]*Resource{rootID: root},
		deployments:           make(map[string]*Deployment),
		stages:                make(map[string]*Stage),
		authorizers:           make(map[string]*Authorizer),
		requestValidators:     make(map[string]*RequestValidator),
		documentationParts:    make(map[string]*DocumentationPart),
		documentationVersions: make(map[string]*DocumentationVersion),
		models:                make(map[string]*Model),
	}
	b.apis[id] = data

	importModels(data, doc)
	importPaths(data, doc)

	cp := data.api

	return &cp, nil
}

// PutRestAPI imports an OpenAPI/Swagger document into an existing API. mode
// "overwrite" replaces the resource tree; "merge" (default) layers the imported
// paths on top of the existing tree.
func (b *InMemoryBackend) PutRestAPI(input PutRestAPIInput) (*RestAPI, error) {
	doc, err := parseOpenAPI(input.Body)
	if err != nil {
		return nil, err
	}

	mode := input.Mode
	if mode == "" {
		mode = importModeMerge
	}
	if mode != importModeMerge && mode != importModeOverwrite {
		return nil, fmt.Errorf("%w: mode must be 'merge' or 'overwrite'", ErrInvalidParameter)
	}

	b.mu.Lock("PutRestAPI")
	defer b.mu.Unlock()

	data, ok := b.apis[input.RestAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	if mode == importModeOverwrite {
		rootID := data.api.RootResourceID
		root := &Resource{
			ID:              rootID,
			Path:            "/",
			RestAPIID:       data.api.ID,
			ResourceMethods: make(map[string]*Method),
		}
		data.resources = map[string]*Resource{rootID: root}
		data.models = make(map[string]*Model)
		data.api.Name = doc.Info.Title
		data.api.Description = doc.Info.Description
	}
	if doc.APIKeySourceExt != "" {
		data.api.APIKeySource = doc.APIKeySourceExt
	}
	if len(doc.BinaryMediaTypes) > 0 {
		data.api.BinaryMediaTypes = doc.BinaryMediaTypes
	}

	importModels(data, doc)
	importPaths(data, doc)

	cp := data.api

	return &cp, nil
}

// importModels registers the document's named schemas as API Gateway models.
func importModels(data *apiData, doc *openAPIDoc) {
	for name, raw := range doc.schemaDefinitions() {
		if _, exists := data.models[name]; exists {
			continue
		}
		schema := string(raw)
		data.models[name] = &Model{
			ID:          randomID(resourceIDLength),
			RestAPIID:   data.api.ID,
			Name:        name,
			ContentType: contentTypeJSON,
			Schema:      schema,
		}
	}
}

// importPaths walks the document paths, creating the resource tree and methods.
func importPaths(data *apiData, doc *openAPIDoc) {
	// Sort paths for deterministic resource creation order.
	pathKeys := make([]string, 0, len(doc.Paths))
	for p := range doc.Paths {
		pathKeys = append(pathKeys, p)
	}
	sort.Strings(pathKeys)

	for _, path := range pathKeys {
		res := ensureResourcePath(data, path)
		if res == nil {
			continue
		}
		for verb, raw := range doc.Paths[path] {
			httpMethod := strings.ToUpper(verb)
			if !isHTTPVerb(httpMethod) {
				continue
			}
			var op openAPIOperation
			if err := json.Unmarshal(raw, &op); err != nil {
				continue
			}
			importMethod(res, httpMethod, &op)
		}
	}
}

// isHTTPVerb reports whether a path-item key is a routable HTTP method (and not
// e.g. "parameters" or a vendor extension).
func isHTTPVerb(verb string) bool {
	switch verb {
	case "GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "ANY":
		return true
	default:
		return false
	}
}

// ensureResourcePath walks/creates the resource tree for an OpenAPI path,
// returning the leaf resource. Existing resources are reused (merge semantics).
func ensureResourcePath(data *apiData, path string) *Resource {
	root := data.resources[data.api.RootResourceID]
	if path == "/" || path == "" {
		return root
	}

	current := root
	for seg := range strings.SplitSeq(strings.Trim(path, "/"), "/") {
		if seg == "" {
			continue
		}
		child := findChildResource(data, current.ID, seg)
		if child == nil {
			child = &Resource{
				ID:              randomID(resourceIDLength),
				ParentID:        current.ID,
				PathPart:        seg,
				Path:            computePath(current.Path, seg),
				RestAPIID:       data.api.ID,
				ResourceMethods: make(map[string]*Method),
			}
			data.resources[child.ID] = child
		}
		current = child
	}

	return current
}

// findChildResource returns the existing child of parent with the given path
// part, or nil.
func findChildResource(data *apiData, parentID, pathPart string) *Resource {
	for _, r := range data.resources {
		if r.ParentID == parentID && r.PathPart == pathPart {
			return r
		}
	}

	return nil
}

// importMethod creates a method (and its integration/responses) on a resource
// from an OpenAPI operation.
func importMethod(res *Resource, httpMethod string, op *openAPIOperation) {
	method := &Method{
		HTTPMethod:        httpMethod,
		AuthorizationType: "NONE",
		OperationName:     op.OperationID,
		RequestParameters: importRequestParameters(op),
		RequestModels:     importRequestModels(op),
		MethodResponses:   make(map[string]*MethodResponse),
	}
	applyImportedSecurity(method, op)
	importMethodResponses(method, op)

	if op.Integration != nil {
		method.MethodIntegration = importIntegration(op.Integration)
	}

	res.ResourceMethods[httpMethod] = method
}

// importRequestParameters maps OpenAPI path/query/header parameters to the
// method.request.<loc>.<name> form used by API Gateway.
func importRequestParameters(op *openAPIOperation) map[string]bool {
	if len(op.Parameters) == 0 {
		return nil
	}
	out := make(map[string]bool)
	for _, p := range op.Parameters {
		var loc string
		switch p.In {
		case paramLocationPath:
			loc = paramLocationPath
		case "query":
			loc = paramLocationQuery
		case paramLocationHeader:
			loc = paramLocationHeader
		default:
			continue
		}
		out[fmt.Sprintf("method.request.%s.%s", loc, p.Name)] = p.Required
	}
	if len(out) == 0 {
		return nil
	}

	return out
}

// importRequestModels extracts request models from a Swagger body parameter or
// an OAS3 requestBody.
func importRequestModels(op *openAPIOperation) map[string]string {
	out := make(map[string]string)
	if op.RequestBody != nil {
		for ct, mt := range op.RequestBody.Content {
			if name := schemaRefName(mt.Schema); name != "" {
				out[ct] = name
			}
		}
	}
	// Swagger 2.0 carries the request model via a "body" parameter's schema $ref.
	for _, p := range op.Parameters {
		if p.In == "body" {
			if name := schemaRefName(p.Schema); name != "" {
				out[contentTypeJSON] = name
			}
		}
	}
	if len(out) == 0 {
		return nil
	}

	return out
}

// importMethodResponses creates method responses from the operation's responses.
func importMethodResponses(method *Method, op *openAPIOperation) {
	for status, rsp := range op.Responses {
		mr := &MethodResponse{StatusCode: status}
		models := make(map[string]string)
		if name := schemaRefName(rsp.Schema); name != "" {
			models[contentTypeJSON] = name
		}
		for ct, mt := range rsp.Content {
			if name := schemaRefName(mt.Schema); name != "" {
				models[ct] = name
			}
		}
		if len(models) > 0 {
			mr.ResponseModels = models
		}
		method.MethodResponses[status] = mr
	}
}

// importIntegration converts an x-amazon-apigateway-integration extension into
// an Integration with its integration responses.
func importIntegration(xi *openAPIIntegration) *Integration {
	timeout := xi.TimeoutInMillis
	if timeout == 0 {
		timeout = defaultIntegrationTimeoutMs
	}
	integ := &Integration{
		Type:                 strings.ToUpper(xi.Type),
		HTTPMethod:           xi.HTTPMethod,
		URI:                  xi.URI,
		PassthroughBehavior:  xi.PassthroughBehavior,
		ConnectionType:       xi.ConnectionType,
		ConnectionID:         xi.ConnectionID,
		ContentHandling:      xi.ContentHandling,
		Credentials:          xi.Credentials,
		CacheNamespace:       xi.CacheNamespace,
		CacheKeyParameters:   xi.CacheKeyParameters,
		TimeoutInMillis:      timeout,
		RequestParameters:    xi.RequestParameters,
		RequestTemplates:     xi.RequestTemplates,
		IntegrationResponses: make(map[string]*IntegrationResponse),
	}
	for key, ir := range xi.Responses {
		status := ir.StatusCode
		if status == "" {
			status = "200"
		}
		selection := ir.SelectionPattern
		// AWS uses the map key "default" to denote the catch-all response with
		// no selection pattern; named keys become the selection pattern.
		if key != importRefDefault && selection == "" {
			selection = key
		}
		integ.IntegrationResponses[key] = &IntegrationResponse{
			StatusCode:         status,
			SelectionPattern:   selection,
			ContentHandling:    ir.ContentHandling,
			ResponseTemplates:  ir.ResponseTemplates,
			ResponseParameters: ir.ResponseParameters,
		}
	}

	return integ
}

// applyImportedSecurity sets API key requirement / authorizer hints based on the
// operation's security requirement, mirroring how AWS interprets imported specs.
func applyImportedSecurity(method *Method, op *openAPIOperation) {
	for _, req := range op.Security {
		for scheme := range req {
			if scheme == exportKeyAPIKey || strings.Contains(strings.ToLower(scheme), "api_key") {
				method.APIKeyRequired = true
			}
		}
	}
}

// schemaRefName extracts the model name from a JSON schema reference such as
// {"$ref":"#/definitions/Foo"} or {"$ref":"#/components/schemas/Foo"}.
func schemaRefName(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s struct {
		Ref string `json:"$ref"`
	}
	if err := json.Unmarshal(raw, &s); err != nil || s.Ref == "" {
		return ""
	}
	idx := strings.LastIndex(s.Ref, "/")
	if idx < 0 || idx+1 >= len(s.Ref) {
		return ""
	}

	return s.Ref[idx+1:]
}
