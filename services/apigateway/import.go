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
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
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

// parseOpenAPI decodes the import body. AWS accepts the API definition as
// either JSON or YAML; since JSON is a syntactic subset of YAML, both forms
// are decoded via a single YAML pass and then normalized to the openAPIDoc
// shape via JSON so struct tags apply uniformly to either input format. A
// non-JSON/YAML or structurally invalid document yields a BadRequestException,
// matching AWS.
func parseOpenAPI(body []byte) (*openAPIDoc, error) {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return nil, fmt.Errorf("%w: import body is empty", ErrInvalidParameter)
	}

	var raw map[string]any
	if yamlErr := yaml.Unmarshal(body, &raw); yamlErr != nil {
		return nil, fmt.Errorf(
			"%w: unable to parse OpenAPI document (expected JSON or YAML): %w", ErrInvalidParameter, yamlErr,
		)
	}

	normalized, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid OpenAPI document: %w", ErrInvalidParameter, err)
	}

	var doc openAPIDoc
	if jsonErr := json.Unmarshal(normalized, &doc); jsonErr != nil {
		return nil, fmt.Errorf("%w: invalid OpenAPI document: %w", ErrInvalidParameter, jsonErr)
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

	b.restApis.Put(&api)
	b.resources.Put(root)

	importModels(b, &api, doc)
	importPaths(b, &api, doc)

	cp := api

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

	api, ok := b.restApis.Get(input.RestAPIID)
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	if mode == importModeOverwrite {
		for _, r := range append([]*Resource{}, b.resourcesByAPI.Get(api.ID)...) {
			b.resources.Delete(resourceKeyFn(r))
		}
		root := &Resource{
			ID:              api.RootResourceID,
			Path:            "/",
			RestAPIID:       api.ID,
			ResourceMethods: make(map[string]*Method),
		}
		b.resources.Put(root)
		for _, m := range append([]*Model{}, b.modelsByAPI.Get(api.ID)...) {
			b.models.Delete(modelKeyFn(m))
		}
	}
	// The API's name/description are updated from the document's info object
	// regardless of mode — "merge" vs "overwrite" governs how the resource/method
	// tree is applied, not whether the top-level RestApi metadata is refreshed.
	if doc.Info.Title != "" {
		api.Name = doc.Info.Title
	}
	if doc.Info.Description != "" {
		api.Description = doc.Info.Description
	}
	if doc.APIKeySourceExt != "" {
		api.APIKeySource = doc.APIKeySourceExt
	}
	if len(doc.BinaryMediaTypes) > 0 {
		api.BinaryMediaTypes = doc.BinaryMediaTypes
	}

	importModels(b, api, doc)
	importPaths(b, api, doc)

	cp := *api

	return &cp, nil
}

// importModels registers the document's named schemas as API Gateway models.
//
// NOTE: the exists check below preserves a pre-existing quirk carried over
// mechanically from the map[string]*apiData days: models are keyed by model
// ID (see modelKeyFn), but name here is the schema's *Name*, not an ID -- so
// this only skips re-creating a model when name happens to equal an existing
// model's ID. This was already the case before the store.Table conversion
// (data.models was ID-keyed but was indexed by name here) and is preserved
// byte-for-byte rather than "fixed" as part of a storage-layer swap -- see the
// identical note on buildModelRef in backend.go.
func importModels(b *InMemoryBackend, api *RestAPI, doc *openAPIDoc) {
	for name, raw := range doc.schemaDefinitions() {
		if b.models.Has(modelKey(api.ID, name)) {
			continue
		}
		schema := string(raw)
		b.models.Put(&Model{
			ID:          randomID(resourceIDLength),
			RestAPIID:   api.ID,
			Name:        name,
			ContentType: contentTypeJSON,
			Schema:      schema,
		})
	}
}

// importPaths walks the document paths, creating the resource tree and methods.
func importPaths(b *InMemoryBackend, api *RestAPI, doc *openAPIDoc) {
	// Sort paths for deterministic resource creation order.
	pathKeys := collections.SortedKeys(doc.Paths)

	for _, path := range pathKeys {
		res := ensureResourcePath(b, api, path)
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
func ensureResourcePath(b *InMemoryBackend, api *RestAPI, path string) *Resource {
	root, _ := b.resources.Get(resourceKey(api.ID, api.RootResourceID))
	if path == "/" || path == "" {
		return root
	}

	current := root
	for seg := range strings.SplitSeq(strings.Trim(path, "/"), "/") {
		if seg == "" {
			continue
		}
		child := findChildResource(b, api.ID, current.ID, seg)
		if child == nil {
			child = &Resource{
				ID:              randomID(resourceIDLength),
				ParentID:        current.ID,
				PathPart:        seg,
				Path:            computePath(current.Path, seg),
				RestAPIID:       api.ID,
				ResourceMethods: make(map[string]*Method),
			}
			b.resources.Put(child)
			b.resourceVersions[api.ID]++
		}
		current = child
	}

	return current
}

// findChildResource returns the existing child of parent with the given path
// part, or nil.
func findChildResource(b *InMemoryBackend, restAPIID, parentID, pathPart string) *Resource {
	for _, r := range b.resourcesByAPI.Get(restAPIID) {
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
