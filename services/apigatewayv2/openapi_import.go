package apigatewayv2

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	basepathIgnore  = "ignore"
	basepathPrepend = "prepend"
	basepathSplit   = "split"

	oaAnyMethod    = "x-amazon-apigateway-any-method"
	oaAuthTypeIAM  = "awsSigv4"
	authTypeNone   = "NONE"
	authTypeIAM    = "AWS_IAM"
	authTypeCustom = "CUSTOM"
)

// openAPISpec is the subset of an OpenAPI 3 / Swagger 2 document an HTTP API import reads.
type openAPISpec struct {
	Paths      map[string]map[string]json.RawMessage `json:"paths"`
	Components struct {
		SecuritySchemes map[string]openAPISecurityScheme `json:"securitySchemes"`
	} `json:"components"`
	Cors     *openAPICors    `json:"x-amazon-apigateway-cors"`
	Info     openAPIInfo     `json:"info"`
	BasePath string          `json:"basePath"`
	Servers  []openAPIServer `json:"servers"`
	Tags     []openAPITag    `json:"tags"`
}

type openAPIInfo struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Version     string `json:"version"`
}

type openAPIServer struct {
	Endpoint *struct {
		DisableExecuteAPIEndpoint *bool `json:"disableExecuteApiEndpoint"`
	} `json:"x-amazon-apigateway-endpoint-configuration"`
	URL string `json:"url"`
}

type openAPITag struct {
	Name  string `json:"name"`
	Value string `json:"x-amazon-apigateway-tag-value"`
}

type openAPICors struct {
	AllowOrigins     []string `json:"allowOrigins"`
	AllowMethods     []string `json:"allowMethods"`
	AllowHeaders     []string `json:"allowHeaders"`
	ExposeHeaders    []string `json:"exposeHeaders"`
	MaxAge           int32    `json:"maxAge"`
	AllowCredentials bool     `json:"allowCredentials"`
}

type openAPISecurityScheme struct {
	Authorizer *openAPIAuthorizer `json:"x-amazon-apigateway-authorizer"`
	Type       string             `json:"type"`
	AuthType   string             `json:"x-amazon-apigateway-authtype"`
}

type openAPIAuthorizer struct {
	JwtConfiguration *JwtConfiguration `json:"jwtConfiguration"`
	TTL              *int32            `json:"authorizerResultTtlInSeconds"`
	IdentitySource   json.RawMessage   `json:"identitySource"`
	Type             string            `json:"type"`
	URI              string            `json:"authorizerUri"`
	Credentials      string            `json:"authorizerCredentials"`
	PayloadFormat    json.RawMessage   `json:"authorizerPayloadFormatVersion"`
	SimpleResponses  bool              `json:"enableSimpleResponses"`
}

type openAPIOperation struct {
	Integration    *openAPIIntegration   `json:"x-amazon-apigateway-integration"`
	OperationID    string                `json:"operationId"`
	Security       []map[string][]string `json:"security"`
	IsDefaultRoute bool                  `json:"isDefaultRoute"`
}

// openAPIIntegration models the x-amazon-apigateway-integration extension.
type openAPIIntegration struct {
	RequestParameters    map[string]string `json:"requestParameters"`
	Type                 string            `json:"type"`
	HTTPMethod           string            `json:"httpMethod"`
	URI                  string            `json:"uri"`
	ConnectionType       string            `json:"connectionType"`
	ConnectionID         string            `json:"connectionId"`
	Credentials          string            `json:"credentials"`
	Description          string            `json:"description"`
	PayloadFormatVersion json.RawMessage   `json:"payloadFormatVersion"`
	TimeoutInMillis      int32             `json:"timeoutInMillis"`
}

// ErrOpenAPIInvalid is returned for an unparsable OpenAPI document.
var ErrOpenAPIInvalid = errors.New("invalid OpenAPI document")

// parseOpenAPISpec decodes a JSON or YAML OpenAPI body, tolerating minimal/partial documents.
func parseOpenAPISpec(body string) (*openAPISpec, error) {
	var spec openAPISpec
	if strings.TrimSpace(body) == "" {
		return &spec, nil
	}

	if !strings.HasPrefix(strings.TrimSpace(body), "{") {
		var doc any
		if err := yaml.Unmarshal([]byte(body), &doc); err != nil {
			return nil, err
		}

		raw, err := json.Marshal(doc)
		if err != nil {
			return nil, err
		}

		body = string(raw)
	}

	if err := json.Unmarshal([]byte(body), &spec); err != nil {
		return nil, err
	}

	return &spec, nil
}

func (s *openAPISpec) operation(path, method string) (openAPIOperation, bool) {
	raw := s.Paths[path][method]

	var op openAPIOperation

	if len(raw) == 0 || raw[0] != '{' || json.Unmarshal(raw, &op) != nil {
		return openAPIOperation{}, false
	}

	return op, true
}

func (s *openAPISpec) sortedPaths() []string {
	paths := make([]string, 0, len(s.Paths))
	for p := range s.Paths {
		paths = append(paths, p)
	}

	sort.Strings(paths)

	return paths
}

// importWarnings lists the documented import warnings: HTTP APIs support only Lambda proxy and HTTP proxy
// integrations (docs: Use OpenAPI definitions for HTTP APIs).
func importWarnings(spec *openAPISpec) []string {
	var warnings []string

	for path, methods := range spec.Paths {
		for method := range methods {
			op, ok := spec.operation(path, method)
			if !ok || op.Integration == nil {
				continue
			}

			if t := strings.ToLower(op.Integration.Type); t != "aws_proxy" && t != "http_proxy" {
				warnings = append(warnings, fmt.Sprintf(
					"%s %s: integration type %q is not supported for HTTP APIs",
					strings.ToUpper(method), path, op.Integration.Type,
				))
			}
		}
	}

	sort.Strings(warnings)

	return warnings
}

// splitBasePath drops the top-most segment of a base path ("/a/b/c" -> "/b/c"), per the
// split option in docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-import-api-basePath.html.
func splitBasePath(basePath string) string {
	trimmed := strings.Trim(basePath, "/")

	_, rest, found := strings.Cut(trimmed, "/")
	if !found {
		return ""
	}

	return "/" + rest
}

// specBasePath returns the document's declared base path: Swagger 2's basePath, or the path of
// OpenAPI 3's first servers[] entry.
func specBasePath(spec *openAPISpec) string {
	if spec.BasePath != "" {
		return spec.BasePath
	}

	if len(spec.Servers) == 0 {
		return ""
	}

	u, err := url.Parse(spec.Servers[0].URL)
	if err != nil {
		return ""
	}

	return u.Path
}

// OpenAPIImportWarnings lists the warnings an import of body would report (FailOnWarnings rejects them).
func OpenAPIImportWarnings(body string) ([]string, error) {
	spec, err := parseOpenAPISpec(body)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrOpenAPIInvalid, err)
	}

	return importWarnings(spec), nil
}

// ApplyOpenAPI imports an OpenAPI document into an existing HTTP API: API metadata, CORS, tags,
// authorizers, and a route (plus integration and authorization) per path and method.
func ApplyOpenAPI(b StorageBackend, apiID, body, basepath string) error {
	spec, err := parseOpenAPISpec(body)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrOpenAPIInvalid, err)
	}

	return applyOpenAPISpec(b, apiID, spec, basepath)
}

func applyOpenAPISpec(b StorageBackend, apiID string, spec *openAPISpec, basepath string) error {
	if err := applyOpenAPIMetadata(b, apiID, spec); err != nil {
		return err
	}

	authorizers, err := importAuthorizers(b, apiID, spec)
	if err != nil {
		return err
	}

	prefix := ""

	switch basepath {
	case basepathPrepend:
		prefix = strings.TrimSuffix(specBasePath(spec), "/")
	case basepathSplit:
		prefix = splitBasePath(specBasePath(spec))
	}

	for _, path := range spec.sortedPaths() {
		methods := slices.Sorted(mapKeys(spec.Paths[path]))
		for _, method := range methods {
			op, ok := spec.operation(path, method)
			if !ok {
				continue
			}

			importOperation(b, apiID, prefix, path, method, op, authorizers)
		}
	}

	return nil
}

func mapKeys[V any](m map[string]V) func(yield func(string) bool) {
	return func(yield func(string) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}

func applyOpenAPIMetadata(b StorageBackend, apiID string, spec *openAPISpec) error {
	update := UpdateAPIInput{
		Name:        spec.Info.Title,
		Description: spec.Info.Description,
		Version:     spec.Info.Version,
	}

	if spec.Cors != nil {
		update.CorsConfiguration = &CorsConfiguration{
			AllowOrigins: spec.Cors.AllowOrigins, AllowMethods: spec.Cors.AllowMethods,
			AllowHeaders: spec.Cors.AllowHeaders, ExposeHeaders: spec.Cors.ExposeHeaders,
			MaxAge: spec.Cors.MaxAge, AllowCredentials: spec.Cors.AllowCredentials,
		}
	}

	if len(spec.Tags) > 0 {
		update.Tags = map[string]string{}

		for _, t := range spec.Tags {
			update.Tags[t.Name] = t.Value
		}
	}

	for _, s := range spec.Servers {
		if s.Endpoint != nil && s.Endpoint.DisableExecuteAPIEndpoint != nil {
			update.DisableExecuteAPIEndpoint = s.Endpoint.DisableExecuteAPIEndpoint
		}
	}

	if _, err := b.UpdateAPI(apiID, update); err != nil {
		return err
	}

	return nil
}

// importedAuthorizer is what a route needs to reference a security scheme.
type importedAuthorizer struct {
	id       string
	authType string
}

func importAuthorizers(b StorageBackend, apiID string, spec *openAPISpec) (map[string]importedAuthorizer, error) {
	out := map[string]importedAuthorizer{}

	names := slices.Sorted(mapKeys(spec.Components.SecuritySchemes))
	for _, name := range names {
		scheme := spec.Components.SecuritySchemes[name]

		if scheme.AuthType == oaAuthTypeIAM {
			out[name] = importedAuthorizer{authType: authTypeIAM}

			continue
		}

		if scheme.Authorizer == nil {
			continue
		}

		in := authorizerInput(name, scheme.Authorizer)

		a, err := b.CreateAuthorizer(apiID, in)
		if err != nil {
			return nil, err
		}

		kind := authTypeCustom
		if in.AuthorizerType == authorizerTypeJWT {
			kind = authorizerTypeJWT
		}

		out[name] = importedAuthorizer{id: a.AuthorizerID, authType: kind}
	}

	return out, nil
}

func authorizerInput(name string, az *openAPIAuthorizer) CreateAuthorizerInput {
	in := CreateAuthorizerInput{Name: name, AuthorizerType: "REQUEST", AuthorizerURI: az.URI,
		AuthorizerCredentialsArn: az.Credentials, EnableSimpleResponses: az.SimpleResponses}
	if strings.EqualFold(az.Type, "jwt") {
		in.AuthorizerType = authorizerTypeJWT
		in.JwtConfiguration = az.JwtConfiguration
	}

	if az.TTL != nil {
		in.AuthorizerResultTTLInSeconds = *az.TTL
	}

	in.AuthorizerPayloadFormatVersion = rawScalar(az.PayloadFormat)
	in.IdentitySource = identitySources(az.IdentitySource)

	return in
}

func rawScalar(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}

	var f float64
	if json.Unmarshal(raw, &f) == nil {
		return fmt.Sprintf("%.1f", f)
	}

	return ""
}

func identitySources(raw json.RawMessage) []string {
	var list []string
	if json.Unmarshal(raw, &list) == nil {
		return list
	}

	var one string
	if json.Unmarshal(raw, &one) == nil && one != "" {
		return strings.Split(one, ",")
	}

	return nil
}

// importOperation creates the route (and integration and authorization, when defined) for one
// path+method entry. Route keys the backend rejects (e.g. path-level "parameters") are skipped.
func importOperation(
	b StorageBackend, apiID, prefix, path, method string, op openAPIOperation, auths map[string]importedAuthorizer,
) {
	routeKey := openAPIRouteKey(prefix, path, method, op)
	if err := validateHTTPRouteKey(routeKey); err != nil {
		return
	}

	in := CreateRouteInput{RouteKey: routeKey, OperationName: op.OperationID}

	if op.Integration != nil {
		integ, err := b.CreateIntegration(apiID, integrationInput(op.Integration))
		if err == nil && integ != nil {
			in.Target = "integrations/" + integ.IntegrationID
		}
	}

	applyOperationSecurity(&in, op, auths)

	_, _ = b.CreateRoute(apiID, in)
}

func openAPIRouteKey(prefix, path, method string, op openAPIOperation) string {
	if op.IsDefaultRoute {
		return routeKeyDefault
	}

	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	verb := strings.ToUpper(method)
	if method == oaAnyMethod {
		verb = "ANY"
	}

	return verb + " " + prefix + path
}

func integrationInput(i *openAPIIntegration) CreateIntegrationInput {
	return CreateIntegrationInput{
		IntegrationType:      openAPIIntegrationType(i.Type),
		IntegrationMethod:    i.HTTPMethod,
		IntegrationURI:       i.URI,
		PayloadFormatVersion: rawScalar(i.PayloadFormatVersion),
		ConnectionType:       i.ConnectionType,
		ConnectionID:         i.ConnectionID,
		CredentialsArn:       i.Credentials,
		RequestParameters:    i.RequestParameters,
		Description:          i.Description,
		TimeoutInMillis:      i.TimeoutInMillis,
	}
}

func applyOperationSecurity(in *CreateRouteInput, op openAPIOperation, auths map[string]importedAuthorizer) {
	for _, req := range op.Security {
		for name, scopes := range req {
			if name == authTypeNone {
				in.AuthorizationType = authTypeNone

				return
			}

			a, ok := auths[name]
			if !ok {
				continue
			}

			in.AuthorizationType, in.AuthorizerID, in.AuthorizationScopes = a.authType, a.id, scopes

			return
		}
	}
}

// openAPIIntegrationType maps an x-amazon-apigateway-integration "type" to the API Gateway v2 IntegrationType.
func openAPIIntegrationType(t string) string {
	switch t {
	case "http", "http_proxy":
		return integrationTypeHTTPProxy
	case "aws", "aws_proxy":
		return IntegrationTypeAWSProxy
	case "mock":
		return integrationTypeMock
	case "":
		return IntegrationTypeAWSProxy
	default:
		return strings.ToUpper(t)
	}
}
