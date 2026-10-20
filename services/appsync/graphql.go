package appsync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/validator/rules"
)

var (
	// ErrNoSchema is returned when no schema is defined for an API.
	ErrNoSchema = errors.New("no schema defined for this API")
	// ErrQueryParse is returned when the GraphQL query cannot be parsed.
	ErrQueryParse = errors.New("query parse error")
	// ErrOperationNotFound is returned when the named operation is not found.
	ErrOperationNotFound = errors.New("operation not found")
	// ErrDataSourceNotFound is returned when a data source is not found.
	ErrDataSourceNotFound = errors.New("data source not found")
	// ErrFunctionNotFound is returned when a PIPELINE resolver's PipelineConfig
	// references a function ID that doesn't exist.
	ErrFunctionNotFound = errors.New("function not found")
	// ErrUnsupportedDataSource is returned for unsupported data source types.
	ErrUnsupportedDataSource = errors.New("unsupported data source type")
	// ErrUnsupportedDynamoDBOp is returned for unsupported DynamoDB operations.
	ErrUnsupportedDynamoDBOp = errors.New("unsupported DynamoDB operation")
	// ErrLambdaNotConfigured is returned when no lambda invoker is set.
	ErrLambdaNotConfigured = errors.New("lambda invoker not configured")
	// ErrLambdaMissingConfig is returned when a lambda data source has no config.
	ErrLambdaMissingConfig = errors.New("lambda data source missing lambdaConfig")
	// ErrDynamoDBNotConfigured is returned when no dynamodb backend is set.
	ErrDynamoDBNotConfigured = errors.New("dynamodb backend not configured")
	// ErrDynamoDBMissingConfig is returned when a dynamodb data source has no config.
	ErrDynamoDBMissingConfig = errors.New("dynamodb data source missing dynamodbConfig")
)

// graphqlRequest is the standard GraphQL HTTP request body.
type graphqlRequest struct {
	Variables     map[string]any `json:"variables"`
	Query         string         `json:"query"`
	OperationName string         `json:"operationName"`
}

// graphqlResponse is the standard GraphQL HTTP response body.
type graphqlResponse struct {
	Data   map[string]any `json:"data"`
	Errors []graphqlError `json:"errors,omitempty"`
}

type graphqlError struct {
	Data      any    `json:"data,omitempty"`
	ErrorInfo any    `json:"errorInfo,omitempty"`
	Message   string `json:"message"`
	ErrorType string `json:"errorType,omitempty"`
	Path      []any  `json:"path,omitempty"`
}

// partialResultError carries the resolver errors of an otherwise executed operation:
// the partial data comes back alongside it.
type partialResultError struct{ errs []graphqlError }

func (f *partialResultError) Error() string { return f.errs[0].Message }

// newGraphQLError maps an execution error to the GraphQL error object;
// DynamoDB failures carry errorType "DynamoDB:<code>" as AppSync reports them.
func newGraphQLError(err error) graphqlError {
	if re, ok := errors.AsType[*resolverError](err); ok {
		return re.graphql()
	}

	if de, ok := errors.AsType[*DynamoDBError](err); ok {
		return graphqlError{Message: de.Error(), ErrorType: "DynamoDB:" + de.Code}
	}

	return graphqlError{Message: err.Error()}
}

// executeGraphQL parses and executes the GraphQL query.
func executeGraphQL(
	ctx context.Context,
	apiID string,
	backend *InMemoryBackend,
	schema *Schema,
	resolvers map[string]*Resolver,
	datasources map[string]*DataSource,
	functions map[string]*Function,
	query, operationName string,
	variables map[string]any,
	headers map[string]any,
) (map[string]any, error) {
	if schema == nil || schema.SDL == "" {
		return nil, ErrNoSchema
	}

	// Use cached parsed schema; re-parse only if the cache is missing.
	gqlSchema := schema.parsedSchema
	if gqlSchema == nil {
		var schemaErr error
		gqlSchema, schemaErr = loadAppSyncSchema(schema.SDL)
		if schemaErr != nil {
			return nil, fmt.Errorf("invalid schema: %w", schemaErr)
		}
	}

	// Parse query document.
	doc, listErr := gqlparser.LoadQueryWithRules(gqlSchema, query, (*rules.Rules)(nil))
	if listErr != nil {
		msgs := make([]string, 0, len(listErr))
		for _, e := range listErr {
			msgs = append(msgs, e.Message)
		}

		return nil, fmt.Errorf("%w: %s", ErrQueryParse, strings.Join(msgs, "; "))
	}

	// Find the operation to execute.
	op := findOperation(doc, operationName)
	if op == nil {
		return nil, fmt.Errorf("%w: %q", ErrOperationNotFound, operationName)
	}

	// Determine the parent type name based on operation type.
	parentTypeName := operationTypeName(op.Operation)

	env := &execEnv{
		backend: backend, resolvers: resolvers, datasources: datasources,
		functions: functions, variables: variables, headers: headers,
	}
	env.addHeader, _ = respHeaderKey.Get(ctx)

	result, err := env.executeSelectionSet(ctx, parentTypeName, op.SelectionSet)
	if err != nil {
		return nil, err
	}

	if len(env.errs) > 0 {
		return result, &partialResultError{errs: env.errs}
	}

	backend.publishMutation(apiID, op, result)

	return result, nil
}

// operationTypeName maps the operation type to the GraphQL type name.
func operationTypeName(op ast.Operation) string {
	switch op {
	case ast.Mutation:
		return "Mutation"
	case ast.Subscription:
		return "Subscription"
	default:
		return "Query"
	}
}

// findOperation locates the operation to execute.
func findOperation(doc *ast.QueryDocument, operationName string) *ast.OperationDefinition {
	if operationName == "" && len(doc.Operations) == 1 {
		return doc.Operations[0]
	}

	for _, op := range doc.Operations {
		if op.Name == operationName {
			return op
		}
	}

	return nil
}

// execEnv carries the per-request state shared by root and nested resolvers.
type execEnv struct {
	backend     *InMemoryBackend
	resolvers   map[string]*Resolver
	datasources map[string]*DataSource
	functions   map[string]*Function
	variables   map[string]any
	headers     map[string]any
	addHeader   func(name, value string)
	errs        []graphqlError
}

// executeSelectionSet resolves all root fields in a selection set.
func (e *execEnv) executeSelectionSet(
	ctx context.Context,
	parentTypeName string,
	selectionSet ast.SelectionSet,
) (map[string]any, error) {
	result := make(map[string]any)

	for _, sel := range selectionSet {
		field, ok := sel.(*ast.Field)
		if !ok {
			continue
		}

		if e.resolvers[resolverKey(parentTypeName, field.Name)] == nil {
			result[field.Alias] = nil

			continue
		}

		val, err := e.resolveFieldValue(ctx, parentTypeName, field, nil)
		if err != nil {
			return nil, err
		}

		result[field.Alias] = val
	}

	return result, nil
}

// resolveFieldValue runs the resolver for parentTypeName.field (with source
// as $ctx.source for non-root fields) and projects the result by the field's
// selection set, resolving nested resolvers on the way.
func (e *execEnv) resolveFieldValue(
	ctx context.Context,
	parentTypeName string,
	field *ast.Field,
	source map[string]any,
) (any, error) {
	resolver := e.resolvers[resolverKey(parentTypeName, field.Name)]
	cc := e.newCallCtx(parentTypeName, field, source == nil)

	val, err := resolveField(
		ctx, e.backend, resolver, e.datasources, e.functions, extractArguments(field, e.variables), source, cc,
	)
	if err != nil {
		if re, ok := errors.AsType[*resolverError](err); ok {
			ge := re.graphql()
			if source == nil {
				ge.Path = []any{field.Alias}
			}

			e.errs = append(e.errs, ge)

			return noValue(), nil
		}

		return nil, fmt.Errorf("error resolving %s.%s: %w", parentTypeName, field.Name, err)
	}

	return e.project(ctx, val, field.SelectionSet)
}

func (e *execEnv) newCallCtx(parentTypeName string, field *ast.Field, root bool) *callCtx {
	vars := e.variables
	if vars == nil {
		vars = map[string]any{}
	}

	list := selectionSetList(field.SelectionSet, "")
	items := make([]any, len(list))

	for i, n := range list {
		items[i] = n
	}

	cc := &callCtx{
		info: map[string]any{
			"fieldName": field.Name, "parentTypeName": parentTypeName, "variables": vars,
			"selectionSetList": items, "selectionSetGraphQL": selectionSetGraphQL(field.SelectionSet),
		},
		request:    map[string]any{keyHeaders: e.headers},
		headerSink: e.addHeader,
		sink: func(ge graphqlError) {
			if root {
				ge.Path = []any{field.Alias}
			}

			e.errs = append(e.errs, ge)
		},
	}

	return cc
}

func selectionSetList(set ast.SelectionSet, prefix string) []string {
	var out []string

	for _, sel := range set {
		f, ok := sel.(*ast.Field)
		if !ok {
			continue
		}

		out = append(out, prefix+f.Alias)
		out = append(out, selectionSetList(f.SelectionSet, prefix+f.Alias+"/")...)
	}

	return out
}

func selectionSetGraphQL(set ast.SelectionSet) string {
	if len(set) == 0 {
		return ""
	}

	parts := make([]string, 0, len(set))

	for _, sel := range set {
		if f, ok := sel.(*ast.Field); ok {
			parts = append(parts, f.Alias+" "+selectionSetGraphQL(f.SelectionSet))
		}
	}

	return "{ " + strings.TrimSpace(strings.Join(parts, " ")) + " }"
}

// extractArguments builds the arguments map for a field.
func extractArguments(field *ast.Field, variables map[string]any) map[string]any {
	args := make(map[string]any)

	for _, arg := range field.Arguments {
		args[arg.Name] = resolveValue(arg.Value, variables)
	}

	return args
}

// resolveValue evaluates a GraphQL value node.
func resolveValue(val *ast.Value, variables map[string]any) any {
	if val == nil {
		return nil
	}

	switch val.Kind {
	case ast.Variable:
		if variables != nil {
			return variables[val.Raw]
		}

		return nil
	case ast.NullValue:
		return nil
	case ast.BooleanValue:
		return val.Raw == "true"
	case ast.IntValue:
		var i float64
		_ = json.Unmarshal([]byte(val.Raw), &i)

		return i
	case ast.FloatValue:
		var f float64
		_ = json.Unmarshal([]byte(val.Raw), &f)

		return f
	case ast.StringValue, ast.BlockValue, ast.EnumValue:
		return val.Raw
	case ast.ListValue:
		list := make([]any, 0, len(val.Children))
		for _, child := range val.Children {
			list = append(list, resolveValue(child.Value, variables))
		}

		return list
	case ast.ObjectValue:
		obj := make(map[string]any)
		for _, child := range val.Children {
			obj[child.Name] = resolveValue(child.Value, variables)
		}

		return obj
	default:
		return val.Raw
	}
}

// mappingUnit is the request/response-mapping configuration shared by a
// UNIT-kind Resolver and a pipeline Function: real AppSync gives both the
// identical RequestMappingTemplate/ResponseMappingTemplate/Code shape (a
// Function is a resolver's data-source step factored out for pipeline
// reuse). FieldName/TypeName are resolver-only -- used to build the default
// AppSync Lambda event when no request mapping is configured -- and left
// empty for pipeline functions, which carry no field/type identity of their
// own and so fall back to a smaller default (see buildLambdaEvent).
type mappingUnit struct {
	Source           any
	Call             *callCtx
	RequestTemplate  string
	ResponseTemplate string
	Code             string
	FieldName        string
	TypeName         string
	PipelineAfter    bool
}

func sourceValue(m map[string]any) any {
	if m == nil {
		return nil
	}

	return m
}

func resolverMappingUnit(r *Resolver, source map[string]any) mappingUnit {
	return mappingUnit{
		Source:           sourceValue(source),
		RequestTemplate:  r.RequestMappingTemplate,
		ResponseTemplate: r.ResponseMappingTemplate,
		Code:             r.Code,
		FieldName:        r.FieldName,
		TypeName:         r.TypeName,
	}
}

func functionMappingUnit(f *Function, source any) mappingUnit {
	return mappingUnit{
		Source:           source,
		RequestTemplate:  f.RequestMappingTemplate,
		ResponseTemplate: f.ResponseMappingTemplate,
		Code:             f.Code,
	}
}

// unmarshalOrRaw JSON-decodes s, returning it as the raw string when it
// isn't valid JSON (a Lambda payload, for instance, need not be a JSON
// object -- callers that require one check the decoded type themselves).
func unmarshalOrRaw(s string) any {
	var v any
	if err := json.Unmarshal([]byte(s), &v); err == nil {
		return v
	}

	return s
}

// resolveField executes a single field resolver -- a UNIT resolver (one
// data source, request/response mapping) or a PIPELINE resolver (a chain of
// Functions, each its own data-source step).
func resolveField(
	ctx context.Context,
	backend *InMemoryBackend,
	resolver *Resolver,
	datasources map[string]*DataSource,
	functions map[string]*Function,
	args map[string]any,
	source map[string]any,
	cc *callCtx,
) (any, error) {
	mu := resolverMappingUnit(resolver, source)
	mu.Call = cc

	if resolver.Kind == resolverKindPipeline {
		return executePipeline(ctx, backend, resolver, datasources, functions, args, source, cc)
	}

	return executeDataSourceStep(ctx, backend, mu, resolver.DataSourceName, datasources, args, nil)
}

// executePipeline runs a PIPELINE resolver: the before mapping (its output is
// the first function's ctx.prev.result), each Function's data-source step in
// order, then the after mapping over the last function's result. An early
// return in the before mapping skips the functions.
func executePipeline(
	ctx context.Context,
	backend *InMemoryBackend,
	resolver *Resolver,
	datasources map[string]*DataSource,
	functions map[string]*Function,
	args map[string]any,
	source map[string]any,
	cc *callCtx,
) (any, error) {
	unit := mappingUnit{
		RequestTemplate: resolver.RequestMappingTemplate, ResponseTemplate: resolver.ResponseMappingTemplate,
		Code: resolver.Code, Source: sourceValue(source), Call: cc,
		FieldName: resolver.FieldName, TypeName: resolver.TypeName,
	}

	prevResult, _, err := evalRequestMapping(unit, args, nil)
	if err != nil {
		early, isEarly := evalEarly(err)
		if !isEarly {
			return nil, err
		}

		unit.PipelineAfter = true

		return evalResponseMapping(unit, args, early)
	}

	for _, funcID := range resolver.PipelineConfig {
		fn := functions[funcID]
		if fn == nil {
			return nil, fmt.Errorf("%w: %q", ErrFunctionNotFound, funcID)
		}

		fmu := functionMappingUnit(fn, sourceValue(source))
		fmu.Call = cc

		result, stepErr := executeDataSourceStep(ctx, backend, fmu, fn.DataSourceName, datasources, args, prevResult)
		if stepErr != nil {
			if _, isRes := errors.AsType[*resolverError](stepErr); isRes {
				return nil, stepErr
			}

			return nil, fmt.Errorf("pipeline function %q: %w", fn.Name, stepErr)
		}

		prevResult = result
	}

	unit.PipelineAfter = true

	return evalResponseMapping(unit, args, prevResult)
}

// executeDataSourceStep builds mu's request mapping, invokes dsName's data
// source, and applies mu's response mapping -- the single-data-source unit
// shared by a UNIT resolver and each function in a PIPELINE resolver's
// chain. An early return in the request mapping skips the data source and
// response mapping.
func executeDataSourceStep(
	ctx context.Context,
	backend *InMemoryBackend,
	mu mappingUnit,
	dsName string,
	datasources map[string]*DataSource,
	args map[string]any,
	prevResult any,
) (any, error) {
	ds := datasources[dsName]
	if ds == nil {
		return nil, fmt.Errorf("%w: %q", ErrDataSourceNotFound, dsName)
	}

	var (
		out any
		err error
	)

	switch ds.Type {
	case DataSourceTypeLambda:
		out, err = invokeLambdaDataSource(ctx, backend, mu, ds, args, prevResult)
	case DataSourceTypeDynamoDB:
		out, err = invokeDynamoDBDataSource(ctx, backend, mu, ds, args, prevResult)
	case DataSourceTypeNone:
		out, err = invokeNoneDataSource(mu, args, prevResult)
	case DataSourceTypeHTTP:
		out, err = invokeHTTPDataSource(ctx, mu, ds, args, prevResult)
	case DataSourceTypeRelational, DataSourceTypeOpenSearch:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedDataSource, ds.Type)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedDataSource, ds.Type)
	}

	if early, ok := evalEarly(err); ok {
		return early, nil
	}

	return out, err
}

// invokeLambdaDataSource invokes the configured Lambda function with mu's
// mapped request as payload.
func invokeLambdaDataSource(
	ctx context.Context,
	backend *InMemoryBackend,
	mu mappingUnit,
	ds *DataSource,
	args map[string]any,
	prevResult any,
) (any, error) {
	if backend.lambdaFn == nil {
		return nil, ErrLambdaNotConfigured
	}

	if ds.LambdaConfig == nil {
		return nil, ErrLambdaMissingConfig
	}

	payload, err := buildLambdaEvent(mu, args, prevResult)
	if err != nil {
		return nil, err
	}

	result, _, err := backend.lambdaFn.InvokeFunction(
		withRegion(ctx, backend.region),
		ds.LambdaConfig.LambdaFunctionARN,
		"RequestResponse",
		payload,
	)
	if err != nil {
		return nil, fmt.Errorf("lambda invocation failed: %w", err)
	}

	var lambdaResult any

	if jsonErr := json.Unmarshal(result, &lambdaResult); jsonErr != nil {
		lambdaResult = string(result)
	}

	return evalResponseMapping(mu, args, lambdaResult)
}

// buildLambdaEvent constructs the AppSync Lambda invocation payload from
// mu's request mapping, falling back to the real default AppSync Lambda
// event shape ({field, typeName, arguments}) when no mapping is configured.
// That default only includes field/typeName for a UNIT resolver
// (mu.FieldName set); a pipeline function has no field/type identity of its
// own, so its default degrades to just {arguments}.
func buildLambdaEvent(mu mappingUnit, args map[string]any, prevResult any) ([]byte, error) {
	value, ok, err := evalRequestMapping(mu, args, prevResult)
	if err != nil {
		return nil, err
	}

	if ok {
		if m, isMap := value.(map[string]any); isMap && m["operation"] == "Invoke" {
			if payload, has := m["payload"]; has {
				value = payload
			}
		}

		return json.Marshal(value)
	}

	event := map[string]any{jsCtxKeyArguments: args}
	if mu.Source != nil {
		event[jsCtxKeySource] = mu.Source
	}

	if mu.FieldName != "" {
		event["field"] = mu.FieldName
		event["typeName"] = mu.TypeName
	}

	return json.Marshal(event)
}

// invokeDynamoDBDataSource executes mu's mapped request against DynamoDB.
func invokeDynamoDBDataSource(
	ctx context.Context,
	backend *InMemoryBackend,
	mu mappingUnit,
	ds *DataSource,
	args map[string]any,
	prevResult any,
) (any, error) {
	if backend.ddbBackend == nil {
		return nil, ErrDynamoDBNotConfigured
	}

	if ds.DynamoDBConfig == nil {
		return nil, ErrDynamoDBMissingConfig
	}

	value, ok, err := evalRequestMapping(mu, args, prevResult)
	if err != nil {
		return nil, err
	}

	var request map[string]any

	if ok {
		if request, ok = value.(map[string]any); !ok {
			return nil, fmt.Errorf("%w: request mapping did not produce a JSON object", ErrValidation)
		}
	} else {
		request = map[string]any{"operation": ddbOpGetItem, "key": args}
	}

	ctx = withRegion(ctx, firstNonEmpty(ds.DynamoDBConfig.AWSRegion, backend.region))

	result, err := executeDDBRequest(ctx, backend.ddbBackend, ds.DynamoDBConfig.TableName, request)
	if err != nil {
		return nil, err
	}

	return evalResponseMapping(mu, args, result)
}

// invokeNoneDataSource executes a NONE data source: mu's request mapping
// output becomes the "result" mu's response mapping (if any) sees. Matches
// the real API's use of NONE data sources for local/pass-through resolvers.
func invokeNoneDataSource(mu mappingUnit, args map[string]any, prevResult any) (any, error) {
	reqResult, ok, err := evalRequestMapping(mu, args, prevResult)
	if err != nil {
		return nil, err
	}

	if !ok {
		reqResult = args
	}

	if m, isMap := reqResult.(map[string]any); isMap {
		if payload, has := m["payload"]; has {
			reqResult = payload
		}
	}

	return evalResponseMapping(mu, args, reqResult)
}

// parseGraphQLRequest parses the GraphQL request body.
func parseGraphQLRequest(body []byte) (*graphqlRequest, error) {
	var req graphqlRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()

	if err := dec.Decode(&req); err != nil {
		return nil, fmt.Errorf("invalid GraphQL request: %w", err)
	}

	return &req, nil
}

// ExecuteGraphQL executes a GraphQL query/mutation against the configured resolvers.
func (b *InMemoryBackend) ExecuteGraphQL(
	ctx context.Context,
	apiID, query, operationName string,
	variables map[string]any,
	auth GraphQLAuth,
) (map[string]any, error) {
	b.mu.RLock("ExecuteGraphQL")

	api, apiOK := b.apis.Get(apiID)
	schema, _ := b.schemas.Get(apiID)

	// Copy resolver and datasource maps under the lock to avoid data races with
	// concurrent Create/Delete operations.
	apiResolvers := b.resolversByAPI.Get(apiID)
	resolversCopy := make(map[string]*Resolver, len(apiResolvers))

	for _, r := range apiResolvers {
		resolversCopy[resolverKey(r.TypeName, r.FieldName)] = r
	}

	apiDatasources := b.datasourcesByAPI.Get(apiID)
	datasourcesCopy := make(map[string]*DataSource, len(apiDatasources))

	for _, ds := range apiDatasources {
		datasourcesCopy[ds.Name] = ds
	}

	apiFunctions := b.functionsByAPI.Get(apiID)
	functionsCopy := make(map[string]*Function, len(apiFunctions))

	for _, fn := range apiFunctions {
		functionsCopy[fn.FunctionID] = fn
	}

	// Snapshot the auth-relevant fields under the lock: UpdateGraphqlAPI
	// mutates *GraphqlAPI in place under b.mu, so api itself is not safe to
	// read once RUnlock below returns.
	var (
		primaryAuth         graphQLAuthConfig
		additionalAuthProvs []AdditionalAuthenticationProvider
	)

	if apiOK {
		primaryAuth = graphQLAuthConfigFromAPI(api)
		additionalAuthProvs = api.AdditionalAuthenticationProviders
	}

	b.mu.RUnlock()

	if !apiOK {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if authErr := b.authorizeGraphQL(
		ctx, apiID, primaryAuth, additionalAuthProvs, query, operationName, variables, auth,
	); authErr != nil {
		return nil, authErr
	}

	return executeGraphQL(
		ctx, apiID, b, schema, resolversCopy, datasourcesCopy, functionsCopy, query, operationName, variables,
		requestHeaders(auth),
	)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}

	return ""
}

func requestHeaders(auth GraphQLAuth) map[string]any {
	out := map[string]any{}

	if auth.Request == nil {
		return out
	}

	for k, v := range auth.Request.Header {
		if len(v) > 0 {
			out[strings.ToLower(k)] = v[0]
		}
	}

	return out
}
