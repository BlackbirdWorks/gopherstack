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
	Message string `json:"message"`
}

// executeGraphQL parses and executes the GraphQL query.
func executeGraphQL(
	ctx context.Context,
	backend *InMemoryBackend,
	schema *Schema,
	resolvers map[string]*Resolver,
	datasources map[string]*DataSource,
	query, operationName string,
	variables map[string]any,
) (map[string]any, error) {
	if schema == nil || schema.SDL == "" {
		return nil, ErrNoSchema
	}

	// Parse schema.
	gqlSchema, gqlErr := gqlparser.LoadSchema(&ast.Source{
		Name:  "schema.graphql",
		Input: schema.SDL,
	})
	if gqlErr != nil {
		return nil, fmt.Errorf("invalid schema: %w", gqlErr)
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

	result, err := executeSelectionSet(ctx, backend, resolvers, datasources, parentTypeName, op.SelectionSet, variables)
	if err != nil {
		return nil, err
	}

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

// executeSelectionSet resolves all fields in a selection set.
func executeSelectionSet(
	ctx context.Context,
	backend *InMemoryBackend,
	resolvers map[string]*Resolver,
	datasources map[string]*DataSource,
	parentTypeName string,
	selectionSet ast.SelectionSet,
	variables map[string]any,
) (map[string]any, error) {
	result := make(map[string]any)

	for _, sel := range selectionSet {
		field, ok := sel.(*ast.Field)
		if !ok {
			continue
		}

		// Build argument map for this field.
		fieldArgs := extractArguments(field, variables)

		// Look up the resolver for this field.
		key := resolverKey(parentTypeName, field.Name)
		resolver := resolvers[key]

		if resolver == nil {
			result[field.Alias] = nil

			continue
		}

		val, err := resolveField(ctx, backend, resolver, datasources, fieldArgs)
		if err != nil {
			return nil, fmt.Errorf("error resolving %s.%s: %w", parentTypeName, field.Name, err)
		}

		result[field.Alias] = val
	}

	return result, nil
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

// resolveField executes a single field resolver.
func resolveField(
	ctx context.Context,
	backend *InMemoryBackend,
	resolver *Resolver,
	datasources map[string]*DataSource,
	args map[string]any,
) (any, error) {
	ds := datasources[resolver.DataSourceName]
	if ds == nil {
		return nil, fmt.Errorf("%w: %q", ErrDataSourceNotFound, resolver.DataSourceName)
	}

	switch ds.Type {
	case DataSourceTypeLambda:
		return invokeLambdaResolver(ctx, backend, resolver, ds, args)
	case DataSourceTypeDynamoDB:
		return invokeDynamoDBResolver(ctx, backend, resolver, ds, args)
	case DataSourceTypeNone:
		return invokeNoneResolver(resolver, args)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedDataSource, ds.Type)
	}
}

// invokeLambdaResolver invokes the configured Lambda function with the request payload.
func invokeLambdaResolver(
	ctx context.Context,
	backend *InMemoryBackend,
	resolver *Resolver,
	ds *DataSource,
	args map[string]any,
) (any, error) {
	if backend.lambdaFn == nil {
		return nil, ErrLambdaNotConfigured
	}

	if ds.LambdaConfig == nil {
		return nil, ErrLambdaMissingConfig
	}

	// Build the AppSync Lambda event payload.
	payload, err := buildLambdaPayload(resolver, args)
	if err != nil {
		return nil, err
	}

	result, _, err := backend.lambdaFn.InvokeFunction(
		ctx,
		ds.LambdaConfig.LambdaFunctionARN,
		"RequestResponse",
		payload,
	)
	if err != nil {
		return nil, fmt.Errorf("lambda invocation failed: %w", err)
	}

	// Apply response mapping template.
	var lambdaResult any

	if jsonErr := json.Unmarshal(result, &lambdaResult); jsonErr != nil {
		lambdaResult = string(result)
	}

	if resolver.ResponseMappingTemplate != "" {
		rendered, vtlErr := renderVTL(resolver.ResponseMappingTemplate, args, lambdaResult)
		if vtlErr != nil {
			return nil, vtlErr
		}

		var out any
		if jsonErr := json.Unmarshal([]byte(rendered), &out); jsonErr == nil {
			return out, nil
		}

		return rendered, nil
	}

	return lambdaResult, nil
}

// buildLambdaPayload constructs the AppSync Lambda invocation payload.
func buildLambdaPayload(resolver *Resolver, args map[string]any) ([]byte, error) {
	if resolver.RequestMappingTemplate == "" {
		// Default: pass through arguments directly.
		event := map[string]any{
			"field":     resolver.FieldName,
			"typeName":  resolver.TypeName,
			"arguments": args,
		}

		return json.Marshal(event)
	}

	// Evaluate request mapping template.
	rendered, err := renderVTL(resolver.RequestMappingTemplate, args, nil)
	if err != nil {
		return nil, err
	}

	return []byte(rendered), nil
}

// invokeDynamoDBResolver executes the request template against DynamoDB.
func invokeDynamoDBResolver(
	ctx context.Context,
	backend *InMemoryBackend,
	resolver *Resolver,
	ds *DataSource,
	args map[string]any,
) (any, error) {
	if backend.ddbBackend == nil {
		return nil, ErrDynamoDBNotConfigured
	}

	if ds.DynamoDBConfig == nil {
		return nil, ErrDynamoDBMissingConfig
	}

	// Apply request mapping template to build the DynamoDB operation.
	var request map[string]any
	if resolver.RequestMappingTemplate != "" {
		rendered, err := renderVTL(resolver.RequestMappingTemplate, args, nil)
		if err != nil {
			return nil, err
		}

		if jsonErr := json.Unmarshal([]byte(rendered), &request); jsonErr != nil {
			return nil, fmt.Errorf("request mapping template did not produce valid JSON: %w", jsonErr)
		}
	} else {
		request = map[string]any{"operation": "GetItem", "key": args}
	}

	operation, _ := request["operation"].(string)

	var result any

	var err error

	switch operation {
	case "GetItem":
		key, _ := request["key"].(map[string]any)
		result, err = backend.ddbBackend.GetItemRaw(ctx, ds.DynamoDBConfig.TableName, key)
	case "PutItem":
		item, _ := request["key"].(map[string]any)
		err = backend.ddbBackend.PutItemRaw(ctx, ds.DynamoDBConfig.TableName, item)

		if err == nil {
			result = item
		}
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedDynamoDBOp, operation)
	}

	if err != nil {
		return nil, err
	}

	// Apply response mapping template.
	if resolver.ResponseMappingTemplate == "" {
		return result, nil
	}

	rendered, vtlErr := renderVTL(resolver.ResponseMappingTemplate, args, result)
	if vtlErr != nil {
		return nil, vtlErr
	}

	var out any
	if jsonErr := json.Unmarshal([]byte(rendered), &out); jsonErr == nil {
		return out, nil
	}

	return rendered, nil
}

// invokeNoneResolver executes a NONE data source using the mapping templates.
func invokeNoneResolver(resolver *Resolver, args map[string]any) (any, error) {
	if resolver.ResponseMappingTemplate == "" {
		return args, nil
	}

	// For NONE type, apply the response template with the request result as context.
	var reqResult any

	if resolver.RequestMappingTemplate != "" {
		rendered, err := renderVTL(resolver.RequestMappingTemplate, args, nil)
		if err != nil {
			return nil, err
		}

		if jsonErr := json.Unmarshal([]byte(rendered), &reqResult); jsonErr != nil {
			reqResult = rendered
		}
	} else {
		reqResult = args
	}

	rendered, err := renderVTL(resolver.ResponseMappingTemplate, args, reqResult)
	if err != nil {
		return nil, err
	}

	var out any
	if jsonErr := json.Unmarshal([]byte(rendered), &out); jsonErr == nil {
		return out, nil
	}

	return rendered, nil
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
