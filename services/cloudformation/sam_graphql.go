package cloudformation

import (
	"maps"
	"slices"
	"strconv"
	"strings"
)

const (
	samTypeAppSyncAPI      = "AWS::AppSync::GraphQLApi"
	samTypeAppSyncSchema   = "AWS::AppSync::GraphQLSchema"
	samTypeAppSyncDS       = "AWS::AppSync::DataSource"
	samTypeAppSyncFunction = "AWS::AppSync::FunctionConfiguration"
	samTypeAppSyncResolver = "AWS::AppSync::Resolver"
	samTypeAppSyncKey      = "AWS::AppSync::ApiKey"
	samTypeAppSyncDomain   = "AWS::AppSync::DomainName"
	samTypeAppSyncAssoc    = "AWS::AppSync::DomainNameApiAssociation"
	samTypeAppSyncCache    = "AWS::AppSync::ApiCache"
	samGraphQLTag          = "graphqlapi:createdBy"
	samAuthLambda          = "AWS_LAMBDA"
	samAuthOIDC            = "OPENID_CONNECT"
	samAuthCognito         = "AMAZON_COGNITO_USER_POOLS"
	samKeyDataSource       = "DataSource"
	samKeyRuntime          = "Runtime"
	samKeyInlineCode       = "InlineCode"
	samKeyCodeURI          = "CodeUri"
	samKeyMaxBatch         = "MaxBatchSize"
	samKeyServiceRoleArn   = "ServiceRoleArn"
	samKeyRoleName         = "RoleName"
	samKeyDefinition       = "Definition"
	samGraphQLNone         = "none"

	samAppSyncResolverCode = "\nexport function request(ctx) {\n    return {};\n}\n\n" +
		"export function response(ctx) {\n    return ctx.prev.result;\n}\n"
)

func samGraphQLProps() map[string]bool {
	return keySet(attrNameName, samKeyTags, "XrayEnabled", samAuthKey, "SchemaInline", "SchemaUri", "Logging",
		"DataSources", "Functions", "Resolvers", "ApiKeys", samKeyDomainNameProp, "Cache", "Visibility",
		"OwnerContact", "IntrospectionConfig", "QueryDepthLimit", "ResolverCountLimit")
}

const samKeyDomainNameProp = "DomainName"

// samGraphQL accumulates the state shared by the pieces of one AWS::Serverless::GraphQLApi.
type samGraphQL struct {
	apiID       any
	t           *samTranslator
	attrs       map[string]any
	dataSources map[string]any
	functionIDs map[string]any
	id          string
	noneDS      bool
}

func (g *samGraphQL) put(id, typ string, props map[string]any, deps any) error {
	res := map[string]any{samKeyType: typ, samKeyProps: props}
	maps.Copy(res, g.attrs)
	if deps != nil {
		res[samKeyDependsOn] = deps
	}

	return g.t.put(id, res)
}

func (t *samTranslator) translateGraphQLApi(id string, r map[string]any) error {
	props := t.resProps("GraphQLApi", r)
	if err := rejectUnknown(id, props, samGraphQLProps()); err != nil {
		return err
	}
	g := &samGraphQL{
		t: t, id: id, apiID: samGetAtt(id, samKeyAPIID), dataSources: map[string]any{},
		functionIDs: map[string]any{}, attrs: map[string]any{},
	}
	for _, k := range []string{samKeyCondition, samKeyDelPolicy, samKeyUpdPolicy, "UpdatePolicy", samKeyMetadata} {
		if v, ok := r[k]; ok {
			g.attrs[k] = v
		}
	}
	depends := r[samKeyDependsOn]
	auth := asMap(props[samAuthKey])
	if auth == nil {
		return samErr(id, "Missing required property 'Auth'.")
	}
	api, role, lambdaARNs, err := g.apiResources(props, auth)
	if err != nil {
		return err
	}
	if err = g.put(id, samTypeAppSyncAPI, api, depends); err != nil {
		return err
	}
	if err = g.putSchema(props, depends); err != nil {
		return err
	}
	for i, arn := range lambdaARNs {
		src := map[string]any{
			samKeyType:   samTypeAppSyncAPI,
			attrNameArn:  samRef(id),
			"ResourceId": samGetAtt(id, samKeyAPIID),
		}
		dst := map[string]any{samKeyType: samTypeFunction, attrNameArn: arn}
		if err = g.connect(id+"ToLambdaAuthConnector"+strconv.Itoa(i+1), src, dst, []string{samPermWrite}); err != nil {
			return err
		}
	}
	if role != nil {
		if err = g.put(id+"CloudWatchRole", resTypeIAMRole, role, depends); err != nil {
			return err
		}
	}

	return g.putRest(props, depends)
}

func (g *samGraphQL) putRest(props map[string]any, depends any) error {
	steps := []func(map[string]any, any) error{
		g.putAPIKeys, g.putCache, g.putDataSources, g.putDomain, g.putFunctions, g.putResolvers,
	}
	for _, step := range steps {
		if err := step(props, depends); err != nil {
			return err
		}
	}
	if g.noneDS {
		return g.put(g.id+"NoneDataSource", samTypeAppSyncDS, map[string]any{
			samKeyAPIID: g.apiID, attrNameName: g.id + "NoneDataSource", samKeyType: valueNone,
		}, depends)
	}

	return nil
}

func (g *samGraphQL) apiResources(props, auth map[string]any) (map[string]any, map[string]any, []any, error) {
	api := map[string]any{attrNameName: g.id}
	if n := props[attrNameName]; n != nil {
		api[attrNameName] = n
	}
	for _, k := range []string{"Visibility", "OwnerContact", "XrayEnabled", "IntrospectionConfig", "QueryDepthLimit",
		"ResolverCountLimit"} {
		if v := props[k]; v != nil {
			api[k] = v
		}
	}
	lambdaARNs, err := g.authProperties(api, auth)
	if err != nil {
		return nil, nil, nil, err
	}
	tags := maps.Clone(asMap(props[samKeyTags]))
	if tags == nil {
		tags = map[string]any{}
	}
	tags[samGraphQLTag] = samCreatedBy
	api[samKeyTags] = plainTagList(tags)

	if logging, isBool := props["Logging"].(bool); isBool && !logging {
		return api, nil, lambdaARNs, nil
	}
	logConfig, role, err := g.logging(props["Logging"])
	if err != nil {
		return nil, nil, nil, err
	}
	api["LogConfig"] = logConfig

	return api, role, lambdaARNs, nil
}

func (g *samGraphQL) logging(v any) (map[string]any, map[string]any, error) {
	cfg, isMap := v.(map[string]any)
	if _, isBool := v.(bool); v != nil && !isMap && !isBool {
		return nil, nil, samErr(g.id, "Property 'Logging' should be a map or a boolean.")
	}
	out := map[string]any{}
	if x := cfg["ExcludeVerboseContent"]; x != nil {
		out["ExcludeVerboseContent"] = x
	}
	out["FieldLogLevel"] = "ALL"
	if x := cfg["FieldLogLevel"]; x != nil && x != "" {
		out["FieldLogLevel"] = x
	}
	if x := cfg["CloudWatchLogsRoleArn"]; x != nil {
		out["CloudWatchLogsRoleArn"] = x

		return out, nil, nil
	}
	out["CloudWatchLogsRoleArn"] = samGetAtt(g.id+"CloudWatchRole", attrNameArn)
	role := map[string]any{
		samKeyAssumeRole: servicePrincipalTrust("appsync.amazonaws.com"),
		samKeyManagedARNs: []any{map[string]any{
			samKeyFnSub: "arn:${AWS::Partition}:iam::aws:policy/service-role/AWSAppSyncPushToCloudWatchLogs",
		}},
	}

	return out, role, nil
}

// authProperties writes the primary and additional authentication providers and returns the Lambda authorizer ARNs.
func (g *samGraphQL) authProperties(api, auth map[string]any) ([]any, error) {
	var arns []any
	api["AuthenticationType"] = auth[samKeyType]
	primary := maps.Clone(auth)
	delete(primary, "Additional")
	name, cfg, err := g.authorizerConfig(primary, -1)
	if err != nil {
		return nil, err
	}
	if name != "" {
		api[name] = cfg
		if name == samKeyLambdaAuthCfg {
			arns = append(arns, cfg["AuthorizerUri"])
		}
	}
	var additional []any
	for i, a := range asList(auth["Additional"]) {
		am := asMap(a)
		name, cfg, err = g.authorizerConfig(am, i)
		if err != nil {
			return nil, err
		}
		entry := map[string]any{"AuthenticationType": am[samKeyType]}
		if name != "" {
			entry[name] = cfg
			if name == samKeyLambdaAuthCfg {
				arns = append(arns, cfg["AuthorizerUri"])
			}
		}
		additional = append(additional, entry)
	}
	if len(additional) > 0 {
		api["AdditionalAuthenticationProviders"] = additional
	}

	return arns, nil
}

func (g *samGraphQL) authorizerConfig(auth map[string]any, index int) (string, map[string]any, error) {
	const maxAuthProperties = 2
	keyPath := "Auth"
	if index > 0 {
		keyPath = "Auth.Additional." + strconv.Itoa(index)
	}
	if len(auth) > maxAuthProperties {
		return "", nil, samErr(g.id, "'%s' has more than one authentication configuration defined.", keyPath)
	}
	for _, spec := range []struct{ typ, prop, cfg, msg string }{
		{samAuthLambda, "LambdaAuthorizer", samKeyLambdaAuthCfg,
			"'LambdaAuthorizer' must be defined if type is 'AWS_LAMBDA'."},
		{samAuthOIDC, "OpenIDConnect", "OpenIDConnectConfig", "'OpenIDConnect' must be defined if type is 'OPENID_CONNECT'."},
		{samAuthCognito, "UserPool", "UserPoolConfig",
			"'UserPool' must be defined if type is 'AMAZON_COGNITO_USER_POOLS'."},
	} {
		if auth[samKeyType] != spec.typ {
			continue
		}
		cfg := asMap(auth[spec.prop])
		if cfg == nil {
			return "", nil, samErr(g.id, "%s", spec.msg)
		}
		cfg = maps.Clone(cfg)
		if spec.cfg == "UserPoolConfig" && index >= 0 {
			delete(cfg, "DefaultAction")
		}

		return spec.cfg, cfg, nil
	}

	return "", nil, nil
}

func (g *samGraphQL) putSchema(props map[string]any, depends any) error {
	inline, uri := props["SchemaInline"], props["SchemaUri"]
	switch {
	case inline == nil && uri == nil:
		return samErr(g.id, "One of 'SchemaInline' or 'SchemaUri' must be set.")
	case inline != nil && uri != nil:
		return samErr(g.id, "Both 'SchemaInline' and 'SchemaUri' cannot be defined at the same time.")
	}
	schema := map[string]any{samKeyAPIID: g.apiID}
	if inline != nil {
		schema[samKeyDefinition] = inline
	} else {
		schema["DefinitionS3Location"] = uri
	}

	return g.put(g.id+"Schema", samTypeAppSyncSchema, schema, depends)
}

func (g *samGraphQL) putAPIKeys(props map[string]any, depends any) error {
	keys := asMap(props["ApiKeys"])
	for _, rel := range slices.Sorted(maps.Keys(keys)) {
		k := asMap(keys[rel])
		out := map[string]any{samKeyAPIID: g.apiID}
		for src, dst := range map[string]string{"ApiKeyId": "ApiKeyId", samKeyDesc: samKeyDesc, "ExpiresOn": "Expires"} {
			if v := k[src]; v != nil {
				out[dst] = v
			}
		}
		if err := g.put(g.id+rel, samTypeAppSyncKey, out, depends); err != nil {
			return err
		}
	}

	return nil
}

func (g *samGraphQL) putCache(props map[string]any, depends any) error {
	cache := asMap(props["Cache"])
	if cache == nil {
		return nil
	}
	out := map[string]any{samKeyAPIID: g.apiID}
	cacheKeys := []string{
		"ApiCachingBehavior",
		samKeyType,
		"Ttl",
		"AtRestEncryptionEnabled",
		"TransitEncryptionEnabled",
	}
	for _, k := range cacheKeys {
		if v := cache[k]; v != nil {
			out[k] = v
		}
	}

	return g.put(g.id+"ApiCache", samTypeAppSyncCache, out, depends)
}

func (g *samGraphQL) putDomain(props map[string]any, depends any) error {
	domain := asMap(props[samKeyDomainNameProp])
	if domain == nil {
		return nil
	}
	out := map[string]any{}
	for _, k := range []string{samKeyCertArn, samKeyDomainNameProp, samKeyDesc} {
		if v := domain[k]; v != nil {
			out[k] = v
		}
	}
	id := g.id + "DomainName"
	if err := g.put(id, samTypeAppSyncDomain, out, depends); err != nil {
		return err
	}

	return g.put(g.id+"DomainNameApiAssociation", samTypeAppSyncAssoc,
		map[string]any{samKeyAPIID: g.apiID, samKeyDomainNameProp: samRef(id)}, depends)
}

func (g *samGraphQL) putDataSources(props map[string]any, depends any) error {
	sources := asMap(props["DataSources"])
	if sources == nil {
		return nil
	}
	ddb := asMap(sources["DynamoDb"])
	for _, rel := range slices.Sorted(maps.Keys(ddb)) {
		if err := g.putDynamoDBSource(rel, asMap(ddb[rel]), depends); err != nil {
			return err
		}
	}
	lambdas := asMap(sources["Lambda"])
	for _, rel := range slices.Sorted(maps.Keys(lambdas)) {
		if err := g.putLambdaSource(rel, asMap(lambdas[rel]), depends); err != nil {
			return err
		}
	}

	return nil
}

func (g *samGraphQL) putDynamoDBSource(rel string, ds map[string]any, depends any) error {
	id := g.id + rel + "DynamoDBDataSource"
	region := ds["Region"]
	if region == nil {
		region = samRef("AWS::Region")
	}
	ddbCfg := map[string]any{"AwsRegion": region, samKeyTableName: ds[samKeyTableName]}
	for _, k := range []string{"UseCallerCredentials", "Versioned"} {
		if v := ds[k]; v != nil && v != false {
			ddbCfg[k] = v
		}
	}
	if v := ds["DeltaSync"]; v != nil {
		ddbCfg["DeltaSyncConfig"] = v
	}
	out := map[string]any{samKeyAPIID: g.apiID, attrNameName: orString(ds[attrNameName], rel),
		samKeyType: "AMAZON_DYNAMODB", "DynamoDBConfig": ddbCfg}
	if d := ds[samKeyDesc]; d != nil {
		out[samKeyDesc] = d
	}
	g.dataSources[rel] = samGetAtt(id, attrNameName)
	if role := ds[samKeyServiceRoleArn]; role != nil {
		out[samKeyServiceRoleArn] = role

		return g.put(id, samTypeAppSyncDS, out, depends)
	}
	out[samKeyServiceRoleArn] = samGetAtt(id+"Role", attrNameArn)
	if err := g.put(id, samTypeAppSyncDS, out, depends); err != nil {
		return err
	}
	if err := g.put(id+"Role", resTypeIAMRole,
		map[string]any{samKeyAssumeRole: servicePrincipalTrust("appsync.amazonaws.com")}, depends); err != nil {
		return err
	}
	table := ds["TableArn"]
	if table == nil {
		table = map[string]any{samKeyFnSub: []any{
			"arn:${AWS::Partition}:dynamodb:${__Region__}:${AWS::AccountId}:table/${__TableName__}",
			map[string]any{"__TableName__": ds[samKeyTableName], "__Region__": region},
		}}
	}
	perms := []string{"Read", samPermWrite}
	if l, ok := ds["Permissions"].([]any); ok {
		perms = perms[:0]
		for _, p := range l {
			perms = append(perms, asString(p))
		}
	}
	src := map[string]any{samKeyType: samTypeAppSyncDS, attrNameArn: samGetAtt(id, "DataSourceArn"),
		samKeyRoleName: samRef(id + "Role")}

	return g.connect(
		id+"ToTableConnector",
		src,
		map[string]any{samKeyType: resTypeDynamoDBTable, attrNameArn: table},
		perms,
	)
}

func orString(v any, def string) any {
	if v == nil || v == "" {
		return def
	}

	return v
}

func (g *samGraphQL) putLambdaSource(rel string, ds map[string]any, depends any) error {
	id := g.id + rel + "LambdaDataSource"
	out := map[string]any{samKeyAPIID: g.apiID, attrNameName: orString(ds[attrNameName], rel),
		samKeyType: "AWS_LAMBDA", "LambdaConfig": map[string]any{"LambdaFunctionArn": ds[samKeyFunctionArn]}}
	if d := ds[samKeyDesc]; d != nil {
		out[samKeyDesc] = d
	}
	g.dataSources[rel] = samGetAtt(id, attrNameName)
	if role := ds[samKeyServiceRoleArn]; role != nil {
		out[samKeyServiceRoleArn] = role

		return g.put(id, samTypeAppSyncDS, out, depends)
	}
	out[samKeyServiceRoleArn] = samGetAtt(id+"Role", attrNameArn)
	if err := g.put(id, samTypeAppSyncDS, out, depends); err != nil {
		return err
	}
	if err := g.put(id+"Role", resTypeIAMRole,
		map[string]any{samKeyAssumeRole: servicePrincipalTrust("appsync.amazonaws.com")}, depends); err != nil {
		return err
	}
	src := map[string]any{samKeyType: samTypeAppSyncDS, attrNameArn: samGetAtt(id, "DataSourceArn"),
		samKeyRoleName: samRef(id + "Role")}
	dst := map[string]any{samKeyType: samTypeFunction, attrNameArn: ds[samKeyFunctionArn]}

	return g.connect(id+"ToLambdaConnector", src, dst, []string{samPermWrite})
}

// connect expands a synthetic connector between two typed endpoints into the policies SAM generates.
func (g *samGraphQL) connect(id string, src, dst map[string]any, perms []string) error {
	_, err := g.t.connectOne(id, map[string]any{}, src, dst, perms, connIndex{})

	return err
}

func (g *samGraphQL) dataSourceName(rel string, fn map[string]any) (any, error) {
	ds := fn[samKeyDataSource]
	if ds == nil {
		return nil, samErr(rel, "'DataSource' must be set.")
	}
	name, isStr := ds.(string)
	if !isStr {
		return ds, nil
	}
	if strings.EqualFold(name, samGraphQLNone) {
		g.noneDS = true

		return samGetAtt(g.id+"NoneDataSource", attrNameName), nil
	}
	if v, ok := g.dataSources[name]; ok {
		return v, nil
	}

	return nil, samErr(rel, "Either define DataSource '%s' in 'DataSources' or use intrinsic function like GetAtt, "+
		"ImportValue, Sub or another one to reference a DataSource defined outside of this GraphQLApi resource.", name)
}

func graphQLRuntime(rel string, res map[string]any) (map[string]any, error) {
	rt := asMap(res[samKeyRuntime])
	if rt == nil {
		return nil, samErr(rel, "'Runtime' must be defined as a property in %s.", rel)
	}

	return map[string]any{attrNameName: rt[attrNameName], "RuntimeVersion": rt["Version"]}, nil
}

func graphQLCode(rel string, res map[string]any) (any, any, error) {
	inline, uri := res[samKeyInlineCode], res[samKeyCodeURI]
	if inline != nil && uri != nil {
		return nil, nil, samErr(rel, "Both 'InlineCode' and 'CodeUri' cannot be defined at the same time.")
	}

	return inline, uri, nil
}

func (g *samGraphQL) putFunctions(props map[string]any, depends any) error {
	fns := asMap(props["Functions"])
	for _, rel := range slices.Sorted(maps.Keys(fns)) {
		fn := asMap(fns[rel])
		if fn[samKeyID] != nil {
			if len(fn) != 1 {
				return samErr(rel, "'Id' cannot be defined with other properties in Function.")
			}
			g.functionIDs[rel] = fn[samKeyID]

			continue
		}
		out, err := g.functionConfig(rel, fn)
		if err != nil {
			return err
		}
		fid := g.id + rel
		g.functionIDs[rel] = samGetAtt(fid, "FunctionId")
		if err = g.put(fid, samTypeAppSyncFunction, out, depends); err != nil {
			return err
		}
	}

	return nil
}

func (g *samGraphQL) functionConfig(rel string, fn map[string]any) (map[string]any, error) {
	inline, uri, err := graphQLCode(rel, fn)
	if err != nil {
		return nil, err
	}
	if inline == nil && uri == nil {
		return nil, samErr(rel, "One of 'InlineCode' or 'CodeUri' must be set.")
	}
	ds, err := g.dataSourceName(rel, fn)
	if err != nil {
		return nil, err
	}
	runtime, err := graphQLRuntime(rel, fn)
	if err != nil {
		return nil, err
	}
	out := map[string]any{samKeyAPIID: g.apiID, attrNameName: orString(fn[attrNameName], rel),
		"DataSourceName": ds, samKeyRuntime: runtime}
	if inline != nil {
		out["Code"] = inline
	} else {
		out["CodeS3Location"] = uri
	}
	passthrough := map[string]string{samKeyMaxBatch: samKeyMaxBatch, samKeyDesc: samKeyDesc, "Sync": "SyncConfig"}
	for src, dst := range passthrough {
		if v := fn[src]; v != nil {
			out[dst] = v
		}
	}

	return out, nil
}

func (g *samGraphQL) putResolvers(props map[string]any, _ any) error {
	resolvers := asMap(props["Resolvers"])
	deps := []any{g.id + "Schema"}
	for _, typeName := range slices.Sorted(maps.Keys(resolvers)) {
		byID := asMap(resolvers[typeName])
		for _, rel := range slices.Sorted(maps.Keys(byID)) {
			res := asMap(byID[rel])
			out, err := g.resolver(typeName, rel, res)
			if err != nil {
				return err
			}
			if err = g.put(g.id+typeName+rel, samTypeAppSyncResolver, out, deps); err != nil {
				return err
			}
		}
	}

	return nil
}

func (g *samGraphQL) resolver(typeName, rel string, res map[string]any) (map[string]any, error) {
	inline, uri, err := graphQLCode(rel, res)
	if err != nil {
		return nil, err
	}
	out := map[string]any{samKeyAPIID: g.apiID, "TypeName": typeName, "FieldName": orString(res["FieldName"], rel)}
	if inline == nil && uri == nil {
		out["Code"] = samAppSyncResolverCode
		out[samKeyRuntime] = map[string]any{attrNameName: "APPSYNC_JS", "RuntimeVersion": "1.0.0"}
	} else {
		if inline != nil {
			out["Code"] = inline
		} else {
			out["CodeS3Location"] = uri
		}
		if out[samKeyRuntime], err = graphQLRuntime(rel, res); err != nil {
			return nil, err
		}
	}
	if res["Pipeline"] == nil {
		return nil, samErr(rel, "Resolver '%s' must have Pipeline defined. Unit resolvers are not supported. If you "+
			"need a Unit resolver you can use AppSync resource.", rel)
	}
	out["Kind"] = "PIPELINE"
	var ids []any
	for _, f := range asList(res["Pipeline"]) {
		name, _ := f.(string)
		id, ok := g.functionIDs[name]
		if !ok {
			return nil, samErr(rel, "Function '%s' does not exist.", name)
		}
		ids = append(ids, id)
	}
	out["PipelineConfig"] = map[string]any{"Functions": ids}
	if c := res["Caching"]; c != nil {
		out["CachingConfig"] = c
	}
	if v := res[samKeyMaxBatch]; v != nil {
		out[samKeyMaxBatch] = v
	}

	return out, nil
}
