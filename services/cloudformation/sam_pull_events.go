package cloudformation

import (
	"maps"
	"slices"
	"strings"
)

const (
	samSQSExecRole    = "arn:${AWS::Partition}:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole"
	samKinesisExec    = "arn:${AWS::Partition}:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole"
	samDynamoDBExec   = "arn:${AWS::Partition}:iam::aws:policy/service-role/AWSLambdaDynamoDBExecutionRole"
	samMSKExec        = "arn:${AWS::Partition}:iam::aws:policy/service-role/AWSLambdaMSKExecutionRole"
	samSecretAction   = "secretsmanager:GetSecretValue"
	samKMSDecrypt     = "kms:Decrypt"
	samKeySAC         = "SourceAccessConfigurations"
	samKeySecretKMS   = "SecretsManagerKmsKeyId"
	samKeySchemaReg   = "SchemaRegistryConfig"
	samKeyBootstrap   = "KafkaBootstrapServers"
	samKeyConsumerGrp = "ConsumerGroupId"
	samKeyTopics      = "Topics"
	samKeyBasicAuth   = "BASIC_AUTH"
	samKeyURI2        = "URI"
)

// samPullBaseProps are accepted by every pull event type, like SAM's PullEventSource.property_types.
func samPullBaseProps() []string {
	return []string{samKeyBatchSize, samKStartingPosition, "StartingPositionTimestamp", samKeyEnabled,
		"MaximumBatchingWindowInSeconds", samKMaximumRetryAttempts, "BisectBatchOnFunctionError",
		"MaximumRecordAgeInSeconds", "DestinationConfig", "ParallelizationFactor", samKeyTopics, samKQueues,
		samKeySAC, samKeySecretKMS, "TumblingWindowInSeconds", "FunctionResponseTypes", samKeyBootstrap,
		"FilterCriteria", "KmsKeyArn", samKeyConsumerGrp, "ScalingConfig", "ProvisionedPollerConfig",
		samKeySchemaReg, "MetricsConfig", "LoggingConfig", samKConsumptionMode}
}

// samPullSource is the source-specific part of a pull event type.
type samPullSource struct {
	srcKey   string
	required []string
	extra    []string
}

func samPullSources() map[string]samPullSource {
	return map[string]samPullSource{
		samEventSQS:      {srcKey: samKQueue, required: []string{samKQueue}},
		samEventKinesis:  {srcKey: samKStream, required: []string{samKStream, samKStartingPosition}},
		samEventDynamoDB: {srcKey: samKStream, required: []string{samKStream, samKStartingPosition}},
		samEventMSK:      {srcKey: samKStream, required: []string{samKStream, samKStartingPosition}},
		samEventMQ:       {srcKey: "Broker", required: []string{"Broker"}, extra: []string{"DynamicPolicyName"}},
		samEventKafka:    {},
		samEventDocDB: {srcKey: "Cluster", required: []string{"Cluster", samKeyDatabaseName},
			extra: []string{"CollectionName", "FullDocument", samKeyDatabaseName}},
	}
}

// pullEvent expands a Kinesis/DynamoDB/SQS/MSK/MQ/SelfManagedKafka/DocumentDB event into an EventSourceMapping
// plus the execution role policies the source needs.
func (t *samTranslator) pullEvent(ev *samEventCtx, name, kind string, props map[string]any) error {
	src := samPullSources()[kind]
	id := ev.id + name
	allowed := slices.Concat(samPullBaseProps(), []string{src.srcKey}, src.required, src.extra)
	if err := rejectUnknown(id, props, keySet(allowed...)); err != nil {
		return err
	}
	for _, k := range src.required {
		if props[k] == nil {
			return samErr(ev.id, "Event [%s] is missing required property '%s'.", name, k)
		}
	}
	if err := validatePullProps(ev.id, name, kind, props); err != nil {
		return err
	}
	esm := map[string]any{samKeyFnName: ev.fnRef()}
	if src.srcKey != "" {
		esm["EventSourceArn"] = props[src.srcKey]
	}
	for _, k := range slices.Sorted(maps.Keys(props)) {
		if !samPullDirect()[k] {
			continue
		}
		if props[k] != nil {
			esm[k] = props[k]
		}
	}
	if err := addPullSourceConfigs(ev.id, name, kind, props, esm); err != nil {
		return err
	}
	destPolicies, err := pullDestination(ev.id, name, props, esm)
	if err != nil {
		return err
	}
	if err = t.pullPolicies(ev, name, kind, props); err != nil {
		return err
	}
	for _, p := range destPolicies {
		ev.role.addInline(p)
	}
	res := map[string]any{samKeyType: samTypeESM, samKeyProps: esm}

	return t.put(id, res)
}

// samPullDirect are the properties copied to the EventSourceMapping under their own names.
func samPullDirect() map[string]bool {
	return keySet(samKeyBatchSize, samKStartingPosition, "StartingPositionTimestamp", samKeyEnabled,
		"MaximumBatchingWindowInSeconds", samKMaximumRetryAttempts, "BisectBatchOnFunctionError",
		"MaximumRecordAgeInSeconds", "ParallelizationFactor", samKeyTopics, samKQueues, samKeySAC,
		"TumblingWindowInSeconds", "FunctionResponseTypes", "FilterCriteria", "KmsKeyArn", "ScalingConfig",
		"ProvisionedPollerConfig", "MetricsConfig", "LoggingConfig")
}

func validatePullProps(fnID, name, kind string, props map[string]any) error {
	if fc := asMap(props["FilterCriteria"]); fc != nil && !isIntrinsicValue(fc) {
		if !containsString(samFilterKinds(), kind) {
			return samErr(fnID, "Event [%s] FilterCriteria is only available for DocumentDB, DynamoDB, Kinesis, MQ, "+
				"MSK, SelfManagedKafka, SQS events.", name)
		}
		for k := range fc {
			if k != "Filters" {
				return samErr(fnID, "Event [%s] FilterCriteria field has a wrong format", name)
			}
		}
	}
	if props[samKConsumptionMode] != nil && kind != samEventKafka {
		return samErr(fnID, "Event [%s] Property ConsumptionMode not defined for resource of type %s.", name, kind)
	}
	if props[samKeyConsumerGrp] != nil && kind != samEventMSK && kind != samEventKafka {
		return samErr(fnID, "Event [%s] Property ConsumerGroupId not defined for resource of type %s.", name, kind)
	}

	return nil
}

func samFilterKinds() []string {
	return []string{samEventDocDB, samEventDynamoDB, samEventKinesis, samEventMQ, samEventMSK, samEventKafka,
		samEventSQS}
}

// addPullSourceConfigs fills the source-specific ESM blocks (self-managed endpoints, consumer group,
// schema registry, consumption mode, DocumentDB config).
func addPullSourceConfigs(fnID, name, kind string, props, esm map[string]any) error {
	if servers := props[samKeyBootstrap]; servers != nil && asList(servers) != nil {
		esm["SelfManagedEventSource"] = map[string]any{"Endpoints": map[string]any{samKeyBootstrap: servers}}
	}
	switch kind {
	case samEventMSK:
		return addKafkaConfig(fnID, name, "AmazonManagedKafkaEventSourceConfig", props, esm)
	case samEventKafka:
		return addKafkaConfig(fnID, name, "SelfManagedKafkaEventSourceConfig", props, esm)
	case samEventDocDB:
		cfg := map[string]any{samKeyDatabaseName: props[samKeyDatabaseName]}
		for _, k := range []string{"CollectionName", "FullDocument"} {
			if props[k] != nil {
				cfg[k] = props[k]
			}
		}
		esm["DocumentDBEventSourceConfig"] = cfg
	}

	return nil
}

func addKafkaConfig(fnID, name, cfgKey string, props, esm map[string]any) error {
	cfg := map[string]any{}
	for _, k := range []string{samKeyConsumerGrp, samKConsumptionMode} {
		if v := props[k]; v != nil {
			cfg[k] = v
		}
	}
	if reg := props[samKeySchemaReg]; reg != nil {
		if err := validateSchemaRegistry(fnID, name, reg); err != nil {
			return err
		}
		cfg[samKeySchemaReg] = reg
	}
	if len(cfg) > 0 {
		esm[cfgKey] = cfg
	}

	return nil
}

func validateSchemaRegistry(fnID, name string, reg any) error {
	m := asMap(reg)
	if m == nil {
		return samErr(fnID, "Event [%s] SchemaRegistryConfig must be a map.", name)
	}
	for _, k := range []string{"SchemaRegistryURI", "EventRecordFormat", "SchemaValidationConfigs"} {
		if _, has := m[k]; !has {
			return samErr(fnID, "Event [%s] Missing required field %s in SchemaRegistryConfig", name, k)
		}
	}
	if f := m["EventRecordFormat"]; f != "JSON" && f != "SOURCE" {
		return samErr(
			fnID,
			"Event [%s] EventRecordFormat in SchemaRegistryConfig must be either 'JSON' or 'SOURCE'",
			name,
		)
	}
	if _, ok := m["SchemaValidationConfigs"].([]any); !ok {
		return samErr(fnID, "Event [%s] SchemaValidationConfigs must be a list", name)
	}

	return nil
}

// pullDestination copies DestinationConfig (dropping SAM's Type) and returns the policies its Type implies.
func pullDestination(fnID, name string, props, esm map[string]any) ([]map[string]any, error) {
	dc := asMap(props["DestinationConfig"])
	if dc == nil {
		return nil, nil
	}
	onFailure := asMap(dc["OnFailure"])
	if onFailure == nil {
		return nil, samErr(fnID, "Event [%s] DestinationConfig.OnFailure must be a map.", name)
	}
	out := maps.Clone(onFailure)
	typ, _ := onFailure[samKeyType].(string)
	delete(out, samKeyType)
	esm["DestinationConfig"] = map[string]any{"OnFailure": out}
	dest := onFailure[samKeyDestination]
	logicalID := fnID + name
	switch typ {
	case "":
		return nil, nil
	case samEventSQS, samEventSNS:
		return []map[string]any{samDestinationPolicy(typ, logicalID, dest)}, nil
	case "S3":
		return []map[string]any{samDestinationPolicy("S3Bucket", logicalID, dest)}, nil
	case "Kafka":
		return nil, nil
	}

	return nil, samErr(fnID, "Event [%s] The only valid values for 'Type' are 'SQS', 'SNS', 'S3', and 'Kafka'", name)
}

// pullPolicies adds the managed policy or inline policies a pull source needs on the function role.
func (t *samTranslator) pullPolicies(ev *samEventCtx, name, kind string, props map[string]any) error {
	id := ev.id + name
	switch kind {
	case samEventSQS:
		ev.role.addManaged(samSQSExecRole)
	case samEventKinesis:
		ev.role.addManaged(samKinesisExec)
	case samEventDynamoDB:
		ev.role.addManaged(samDynamoDBExec)
	case samEventMSK:
		ev.role.addManaged(samMSKExec)
		if statements := mskStatements(props); len(statements) > 0 {
			ev.role.addInline(map[string]any{samKeyPolicyName: "MSKExecutionRolePolicy",
				samKeyPolicyDoc: map[string]any{samKeyStatement: statements}})
		}
	case samEventMQ:
		return mqPolicy(ev, id, props)
	case samEventKafka:
		return kafkaPolicy(ev, id, props)
	case samEventDocDB:
		return docDBPolicy(ev, id, props)
	}

	return nil
}

func secretStatement(resource any) map[string]any {
	return map[string]any{
		samKeyAction:       []any{samSecretAction},
		samKeyEffect:       stackPolicyEffectAllow,
		changeTypeResource: resource,
	}
}

func kmsDecryptStatement(keyID any, list bool) map[string]any {
	action := any(samKMSDecrypt)
	if list {
		action = []any{samKMSDecrypt}
	}

	return map[string]any{
		samKeyAction: action,
		samKeyEffect: stackPolicyEffectAllow,
		changeTypeResource: map[string]any{
			samKeyFnSub: "arn:${AWS::Partition}:kms:${AWS::Region}:${AWS::AccountId}:key/" +
				asString(keyID),
		},
	}
}

func mskStatements(props map[string]any) []any {
	var statements []any
	if reg := asMap(props[samKeySchemaReg]); reg != nil {
		statements = append(statements, schemaRegistryStatements(reg)...)
	}
	for _, c := range asList(props[samKeySAC]) {
		conf := asMap(c)
		if conf["Type"] == "CLIENT_CERTIFICATE_TLS_AUTH" && conf[samKeyURI2] != nil {
			statements = append(statements, secretStatement(conf[samKeyURI2]))
		}
	}

	return statements
}

// schemaRegistryStatements grants the Glue Schema Registry reads a SchemaRegistryConfig implies.
func schemaRegistryStatements(reg map[string]any) []any {
	var out []any
	for _, c := range asList(reg["AccessConfigs"]) {
		if conf := asMap(c); conf[samKeyURI2] != nil {
			out = append(out, secretStatement(conf[samKeyURI2]))
		}
	}
	uri := reg["SchemaRegistryURI"]
	var arnText string
	switch u := uri.(type) {
	case string:
		arnText = u
	case map[string]any:
		arnText, _ = u[samKeyFnSub].(string)
	}
	if name := glueRegistryName(arnText); name != "" {
		out = append(
			out,
			map[string]any{
				samKeyAction:       []any{"glue:GetRegistry"},
				samKeyEffect:       stackPolicyEffectAllow,
				changeTypeResource: uri,
			},
			map[string]any{samKeyAction: []any{"glue:GetSchemaVersion"}, samKeyEffect: stackPolicyEffectAllow,
				changeTypeResource: []any{map[string]any{samKeyFnSub: "arn:${AWS::Partition}:glue:${AWS::Region}:" +
					"${AWS::AccountId}:schema/" + name + "/*"}}},
		)
	}

	return out
}

func glueRegistryName(registryARN string) string {
	const arnSegments, registrySegment = 6, 5
	if !strings.HasPrefix(registryARN, "arn") {
		return ""
	}
	parts := strings.Split(registryARN, ":")
	if len(parts) >= arnSegments && strings.HasPrefix(parts[registrySegment], "registry/") {
		return strings.TrimPrefix(parts[registrySegment], "registry/")
	}

	return ""
}

func mqPolicy(ev *samEventCtx, id string, props map[string]any) error {
	uri, err := requiredSACURI(id, "MQ", props, []string{"BASIC_AUTH", "VIRTUAL_HOST"}, samKeyBasicAuth)
	if err != nil {
		return err
	}
	name := "SamAutoGeneratedAMQPolicy"
	if props["DynamicPolicyName"] == true {
		name = id + "AMQPolicy"
	}
	statements := []any{
		secretStatement(uri),
		map[string]any{samKeyAction: []any{"mq:DescribeBroker"}, samKeyEffect: stackPolicyEffectAllow,
			changeTypeResource: props["Broker"]},
	}
	if k := props[samKeySecretKMS]; k != nil {
		if _, ok := k.(string); !ok {
			return samErr(id, "SecretsManagerKmsKeyId must be a string.")
		}
		statements = append(statements, kmsDecryptStatement(k, false))
	}
	ev.role.addInline(map[string]any{samKeyPolicyName: name,
		samKeyPolicyDoc: map[string]any{samKeyStatement: statements}})

	return nil
}

func docDBPolicy(ev *samEventCtx, id string, props map[string]any) error {
	uri, err := requiredSACURI(id, "DocumentDB", props, []string{samKeyBasicAuth}, samKeyBasicAuth)
	if err != nil {
		return err
	}
	rds := func(action string, resource any) map[string]any {
		return map[string]any{
			samKeyAction:       []any{action},
			samKeyEffect:       stackPolicyEffectAllow,
			changeTypeResource: resource,
		}
	}
	statements := []any{
		secretStatement(uri),
		rds("rds:DescribeDBClusterParameters",
			map[string]any{samKeyFnSub: "arn:${AWS::Partition}:rds:${AWS::Region}:${AWS::AccountId}:cluster-pg:*"}),
		rds("rds:DescribeDBSubnetGroups",
			map[string]any{samKeyFnSub: "arn:${AWS::Partition}:rds:${AWS::Region}:${AWS::AccountId}:subgrp:*"}),
		rds("rds:DescribeDBClusters", props["Cluster"]),
		map[string]any{
			samKeyAction:       ec2NetworkActions(),
			samKeyEffect:       stackPolicyEffectAllow,
			changeTypeResource: "*",
		},
	}
	if k := props[samKeySecretKMS]; k != nil {
		if _, ok := k.(string); !ok {
			return samErr(id, "SecretsManagerKmsKeyId must be a string.")
		}
		statements = append(statements, kmsDecryptStatement(k, true))
	}
	ev.role.addInline(map[string]any{samKeyPolicyName: "SamAutoGeneratedDocumentDBPolicy",
		samKeyPolicyDoc: map[string]any{samKeyStatement: statements}})

	return nil
}

func ec2NetworkActions() []any {
	return []any{"ec2:CreateNetworkInterface", "ec2:DescribeNetworkInterfaces", "ec2:DeleteNetworkInterface",
		"ec2:DescribeVpcs", "ec2:DescribeSubnets", "ec2:DescribeSecurityGroups"}
}

// requiredSACURI validates SourceAccessConfigurations and returns the URI of the one required type.
func requiredSACURI(id, label string, props map[string]any, supported []string, required string) (any, error) {
	list := props[samKeySAC]
	if list == nil || len(asList(list)) == 0 {
		return nil, samErr(id, "No SourceAccessConfigurations for Amazon %s event provided.", label)
	}
	if _, isList := list.([]any); !isList {
		return nil, samErr(id, "Provided SourceAccessConfigurations cannot be parsed into a list.")
	}
	var uri any
	for i, c := range asList(list) {
		conf := asMap(c)
		if conf == nil {
			return nil, samErr(id, "SourceAccessConfigurations[%d] must be a map.", i)
		}
		typ, ok := conf["Type"].(string)
		if !ok {
			return nil, samErr(id, "SourceAccessConfigurations[%d].Type must be a string.", i)
		}
		if !containsString(supported, typ) {
			return nil, samErr(id, "Invalid property Type specified in SourceAccessConfigurations. The supported "+
				"values are: %v.", supported)
		}
		if typ != required {
			continue
		}
		if uri != nil {
			return nil, samErr(id, "Multiple %s properties specified in SourceAccessConfigurations.", required)
		}
		if uri = conf[samKeyURI2]; uri == nil || uri == "" {
			return nil, samErr(id, "No %s URI property specified in SourceAccessConfigurations.", required)
		}
	}
	if uri == nil {
		return nil, samErr(id, "No %s property specified in SourceAccessConfigurations.", required)
	}

	return uri, nil
}

func samKafkaAuthTypes() []string {
	return []string{"SASL_SCRAM_256_AUTH", "SASL_SCRAM_512_AUTH", "BASIC_AUTH", "CLIENT_CERTIFICATE_TLS_AUTH",
		"OAUTHBEARER_AUTH", "IAM_AUTH", "IAM_OAUTHBEARER_AUTH"}
}

func samKafkaOAuthMetadata() []string {
	return []string{"OAUTHBEARER_SCOPE", "OAUTHBEARER_AUDIENCE", "OAUTHBEARER_LOGICAL_CLUSTER",
		"OAUTHBEARER_IDENTITY_POOL"}
}

func kafkaPolicy(ev *samEventCtx, id string, props map[string]any) error {
	if props[samKeyBootstrap] == nil {
		return samErr(id, "No KafkaBootstrapServers provided for self managed kafka as an event source")
	}
	topics := props[samKeyTopics]
	if topics == nil {
		return samErr(id, "No Topics provided for self managed kafka as an event source")
	}
	const singleTopic = 1
	if len(asList(topics)) != singleTopic {
		return samErr(id, "Topics for self managed kafka only supports single configuration entry.")
	}
	if props[samKeySAC] == nil {
		return samErr(id, "No SourceAccessConfigurations for self managed kafka event provided.")
	}
	authURI, caURI, vpc, err := kafkaSecrets(id, props[samKeySAC])
	if err != nil {
		return err
	}
	var statements []any
	for _, u := range []any{authURI, caURI} {
		if u != nil {
			statements = append(statements, secretStatement(u))
		}
	}
	if vpc {
		statements = append(statements, map[string]any{samKeyAction: ec2NetworkActions(),
			samKeyEffect: stackPolicyEffectAllow, changeTypeResource: "*"})
	}
	if k := props[samKeySecretKMS]; k != nil {
		if _, ok := k.(string); !ok {
			return samErr(id, "SecretsManagerKmsKeyId must be a string.")
		}
		statements = append(statements, kmsDecryptStatement(k, true))
	}
	if reg := asMap(props[samKeySchemaReg]); reg != nil {
		statements = append(statements, schemaRegistryStatements(reg)...)
	}
	if len(statements) == 0 {
		return nil
	}
	ev.role.addInline(map[string]any{samKeyPolicyName: "SelfManagedKafkaExecutionRolePolicy",
		samKeyPolicyDoc: map[string]any{samKeyStatement: statements, samKeyVersion: samPolicyVersion}})

	return nil
}

// kafkaSAC is what a SelfManagedKafka event's SourceAccessConfigurations contribute to the role.
type kafkaSAC struct {
	authURI, caURI         any
	subnet, group, hasAuth bool
}

func kafkaSecrets(id string, sac any) (any, any, bool, error) {
	list, isList := sac.([]any)
	if !isList {
		return nil, nil, false, samErr(id, "SourceAccessConfigurations for self managed kafka event should be a list.")
	}
	var state kafkaSAC
	for _, c := range list {
		conf := asMap(c)
		if conf == nil {
			return nil, nil, false, samErr(id, "SourceAccessConfigurations must be a map.")
		}
		if err := state.add(id, conf); err != nil {
			return nil, nil, false, err
		}
	}
	if state.subnet != state.group {
		return nil, nil, false, samErr(id, "VPC_SUBNET and VPC_SECURITY_GROUP in SourceAccessConfigurations for "+
			"SelfManagedKafka must be both provided.")
	}

	return state.authURI, state.caURI, state.subnet && state.group, nil
}

func (s *kafkaSAC) add(id string, conf map[string]any) error {
	typ, _ := conf["Type"].(string)
	uri := conf[samKeyURI2]
	switch {
	case typ == "VPC_SUBNET", typ == "VPC_SECURITY_GROUP":
		s.subnet = s.subnet || typ == "VPC_SUBNET"
		s.group = s.group || typ == "VPC_SECURITY_GROUP"

		return kafkaURI(id, uri, typ)
	case containsString(samKafkaAuthTypes(), typ):
		if s.hasAuth {
			return samErr(id, "Multiple auth mechanism properties specified in SourceAccessConfigurations for self "+
				"managed kafka event.")
		}
		s.hasAuth = true
		if typ == "IAM_AUTH" || typ == "IAM_OAUTHBEARER_AUTH" {
			return nil
		}
		s.authURI = uri

		return kafkaURI(id, uri, "auth mechanism")
	case typ == "SERVER_ROOT_CA_CERTIFICATE":
		s.caURI = uri

		return kafkaURI(id, uri, typ)
	case containsString(samKafkaOAuthMetadata(), typ):
		return kafkaURI(id, uri, typ)
	}

	return samErr(id, "Invalid SourceAccessConfigurations Type provided for self managed kafka event.")
}

func kafkaURI(id string, uri any, msg string) error {
	if uri == nil || uri == "" {
		return samErr(
			id,
			"No %s URI property specified in SourceAccessConfigurations for self managed kafka event.",
			msg,
		)
	}
	if _, isStr := uri.(string); !isStr && !isIntrinsicValue(uri) {
		return samErr(id, "Wrong Type for %s URI property specified in SourceAccessConfigurations for self managed "+
			"kafka event.", msg)
	}

	return nil
}
