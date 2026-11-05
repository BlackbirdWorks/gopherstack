package cloudformation

import (
	"maps"
	"regexp"
	"slices"
	"strings"
)

const (
	samSQSExecRole     = "arn:${AWS::Partition}:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole"
	samKinesisExec     = "arn:${AWS::Partition}:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole"
	samDynamoDBExec    = "arn:${AWS::Partition}:iam::aws:policy/service-role/AWSLambdaDynamoDBExecutionRole"
	samEventSQS        = "SQS"
	samEventKinesis    = "Kinesis"
	samEventDynamoDB   = "DynamoDB"
	samEventAPI        = "Api"
	samEventSchedule   = "Schedule"
	samEventScheduleV2 = "ScheduleV2"
	samEventS3         = "S3"
	samEventSNS        = "SNS"
	samEventRule       = "EventBridgeRule"
	samEventCWEvent    = "CloudWatchEvent"
	samEventsPrincipl  = "events.amazonaws.com"
)

func samESMCommon() []string {
	return []string{"BatchSize", "Enabled", "MaximumBatchingWindowInSeconds", "FunctionResponseTypes",
		"FilterCriteria"}
}

func samStreamExtra() []string {
	return []string{"StartingPosition", "ParallelizationFactor", "MaximumRetryAttempts",
		"BisectBatchOnFunctionError", "MaximumRecordAgeInSeconds", "DestinationConfig",
		"TumblingWindowInSeconds"}
}

type samEventCtx struct {
	managed *[]any
	id      string
	alias   string
}

// target is the logical id events invoke: the AutoPublishAlias alias when the function has one.
func (ev *samEventCtx) target() string {
	if ev.alias != "" {
		return ev.alias
	}

	return ev.id
}

func (ev *samEventCtx) fnRef() map[string]any { return samRef(ev.target()) }

func (ev *samEventCtx) fnArn() map[string]any {
	if ev.alias != "" {
		return samRef(ev.alias)
	}

	return samGetAtt(ev.id, attrNameArn)
}

// invokeRef is the Fn::Sub token for the Lambda invoke ARN of the event target.
func (ev *samEventCtx) invokeRef() string {
	if ev.alias != "" {
		return ev.alias
	}

	return ev.id + ".Arn"
}

type samEventHandler func(t *samTranslator, ev *samEventCtx, name string, props map[string]any) error

func samEventHandlers() map[string]samEventHandler {
	pull := func(srcKey, policy string, extra []string) samEventHandler {
		return func(t *samTranslator, ev *samEventCtx, name string, props map[string]any) error {
			return t.mappingEvent(ev, name, props, srcKey, policy, extra)
		}
	}

	return map[string]samEventHandler{
		samEventAPI:        (*samTranslator).apiEvent,
		samEventHTTPAPI:    (*samTranslator).httpAPIEvent,
		samEventS3:         (*samTranslator).s3Event,
		samEventScheduleV2: (*samTranslator).scheduleV2Event,
		samEventSQS:        pull("Queue", samSQSExecRole, nil),
		samEventKinesis:    pull("Stream", samKinesisExec, samStreamExtra()),
		samEventDynamoDB:   pull("Stream", samDynamoDBExec, samStreamExtra()),
		samEventSNS:        (*samTranslator).snsEvent,
		samEventSchedule:   (*samTranslator).scheduleEvent,
		samEventRule:       (*samTranslator).ruleEvent,
		samEventCWEvent:    (*samTranslator).ruleEvent,
	}
}

func (t *samTranslator) translateEvents(ev *samEventCtx, events map[string]any) error {
	handlers := samEventHandlers()
	for _, name := range slices.Sorted(maps.Keys(events)) {
		e := asMap(events[name])
		typ, _ := e[samKeyType].(string)
		if raw, has := e[samKeyProps]; has && raw != nil {
			if _, isMap := raw.(map[string]any); !isMap {
				return samErr(ev.id, "Event with id [%s] is invalid. Properties should be a map.", name)
			}
		}
		handle, ok := handlers[typ]
		if !ok {
			return samErr(ev.id, "Event type '%s' (event [%s]) is not supported by this emulator's SAM transform",
				typ, name)
		}
		if err := handle(t, ev, name, asMap(e[samKeyProps])); err != nil {
			return err
		}
	}

	return nil
}

func lambdaPermission(fnID, principal string, sourceArn any) map[string]any {
	p := map[string]any{
		samKeyAction:    samActionInvokeFn,
		samKeyFnName:    samRef(fnID),
		samKeyPrincipal: principal,
	}
	if sourceArn != nil {
		p["SourceArn"] = sourceArn
	}

	return map[string]any{samKeyType: samTypePerm, samKeyProps: p}
}

func (t *samTranslator) mappingEvent(
	ev *samEventCtx, name string, props map[string]any, srcKey, execPolicy string, extra []string,
) error {
	allowed := slices.Concat([]string{srcKey}, samESMCommon(), extra)
	if err := rejectUnknown(ev.id+name, props, keySet(allowed...)); err != nil {
		return err
	}
	if props[srcKey] == nil {
		return samErr(ev.id, "Event [%s] is missing required property '%s'.", name, srcKey)
	}
	m := map[string]any{samKeyFnName: ev.fnRef(), "EventSourceArn": props[srcKey]}
	for k, v := range props {
		if k != srcKey {
			m[k] = v
		}
	}
	*ev.managed = append(*ev.managed, execPolicy)

	return t.put(ev.id+name, map[string]any{samKeyType: samTypeESM, samKeyProps: m})
}

func (t *samTranslator) snsEvent(ev *samEventCtx, name string, props map[string]any) error {
	if err := rejectUnknown(ev.id+name, props, keySet("Topic", "FilterPolicy", "Region")); err != nil {
		return err
	}
	sub := map[string]any{
		"Protocol":     "lambda",
		samKeyTopicArn: props["Topic"],
		"Endpoint":     ev.fnArn(),
	}
	if props["FilterPolicy"] != nil {
		sub["FilterPolicy"] = props["FilterPolicy"]
	}
	if props["Region"] != nil {
		sub["Region"] = props["Region"]
	}
	if err := t.put(ev.id+name, map[string]any{samKeyType: samTypeSubscr, samKeyProps: sub}); err != nil {
		return err
	}

	return t.put(ev.id+name+"Permission", lambdaPermission(ev.target(), "sns.amazonaws.com", props["Topic"]))
}

func (t *samTranslator) scheduleEvent(ev *samEventCtx, name string, props map[string]any) error {
	if err := rejectUnknown(ev.id+name, props, keySet("Schedule", "Input", "Enabled", samKeyState, "Name",
		samKeyDesc)); err != nil {
		return err
	}
	rule := map[string]any{"ScheduleExpression": props["Schedule"], samKeyState: ruleState(props)}
	for _, k := range []string{attrNameName, samKeyDesc} {
		if props[k] != nil {
			rule[k] = props[k]
		}
	}

	return t.putRule(ev, name, rule, props["Input"], nil)
}

func (t *samTranslator) ruleEvent(ev *samEventCtx, name string, props map[string]any) error {
	if err := rejectUnknown(ev.id+name, props, keySet("Pattern", "Input", "InputPath", "EventBusName",
		"Enabled", samKeyState)); err != nil {
		return err
	}
	rule := map[string]any{"EventPattern": props["Pattern"], samKeyState: ruleState(props)}
	if props["EventBusName"] != nil {
		rule["EventBusName"] = props["EventBusName"]
	}

	return t.putRule(ev, name, rule, props["Input"], props["InputPath"])
}

func ruleState(props map[string]any) any {
	if s, ok := props[samKeyState]; ok {
		return s
	}
	if en, ok := props["Enabled"].(bool); ok && !en {
		return samStateDisabled
	}

	return samStateEnabled
}

func (t *samTranslator) putRule(ev *samEventCtx, name string, rule map[string]any, input, inputPath any) error {
	target := map[string]any{attrNameArn: ev.fnArn(), "Id": name + "LambdaTarget"}
	if input != nil {
		target["Input"] = input
	}
	if inputPath != nil {
		target["InputPath"] = inputPath
	}
	rule["Targets"] = []any{target}
	ruleID := ev.id + name
	if err := t.put(ruleID, map[string]any{samKeyType: samTypeRule, samKeyProps: rule}); err != nil {
		return err
	}

	return t.put(ruleID+"Permission", lambdaPermission(ev.target(), samEventsPrincipl, samGetAtt(ruleID, attrNameArn)))
}

func (t *samTranslator) apiEvent(ev *samEventCtx, name string, props map[string]any) error {
	if err := rejectUnknown(ev.id+name, props, keySet("Path", "Method", "RestApiId", samAuthKey,
		"RequestParameters", "TimeoutInMillis")); err != nil {
		return err
	}
	path, _ := props["Path"].(string)
	method, _ := props["Method"].(string)
	if path == "" || method == "" {
		return samErr(ev.id, "Event [%s] of type Api requires 'Path' and 'Method'.", name)
	}
	api, err := t.apiFor(ev.id, props["RestApiId"])
	if err != nil {
		return err
	}
	authorizer, apiKey, err := eventAuth(ev.id, name, props)
	if err != nil {
		return err
	}
	params, err := parseRequestParameters(ev.id, name, props["RequestParameters"])
	if err != nil {
		return err
	}
	timeout, err := parseAPITimeout(ev.id, name, props["TimeoutInMillis"])
	if err != nil {
		return err
	}
	api.addRoute(
		samRoute{
			fnID:       ev.id,
			target:     ev.invokeRef(),
			path:       path,
			method:     method,
			authorizer: authorizer,
			apiKey:     apiKey,
			params:     params,
			timeout:    timeout,
		},
	)
	suffix := api.permSuffix()
	perm := lambdaPermission(
		ev.target(),
		"apigateway.amazonaws.com",
		executeAPISourceArn(api.id, "*", permissionMethod(method),
			permissionPath(path)),
	)

	return t.put(samPermissionID(ev.id+name, suffix), perm)
}

// samPermissionID is SAM's PushEventSource permission id: <prefix>Permission<suffix>; a non-alphanumeric
// suffix is hashed.
func samPermissionID(prefix, suffix string) string {
	if suffix != "" && isAlnum(suffix) {
		return prefix + "Permission" + suffix
	}

	return samLogicalID(prefix+"Permission", suffix)
}

func isAlnum(s string) bool {
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
			return false
		}
	}

	return true
}

// executeAPISourceArn is SAM's Fn::Sub list form with __ApiId__/__Stage__ placeholders.
func executeAPISourceArn(apiID, stage, method, path string) map[string]any {
	return map[string]any{samKeyFnSub: []any{
		"arn:${AWS::Partition}:execute-api:${AWS::Region}:${AWS::AccountId}:${__ApiId__}/${__Stage__}/" + method + path,
		map[string]any{"__ApiId__": samRef(apiID), "__Stage__": stage},
	}}
}

func permissionMethod(m string) string {
	if strings.EqualFold(m, "any") {
		return "*"
	}

	return strings.ToUpper(m)
}

var samPathParam = regexp.MustCompile(`\{([a-zA-Z0-9._-]+|[a-zA-Z0-9._-]+\+|proxy\+)\}`)

func permissionPath(p string) string {
	if len(p) > 1 {
		p = strings.TrimSuffix(p, "/")
	}

	return samPathParam.ReplaceAllString(p, "*")
}

// eventAuth reads Auth.Authorizer and Auth.ApiKeyRequired; any other Auth key is rejected.
func eventAuth(fnID, name string, props map[string]any) (string, *bool, error) {
	auth := asMap(props[samAuthKey])
	if err := rejectUnknown(fnID+name, auth, keySet("Authorizer", "ApiKeyRequired")); err != nil {
		return "", nil, err
	}
	a, _ := auth["Authorizer"].(string)
	var apiKey *bool
	if v, ok := auth["ApiKeyRequired"]; ok {
		b, isBool := v.(bool)
		if !isBool {
			return "", nil, samErr(fnID, "Event [%s] Auth.ApiKeyRequired must be a boolean.", name)
		}
		apiKey = &b
	}

	return a, apiKey, nil
}
