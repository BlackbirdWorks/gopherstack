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
}

func (t *samTranslator) translateEvents(ev *samEventCtx, events map[string]any) error {
	for _, name := range slices.Sorted(maps.Keys(events)) {
		e := asMap(events[name])
		typ, _ := e[samKeyType].(string)
		props := asMap(e[samKeyProps])
		var err error
		switch typ {
		case samEventAPI:
			err = t.apiEvent(ev, name, props)
		case samEventHTTPAPI:
			err = t.httpAPIEvent(ev, name, props)
		case samEventS3:
			err = t.s3Event(ev, name, props)
		case samEventScheduleV2:
			err = t.scheduleV2Event(ev, name, props)
		case samEventSQS:
			err = t.mappingEvent(ev, name, props, "Queue", samSQSExecRole, nil)
		case samEventKinesis:
			err = t.mappingEvent(ev, name, props, "Stream", samKinesisExec, samStreamExtra())
		case samEventDynamoDB:
			err = t.mappingEvent(ev, name, props, "Stream", samDynamoDBExec, samStreamExtra())
		case samEventSNS:
			err = t.snsEvent(ev, name, props)
		case samEventSchedule:
			err = t.scheduleEvent(ev, name, props)
		case samEventRule, samEventCWEvent:
			err = t.ruleEvent(ev, name, props)
		default:
			err = samErr(ev.id, "Event type '%s' (event [%s]) is not supported by this emulator's SAM transform",
				typ, name)
		}
		if err != nil {
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
	m := map[string]any{samKeyFnName: samRef(ev.id), "EventSourceArn": props[srcKey]}
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
		"Endpoint":     samGetAtt(ev.id, attrNameArn),
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

	return t.put(ev.id+name+"Permission", lambdaPermission(ev.id, "sns.amazonaws.com", props["Topic"]))
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
	target := map[string]any{attrNameArn: samGetAtt(ev.id, attrNameArn), "Id": name + "LambdaTarget"}
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

	return t.put(ruleID+"Permission", lambdaPermission(ev.id, samEventsPrincipl, samGetAtt(ruleID, attrNameArn)))
}

func (t *samTranslator) apiEvent(ev *samEventCtx, name string, props map[string]any) error {
	if err := rejectUnknown(ev.id+name, props, keySet("Path", "Method", "RestApiId", samAuthKey)); err != nil {
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
	authorizer, err := eventAuthorizer(ev.id, name, props)
	if err != nil {
		return err
	}
	api.addRoute(samRoute{fnID: ev.id, path: path, method: method, authorizer: authorizer})
	for _, stage := range []string{api.stageName, "Test"} {
		src := map[string]any{samKeyFnSub: "arn:${AWS::Partition}:execute-api:${AWS::Region}:${AWS::AccountId}:${" +
			api.id + "}/" + stageSegment(stage) + "/" + permissionMethod(method) + permissionPath(path)}
		perm := lambdaPermission(ev.id, "apigateway.amazonaws.com", src)
		if err = t.put(ev.id+name+"Permission"+stage, perm); err != nil {
			return err
		}
	}

	return nil
}

func stageSegment(stage string) string {
	if stage == "Test" {
		return "*"
	}

	return stage
}

func permissionMethod(m string) string {
	if strings.EqualFold(m, "any") {
		return "*"
	}

	return strings.ToUpper(m)
}

var samPathParam = regexp.MustCompile(`\{[^}]+\}`)

func permissionPath(p string) string {
	return samPathParam.ReplaceAllString(p, "*")
}

// eventAuthorizer reads Auth.Authorizer; any other Auth key is rejected.
func eventAuthorizer(fnID, name string, props map[string]any) (string, error) {
	auth := asMap(props[samAuthKey])
	if err := rejectUnknown(fnID+name, auth, keySet("Authorizer")); err != nil {
		return "", err
	}
	a, _ := auth["Authorizer"].(string)

	return a, nil
}
