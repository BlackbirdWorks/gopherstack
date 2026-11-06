package cloudformation

import (
	"maps"
	"regexp"
	"slices"
	"strings"
)

const (
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
	samEventMSK        = "MSK"
	samEventMQ         = "MQ"
	samEventKafka      = "SelfManagedKafka"
	samEventDocDB      = "DocumentDB"
	samEventsPrincipl  = "events.amazonaws.com"
)

type samEventCtx struct {
	role  *samRolePolicies
	id    string
	alias string
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
	pull := func(kind string) samEventHandler {
		return func(t *samTranslator, ev *samEventCtx, name string, props map[string]any) error {
			return t.pullEvent(ev, name, kind, props)
		}
	}

	return map[string]samEventHandler{
		samEventAPI:        (*samTranslator).apiEvent,
		samEventHTTPAPI:    (*samTranslator).httpAPIEvent,
		samEventS3:         (*samTranslator).s3Event,
		samEventScheduleV2: (*samTranslator).scheduleV2Event,
		samEventSQS:        pull(samEventSQS),
		samEventKinesis:    pull(samEventKinesis),
		samEventDynamoDB:   pull(samEventDynamoDB),
		samEventMSK:        pull(samEventMSK),
		samEventMQ:         pull(samEventMQ),
		samEventKafka:      pull(samEventKafka),
		samEventDocDB:      pull(samEventDocDB),
		samEventSNS:        (*samTranslator).snsEvent,
		samEventSchedule:   (*samTranslator).scheduleEvent,
		samEventRule:       (*samTranslator).ruleEvent,
		samEventCWEvent:    (*samTranslator).ruleEvent,
		samEventCWLogs:     (*samTranslator).cwLogsEvent,
		samEventIoTRule:    (*samTranslator).iotRuleEvent,
		samEventAlexa:      (*samTranslator).alexaEvent,
		samEventCognito:    (*samTranslator).cognitoEvent,
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

func (t *samTranslator) apiEvent(ev *samEventCtx, name string, props map[string]any) error {
	if err := rejectUnknown(ev.id+name, props, keySet("Path", "Method", samKRestAPIID, samAuthKey,
		"RequestParameters", "TimeoutInMillis", "RequestModel")); err != nil {
		return err
	}
	path, _ := props["Path"].(string)
	method, _ := props["Method"].(string)
	if path == "" || method == "" {
		return samErr(ev.id, "Event [%s] of type Api requires 'Path' and 'Method'.", name)
	}
	api, err := t.apiFor(ev.id, props[samKRestAPIID])
	if err != nil {
		return err
	}
	authorizer, apiKey, scopes, err := eventAuth(ev.id, name, props)
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
	model, err := t.parseRequestModel(ev.id, name, api, method, path, props["RequestModel"])
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
			model:      model,
			scopes:     scopes,
			auth:       asMap(props[samAuthKey]),
		},
		t.pass,
	)
	suffix := api.permSuffix()
	perm := lambdaPermission(
		ev.target(),
		"apigateway.amazonaws.com",
		executeAPISourceArn(api.eventIDValue(props[samKRestAPIID]), "*", permissionMethod(method),
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
func executeAPISourceArn(apiID any, stage, method, path string) map[string]any {
	return map[string]any{samKeyFnSub: []any{
		"arn:${AWS::Partition}:execute-api:${AWS::Region}:${AWS::AccountId}:${__ApiId__}/${__Stage__}/" + method + path,
		map[string]any{samAPIIDMarker: apiID, "__Stage__": stage},
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
func eventAuth(fnID, name string, props map[string]any) (string, *bool, any, error) {
	auth := asMap(props[samAuthKey])
	if err := rejectUnknown(
		fnID+name,
		auth,
		keySet("Authorizer", "ApiKeyRequired", "AuthorizationScopes", "InvokeRole", "ResourcePolicy"),
	); err != nil {
		return "", nil, nil, err
	}
	a, _ := auth["Authorizer"].(string)
	if v, has := auth["Authorizer"]; has && v != nil && a == "" {
		return "", nil, nil, samErr(fnID, "Event with id [%s] is invalid. Unable to set Authorizer [%v] on API method "+
			"[%v] for path [%v]. The method authorizer must be a string with a corresponding dict entry in the api "+
			"authorizer.", name, v, props["Method"], props["Path"])
	}
	var apiKey *bool
	if v, ok := auth["ApiKeyRequired"]; ok {
		b, isBool := v.(bool)
		if !isBool {
			return "", nil, nil, samErr(fnID, "Event [%s] Auth.ApiKeyRequired must be a boolean.", name)
		}
		apiKey = &b
	}

	return a, apiKey, auth["AuthorizationScopes"], nil
}
