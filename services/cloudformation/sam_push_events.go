package cloudformation

import (
	"maps"
	"slices"
)

const (
	samEventCWLogs     = "CloudWatchLogs"
	samEventCognito    = "Cognito"
	samEventIoTRule    = "IoTRule"
	samEventAlexa      = "AlexaSkill"
	samTypeQueuePolicy = "AWS::SQS::QueuePolicy"
	samTypeSubFilter   = "AWS::Logs::SubscriptionFilter"
	samTypeIoTRule     = "AWS::IoT::TopicRule"
	samTargetIDMax     = 64
	samTargetSuffix    = "LambdaTarget"
)

// samTargetID is the EventBridge target id: <event logical id>LambdaTarget, truncated to 64 characters.
func samTargetID(logicalID string) string { return samTargetIDSuffix(logicalID, samTargetSuffix) }

func samTargetIDSuffix(logicalID, suffix string) string {
	if len(logicalID)+len(suffix) <= samTargetIDMax {
		return logicalID + suffix
	}

	return logicalID[:samTargetIDMax-len(suffix)] + suffix
}

func (t *samTranslator) s3Event(ev *samEventCtx, name string, props map[string]any) error {
	if err := rejectUnknown(ev.id+name, props, keySet("Bucket", samKeyEvents, "Filter")); err != nil {
		return err
	}
	bucket, _ := asMap(props["Bucket"])[samKeyRef].(string)
	if bucket == "" {
		return samErr(ev.id, "Event [%s] S3 events must reference an S3 bucket in the same template.", name)
	}
	events := asList(props[samKeyEvents])
	if len(events) == 0 {
		return samErr(ev.id, "Event [%s] of type S3 is missing required property 'Events'.", name)
	}
	perm := lambdaPermission(ev.target(), "s3.amazonaws.com", nil)
	asMap(perm[samKeyProps])["SourceAccount"] = samRef("AWS::AccountId")
	permID := samPermissionID(ev.id+name, "")
	if err := t.put(permID, perm); err != nil {
		return err
	}
	t.s3Perms[bucket] = append(t.s3Perms[bucket], permID)
	if len(t.curTags) > 0 {
		if t.bucketTags[bucket] == nil {
			t.bucketTags[bucket] = map[string]any{}
		}
		maps.Copy(t.bucketTags[bucket], t.curTags)
	}
	for _, e := range events {
		cfg := map[string]any{"Event": e, samKeyFunction: ev.fnArn()}
		if props["Filter"] != nil {
			cfg["Filter"] = props["Filter"]
		}
		var entry any = cfg
		if cond := asString(t.pass[samKeyCondition]); cond != "" {
			entry = map[string]any{fnIf: []any{cond, cfg, samRef(pseudoNoValue)}}
		}
		t.s3Configs[bucket] = append(t.s3Configs[bucket], entry)
	}

	return nil
}

// finalizeS3Events adds each bucket's Lambda notifications and makes the bucket wait for the permissions,
// because S3 checks it may invoke the function when the notification is configured.
func (t *samTranslator) finalizeS3Events() error {
	for _, id := range slices.Sorted(maps.Keys(t.s3Configs)) {
		b := asMap(t.out[id])
		if b[samKeyType] != samTypeBucket {
			return samErr(id, "S3 event Bucket must reference an AWS::S3::Bucket resource in this template.")
		}
		props := maps.Clone(asMap(b[samKeyProps]))
		if props == nil {
			props = map[string]any{}
		}
		nc := maps.Clone(asMap(props[samKeyNotifCfg]))
		if nc == nil {
			nc = map[string]any{}
		}
		existing := asList(nc[samKeyLambdaCfg])
		for _, cfg := range t.s3Configs[id] {
			if !containsJSON(existing, cfg) {
				existing = append(existing, cfg)
			}
		}
		nc[samKeyLambdaCfg] = existing
		props[samKeyNotifCfg] = nc
		if tags := t.bucketTags[id]; len(tags) > 0 {
			props[samKeyTags] = plainTagList(tags)
		}
		nb := maps.Clone(b)
		nb[samKeyProps] = props
		t.dependOnPermissions(nb, t.s3Perms[id])
		t.out[id] = nb
	}

	return nil
}

// dependOnPermissions adds permission ids to the bucket's DependsOn; a conditional permission is referenced
// through a conditional tag instead, since DependsOn cannot be conditional.
func (t *samTranslator) dependOnPermissions(bucket map[string]any, perms []string) {
	deps := bucket[samKeyDependsOn]
	var list []any
	switch d := deps.(type) {
	case string:
		list = []any{d}
	case []any:
		list = slices.Clone(d)
	}
	for _, p := range perms {
		perm := asMap(t.out[p])
		cond := asString(perm[samKeyCondition])
		if cond == "" {
			if !slices.Contains(list, any(p)) {
				list = append(list, p)
			}

			continue
		}
		props := asMap(bucket[samKeyProps])
		tags := asList(props[samKeyTags])
		tags = append(tags, map[string]any{samKeyKey: "sam:ConditionalDependsOn:" + p,
			samKValue: map[string]any{fnIf: []any{cond, samRef(p), "no dependency"}}})
		props[samKeyTags] = tags
	}
	if len(list) > 0 {
		bucket[samKeyDependsOn] = list
	}
}

func (t *samTranslator) snsEvent(ev *samEventCtx, name string, props map[string]any) error {
	if err := rejectUnknown(ev.id+name, props, keySet("Topic", "FilterPolicy", "FilterPolicyScope", "Region",
		"SqsSubscription", "RedrivePolicy")); err != nil {
		return err
	}
	if props["Topic"] == nil {
		return samErr(ev.id, "Event [%s] is missing required property 'Topic'.", name)
	}
	id := ev.id + name
	sqsSub := props["SqsSubscription"]
	if sqsSub == nil || sqsSub == false {
		if err := t.put(id, t.snsSubscription("lambda", ev.fnArn(), props)); err != nil {
			return err
		}

		return t.put(samPermissionID(id, ""), lambdaPermission(ev.target(), "sns.amazonaws.com", props["Topic"]))
	}
	if sqsSub == true {
		return t.snsNewQueue(ev, name, props)
	}

	return t.snsExistingQueue(ev, name, props, asMap(sqsSub))
}

func (t *samTranslator) snsSubscription(protocol string, endpoint any, props map[string]any) map[string]any {
	sub := map[string]any{"Protocol": protocol, "Endpoint": endpoint, samKeyTopicArn: props["Topic"]}
	for _, k := range []string{"Region", "FilterPolicy", "FilterPolicyScope", "RedrivePolicy"} {
		if props[k] != nil {
			sub[k] = props[k]
		}
	}

	return map[string]any{samKeyType: samTypeSubscr, samKeyProps: sub}
}

func snsQueuePolicy(topic, queueARN, queueURL any) map[string]any {
	return map[string]any{samKeyType: samTypeQueuePolicy, samKeyProps: map[string]any{
		samKeyPolicyDoc: map[string]any{
			samKeyVersion: samPolicyVersion,
			samKeyStatement: []any{
				map[string]any{
					samKeyAction:       samKSqsSendMessage,
					samKeyEffect:       stackPolicyEffectAllow,
					samKeyPrincipal:    "*",
					changeTypeResource: queueARN,
					samKeyCondition:    map[string]any{"ArnEquals": map[string]any{"aws:SourceArn": topic}},
				},
			},
		},
		samKQueues: []any{queueURL},
	}}
}

// snsNewQueue is SNS -> new SQS queue -> function.
func (t *samTranslator) snsNewQueue(ev *samEventCtx, name string, props map[string]any) error {
	id := ev.id + name
	queue := map[string]any{samKeyType: samTypeSQSQueue, samKeyProps: map[string]any{}}
	if fifo := t.snsFifoTopic(props["Topic"]); fifo != nil {
		asMap(queue[samKeyProps])["FifoQueue"] = fifo
	}
	queueARN, queueURL := samGetAtt(id+samKQueue, attrNameArn), samRef(id+samKQueue)
	if err := t.put(id+samKQueue, queue); err != nil {
		return err
	}
	if err := t.put(id+"QueuePolicy", snsQueuePolicy(props["Topic"], queueARN, queueURL)); err != nil {
		return err
	}
	if err := t.put(id, t.snsSubscription("sqs", queueARN, props)); err != nil {
		return err
	}

	return t.snsQueueMapping(ev, id, queueARN, nil, nil)
}

// snsFifoTopic reads FifoTopic from a Topic declared in the template, so the generated queue matches it.
func (t *samTranslator) snsFifoTopic(topic any) any {
	ref, _ := asMap(topic)[samKeyRef].(string)
	res := asMap(t.input[ref])

	return asMap(res[samKeyProps])["FifoTopic"]
}

func (t *samTranslator) snsExistingQueue(ev *samEventCtx, name string, props, sub map[string]any) error {
	id := ev.id + name
	queueARN, queueURL := sub["QueueArn"], sub["QueueUrl"]
	if queueARN == nil || queueURL == nil {
		return samErr(ev.id, "Event [%s] No QueueARN or QueueURL provided.", name)
	}
	policyID := id + "QueuePolicy"
	if v, ok := sub["QueuePolicyLogicalId"].(string); ok && v != "" {
		policyID = v
	}
	if err := t.put(policyID, snsQueuePolicy(props["Topic"], queueARN, queueURL)); err != nil {
		return err
	}
	if err := t.put(id, t.snsSubscription("sqs", queueARN, props)); err != nil {
		return err
	}

	return t.snsQueueMapping(ev, id, queueARN, sub[samKeyBatchSize], sub[samKeyEnabled])
}

func (t *samTranslator) snsQueueMapping(ev *samEventCtx, id string, queueARN, batch, enabled any) error {
	if batch == nil {
		batch = float64(snsDefaultBatchSize)
	}
	if enabled == nil {
		enabled = true
	}
	esm := map[string]any{
		samKeyFnName:     ev.fnRef(),
		"EventSourceArn": queueARN,
		samKeyBatchSize:  batch,
		samKeyEnabled:    enabled,
	}
	ev.role.addManaged(samSQSExecRole)

	return t.put(id+"EventSourceMapping", map[string]any{samKeyType: samTypeESM, samKeyProps: esm})
}

const snsDefaultBatchSize = 10

// eventBridgeDLQ resolves a rule event's DeadLetterConfig to a queue ARN, generating the queue and
// (for rules) the queue policy that lets EventBridge send to it.
func (t *samTranslator) eventBridgeDLQ(
	id string, cfg map[string]any, ruleARN any, withPolicy bool,
) (any, error) {
	arn, hasARN := cfg[attrNameArn]
	typ, hasType := cfg[samKeyType]
	switch {
	case hasARN && hasType:
		return nil, samErr(id, "You can either define 'Arn' or 'Type' property of DeadLetterConfig.")
	case hasType && typ != samEventSQS:
		return nil, samErr(id, "The only valid value for 'Type' property of DeadLetterConfig is 'SQS'.")
	case !hasARN && !hasType:
		return nil, samErr(id, "No 'Arn' or 'Type' property provided for DeadLetterConfig.")
	}
	if hasARN {
		return arn, nil
	}
	queueID := id + samKQueue
	if q, has := cfg["QueueLogicalId"]; has {
		s, ok := q.(string)
		if !ok {
			return nil, samErr(id, "QueueLogicalId must be a string")
		}
		queueID = s
	}
	if err := t.put(queueID, map[string]any{samKeyType: samTypeSQSQueue, samKeyProps: map[string]any{}}); err != nil {
		return nil, err
	}
	if withPolicy {
		policy := map[string]any{samKeyType: samTypeQueuePolicy, samKeyProps: map[string]any{
			samKeyPolicyDoc: map[string]any{
				samKeyVersion: samPolicyVersion,
				samKeyStatement: []any{map[string]any{
					samKeyAction: samKSqsSendMessage, samKeyEffect: stackPolicyEffectAllow,
					samKeyPrincipal:    map[string]any{samKService: "events.amazonaws.com"},
					changeTypeResource: samGetAtt(queueID, attrNameArn),
					samKeyCondition:    map[string]any{"ArnEquals": map[string]any{"aws:SourceArn": ruleARN}},
				}},
			},
			samKQueues: []any{samRef(queueID)},
		}}
		if err := t.put(id+"QueuePolicy", policy); err != nil {
			return nil, err
		}
	}

	return samGetAtt(queueID, attrNameArn), nil
}

func (t *samTranslator) scheduleEvent(ev *samEventCtx, name string, props map[string]any) error {
	id := ev.id + name
	if err := rejectUnknown(id, props, keySet("Schedule", samKRuleName, "Input", samKeyEnabled, samKeyState, "Name",
		samKeyDesc, "DeadLetterConfig", "RetryPolicy")); err != nil {
		return err
	}
	if props["Schedule"] == nil {
		return samErr(ev.id, "Event [%s] is missing required property 'Schedule'.", name)
	}
	rule := map[string]any{"ScheduleExpression": props["Schedule"]}
	if err := ruleStateProps(ev.id, name, props, rule); err != nil {
		return err
	}
	for _, k := range []string{attrNameName, samKeyDesc} {
		if props[k] != nil {
			rule[k] = props[k]
		}
	}

	return t.putRule(ev, name, props, rule)
}

func (t *samTranslator) ruleEvent(ev *samEventCtx, name string, props map[string]any) error {
	id := ev.id + name
	if err := rejectUnknown(
		id,
		props,
		keySet("Pattern", "Input", "InputPath", "EventBusName", samKRuleName, samKeyEnabled,
			samKeyState, "DeadLetterConfig", "RetryPolicy", "Target", "InputTransformer"),
	); err != nil {
		return err
	}
	rule := map[string]any{}
	for src, dst := range map[string]string{samKeyEventBus: "EventBusName", "Pattern": "EventPattern",
		samKRuleName: attrNameName} {
		if props[src] != nil {
			rule[dst] = props[src]
		}
	}
	if err := ruleStateProps(ev.id, name, props, rule); err != nil {
		return err
	}

	return t.putRule(ev, name, props, rule)
}

func ruleStateProps(fnID, name string, props, rule map[string]any) error {
	state, hasState := props[samKeyState]
	enabled, hasEnabled := props[samKeyEnabled].(bool)
	if hasState && state != nil && hasEnabled {
		return samErr(fnID, "Event [%s] State and Enabled Properties cannot both be specified.", name)
	}
	if hasState && state != nil {
		rule[samKeyState] = state
	}
	if hasEnabled {
		rule[samKeyState] = samStateDisabled
		if enabled {
			rule[samKeyState] = samStateEnabled
		}
	}

	return nil
}

// putRule emits the Events::Rule with its single Lambda target and the permission that lets it invoke.
func (t *samTranslator) putRule(ev *samEventCtx, name string, props, rule map[string]any) error {
	id := ev.id + name
	ruleARN := samGetAtt(id, attrNameArn)
	var dlqARN any
	if cfg := asMap(props["DeadLetterConfig"]); cfg != nil {
		arn, err := t.eventBridgeDLQ(id, cfg, ruleARN, true)
		if err != nil {
			return err
		}
		dlqARN = arn
	}
	targetID := samTargetID(id)
	if tgt := asMap(props["Target"]); tgt != nil && tgt["Id"] != nil {
		targetID = asString(tgt["Id"])
	}
	target := map[string]any{attrNameArn: ev.fnArn(), "Id": targetID}
	for _, k := range []string{"Input", "InputPath", "RetryPolicy", "InputTransformer"} {
		if props[k] != nil {
			target[k] = props[k]
		}
	}
	if dlqARN != nil {
		target["DeadLetterConfig"] = map[string]any{attrNameArn: dlqARN}
	}
	rule["Targets"] = []any{target}
	if err := t.put(id, map[string]any{samKeyType: samTypeRule, samKeyProps: rule}); err != nil {
		return err
	}

	return t.put(samPermissionID(id, ""), lambdaPermission(ev.target(), samEventsPrincipl, ruleARN))
}

func (t *samTranslator) cwLogsEvent(ev *samEventCtx, name string, props map[string]any) error {
	id := ev.id + name
	if err := rejectUnknown(id, props, keySet(samKeyLogGroup, "FilterPattern")); err != nil {
		return err
	}
	for _, k := range []string{samKeyLogGroup, "FilterPattern"} {
		if props[k] == nil {
			return samErr(ev.id, "Event [%s] is missing required property '%s'.", name, k)
		}
	}
	permID := samPermissionID(id, "")
	src := map[string]any{samKeyFnSub: []any{
		"arn:${AWS::Partition}:logs:${AWS::Region}:${AWS::AccountId}:log-group:${__LogGroupName__}:*",
		map[string]any{"__LogGroupName__": props[samKeyLogGroup]},
	}}
	if err := t.put(permID, lambdaPermission(ev.target(), "logs.amazonaws.com", src)); err != nil {
		return err
	}

	return t.put(
		id,
		map[string]any{samKeyType: samTypeSubFilter, samKeyDependsOn: []any{permID}, samKeyProps: map[string]any{
			samKeyLogGroup: props[samKeyLogGroup], "FilterPattern": props["FilterPattern"],
			"DestinationArn": ev.fnArn(),
		}},
	)
}

func (t *samTranslator) iotRuleEvent(ev *samEventCtx, name string, props map[string]any) error {
	id := ev.id + name
	if err := rejectUnknown(id, props, keySet("Sql", "AwsIotSqlVersion")); err != nil {
		return err
	}
	if props["Sql"] == nil {
		return samErr(ev.id, "Event [%s] is missing required property 'Sql'.", name)
	}
	src := map[string]any{
		samKeyFnSub: []any{"arn:${AWS::Partition}:iot:${AWS::Region}:${AWS::AccountId}:rule/${RuleName}",
			map[string]any{samKRuleName: samRef(id)}},
	}
	perm := lambdaPermission(ev.target(), "iot.amazonaws.com", src)
	asMap(perm[samKeyProps])["SourceAccount"] = map[string]any{samKeyFnSub: "${AWS::AccountId}"}
	if err := t.put(samPermissionID(id, ""), perm); err != nil {
		return err
	}
	payload := map[string]any{
		"Sql": props["Sql"], "RuleDisabled": false,
		"Actions": []any{map[string]any{samDestLambda: map[string]any{"FunctionArn": ev.fnArn()}}},
	}
	if props["AwsIotSqlVersion"] != nil {
		payload["AwsIotSqlVersion"] = props["AwsIotSqlVersion"]
	}

	return t.put(
		id,
		map[string]any{samKeyType: samTypeIoTRule, samKeyProps: map[string]any{"TopicRulePayload": payload}},
	)
}

func (t *samTranslator) alexaEvent(ev *samEventCtx, name string, props map[string]any) error {
	if err := rejectUnknown(ev.id+name, props, keySet("SkillId")); err != nil {
		return err
	}
	perm := lambdaPermission(ev.target(), "alexa-appkit.amazon.com", nil)
	if props["SkillId"] != nil {
		asMap(perm[samKeyProps])["EventSourceToken"] = props["SkillId"]
	}

	return t.put(samPermissionID(ev.id+name, ""), perm)
}

// cognitoEvent adds the trigger to the UserPool's LambdaConfig, which is finalised with the other resources.
func (t *samTranslator) cognitoEvent(ev *samEventCtx, name string, props map[string]any) error {
	id := ev.id + name
	if err := rejectUnknown(id, props, keySet("UserPool", "Trigger")); err != nil {
		return err
	}
	pool, _ := asMap(props["UserPool"])[samKeyRef].(string)
	if pool == "" {
		return samErr(ev.id, "Event [%s] Cognito events must reference a Cognito UserPool in the same template.", name)
	}
	triggers := asList(props["Trigger"])
	if len(triggers) == 0 {
		return samErr(ev.id, "Event [%s] is missing required property 'Trigger'.", name)
	}
	perm := lambdaPermission(ev.target(), "cognito-idp.amazonaws.com", samGetAtt(pool, attrNameArn))
	if err := t.put(samPermissionID(ev.id+samEventCognito, ""), perm); err != nil {
		return err
	}
	for _, trig := range triggers {
		key, _ := trig.(string)
		t.cognito[pool] = append(t.cognito[pool], samCognitoTrigger{event: name, trigger: key, arn: ev.fnArn()})
	}

	return nil
}

type samCognitoTrigger struct {
	arn     any
	event   string
	trigger string
}

// finalizeCognito writes each collected trigger into its UserPool's LambdaConfig.
func (t *samTranslator) finalizeCognito() error {
	for _, id := range slices.Sorted(maps.Keys(t.cognito)) {
		pool := asMap(t.out[id])
		if pool[samKeyType] != samTypeUserPool {
			return samErr(id, "Cognito events must reference a Cognito UserPool in the same template.")
		}
		props := maps.Clone(asMap(pool[samKeyProps]))
		if props == nil {
			props = map[string]any{}
		}
		lambdaCfg := maps.Clone(asMap(props["LambdaConfig"]))
		if lambdaCfg == nil {
			lambdaCfg = map[string]any{}
		}
		for _, tr := range t.cognito[id] {
			if _, dup := lambdaCfg[tr.trigger]; dup {
				return samErr(id, "Cognito trigger \"%s\" defined multiple times.", tr.trigger)
			}
			lambdaCfg[tr.trigger] = tr.arn
		}
		props["LambdaConfig"] = lambdaCfg
		np := maps.Clone(pool)
		np[samKeyProps] = props
		t.out[id] = np
	}

	return nil
}
