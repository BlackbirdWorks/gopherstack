package iot

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

const (
	maxBatchRecords = 500
	maxRepublishHop = 8
	arnRegionField  = 3
	arnAccountField = 4
	arnFieldCount   = 6
	defaultPayload  = "payload"
	keyTypeNumber   = "NUMBER"
	opInsert        = "INSERT"
	opUpdate        = "UPDATE"
	opDelete        = "DELETE"
)

type actionRunner func(h *ruleHook, rule *TopicRule, msg *ruleMessage, raw json.RawMessage) error

type otherRunner struct {
	run  actionRunner
	name string
}

// otherRunners maps each untyped action key to its runner and AWS action name.
func otherRunners() map[string]otherRunner {
	return map[string]otherRunner{
		"republish":        {(*ruleHook).runRepublish, "RepublishAction"},
		"kinesis":          {(*ruleHook).runKinesis, "KinesisAction"},
		"firehose":         {(*ruleHook).runFirehose, "FirehoseAction"},
		"dynamoDB":         {(*ruleHook).runDynamoDB, "DynamoDBAction"},
		"dynamoDBv2":       {(*ruleHook).runDynamoDBv2, "DynamoDBv2Action"},
		"s3":               {(*ruleHook).runS3, "S3Action"},
		"cloudwatchMetric": {(*ruleHook).runCloudwatchMetric, "CloudwatchMetricAction"},
		"cloudwatchAlarm":  {(*ruleHook).runCloudwatchAlarm, "CloudwatchAlarmAction"},
		"cloudwatchLogs":   {(*ruleHook).runCloudwatchLogs, "CloudwatchLogsAction"},
		"stepFunctions":    {(*ruleHook).runStepFunctions, "StepFunctionsAction"},
		"iotAnalytics":     {(*ruleHook).runIoTAnalytics, "IotAnalyticsAction"},
		actionHTTP:         {(*ruleHook).runHTTP, "HttpAction"},
	}
}

// runOtherAction runs the action's untyped member; unmodelled action types are recorded only.
func (h *ruleHook) runOtherAction(rule *TopicRule, action RuleAction, msg *ruleMessage) (string, error) {
	runners := otherRunners()

	for _, key := range slices.Sorted(maps.Keys(action.Other)) {
		r, ok := runners[key]
		if !ok {
			continue
		}

		return r.name, r.run(h, rule, msg, action.Other[key])
	}

	return "", nil
}

func (h *ruleHook) allow(roleARN, resource string, actions ...string) error {
	auth := h.backend.ruleAuthorizer()

	for _, a := range actions {
		if err := roleauth.Authorize(auth, roleauth.PrincipalIoT, roleARN, a, resource); err != nil {
			return fmt.Errorf("%w: %w", errRuleActionDenied, err)
		}
	}

	return nil
}

func decodeAction(raw json.RawMessage, dst any) error {
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("decode action: %w", err)
	}

	return nil
}

// expandAll expands every template in fields in place.
func (m *ruleMessage) expandAll(fields ...*string) error {
	for _, f := range fields {
		v, err := m.expand(*f)
		if err != nil {
			return err
		}

		*f = v
	}

	return nil
}

func (h *ruleHook) runSNS(_ *TopicRule, a *SNSAction, msg *ruleMessage) error {
	topicARN := a.TargetARN

	if err := msg.expandAll(&topicARN); err != nil {
		return err
	}

	if err := h.allow(a.RoleARN, topicARN, "sns:Publish"); err != nil {
		return err
	}

	t := h.backend.actionTargets().SNS
	if t == nil {
		return ErrActionTargetUnavailable
	}

	jsonFormat := strings.EqualFold(a.MessageFormat, "JSON")

	return t.PublishToTopic(h.ctx, msg.region, topicARN, string(msg.payload), jsonFormat)
}

type kinesisWire struct {
	RoleARN      string `json:"roleArn"`
	StreamName   string `json:"streamName"`
	PartitionKey string `json:"partitionKey"`
}

func (h *ruleHook) runKinesis(_ *TopicRule, msg *ruleMessage, raw json.RawMessage) error {
	var w kinesisWire
	if err := decodeAction(raw, &w); err != nil {
		return err
	}

	if err := msg.expandAll(&w.StreamName, &w.PartitionKey); err != nil {
		return err
	}

	if w.PartitionKey == "" {
		w.PartitionKey = uuid.NewString()
	}

	res := arn.Build("kinesis", msg.region, msg.account, "stream/"+w.StreamName)
	if err := h.allow(w.RoleARN, res, "kinesis:PutRecord"); err != nil {
		return err
	}

	t := h.backend.actionTargets().Kinesis
	if t == nil {
		return ErrActionTargetUnavailable
	}

	return t.PutRecord(h.ctx, msg.region, w.StreamName, w.PartitionKey, msg.payload)
}

type firehoseWire struct {
	RoleARN            string `json:"roleArn"`
	DeliveryStreamName string `json:"deliveryStreamName"`
	Separator          string `json:"separator"`
	BatchMode          bool   `json:"batchMode"`
}

func (h *ruleHook) runFirehose(_ *TopicRule, msg *ruleMessage, raw json.RawMessage) error {
	var w firehoseWire
	if err := decodeAction(raw, &w); err != nil {
		return err
	}

	if err := msg.expandAll(&w.DeliveryStreamName); err != nil {
		return err
	}

	parts, err := msg.batchElements(w.BatchMode)
	if err != nil {
		return err
	}

	records := make([][]byte, len(parts))
	for i, p := range parts {
		records[i] = append(slices.Clone(p), w.Separator...)
	}

	perm := "firehose:PutRecord"
	if w.BatchMode {
		perm = "firehose:PutRecordBatch"
	}

	res := arn.Build("firehose", msg.region, msg.account, "deliverystream/"+w.DeliveryStreamName)
	if aerr := h.allow(w.RoleARN, res, perm); aerr != nil {
		return aerr
	}

	t := h.backend.actionTargets().Firehose
	if t == nil {
		return ErrActionTargetUnavailable
	}

	return t.PutRecords(h.ctx, msg.region, w.DeliveryStreamName, records)
}

// batchElements returns the whole payload, or each element of a JSON array payload when batch is set.
func (m *ruleMessage) batchElements(batch bool) ([][]byte, error) {
	if !batch {
		return [][]byte{m.payload}, nil
	}

	if !bytes.HasPrefix(bytes.TrimSpace(m.payload), []byte("[")) {
		return [][]byte{m.payload}, nil
	}

	var elems []json.RawMessage
	if err := json.Unmarshal(m.payload, &elems); err != nil {
		return nil, fmt.Errorf("%w: payload is not a valid JSON array", errTemplate)
	}

	if len(elems) == 0 || len(elems) > maxBatchRecords {
		return nil, fmt.Errorf("%w: batch must hold 1 to %d records", errTemplate, maxBatchRecords)
	}

	out := make([][]byte, len(elems))
	for i, e := range elems {
		var s string
		if json.Unmarshal(e, &s) == nil {
			out[i] = []byte(s)

			continue
		}

		out[i] = bytes.Clone(e)
	}

	return out, nil
}

type dynamoDBWire struct {
	RoleARN       string `json:"roleArn"`
	TableName     string `json:"tableName"`
	HashKeyField  string `json:"hashKeyField"`
	HashKeyValue  string `json:"hashKeyValue"`
	HashKeyType   string `json:"hashKeyType"`
	RangeKeyField string `json:"rangeKeyField"`
	RangeKeyValue string `json:"rangeKeyValue"`
	RangeKeyType  string `json:"rangeKeyType"`
	PayloadField  string `json:"payloadField"`
	Operation     string `json:"operation"`
}

func (h *ruleHook) runDynamoDB(_ *TopicRule, msg *ruleMessage, raw json.RawMessage) error {
	var w dynamoDBWire
	if err := decodeAction(raw, &w); err != nil {
		return err
	}

	if err := msg.expandAll(&w.TableName, &w.HashKeyField, &w.HashKeyValue, &w.HashKeyType,
		&w.RangeKeyField, &w.RangeKeyValue, &w.RangeKeyType, &w.PayloadField, &w.Operation); err != nil {
		return err
	}

	key := map[string]any{w.HashKeyField: keyAttribute(w.HashKeyType, w.HashKeyValue)}
	if w.RangeKeyField != "" {
		key[w.RangeKeyField] = keyAttribute(w.RangeKeyType, w.RangeKeyValue)
	}

	if w.PayloadField == "" {
		w.PayloadField = defaultPayload
	}

	op := strings.ToUpper(w.Operation)
	if op == "" {
		op = opInsert
	}

	perm, ok := map[string]string{
		opInsert: "dynamodb:PutItem", opUpdate: "dynamodb:UpdateItem", opDelete: "dynamodb:DeleteItem",
	}[op]
	if !ok {
		return fmt.Errorf("%w: unsupported dynamoDB operation %q", errTemplate, w.Operation)
	}

	res := arn.Build("dynamodb", msg.region, msg.account, "table/"+w.TableName)
	if err := h.allow(w.RoleARN, res, perm); err != nil {
		return err
	}

	t := h.backend.actionTargets().DynamoDB
	if t == nil {
		return ErrActionTargetUnavailable
	}

	return h.applyDynamoOp(t, msg, w, op, key)
}

func (h *ruleHook) applyDynamoOp(
	t DynamoItemWriter, msg *ruleMessage, w dynamoDBWire, op string, key map[string]any,
) error {
	switch op {
	case opDelete:
		return t.DeleteItem(h.ctx, msg.region, w.TableName, key)
	case opUpdate:
		return t.SetAttribute(h.ctx, msg.region, w.TableName, key, w.PayloadField, payloadAttribute(msg.payload))
	default:
		item := map[string]any{w.PayloadField: payloadAttribute(msg.payload)}
		maps.Copy(item, key)

		return t.PutItem(h.ctx, msg.region, w.TableName, item)
	}
}

func keyAttribute(keyType, value string) map[string]any {
	if strings.EqualFold(keyType, keyTypeNumber) {
		return map[string]any{"N": value}
	}

	return map[string]any{"S": value}
}

// payloadAttribute stores a JSON object as a map and anything else as binary.
func payloadAttribute(payload []byte) map[string]any {
	var obj map[string]any

	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()

	if err := dec.Decode(&obj); err == nil && obj != nil {
		return jsonToAttribute(obj)
	}

	return map[string]any{"B": base64.StdEncoding.EncodeToString(payload)}
}

func jsonToAttribute(v any) map[string]any {
	switch t := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(t))
		for k, e := range t {
			m[k] = jsonToAttribute(e)
		}

		return map[string]any{"M": m}
	case []any:
		l := make([]any, len(t))
		for i, e := range t {
			l[i] = jsonToAttribute(e)
		}

		return map[string]any{"L": l}
	case json.Number:
		return map[string]any{"N": t.String()}
	case string:
		return map[string]any{"S": t}
	case bool:
		return map[string]any{"BOOL": t}
	default:
		return map[string]any{"NULL": true}
	}
}

type dynamoDBv2Wire struct {
	RoleARN string `json:"roleArn"`
	PutItem struct {
		TableName string `json:"tableName"`
	} `json:"putItem"`
}

func (h *ruleHook) runDynamoDBv2(_ *TopicRule, msg *ruleMessage, raw json.RawMessage) error {
	var w dynamoDBv2Wire
	if err := decodeAction(raw, &w); err != nil {
		return err
	}

	if err := msg.expandAll(&w.PutItem.TableName); err != nil {
		return err
	}

	attr := payloadAttribute(msg.payload)

	item, ok := attr["M"].(map[string]any)
	if !ok {
		return fmt.Errorf("%w: dynamoDBv2 needs a JSON object payload", errTemplate)
	}

	res := arn.Build("dynamodb", msg.region, msg.account, "table/"+w.PutItem.TableName)
	if err := h.allow(w.RoleARN, res, "dynamodb:PutItem"); err != nil {
		return err
	}

	t := h.backend.actionTargets().DynamoDB
	if t == nil {
		return ErrActionTargetUnavailable
	}

	return t.PutItem(h.ctx, msg.region, w.PutItem.TableName, item)
}

type s3Wire struct {
	RoleARN    string `json:"roleArn"`
	BucketName string `json:"bucketName"`
	Key        string `json:"key"`
	CannedACL  string `json:"cannedAcl"`
}

func (h *ruleHook) runS3(_ *TopicRule, msg *ruleMessage, raw json.RawMessage) error {
	var w s3Wire
	if err := decodeAction(raw, &w); err != nil {
		return err
	}

	if err := msg.expandAll(&w.BucketName, &w.Key); err != nil {
		return err
	}

	if err := h.allow(w.RoleARN, arn.BuildS3(w.BucketName+"/"+w.Key), "s3:PutObject"); err != nil {
		return err
	}

	t := h.backend.actionTargets().S3
	if t == nil {
		return ErrActionTargetUnavailable
	}

	return t.PutObject(h.ctx, msg.region, w.BucketName, w.Key, msg.payload, w.CannedACL)
}

type cloudwatchMetricWire struct {
	RoleARN         string `json:"roleArn"`
	MetricNamespace string `json:"metricNamespace"`
	MetricName      string `json:"metricName"`
	MetricValue     string `json:"metricValue"`
	MetricUnit      string `json:"metricUnit"`
	MetricTimestamp string `json:"metricTimestamp"`
}

func (h *ruleHook) runCloudwatchMetric(_ *TopicRule, msg *ruleMessage, raw json.RawMessage) error {
	var w cloudwatchMetricWire
	if err := decodeAction(raw, &w); err != nil {
		return err
	}

	if err := msg.expandAll(&w.MetricNamespace, &w.MetricName, &w.MetricValue,
		&w.MetricUnit, &w.MetricTimestamp); err != nil {
		return err
	}

	value, err := strconv.ParseFloat(strings.TrimSpace(w.MetricValue), 64)
	if err != nil {
		return fmt.Errorf("%w: metricValue is not a number", errTemplate)
	}

	ts := msg.received
	if w.MetricTimestamp != "" {
		secs, perr := strconv.ParseInt(strings.TrimSpace(w.MetricTimestamp), 10, 64)
		if perr != nil {
			return fmt.Errorf("%w: metricTimestamp is not epoch seconds", errTemplate)
		}

		ts = time.Unix(secs, 0).UTC()
	}

	if aerr := h.allow(w.RoleARN, "*", "cloudwatch:PutMetricData"); aerr != nil {
		return aerr
	}

	t := h.backend.actionTargets().Metrics
	if t == nil {
		return ErrActionTargetUnavailable
	}

	return t.PutMetric(h.ctx, msg.region, w.MetricNamespace, w.MetricName, w.MetricUnit, value, ts)
}

type cloudwatchAlarmWire struct {
	RoleARN     string `json:"roleArn"`
	AlarmName   string `json:"alarmName"`
	StateReason string `json:"stateReason"`
	StateValue  string `json:"stateValue"`
}

func (h *ruleHook) runCloudwatchAlarm(_ *TopicRule, msg *ruleMessage, raw json.RawMessage) error {
	var w cloudwatchAlarmWire
	if err := decodeAction(raw, &w); err != nil {
		return err
	}

	if err := msg.expandAll(&w.AlarmName, &w.StateReason, &w.StateValue); err != nil {
		return err
	}

	res := arn.Build("cloudwatch", msg.region, msg.account, "alarm:"+w.AlarmName)
	if err := h.allow(w.RoleARN, res, "cloudwatch:SetAlarmState"); err != nil {
		return err
	}

	t := h.backend.actionTargets().Alarms
	if t == nil {
		return ErrActionTargetUnavailable
	}

	return t.SetAlarmState(h.ctx, msg.region, w.AlarmName, w.StateValue, w.StateReason)
}

type cloudwatchLogsWire struct {
	RoleARN      string `json:"roleArn"`
	LogGroupName string `json:"logGroupName"`
	BatchMode    bool   `json:"batchMode"`
}

type batchedLogEvent struct {
	Timestamp *int64  `json:"timestamp"`
	Message   *string `json:"message"`
}

func (h *ruleHook) runCloudwatchLogs(rule *TopicRule, msg *ruleMessage, raw json.RawMessage) error {
	var w cloudwatchLogsWire
	if err := decodeAction(raw, &w); err != nil {
		return err
	}

	if err := msg.expandAll(&w.LogGroupName); err != nil {
		return err
	}

	events, err := msg.logEvents(w.BatchMode)
	if err != nil {
		return err
	}

	res := arn.Build("logs", msg.region, msg.account, "log-group:"+w.LogGroupName+":*")
	if aerr := h.allow(w.RoleARN, res,
		"logs:CreateLogStream", "logs:DescribeLogStreams", "logs:PutLogEvents"); aerr != nil {
		return aerr
	}

	t := h.backend.actionTargets().Logs
	if t == nil {
		return ErrActionTargetUnavailable
	}

	return t.PutLogEvents(h.ctx, msg.region, w.LogGroupName, rule.RuleName, events)
}

// logEvents returns one event for the payload, or one per {timestamp,message} element in batch mode.
func (m *ruleMessage) logEvents(batch bool) ([]LogEvent, error) {
	now := m.received.UnixMilli()
	if !batch {
		return []LogEvent{{Message: string(m.payload), Timestamp: now}}, nil
	}

	var elems []batchedLogEvent
	if err := json.Unmarshal(m.payload, &elems); err != nil {
		return nil, fmt.Errorf("%w: batchMode needs an array of {timestamp,message}", errTemplate)
	}

	if len(elems) == 0 || len(elems) > maxBatchRecords {
		return nil, fmt.Errorf("%w: batch must hold 1 to %d records", errTemplate, maxBatchRecords)
	}

	out := make([]LogEvent, len(elems))

	for i, e := range elems {
		if e.Message == nil || e.Timestamp == nil {
			return nil, fmt.Errorf("%w: batch element %d needs timestamp and message", errTemplate, i)
		}

		out[i] = LogEvent{Message: *e.Message, Timestamp: *e.Timestamp}
	}

	return out, nil
}

type stepFunctionsWire struct {
	RoleARN             string `json:"roleArn"`
	StateMachineName    string `json:"stateMachineName"`
	ExecutionNamePrefix string `json:"executionNamePrefix"`
}

func (h *ruleHook) runStepFunctions(_ *TopicRule, msg *ruleMessage, raw json.RawMessage) error {
	var w stepFunctionsWire
	if err := decodeAction(raw, &w); err != nil {
		return err
	}

	if err := msg.expandAll(&w.StateMachineName, &w.ExecutionNamePrefix); err != nil {
		return err
	}

	res := arn.Build("states", msg.region, msg.account, "stateMachine:"+w.StateMachineName)
	if err := h.allow(w.RoleARN, res, "states:StartExecution"); err != nil {
		return err
	}

	t := h.backend.actionTargets().StepFunctions
	if t == nil {
		return ErrActionTargetUnavailable
	}

	name := w.ExecutionNamePrefix + uuid.NewString()

	return t.StartExecution(h.ctx, msg.region, msg.account, w.StateMachineName, name, string(msg.payload))
}

type iotAnalyticsWire struct {
	RoleARN     string `json:"roleArn"`
	ChannelName string `json:"channelName"`
	ChannelARN  string `json:"channelArn"`
	BatchMode   bool   `json:"batchMode"`
}

func (h *ruleHook) runIoTAnalytics(_ *TopicRule, msg *ruleMessage, raw json.RawMessage) error {
	var w iotAnalyticsWire
	if err := decodeAction(raw, &w); err != nil {
		return err
	}

	if err := msg.expandAll(&w.ChannelName, &w.ChannelARN); err != nil {
		return err
	}

	if w.ChannelName == "" {
		w.ChannelName = w.ChannelARN[strings.LastIndex(w.ChannelARN, "/")+1:]
	}

	payloads, err := msg.batchElements(w.BatchMode)
	if err != nil {
		return err
	}

	res := arn.Build("iotanalytics", msg.region, msg.account, "channel/"+w.ChannelName)
	if aerr := h.allow(w.RoleARN, res, "iotanalytics:BatchPutMessage"); aerr != nil {
		return aerr
	}

	t := h.backend.actionTargets().Analytics
	if t == nil {
		return ErrActionTargetUnavailable
	}

	return t.PutChannelMessages(h.ctx, msg.region, w.ChannelName, payloads)
}

// ruleRegionAccount returns the region and account a rule ARN names.
func ruleRegionAccount(ruleARN string) (string, string) {
	parts := strings.SplitN(ruleARN, ":", arnFieldCount)
	if len(parts) < arnFieldCount {
		return "", ""
	}

	return parts[arnRegionField], parts[arnAccountField]
}
