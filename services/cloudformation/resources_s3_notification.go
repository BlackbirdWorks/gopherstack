package cloudformation

import (
	"context"
	"encoding/xml"
	"fmt"

	"github.com/google/uuid"
)

type cfnNotifRule struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

type cfnNotifKey struct {
	Rules []cfnNotifRule `xml:"FilterRule"`
}

type cfnNotifFilter struct {
	S3Key cfnNotifKey `xml:"S3Key"`
}

type cfnNotifConfig struct {
	Filter   *cfnNotifFilter `xml:"Filter,omitempty"`
	ID       string          `xml:"Id"`
	Queue    string          `xml:"Queue,omitempty"`
	Topic    string          `xml:"Topic,omitempty"`
	Function string          `xml:"CloudFunction,omitempty"`
	Event    string          `xml:"Event"`
}

type cfnQueueConfig struct {
	cfnNotifConfig
	XMLName xml.Name `xml:"QueueConfiguration"`
}

type cfnTopicConfig struct {
	cfnNotifConfig
	XMLName xml.Name `xml:"TopicConfiguration"`
}

type cfnFunctionConfig struct {
	cfnNotifConfig
	XMLName xml.Name `xml:"CloudFunctionConfiguration"`
}

type cfnNotifDoc struct {
	XMLName     xml.Name            `xml:"NotificationConfiguration"`
	EventBridge *struct{}           `xml:"EventBridgeConfiguration"`
	Queues      []cfnQueueConfig    `xml:""`
	Topics      []cfnTopicConfig    `xml:""`
	Functions   []cfnFunctionConfig `xml:""`
}

// applyBucketNotifications stores AWS::S3::Bucket NotificationConfiguration
// (Lambda/Queue/Topic configurations and EventBridgeConfiguration) on the bucket.
func (rc *ResourceCreator) applyBucketNotifications(
	ctx context.Context, bucket string, props map[string]any, params, physicalIDs map[string]string,
) error {
	nc := asMap(resolveDeep(props["NotificationConfiguration"], params, physicalIDs))
	if len(nc) == 0 {
		return nil
	}

	doc := cfnNotifDoc{}
	for _, c := range asList(nc["LambdaConfigurations"]) {
		doc.Functions = append(doc.Functions, cfnFunctionConfig{cfnNotifConfig: notifConfig(asMap(c), samKeyFunction)})
	}
	for _, c := range asList(nc["QueueConfigurations"]) {
		doc.Queues = append(doc.Queues, cfnQueueConfig{cfnNotifConfig: notifConfig(asMap(c), "Queue")})
	}
	for _, c := range asList(nc["TopicConfigurations"]) {
		doc.Topics = append(doc.Topics, cfnTopicConfig{cfnNotifConfig: notifConfig(asMap(c), "Topic")})
	}
	if eb := asMap(nc["EventBridgeConfiguration"]); eb["EventBridgeEnabled"] == true {
		doc.EventBridge = &struct{}{}
	}

	raw, err := xml.Marshal(doc)
	if err != nil {
		return fmt.Errorf("encode S3 notification configuration: %w", err)
	}
	if err = rc.backends.S3.Backend.PutBucketNotificationConfiguration(ctx, bucket, string(raw)); err != nil {
		return fmt.Errorf("apply S3 notification configuration to %s: %w", bucket, err)
	}

	return nil
}

func notifConfig(c map[string]any, targetKey string) cfnNotifConfig {
	out := cfnNotifConfig{ID: uuid.NewString()}
	target, _ := c[targetKey].(string)
	event, _ := c["Event"].(string)
	out.Event = event
	switch targetKey {
	case samKeyFunction:
		out.Function = target
	case "Queue":
		out.Queue = target
	default:
		out.Topic = target
	}
	if rules := asList(asMap(asMap(c["Filter"])["S3Key"])["Rules"]); len(rules) > 0 {
		f := &cfnNotifFilter{}
		for _, r := range rules {
			rm := asMap(r)
			name, _ := rm["Name"].(string)
			val, _ := rm["Value"].(string)
			f.S3Key.Rules = append(f.S3Key.Rules, cfnNotifRule{Name: name, Value: val})
		}
		out.Filter = f
	}

	return out
}
