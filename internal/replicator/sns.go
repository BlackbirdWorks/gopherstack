package replicator

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"

	"github.com/blackbirdworks/gopherstack/internal/awswire/sns"
)

const pendingConfirmation = "PendingConfirmation"

func snsTopicAttrs() []string {
	return []string{
		"DisplayName", "Policy", "DeliveryPolicy", "KmsMasterKeyId", "FifoTopic",
		"ContentBasedDeduplication", "SignatureVersion", "TracingConfig",
	}
}

func snsSubAttrs() []string {
	return []string{
		"RawMessageDelivery", "FilterPolicy", "FilterPolicyScope", "RedrivePolicy", "DeliveryPolicy",
		"SubscriptionRoleArn",
	}
}

func remapsEndpoint(protocol string) bool {
	return protocol == "sqs" || protocol == "lambda" || protocol == "firehose"
}

func (r *run) replicateSNS(ctx context.Context) {
	pager := sns.NewListTopicsPaginator(r.src.sns, &sns.ListTopicsInput{})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			r.listFail(TypeSNS, err)

			return
		}

		for _, t := range page.Topics {
			arnStr := aws.ToString(t.TopicArn)

			name := lastARNSegment(arnStr)
			if r.match(name, arnStr) {
				r.copyTopic(ctx, arnStr, name)
			}
		}
	}
}

func (r *run) copyTopic(ctx context.Context, srcARN, name string) {
	got, err := r.src.sns.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: &srcARN})
	if err != nil {
		r.fail(TypeSNS, name, err)

		return
	}

	tags, err := r.src.sns.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{ResourceArn: &srcARN})
	if err != nil {
		r.fail(TypeSNS, name, fmt.Errorf("list tags: %w", err))

		return
	}

	attrs := map[string]string{}

	for _, k := range snsTopicAttrs() {
		if v := got.Attributes[k]; v != "" {
			attrs[k] = r.rm.Text(v)
		}
	}

	dstARN := r.rm.ARN(srcARN)
	_, err = r.dst.sns.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: &dstARN})

	exists, err := existsErr(err)
	if err != nil {
		r.fail(TypeSNS, name, err)

		return
	}

	if !r.cfg.DryRun {
		if err = r.writeTopic(ctx, name, dstARN, exists, attrs, tags.Tags); err != nil {
			r.fail(TypeSNS, name, err)

			return
		}
	}

	r.add(TypeSNS, name, upsert(exists), "")
	r.copySubscriptions(ctx, srcARN, dstARN, name, exists)
}

func (r *run) writeTopic(
	ctx context.Context, name, dstARN string, exists bool, attrs map[string]string, tags []snstypes.Tag,
) error {
	if !exists {
		_, err := r.dst.sns.CreateTopic(ctx, &sns.CreateTopicInput{Name: &name, Attributes: attrs, Tags: tags})

		return err
	}

	for k, v := range attrs {
		if k == "FifoTopic" {
			continue
		}

		if _, err := r.dst.sns.SetTopicAttributes(ctx, &sns.SetTopicAttributesInput{
			TopicArn: &dstARN, AttributeName: aws.String(k), AttributeValue: aws.String(v),
		}); err != nil {
			return fmt.Errorf("set %s: %w", k, err)
		}
	}

	if len(tags) > 0 {
		if _, err := r.dst.sns.TagResource(ctx, &sns.TagResourceInput{ResourceArn: &dstARN, Tags: tags}); err != nil {
			return fmt.Errorf("tag: %w", err)
		}
	}

	return nil
}

func (r *run) copySubscriptions(ctx context.Context, srcARN, dstARN, topic string, topicExists bool) {
	existing := map[string]string{}

	if topicExists {
		pager := sns.NewListSubscriptionsByTopicPaginator(
			r.dst.sns,
			&sns.ListSubscriptionsByTopicInput{TopicArn: &dstARN},
		)
		for pager.HasMorePages() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				r.fail("sns-subscription", topic, err)

				return
			}

			for _, s := range page.Subscriptions {
				existing[aws.ToString(s.Protocol)+"|"+aws.ToString(s.Endpoint)] = aws.ToString(s.SubscriptionArn)
			}
		}
	}

	pager := sns.NewListSubscriptionsByTopicPaginator(r.src.sns, &sns.ListSubscriptionsByTopicInput{TopicArn: &srcARN})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			r.fail("sns-subscription", topic, err)

			return
		}

		for _, s := range page.Subscriptions {
			r.copySubscription(ctx, s, dstARN, topic, existing)
		}
	}
}

func (r *run) copySubscription(
	ctx context.Context, s snstypes.Subscription, dstTopic, topic string, existing map[string]string,
) {
	proto, endpoint := aws.ToString(s.Protocol), aws.ToString(s.Endpoint)
	id := topic + "/" + proto + ":" + endpoint
	subARN := aws.ToString(s.SubscriptionArn)

	if subARN == pendingConfirmation || strings.HasSuffix(subARN, pendingConfirmation) {
		r.add("sns-subscription", id, ActionSkipped, "pending confirmation")

		return
	}

	if remapsEndpoint(proto) {
		endpoint = r.rm.ARN(endpoint)
	}

	attrs, err := r.src.sns.GetSubscriptionAttributes(
		ctx,
		&sns.GetSubscriptionAttributesInput{SubscriptionArn: &subARN},
	)
	if err != nil {
		r.fail("sns-subscription", id, err)

		return
	}

	want := map[string]string{}

	for _, k := range snsSubAttrs() {
		if v := attrs.Attributes[k]; v != "" {
			want[k] = r.rm.Text(v)
		}
	}

	dstSub, exists := existing[proto+"|"+endpoint]
	if r.cfg.DryRun {
		r.add("sns-subscription", id, upsert(exists), "")

		return
	}

	if !exists {
		_, err = r.dst.sns.Subscribe(ctx, &sns.SubscribeInput{
			TopicArn: &dstTopic, Protocol: &proto, Endpoint: &endpoint,
			Attributes: want, ReturnSubscriptionArn: true,
		})
		r.done("sns-subscription", id, ActionCreated, err)

		return
	}

	for k, v := range want {
		if _, err = r.dst.sns.SetSubscriptionAttributes(ctx, &sns.SetSubscriptionAttributesInput{
			SubscriptionArn: &dstSub, AttributeName: aws.String(k), AttributeValue: aws.String(v),
		}); err != nil {
			r.fail("sns-subscription", id, err)

			return
		}
	}

	r.add("sns-subscription", id, ActionUpdated, "")
}
