package replicator

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"slices"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func sqsCreateAttrs() []sqstypes.QueueAttributeName {
	return []sqstypes.QueueAttributeName{
		sqstypes.QueueAttributeNameDelaySeconds, sqstypes.QueueAttributeNameMaximumMessageSize,
		sqstypes.QueueAttributeNameMessageRetentionPeriod, sqstypes.QueueAttributeNamePolicy,
		sqstypes.QueueAttributeNameReceiveMessageWaitTimeSeconds, sqstypes.QueueAttributeNameRedrivePolicy,
		sqstypes.QueueAttributeNameVisibilityTimeout, sqstypes.QueueAttributeNameKmsMasterKeyId,
		sqstypes.QueueAttributeNameKmsDataKeyReusePeriodSeconds, sqstypes.QueueAttributeNameSqsManagedSseEnabled,
		sqstypes.QueueAttributeNameFifoQueue, sqstypes.QueueAttributeNameContentBasedDeduplication,
		sqstypes.QueueAttributeNameDeduplicationScope, sqstypes.QueueAttributeNameFifoThroughputLimit,
		sqstypes.QueueAttributeNameRedriveAllowPolicy,
	}
}

func sqsRemapped(k sqstypes.QueueAttributeName) bool {
	return slices.Contains([]sqstypes.QueueAttributeName{
		sqstypes.QueueAttributeNamePolicy, sqstypes.QueueAttributeNameRedrivePolicy,
		sqstypes.QueueAttributeNameRedriveAllowPolicy, sqstypes.QueueAttributeNameKmsMasterKeyId,
	}, k)
}

type queueItem struct {
	attrs map[string]string
	tags  map[string]string
	name  string
	dlq   string
}

// OrderQueues returns names with every dead-letter queue ahead of the queues that redrive to it.
// dlqOf maps a queue name to its DLQ name ("" when none); cycles are broken arbitrarily.
func OrderQueues(names []string, dlqOf map[string]string) []string {
	out := make([]string, 0, len(names))
	state := make(map[string]bool, len(names))

	var visit func(string)

	visit = func(n string) {
		if _, seen := state[n]; seen {
			return
		}

		state[n] = true

		if d := dlqOf[n]; d != "" {
			if _, ok := dlqOf[d]; ok {
				visit(d)
			}
		}

		out = append(out, n)
	}

	for _, n := range names {
		visit(n)
	}

	return out
}

func dlqName(redrive string) string {
	if redrive == "" {
		return ""
	}

	var rp struct {
		Arn string `json:"deadLetterTargetArn"`
	}

	if json.Unmarshal([]byte(redrive), &rp) != nil || rp.Arn == "" {
		return ""
	}

	return lastARNSegment(rp.Arn)
}

func lastARNSegment(a string) string {
	for i := len(a) - 1; i >= 0; i-- {
		if a[i] == ':' {
			return a[i+1:]
		}
	}

	return a
}

func (r *run) replicateSQS(ctx context.Context) {
	items := map[string]*queueItem{}

	var names []string

	pager := sqs.NewListQueuesPaginator(r.src.sqs, &sqs.ListQueuesInput{})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			r.listFail(TypeSQS, err)

			return
		}

		for _, u := range page.QueueUrls {
			name := path.Base(u)
			if !r.match(name) {
				continue
			}

			it, qerr := r.readQueue(ctx, u, name)
			if qerr != nil {
				r.fail(TypeSQS, name, qerr)

				continue
			}

			items[name] = it
			names = append(names, name)
		}
	}

	dlqOf := make(map[string]string, len(items))
	for n, it := range items {
		dlqOf[n] = it.dlq
	}

	for _, n := range OrderQueues(names, dlqOf) {
		r.applyQueue(ctx, items[n])
	}
}

func (r *run) readQueue(ctx context.Context, url, name string) (*queueItem, error) {
	attrs, err := r.src.sqs.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: &url, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll},
	})
	if err != nil {
		return nil, fmt.Errorf("get attributes: %w", err)
	}

	tags, err := r.src.sqs.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: &url})
	if err != nil {
		return nil, fmt.Errorf("list tags: %w", err)
	}

	it := &queueItem{name: name, attrs: map[string]string{}, tags: tags.Tags}

	for _, k := range sqsCreateAttrs() {
		v, ok := attrs.Attributes[string(k)]
		if !ok || v == "" {
			continue
		}

		if sqsRemapped(k) {
			v = r.rm.Text(v)
		}

		it.attrs[string(k)] = v
	}

	it.dlq = dlqName(it.attrs[string(sqstypes.QueueAttributeNameRedrivePolicy)])

	return it, nil
}

func (r *run) applyQueue(ctx context.Context, it *queueItem) {
	out, err := r.dst.sqs.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &it.name})

	exists, err := existsErr(err)
	if err != nil {
		r.fail(TypeSQS, it.name, err)

		return
	}

	if r.cfg.DryRun {
		r.add(TypeSQS, it.name, upsert(exists), "")

		return
	}

	if !exists {
		_, err = r.dst.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{
			QueueName: &it.name, Attributes: it.attrs, Tags: it.tags,
		})
		r.done(TypeSQS, it.name, ActionCreated, err)

		return
	}

	set := make(map[string]string, len(it.attrs))

	for k, v := range it.attrs {
		if k != string(sqstypes.QueueAttributeNameFifoQueue) {
			set[k] = v
		}
	}

	if _, err = r.dst.sqs.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl: out.QueueUrl, Attributes: set,
	}); err == nil && len(it.tags) > 0 {
		_, err = r.dst.sqs.TagQueue(ctx, &sqs.TagQueueInput{QueueUrl: out.QueueUrl, Tags: it.tags})
	}

	r.done(TypeSQS, it.name, ActionUpdated, err)
}

func (r *run) done(typ, id, action string, err error) {
	if err != nil {
		r.fail(typ, id, err)

		return
	}

	r.add(typ, id, action, "")
}
