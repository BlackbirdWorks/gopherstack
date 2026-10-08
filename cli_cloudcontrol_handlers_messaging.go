package main

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	ecrtypes "github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	kintypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/ssm"

	cloudcontrolbackend "github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

// ccSyncModelTags diffs the Tags property of current against desired and applies it with set/unset.
func ccSyncModelTags(
	current, desired map[string]any,
	set func(map[string]string) error, unset func([]string) error,
) error {
	have, err := ccModelTags(current)
	if err != nil {
		return err
	}

	want, err := ccModelTags(desired)
	if err != nil {
		return err
	}

	add, remove := ccTagDelta(have, want)

	if len(add) > 0 {
		if err = set(add); err != nil {
			return ccMapError(err)
		}
	}

	if len(remove) > 0 {
		return ccMapError(unset(remove))
	}

	return nil
}

// --- AWS::SNS::Topic ---

type ccTopic struct{ client *sns.Client }

func ccSNSTags(m map[string]string) []snstypes.Tag {
	out := make([]snstypes.Tag, 0, len(m))
	for k, v := range m {
		out = append(out, snstypes.Tag{Key: aws.String(k), Value: aws.String(v)})
	}

	return out
}

func (h *ccTopic) Create(ctx context.Context, desired map[string]any) (string, error) {
	fifo, _ := desired["FifoTopic"].(bool)

	name := ccString(desired, "TopicName")
	if name == "" {
		name = ccGeneratedName("cc-topic-")
		if fifo {
			name += ".fifo"
		}
	}

	tags, err := ccModelTags(desired)
	if err != nil {
		return "", err
	}

	attrs := map[string]string{}
	if fifo {
		attrs["FifoTopic"] = ccTrue
	}

	for _, k := range []string{ccDisplayName, ccKmsMasterKeyID} {
		if v := ccString(desired, k); v != "" {
			attrs[k] = v
		}
	}

	if v, ok := desired["ContentBasedDeduplication"].(bool); ok && v {
		attrs["ContentBasedDeduplication"] = ccTrue
	}

	out, err := h.client.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(name), Attributes: attrs, Tags: ccSNSTags(tags),
	})
	if err != nil {
		return "", ccMapError(err)
	}

	return aws.ToString(out.TopicArn), nil
}

func (h *ccTopic) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(id)})
	if err != nil {
		return nil, ccMapError(err)
	}

	model := map[string]any{"TopicArn": id, "TopicName": id[strings.LastIndex(id, ":")+1:]}

	for _, k := range []string{ccDisplayName, ccKmsMasterKeyID} {
		if v := out.Attributes[k]; v != "" {
			model[k] = v
		}
	}

	if out.Attributes["FifoTopic"] == ccTrue {
		model["FifoTopic"] = true
		model["ContentBasedDeduplication"] = out.Attributes["ContentBasedDeduplication"] == ccTrue
	}

	tags, err := h.client.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{ResourceArn: aws.String(id)})
	if err == nil && len(tags.Tags) > 0 {
		m := make(map[string]string, len(tags.Tags))
		for _, t := range tags.Tags {
			m[aws.ToString(t.Key)] = aws.ToString(t.Value)
		}

		model["Tags"] = tagsProperty(m)
	}

	return model, nil
}

func (h *ccTopic) Update(ctx context.Context, id string, current, desired map[string]any) error {
	if err := ccRejectUnsupportedChanges(
		current, desired, ccDisplayName, ccKmsMasterKeyID, "ContentBasedDeduplication", "Tags",
	); err != nil {
		return err
	}

	for _, k := range []string{ccDisplayName, ccKmsMasterKeyID} {
		if v := ccString(desired, k); v != ccString(current, k) {
			if _, err := h.client.SetTopicAttributes(ctx, &sns.SetTopicAttributesInput{
				TopicArn: aws.String(id), AttributeName: aws.String(k), AttributeValue: aws.String(v),
			}); err != nil {
				return ccMapError(err)
			}
		}
	}

	if v, ok := desired["ContentBasedDeduplication"].(bool); ok && v != (current["ContentBasedDeduplication"] == true) {
		if _, err := h.client.SetTopicAttributes(ctx, &sns.SetTopicAttributesInput{
			TopicArn: aws.String(id), AttributeName: aws.String("ContentBasedDeduplication"),
			AttributeValue: aws.String(strconv.FormatBool(v)),
		}); err != nil {
			return ccMapError(err)
		}
	}

	return ccSyncModelTags(current, desired,
		func(m map[string]string) error {
			_, err := h.client.TagResource(ctx, &sns.TagResourceInput{ResourceArn: aws.String(id), Tags: ccSNSTags(m)})

			return err
		},
		func(keys []string) error {
			_, err := h.client.UntagResource(ctx, &sns.UntagResourceInput{ResourceArn: aws.String(id), TagKeys: keys})

			return err
		})
}

func (h *ccTopic) Delete(ctx context.Context, id string) error {
	if _, err := h.client.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(id)}); err != nil {
		return ccMapError(err)
	}

	_, err := h.client.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: aws.String(id)})

	return ccMapError(err)
}

func (h *ccTopic) List(ctx context.Context) ([]string, error) {
	var arns []string

	p := sns.NewListTopicsPaginator(h.client, &sns.ListTopicsInput{})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		for _, t := range out.Topics {
			arns = append(arns, aws.ToString(t.TopicArn))
		}
	}

	return arns, nil
}

// --- AWS::ECR::Repository ---

type ccRepository struct{ client *ecr.Client }

func ccECRTags(m map[string]string) []ecrtypes.Tag {
	out := make([]ecrtypes.Tag, 0, len(m))
	for k, v := range m {
		out = append(out, ecrtypes.Tag{Key: aws.String(k), Value: aws.String(v)})
	}

	return out
}

func (h *ccRepository) Create(ctx context.Context, desired map[string]any) (string, error) {
	name := ccString(desired, "RepositoryName")
	if name == "" {
		name = ccGeneratedName("cc-repo-")
	}

	tags, err := ccModelTags(desired)
	if err != nil {
		return "", err
	}

	in := &ecr.CreateRepositoryInput{RepositoryName: aws.String(name), Tags: ccECRTags(tags)}

	if m := ccString(desired, "ImageTagMutability"); m != "" {
		in.ImageTagMutability = ecrtypes.ImageTagMutability(m)
	}

	if raw, ok := desired["ImageScanningConfiguration"]; ok {
		scan, decErr := ccDecode[ecrtypes.ImageScanningConfiguration](raw)
		if decErr != nil {
			return "", decErr
		}

		in.ImageScanningConfiguration = &scan
	}

	if raw, ok := desired["EncryptionConfiguration"]; ok {
		enc, decErr := ccDecode[ecrtypes.EncryptionConfiguration](raw)
		if decErr != nil {
			return "", decErr
		}

		in.EncryptionConfiguration = &enc
	}

	if _, err = h.client.CreateRepository(ctx, in); err != nil {
		return "", ccMapError(err)
	}

	return name, nil
}

func (h *ccRepository) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.DescribeRepositories(ctx, &ecr.DescribeRepositoriesInput{
		RepositoryNames: []string{id},
	})
	if err != nil {
		return nil, ccMapError(err)
	}

	if len(out.Repositories) == 0 {
		return nil, fmt.Errorf("%w: repository %s", cloudcontrolbackend.ErrNotFound, id)
	}

	r := out.Repositories[0]
	model := map[string]any{
		"RepositoryName": id, ccKeyArn: aws.ToString(r.RepositoryArn),
		"RepositoryUri": aws.ToString(r.RepositoryUri), "ImageTagMutability": string(r.ImageTagMutability),
	}

	if r.ImageScanningConfiguration != nil {
		model["ImageScanningConfiguration"] = map[string]bool{"ScanOnPush": r.ImageScanningConfiguration.ScanOnPush}
	}

	if r.EncryptionConfiguration != nil {
		enc := map[string]string{"EncryptionType": string(r.EncryptionConfiguration.EncryptionType)}
		if r.EncryptionConfiguration.KmsKey != nil {
			enc["KmsKey"] = *r.EncryptionConfiguration.KmsKey
		}

		model["EncryptionConfiguration"] = enc
	}

	tags, err := h.client.ListTagsForResource(ctx, &ecr.ListTagsForResourceInput{ResourceArn: r.RepositoryArn})
	if err == nil && len(tags.Tags) > 0 {
		m := make(map[string]string, len(tags.Tags))
		for _, t := range tags.Tags {
			m[aws.ToString(t.Key)] = aws.ToString(t.Value)
		}

		model["Tags"] = tagsProperty(m)
	}

	return model, nil
}

func (h *ccRepository) Update(ctx context.Context, id string, current, desired map[string]any) error {
	if err := ccRejectUnsupportedChanges(
		current, desired, "ImageTagMutability", "ImageScanningConfiguration", "Tags",
	); err != nil {
		return err
	}

	if m := ccString(desired, "ImageTagMutability"); m != "" && m != ccString(current, "ImageTagMutability") {
		if _, err := h.client.PutImageTagMutability(ctx, &ecr.PutImageTagMutabilityInput{
			RepositoryName: aws.String(id), ImageTagMutability: ecrtypes.ImageTagMutability(m),
		}); err != nil {
			return ccMapError(err)
		}
	}

	if raw, ok := desired["ImageScanningConfiguration"]; ok {
		scan, err := ccDecode[ecrtypes.ImageScanningConfiguration](raw)
		if err != nil {
			return err
		}

		if _, err = h.client.PutImageScanningConfiguration(ctx, &ecr.PutImageScanningConfigurationInput{
			RepositoryName: aws.String(id), ImageScanningConfiguration: &scan,
		}); err != nil {
			return ccMapError(err)
		}
	}

	arn := ccString(current, ccKeyArn)

	return ccSyncModelTags(current, desired,
		func(m map[string]string) error {
			_, err := h.client.TagResource(ctx, &ecr.TagResourceInput{ResourceArn: aws.String(arn), Tags: ccECRTags(m)})

			return err
		},
		func(keys []string) error {
			_, err := h.client.UntagResource(ctx, &ecr.UntagResourceInput{ResourceArn: aws.String(arn), TagKeys: keys})

			return err
		})
}

func (h *ccRepository) Delete(ctx context.Context, id string) error {
	_, err := h.client.DeleteRepository(ctx, &ecr.DeleteRepositoryInput{
		RepositoryName: aws.String(id), Force: true,
	})

	return ccMapError(err)
}

func (h *ccRepository) List(ctx context.Context) ([]string, error) {
	var names []string

	p := ecr.NewDescribeRepositoriesPaginator(h.client, &ecr.DescribeRepositoriesInput{})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		for _, r := range out.Repositories {
			names = append(names, aws.ToString(r.RepositoryName))
		}
	}

	return names, nil
}

// --- AWS::Kinesis::Stream ---

type ccStream struct{ client *kinesis.Client }

func (h *ccStream) Create(ctx context.Context, desired map[string]any) (string, error) {
	name := ccString(desired, "Name")
	if name == "" {
		name = ccGeneratedName("cc-stream-")
	}

	tags, err := ccModelTags(desired)
	if err != nil {
		return "", err
	}

	in := &kinesis.CreateStreamInput{StreamName: aws.String(name)}

	if n, ok := ccInt32(desired, "ShardCount"); ok {
		in.ShardCount = aws.Int32(n)
	}

	if raw, ok := desired["StreamModeDetails"]; ok {
		mode, decErr := ccDecode[kintypes.StreamModeDetails](raw)
		if decErr != nil {
			return "", decErr
		}

		in.StreamModeDetails = &mode
	}

	if _, err = h.client.CreateStream(ctx, in); err != nil {
		return "", ccMapError(err)
	}

	if err = h.waitActive(ctx, name); err != nil {
		return "", err
	}

	if len(tags) > 0 {
		if _, err = h.client.AddTagsToStream(ctx, &kinesis.AddTagsToStreamInput{
			StreamName: aws.String(name), Tags: tags,
		}); err != nil {
			return "", ccMapError(err)
		}
	}

	if hours, ok := ccInt32(desired, "RetentionPeriodHours"); ok {
		if err = h.setRetention(ctx, name, ccKinesisDefaultRetentionHours, hours); err != nil {
			return "", err
		}

		return name, h.waitActive(ctx, name)
	}

	return name, nil
}

const (
	ccStreamWait                   = 10 * time.Second
	ccStreamPollFloor              = 20 * time.Millisecond
	ccStreamPollCeil               = 100 * time.Millisecond
	ccKinesisDefaultRetentionHours = 24
)

func (h *ccStream) waitActive(ctx context.Context, name string) error {
	w := kinesis.NewStreamExistsWaiter(h.client, func(o *kinesis.StreamExistsWaiterOptions) {
		o.MinDelay = ccStreamPollFloor
		o.MaxDelay = ccStreamPollCeil
	})

	return ccMapError(w.Wait(ctx, &kinesis.DescribeStreamInput{StreamName: aws.String(name)}, ccStreamWait))
}

func (h *ccStream) setRetention(ctx context.Context, name string, have, want int32) error {
	var err error

	switch {
	case want > have:
		_, err = h.client.IncreaseStreamRetentionPeriod(ctx, &kinesis.IncreaseStreamRetentionPeriodInput{
			StreamName: aws.String(name), RetentionPeriodHours: aws.Int32(want),
		})
	case want < have:
		_, err = h.client.DecreaseStreamRetentionPeriod(ctx, &kinesis.DecreaseStreamRetentionPeriodInput{
			StreamName: aws.String(name), RetentionPeriodHours: aws.Int32(want),
		})
	}

	return ccMapError(err)
}

func (h *ccStream) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{StreamName: aws.String(id)})
	if err != nil {
		return nil, ccMapError(err)
	}

	s := out.StreamDescriptionSummary
	model := map[string]any{
		ccKeyName: id, ccKeyArn: aws.ToString(s.StreamARN),
		"RetentionPeriodHours": aws.ToInt32(s.RetentionPeriodHours),
	}

	if s.StreamModeDetails != nil {
		model["StreamModeDetails"] = map[string]string{"StreamMode": string(s.StreamModeDetails.StreamMode)}

		if s.StreamModeDetails.StreamMode == kintypes.StreamModeProvisioned {
			model["ShardCount"] = aws.ToInt32(s.OpenShardCount)
		}
	}

	tags, err := h.client.ListTagsForStream(ctx, &kinesis.ListTagsForStreamInput{StreamName: aws.String(id)})
	if err == nil && len(tags.Tags) > 0 {
		m := make(map[string]string, len(tags.Tags))
		for _, t := range tags.Tags {
			m[aws.ToString(t.Key)] = aws.ToString(t.Value)
		}

		model["Tags"] = tagsProperty(m)
	}

	return model, nil
}

func (h *ccStream) Update(ctx context.Context, id string, current, desired map[string]any) error {
	if err := ccRejectUnsupportedChanges(current, desired, "RetentionPeriodHours", "Tags"); err != nil {
		return err
	}

	if want, ok := ccInt32(desired, "RetentionPeriodHours"); ok {
		have, _ := current["RetentionPeriodHours"].(int32)
		if f, isF := current["RetentionPeriodHours"].(float64); isF {
			have = int32(f)
		}

		if err := h.setRetention(ctx, id, have, want); err != nil {
			return err
		}

		if err := h.waitActive(ctx, id); err != nil {
			return err
		}
	}

	return ccSyncModelTags(current, desired,
		func(m map[string]string) error {
			_, err := h.client.AddTagsToStream(ctx, &kinesis.AddTagsToStreamInput{StreamName: aws.String(id), Tags: m})

			return err
		},
		func(keys []string) error {
			_, err := h.client.RemoveTagsFromStream(ctx, &kinesis.RemoveTagsFromStreamInput{
				StreamName: aws.String(id), TagKeys: keys,
			})

			return err
		})
}

func (h *ccStream) Delete(ctx context.Context, id string) error {
	if err := h.waitActive(ctx, id); err != nil {
		return err
	}

	if _, err := h.client.DeleteStream(ctx, &kinesis.DeleteStreamInput{
		StreamName: aws.String(id), EnforceConsumerDeletion: aws.Bool(true),
	}); err != nil {
		return ccMapError(err)
	}

	w := kinesis.NewStreamNotExistsWaiter(h.client, func(o *kinesis.StreamNotExistsWaiterOptions) {
		o.MinDelay = ccStreamPollFloor
		o.MaxDelay = ccStreamPollCeil
	})

	return ccMapError(w.Wait(ctx, &kinesis.DescribeStreamInput{StreamName: aws.String(id)}, ccStreamWait))
}

func (h *ccStream) List(ctx context.Context) ([]string, error) {
	var names []string

	p := kinesis.NewListStreamsPaginator(h.client, &kinesis.ListStreamsInput{})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		names = append(names, out.StreamNames...)
	}

	return names, nil
}

// --- AWS::Events::EventBus ---

type ccEventBus struct{ client *eventbridge.Client }

func ccEventTags(m map[string]string) []ebtypes.Tag {
	out := make([]ebtypes.Tag, 0, len(m))
	for k, v := range m {
		out = append(out, ebtypes.Tag{Key: aws.String(k), Value: aws.String(v)})
	}

	return out
}

func (h *ccEventBus) Create(ctx context.Context, desired map[string]any) (string, error) {
	name := ccString(desired, "Name")
	if name == "" {
		name = ccGeneratedName("cc-bus-")
	}

	tags, err := ccModelTags(desired)
	if err != nil {
		return "", err
	}

	in := &eventbridge.CreateEventBusInput{Name: aws.String(name), Tags: ccEventTags(tags)}
	if v := ccString(desired, "Description"); v != "" {
		in.Description = aws.String(v)
	}

	if v := ccString(desired, "EventSourceName"); v != "" {
		in.EventSourceName = aws.String(v)
	}

	if v := ccString(desired, "KmsKeyIdentifier"); v != "" {
		in.KmsKeyIdentifier = aws.String(v)
	}

	if _, err = h.client.CreateEventBus(ctx, in); err != nil {
		return "", ccMapError(err)
	}

	return name, nil
}

func (h *ccEventBus) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.DescribeEventBus(ctx, &eventbridge.DescribeEventBusInput{Name: aws.String(id)})
	if err != nil {
		return nil, ccMapError(err)
	}

	model := map[string]any{"Name": id, ccKeyArn: aws.ToString(out.Arn)}

	if out.Description != nil {
		model["Description"] = *out.Description
	}

	if out.KmsKeyIdentifier != nil {
		model["KmsKeyIdentifier"] = *out.KmsKeyIdentifier
	}

	tags, err := h.client.ListTagsForResource(ctx, &eventbridge.ListTagsForResourceInput{ResourceARN: out.Arn})
	if err == nil && len(tags.Tags) > 0 {
		m := make(map[string]string, len(tags.Tags))
		for _, t := range tags.Tags {
			m[aws.ToString(t.Key)] = aws.ToString(t.Value)
		}

		model["Tags"] = tagsProperty(m)
	}

	return model, nil
}

func (h *ccEventBus) Update(ctx context.Context, id string, current, desired map[string]any) error {
	if err := ccRejectUnsupportedChanges(current, desired, "Description", "KmsKeyIdentifier", "Tags"); err != nil {
		return err
	}

	in := &eventbridge.UpdateEventBusInput{Name: aws.String(id)}
	if v := ccString(desired, "Description"); v != "" {
		in.Description = aws.String(v)
	}

	if v := ccString(desired, "KmsKeyIdentifier"); v != "" {
		in.KmsKeyIdentifier = aws.String(v)
	}

	if in.Description != nil || in.KmsKeyIdentifier != nil {
		if _, err := h.client.UpdateEventBus(ctx, in); err != nil {
			return ccMapError(err)
		}
	}

	arn := ccString(current, ccKeyArn)

	return ccSyncModelTags(current, desired,
		func(m map[string]string) error {
			_, err := h.client.TagResource(ctx, &eventbridge.TagResourceInput{
				ResourceARN: aws.String(arn), Tags: ccEventTags(m),
			})

			return err
		},
		func(keys []string) error {
			_, err := h.client.UntagResource(ctx, &eventbridge.UntagResourceInput{
				ResourceARN: aws.String(arn), TagKeys: keys,
			})

			return err
		})
}

func (h *ccEventBus) Delete(ctx context.Context, id string) error {
	_, err := h.client.DeleteEventBus(ctx, &eventbridge.DeleteEventBusInput{Name: aws.String(id)})

	return ccMapError(err)
}

func (h *ccEventBus) List(ctx context.Context) ([]string, error) {
	var names []string

	var token *string

	for {
		out, err := h.client.ListEventBuses(ctx, &eventbridge.ListEventBusesInput{NextToken: token})
		if err != nil {
			return nil, ccMapError(err)
		}

		for _, b := range out.EventBuses {
			names = append(names, aws.ToString(b.Name))
		}

		if out.NextToken == nil {
			return names, nil
		}

		token = out.NextToken
	}
}

// --- AWS::StepFunctions::StateMachine ---

type ccStateMachine struct{ client *sfn.Client }

func ccSFNTags(m map[string]string) []sfntypes.Tag {
	out := make([]sfntypes.Tag, 0, len(m))
	for k, v := range m {
		out = append(out, sfntypes.Tag{Key: aws.String(k), Value: aws.String(v)})
	}

	return out
}

func ccDefinition(m map[string]any) (string, error) {
	if s := ccString(m, "DefinitionString"); s != "" {
		return s, nil
	}

	if d, ok := m["Definition"]; ok {
		return ccPolicyJSON(d)
	}

	return "", fmt.Errorf("%w: DefinitionString or Definition is required", cloudcontrolbackend.ErrValidation)
}

func (h *ccStateMachine) Create(ctx context.Context, desired map[string]any) (string, error) {
	name := ccString(desired, "StateMachineName")
	if name == "" {
		name = ccGeneratedName("cc-sm-")
	}

	def, err := ccDefinition(desired)
	if err != nil {
		return "", err
	}

	tags, err := ccModelTags(desired)
	if err != nil {
		return "", err
	}

	in := &sfn.CreateStateMachineInput{
		Name: aws.String(name), Definition: aws.String(def), RoleArn: aws.String(ccString(desired, "RoleArn")),
		Tags: ccSFNTags(tags),
	}

	if t := ccString(desired, "StateMachineType"); t != "" {
		in.Type = sfntypes.StateMachineType(t)
	}

	if err = ccStateMachineConfigs(desired, &in.LoggingConfiguration, &in.TracingConfiguration); err != nil {
		return "", err
	}

	out, err := h.client.CreateStateMachine(ctx, in)
	if err != nil {
		return "", ccMapError(err)
	}

	return aws.ToString(out.StateMachineArn), nil
}

func ccStateMachineConfigs(
	m map[string]any, logging **sfntypes.LoggingConfiguration, tracing **sfntypes.TracingConfiguration,
) error {
	if raw, ok := m["LoggingConfiguration"]; ok {
		l, err := ccDecode[sfntypes.LoggingConfiguration](raw)
		if err != nil {
			return err
		}

		*logging = &l
	}

	if raw, ok := m["TracingConfiguration"]; ok {
		t, err := ccDecode[sfntypes.TracingConfiguration](raw)
		if err != nil {
			return err
		}

		*tracing = &t
	}

	return nil
}

func (h *ccStateMachine) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.DescribeStateMachine(ctx, &sfn.DescribeStateMachineInput{StateMachineArn: aws.String(id)})
	if err != nil {
		return nil, ccMapError(err)
	}

	model := map[string]any{
		ccKeyArn: id, "StateMachineName": aws.ToString(out.Name), "DefinitionString": aws.ToString(out.Definition),
		"RoleArn": aws.ToString(out.RoleArn), "StateMachineType": string(out.Type),
		"StateMachineRevisionId": aws.ToString(out.RevisionId),
	}

	if out.LoggingConfiguration != nil {
		model["LoggingConfiguration"] = out.LoggingConfiguration
	}

	if out.TracingConfiguration != nil {
		model["TracingConfiguration"] = out.TracingConfiguration
	}

	tags, err := h.client.ListTagsForResource(ctx, &sfn.ListTagsForResourceInput{ResourceArn: aws.String(id)})
	if err == nil && len(tags.Tags) > 0 {
		m := make(map[string]string, len(tags.Tags))
		for _, t := range tags.Tags {
			m[aws.ToString(t.Key)] = aws.ToString(t.Value)
		}

		model["Tags"] = tagsProperty(m)
	}

	normalized, _ := normalizeJSON(model).(map[string]any)

	return normalized, nil
}

func (h *ccStateMachine) Update(ctx context.Context, id string, current, desired map[string]any) error {
	if err := ccRejectUnsupportedChanges(
		current, desired, "DefinitionString", "Definition", "RoleArn", "LoggingConfiguration",
		"TracingConfiguration", "Tags", "StateMachineRevisionId",
	); err != nil {
		return err
	}

	in := &sfn.UpdateStateMachineInput{StateMachineArn: aws.String(id)}

	if def, err := ccDefinition(desired); err == nil && def != ccString(current, "DefinitionString") {
		in.Definition = aws.String(def)
	}

	if r := ccString(desired, "RoleArn"); r != "" && r != ccString(current, "RoleArn") {
		in.RoleArn = aws.String(r)
	}

	if err := ccStateMachineConfigs(desired, &in.LoggingConfiguration, &in.TracingConfiguration); err != nil {
		return err
	}

	if _, err := h.client.UpdateStateMachine(ctx, in); err != nil {
		return ccMapError(err)
	}

	return ccSyncModelTags(current, desired,
		func(m map[string]string) error {
			_, err := h.client.TagResource(ctx, &sfn.TagResourceInput{ResourceArn: aws.String(id), Tags: ccSFNTags(m)})

			return err
		},
		func(keys []string) error {
			_, err := h.client.UntagResource(ctx, &sfn.UntagResourceInput{ResourceArn: aws.String(id), TagKeys: keys})

			return err
		})
}

func (h *ccStateMachine) Delete(ctx context.Context, id string) error {
	if _, err := h.Read(ctx, id); err != nil {
		return err
	}

	_, err := h.client.DeleteStateMachine(ctx, &sfn.DeleteStateMachineInput{StateMachineArn: aws.String(id)})

	return ccMapError(err)
}

func (h *ccStateMachine) List(ctx context.Context) ([]string, error) {
	var arns []string

	p := sfn.NewListStateMachinesPaginator(h.client, &sfn.ListStateMachinesInput{})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		for _, s := range out.StateMachines {
			arns = append(arns, aws.ToString(s.StateMachineArn))
		}
	}

	return arns, nil
}

func wireCloudControlMoreHandlers(bk *cloudcontrolbackend.InMemoryBackend, cfg aws.Config) {
	bk.RegisterTypeHandler("AWS::SNS::Topic", &ccTopic{client: sns.NewFromConfig(cfg)})
	bk.RegisterTypeHandler("AWS::IAM::Role", &ccRole{client: iam.NewFromConfig(cfg)})
	bk.RegisterTypeHandler("AWS::KMS::Key", &ccKey{client: kms.NewFromConfig(cfg)})
	bk.RegisterTypeHandler("AWS::SecretsManager::Secret", &ccSecret{client: secretsmanager.NewFromConfig(cfg)})
	bk.RegisterTypeHandler("AWS::SSM::Parameter", &ccParameter{client: ssm.NewFromConfig(cfg)})
	bk.RegisterTypeHandler("AWS::ECR::Repository", &ccRepository{client: ecr.NewFromConfig(cfg)})
	bk.RegisterTypeHandler("AWS::Kinesis::Stream", &ccStream{client: kinesis.NewFromConfig(cfg)})
	bk.RegisterTypeHandler("AWS::Events::EventBus", &ccEventBus{client: eventbridge.NewFromConfig(cfg)})
	bk.RegisterTypeHandler("AWS::Lambda::Function", &ccFunction{client: lambda.NewFromConfig(cfg)})
	bk.RegisterTypeHandler("AWS::StepFunctions::StateMachine", &ccStateMachine{client: sfn.NewFromConfig(cfg)})
}

// --- AWS::Lambda::Function ---

type ccFunction struct{ client *lambda.Client }

func ccInlineZip(runtime, source string) ([]byte, error) {
	name := "index"

	switch {
	case strings.HasPrefix(runtime, "python"):
		name += ".py"
	case strings.HasPrefix(runtime, "nodejs"):
		name += ".js"
	}

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	w, err := zw.Create(name)
	if err != nil {
		return nil, err
	}

	if _, err = w.Write([]byte(source)); err != nil {
		return nil, err
	}

	if err = zw.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

type ccLambdaCode struct {
	ZipFile  string
	S3Bucket string
	S3Key    string
	ImageURI string
}

func (c ccLambdaCode) functionCode(runtime string) (*lambdatypes.FunctionCode, error) {
	out := &lambdatypes.FunctionCode{}

	switch {
	case c.ZipFile != "":
		zipped, err := ccInlineZip(runtime, c.ZipFile)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", cloudcontrolbackend.ErrValidation, err)
		}

		out.ZipFile = zipped
	case c.ImageURI != "":
		out.ImageUri = aws.String(c.ImageURI)
	case c.S3Bucket != "":
		out.S3Bucket = aws.String(c.S3Bucket)
		out.S3Key = aws.String(c.S3Key)
	default:
		return nil, fmt.Errorf("%w: Code requires ZipFile, S3Bucket or ImageUri", cloudcontrolbackend.ErrValidation)
	}

	return out, nil
}

func ccLambdaEnv(desired map[string]any) (*lambdatypes.Environment, error) {
	raw, ok := desired["Environment"]
	if !ok {
		return &lambdatypes.Environment{Variables: map[string]string{}}, nil
	}

	env, err := ccDecode[struct{ Variables map[string]string }](raw)
	if err != nil {
		return nil, err
	}

	return &lambdatypes.Environment{Variables: env.Variables}, nil
}

func (h *ccFunction) Create(ctx context.Context, desired map[string]any) (string, error) {
	name := ccString(desired, "FunctionName")
	if name == "" {
		name = ccGeneratedName("cc-fn-")
	}

	tags, err := ccModelTags(desired)
	if err != nil {
		return "", err
	}

	codeProp, err := ccDecode[ccLambdaCode](desired["Code"])
	if err != nil {
		return "", err
	}

	runtime := ccString(desired, "Runtime")

	code, err := codeProp.functionCode(runtime)
	if err != nil {
		return "", err
	}

	env, err := ccLambdaEnv(desired)
	if err != nil {
		return "", err
	}

	in := &lambda.CreateFunctionInput{
		FunctionName: aws.String(name), Role: aws.String(ccString(desired, "Role")), Code: code,
		Runtime: lambdatypes.Runtime(runtime), Environment: env, Tags: tags,
	}

	if v := ccString(desired, "Handler"); v != "" {
		in.Handler = aws.String(v)
	}

	if v := ccString(desired, "Description"); v != "" {
		in.Description = aws.String(v)
	}

	if n, ok := ccInt32(desired, "MemorySize"); ok {
		in.MemorySize = aws.Int32(n)
	}

	if n, ok := ccInt32(desired, "Timeout"); ok {
		in.Timeout = aws.Int32(n)
	}

	if codeProp.ImageURI != "" {
		in.PackageType = lambdatypes.PackageTypeImage
	}

	if _, err = h.client.CreateFunction(ctx, in); err != nil {
		return "", ccMapError(err)
	}

	return name, nil
}

func (h *ccFunction) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String(id)})
	if err != nil {
		return nil, ccMapError(err)
	}

	c := out.Configuration
	model := map[string]any{
		"FunctionName": id, ccKeyArn: aws.ToString(c.FunctionArn), "Role": aws.ToString(c.Role),
		"MemorySize": aws.ToInt32(c.MemorySize), "Timeout": aws.ToInt32(c.Timeout),
	}

	if c.Runtime != "" {
		model["Runtime"] = string(c.Runtime)
	}

	if c.Handler != nil {
		model["Handler"] = *c.Handler
	}

	if c.Description != nil && *c.Description != "" {
		model["Description"] = *c.Description
	}

	if c.Environment != nil && len(c.Environment.Variables) > 0 {
		model["Environment"] = map[string]map[string]string{"Variables": c.Environment.Variables}
	}

	if len(out.Tags) > 0 {
		model["Tags"] = tagsProperty(out.Tags)
	}

	normalized, _ := normalizeJSON(model).(map[string]any)

	return normalized, nil
}

func (h *ccFunction) Update(ctx context.Context, id string, current, desired map[string]any) error {
	if err := ccRejectUnsupportedChanges(
		current, desired, "Runtime", "Handler", "Role", "Description", "MemorySize", "Timeout", "Environment",
		"Code", "Tags",
	); err != nil {
		return err
	}

	runtime := ccString(desired, "Runtime")

	if raw, ok := desired["Code"]; ok {
		codeProp, err := ccDecode[ccLambdaCode](raw)
		if err != nil {
			return err
		}

		code, err := codeProp.functionCode(runtime)
		if err != nil {
			return err
		}

		if _, err = h.client.UpdateFunctionCode(ctx, &lambda.UpdateFunctionCodeInput{
			FunctionName: aws.String(id), ZipFile: code.ZipFile, S3Bucket: code.S3Bucket, S3Key: code.S3Key,
			ImageUri: code.ImageUri,
		}); err != nil {
			return ccMapError(err)
		}
	}

	if err := h.updateConfiguration(ctx, id, desired); err != nil {
		return err
	}

	have, err := ccModelTags(current)
	if err != nil {
		return err
	}

	want, err := ccModelTags(desired)
	if err != nil {
		return err
	}

	arn := ccString(current, ccKeyArn)
	set, unset := ccTagDelta(have, want)

	if len(set) > 0 {
		if _, err = h.client.TagResource(
			ctx,
			&lambda.TagResourceInput{Resource: aws.String(arn), Tags: set},
		); err != nil {
			return ccMapError(err)
		}
	}

	if len(unset) > 0 {
		_, err = h.client.UntagResource(ctx, &lambda.UntagResourceInput{Resource: aws.String(arn), TagKeys: unset})

		return ccMapError(err)
	}

	return nil
}

func (h *ccFunction) updateConfiguration(ctx context.Context, id string, desired map[string]any) error {
	env, err := ccLambdaEnv(desired)
	if err != nil {
		return err
	}

	in := &lambda.UpdateFunctionConfigurationInput{
		FunctionName: aws.String(id), Environment: env, Runtime: lambdatypes.Runtime(ccString(desired, "Runtime")),
	}

	if v := ccString(desired, "Handler"); v != "" {
		in.Handler = aws.String(v)
	}

	if v := ccString(desired, "Role"); v != "" {
		in.Role = aws.String(v)
	}

	if v := ccString(desired, "Description"); v != "" {
		in.Description = aws.String(v)
	}

	if n, ok := ccInt32(desired, "MemorySize"); ok {
		in.MemorySize = aws.Int32(n)
	}

	if n, ok := ccInt32(desired, "Timeout"); ok {
		in.Timeout = aws.Int32(n)
	}

	_, err = h.client.UpdateFunctionConfiguration(ctx, in)

	return ccMapError(err)
}

func (h *ccFunction) Delete(ctx context.Context, id string) error {
	_, err := h.client.DeleteFunction(ctx, &lambda.DeleteFunctionInput{FunctionName: aws.String(id)})

	return ccMapError(err)
}

func (h *ccFunction) List(ctx context.Context) ([]string, error) {
	var names []string

	p := lambda.NewListFunctionsPaginator(h.client, &lambda.ListFunctionsInput{})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		for _, f := range out.Functions {
			names = append(names, aws.ToString(f.FunctionName))
		}
	}

	return names, nil
}
