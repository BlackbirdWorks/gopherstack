package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cloudcontrolbackend "github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

const (
	ccEndpoint = "http://cloudcontrol.local"
	ccKeyArn   = "Arn"
)

type ccDoer struct{ handler http.Handler }

func (d *ccDoer) Do(req *http.Request) (*http.Response, error) {
	if req.Body == nil {
		req.Body = http.NoBody
	}

	rec := httptest.NewRecorder()
	d.handler.ServeHTTP(rec, req)

	return rec.Result(), nil
}

// ccTags is the CloudFormation Tags property: a list of {Key, Value} pairs.
type ccTags []struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

func (t ccTags) toMap() map[string]string {
	if len(t) == 0 {
		return nil
	}

	m := make(map[string]string, len(t))
	for _, kv := range t {
		m[kv.Key] = kv.Value
	}

	return m
}

func tagsProperty(m map[string]string) []map[string]string {
	out := make([]map[string]string, 0, len(m))
	for k, v := range m {
		out = append(out, map[string]string{"Key": k, "Value": v})
	}

	return out
}

func ccDecode[T any](raw any) (T, error) {
	var out T

	b, err := json.Marshal(raw)
	if err != nil {
		return out, fmt.Errorf("%w: %w", cloudcontrolbackend.ErrValidation, err)
	}

	if err = json.Unmarshal(b, &out); err != nil {
		return out, fmt.Errorf("%w: %w", cloudcontrolbackend.ErrValidation, err)
	}

	return out, nil
}

// ccMapError translates a service SDK error into the CloudControl handler error codes.
func ccMapError(err error) error {
	if err == nil {
		return nil
	}

	apiErr, ok := errors.AsType[smithy.APIError](err)
	if !ok {
		return err
	}

	code := apiErr.ErrorCode()

	switch {
	case strings.Contains(code, "NotFound"), strings.Contains(code, "NoSuch"),
		strings.Contains(code, "NonExistent"), strings.Contains(code, "DoesNotExist"):
		return fmt.Errorf("%w: %w", cloudcontrolbackend.ErrNotFound, err)
	case strings.Contains(code, "AlreadyExists"), strings.Contains(code, "AlreadyOwned"),
		strings.Contains(code, "NameExists"), strings.Contains(code, "ResourceInUse"),
		strings.Contains(code, "ResourceExists"):
		return fmt.Errorf("%w: %w", cloudcontrolbackend.ErrAlreadyExists, err)
	case apiErr.ErrorFault() == smithy.FaultClient || ccClientStatus(err):
		return fmt.Errorf("%w: %w", cloudcontrolbackend.ErrValidation, err)
	default:
		return err
	}
}

func ccClientStatus(err error) bool {
	re, ok := errors.AsType[*smithyhttp.ResponseError](err)

	return ok && re.HTTPStatusCode() >= http.StatusBadRequest && re.HTTPStatusCode() < http.StatusInternalServerError
}

// ccRejectUnsupportedChanges fails when desired changes a property outside allowed.
func ccRejectUnsupportedChanges(current, desired map[string]any, allowed ...string) error {
	for k, want := range desired {
		if slices.Contains(allowed, k) {
			continue
		}

		have, ok := current[k]
		if ok && reflect.DeepEqual(normalizeJSON(have), normalizeJSON(want)) {
			continue
		}

		return fmt.Errorf("%w: property %s cannot be updated", cloudcontrolbackend.ErrValidation, k)
	}

	return nil
}

func normalizeJSON(v any) any {
	b, err := json.Marshal(v)
	if err != nil {
		return v
	}

	var out any
	if json.Unmarshal(b, &out) != nil {
		return v
	}

	return out
}

func ccGeneratedName(prefix string) string {
	return prefix + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
}

// wireCloudControlHandlers makes CloudControl operate real service resources through the in-process
// server for the types registered here and in wireCloudControlMoreHandlers.
func wireCloudControlHandlers(e http.Handler, services []service.Registerable) {
	ccH, ok := serviceByName(services)["CloudControl"].(*cloudcontrolbackend.Handler)
	if !ok {
		return
	}

	bk := ccH.Backend

	cfg := aws.Config{
		Region:       bk.Region(),
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		HTTPClient:   &ccDoer{handler: e},
		Retryer:      func() aws.Retryer { return aws.NopRetryer{} },
		BaseEndpoint: aws.String(ccEndpoint),
	}

	bk.RegisterTypeHandler("AWS::S3::Bucket", &ccBucket{
		client: s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true }), region: bk.Region(),
	})
	bk.RegisterTypeHandler("AWS::SQS::Queue", &ccQueue{client: sqs.NewFromConfig(cfg)})
	bk.RegisterTypeHandler("AWS::DynamoDB::Table", &ccTable{client: dynamodb.NewFromConfig(cfg)})
	bk.RegisterTypeHandler("AWS::Logs::LogGroup", &ccLogGroup{client: cloudwatchlogs.NewFromConfig(cfg)})
	wireCloudControlMoreHandlers(bk, cfg)
	wireCloudControlInfraHandlers(bk, cfg)
}

// --- AWS::S3::Bucket ---

type ccBucket struct {
	client *s3.Client
	region string
}

func (h *ccBucket) Create(ctx context.Context, desired map[string]any) (string, error) {
	name, _ := desired["BucketName"].(string)
	if name == "" {
		name = ccGeneratedName("cc-bucket-")
	}

	in := &s3.CreateBucketInput{Bucket: aws.String(name)}
	if h.region != config.DefaultRegion {
		in.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(h.region),
		}
	}

	if _, err := h.client.CreateBucket(ctx, in); err != nil {
		return "", ccMapError(err)
	}

	tags, err := ccDecode[ccTags](desired["Tags"])
	if err != nil {
		return "", err
	}

	return name, ccMapError(h.putTags(ctx, name, tags.toMap()))
}

func (h *ccBucket) putTags(ctx context.Context, bucket string, tags map[string]string) error {
	if len(tags) == 0 {
		_, err := h.client.DeleteBucketTagging(ctx, &s3.DeleteBucketTaggingInput{Bucket: aws.String(bucket)})

		return err
	}

	set := make([]s3types.Tag, 0, len(tags))
	for k, v := range tags {
		set = append(set, s3types.Tag{Key: aws.String(k), Value: aws.String(v)})
	}

	_, err := h.client.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{
		Bucket: aws.String(bucket), Tagging: &s3types.Tagging{TagSet: set},
	})

	return err
}

func (h *ccBucket) Read(ctx context.Context, id string) (map[string]any, error) {
	if _, err := h.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(id)}); err != nil {
		return nil, ccMapError(err)
	}

	model := map[string]any{
		"BucketName":         id,
		ccKeyArn:             "arn:aws:s3:::" + id,
		"DomainName":         id + ".s3.amazonaws.com",
		"RegionalDomainName": id + ".s3." + h.region + ".amazonaws.com",
	}

	if out, err := h.client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(id)}); err == nil &&
		len(out.TagSet) > 0 {
		tags := make(map[string]string, len(out.TagSet))
		for _, t := range out.TagSet {
			tags[aws.ToString(t.Key)] = aws.ToString(t.Value)
		}

		model["Tags"] = tagsProperty(tags)
	}

	return model, nil
}

func (h *ccBucket) Update(ctx context.Context, id string, current, desired map[string]any) error {
	if err := ccRejectUnsupportedChanges(current, desired, "Tags"); err != nil {
		return err
	}

	tags, err := ccDecode[ccTags](desired["Tags"])
	if err != nil {
		return err
	}

	return ccMapError(h.putTags(ctx, id, tags.toMap()))
}

func (h *ccBucket) Delete(ctx context.Context, id string) error {
	_, err := h.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(id)})

	return ccMapError(err)
}

func (h *ccBucket) List(ctx context.Context) ([]string, error) {
	out, err := h.client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, ccMapError(err)
	}

	ids := make([]string, 0, len(out.Buckets))
	for _, b := range out.Buckets {
		ids = append(ids, aws.ToString(b.Name))
	}

	return ids, nil
}

// --- AWS::SQS::Queue ---

type ccQueue struct{ client *sqs.Client }

//nolint:gochecknoglobals // lookup table
var ccQueueIntAttrs = map[string]string{
	"VisibilityTimeout":             "VisibilityTimeout",
	"DelaySeconds":                  "DelaySeconds",
	"MaximumMessageSize":            "MaximumMessageSize",
	"MessageRetentionPeriod":        "MessageRetentionPeriod",
	"ReceiveMessageWaitTimeSeconds": "ReceiveMessageWaitTimeSeconds",
}

func ccQueueAttributes(desired map[string]any) map[string]string {
	attrs := map[string]string{}

	for prop, attr := range ccQueueIntAttrs {
		if v, ok := desired[prop]; ok {
			attrs[attr] = fmt.Sprint(normalizeNumber(v))
		}
	}

	for _, prop := range []string{"FifoQueue", "ContentBasedDeduplication"} {
		if v, ok := desired[prop]; ok {
			attrs[prop] = fmt.Sprint(v)
		}
	}

	return attrs
}

func normalizeNumber(v any) any {
	if f, ok := v.(float64); ok {
		return strconv.FormatInt(int64(f), 10)
	}

	return v
}

func (h *ccQueue) Create(ctx context.Context, desired map[string]any) (string, error) {
	name, _ := desired["QueueName"].(string)
	if name == "" {
		name = ccGeneratedName("cc-queue-")
		if fifo, _ := desired["FifoQueue"].(bool); fifo {
			name += ".fifo"
		}
	}

	tags, err := ccDecode[ccTags](desired["Tags"])
	if err != nil {
		return "", err
	}

	out, err := h.client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(name), Attributes: ccQueueAttributes(desired), Tags: tags.toMap(),
	})
	if err != nil {
		return "", ccMapError(err)
	}

	return aws.ToString(out.QueueUrl), nil
}

func (h *ccQueue) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(id), AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll},
	})
	if err != nil {
		return nil, ccMapError(err)
	}

	model := map[string]any{
		"QueueUrl":  id,
		"QueueName": id[strings.LastIndex(id, "/")+1:],
		ccKeyArn:    out.Attributes["QueueArn"],
	}

	for prop, attr := range ccQueueIntAttrs {
		if v, ok := out.Attributes[attr]; ok {
			if n, convErr := strconv.Atoi(v); convErr == nil {
				model[prop] = n
			}
		}
	}

	if out.Attributes["FifoQueue"] == ccTrue {
		model["FifoQueue"] = true
	}

	if tags, tagErr := h.client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: aws.String(id)}); tagErr == nil &&
		len(tags.Tags) > 0 {
		model["Tags"] = tagsProperty(tags.Tags)
	}

	return model, nil
}

func (h *ccQueue) Update(ctx context.Context, id string, current, desired map[string]any) error {
	mutable := slices.AppendSeq([]string{"Tags", "ContentBasedDeduplication"}, maps.Keys(ccQueueIntAttrs))

	if err := ccRejectUnsupportedChanges(current, desired, mutable...); err != nil {
		return err
	}

	if attrs := ccQueueAttributes(desired); len(attrs) > 0 {
		delete(attrs, "FifoQueue")

		if _, err := h.client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
			QueueUrl: aws.String(id), Attributes: attrs,
		}); err != nil {
			return ccMapError(err)
		}
	}

	tags, err := ccDecode[ccTags](desired["Tags"])
	if err != nil {
		return err
	}

	if m := tags.toMap(); len(m) > 0 {
		if _, err = h.client.TagQueue(ctx, &sqs.TagQueueInput{QueueUrl: aws.String(id), Tags: m}); err != nil {
			return ccMapError(err)
		}
	}

	return nil
}

func (h *ccQueue) Delete(ctx context.Context, id string) error {
	_, err := h.client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: aws.String(id)})

	return ccMapError(err)
}

func (h *ccQueue) List(ctx context.Context) ([]string, error) {
	var urls []string

	p := sqs.NewListQueuesPaginator(h.client, &sqs.ListQueuesInput{})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		urls = append(urls, out.QueueUrls...)
	}

	return urls, nil
}

// --- AWS::DynamoDB::Table ---

type ccTable struct{ client *dynamodb.Client }

func (h *ccTable) Create(ctx context.Context, desired map[string]any) (string, error) {
	in, err := ccDecode[dynamodb.CreateTableInput](desired)
	if err != nil {
		return "", err
	}

	if aws.ToString(in.TableName) == "" {
		in.TableName = aws.String(ccGeneratedName("cc-table-"))
	}

	if _, err = h.client.CreateTable(ctx, &in); err != nil {
		return "", ccMapError(err)
	}

	return aws.ToString(in.TableName), nil
}

func (h *ccTable) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(id)})
	if err != nil {
		return nil, ccMapError(err)
	}

	t := out.Table
	model := map[string]any{
		"TableName":            id,
		ccKeyArn:               aws.ToString(t.TableArn),
		"KeySchema":            t.KeySchema,
		"AttributeDefinitions": t.AttributeDefinitions,
		"BillingMode":          string(ddbtypes.BillingModeProvisioned),
	}

	if t.BillingModeSummary != nil {
		model["BillingMode"] = string(t.BillingModeSummary.BillingMode)
	}

	if t.ProvisionedThroughput != nil && model["BillingMode"] == string(ddbtypes.BillingModeProvisioned) {
		model["ProvisionedThroughput"] = map[string]int64{
			"ReadCapacityUnits":  aws.ToInt64(t.ProvisionedThroughput.ReadCapacityUnits),
			"WriteCapacityUnits": aws.ToInt64(t.ProvisionedThroughput.WriteCapacityUnits),
		}
	}

	if t.LatestStreamArn != nil {
		model["StreamArn"] = aws.ToString(t.LatestStreamArn)
	}

	if t.DeletionProtectionEnabled != nil {
		model["DeletionProtectionEnabled"] = *t.DeletionProtectionEnabled
	}

	normalized, _ := normalizeJSON(model).(map[string]any)

	return normalized, nil
}

func (h *ccTable) Update(ctx context.Context, id string, current, desired map[string]any) error {
	if err := ccRejectUnsupportedChanges(
		current, desired, "BillingMode", "ProvisionedThroughput", "DeletionProtectionEnabled",
	); err != nil {
		return err
	}

	mutable, err := ccDecode[struct {
		ProvisionedThroughput     *ddbtypes.ProvisionedThroughput
		DeletionProtectionEnabled *bool
		BillingMode               ddbtypes.BillingMode
	}](desired)
	if err != nil {
		return err
	}

	_, err = h.client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName: aws.String(id), BillingMode: mutable.BillingMode,
		ProvisionedThroughput:     mutable.ProvisionedThroughput,
		DeletionProtectionEnabled: mutable.DeletionProtectionEnabled,
	})

	return ccMapError(err)
}

func (h *ccTable) Delete(ctx context.Context, id string) error {
	_, err := h.client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(id)})

	return ccMapError(err)
}

func (h *ccTable) List(ctx context.Context) ([]string, error) {
	var names []string

	p := dynamodb.NewListTablesPaginator(h.client, &dynamodb.ListTablesInput{})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		names = append(names, out.TableNames...)
	}

	return names, nil
}

// --- AWS::Logs::LogGroup ---

type ccLogGroup struct{ client *cloudwatchlogs.Client }

func (h *ccLogGroup) Create(ctx context.Context, desired map[string]any) (string, error) {
	name, _ := desired["LogGroupName"].(string)
	if name == "" {
		name = ccGeneratedName("cc-loggroup-")
	}

	tags, err := ccDecode[ccTags](desired["Tags"])
	if err != nil {
		return "", err
	}

	in := &cloudwatchlogs.CreateLogGroupInput{LogGroupName: aws.String(name), Tags: tags.toMap()}
	if k, _ := desired["KmsKeyId"].(string); k != "" {
		in.KmsKeyId = aws.String(k)
	}

	if c, _ := desired["LogGroupClass"].(string); c != "" {
		in.LogGroupClass = cwltypes.LogGroupClass(c)
	}

	if _, err = h.client.CreateLogGroup(ctx, in); err != nil {
		return "", ccMapError(err)
	}

	if days, ok := desired["RetentionInDays"].(float64); ok {
		if err = h.setRetention(ctx, name, int32(days)); err != nil {
			return "", err
		}
	}

	return name, nil
}

func (h *ccLogGroup) setRetention(ctx context.Context, name string, days int32) error {
	if days <= 0 {
		_, err := h.client.DeleteRetentionPolicy(ctx, &cloudwatchlogs.DeleteRetentionPolicyInput{
			LogGroupName: aws.String(name),
		})

		return ccMapError(err)
	}

	_, err := h.client.PutRetentionPolicy(ctx, &cloudwatchlogs.PutRetentionPolicyInput{
		LogGroupName: aws.String(name), RetentionInDays: aws.Int32(days),
	})

	return ccMapError(err)
}

func (h *ccLogGroup) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.DescribeLogGroups(ctx, &cloudwatchlogs.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String(id),
	})
	if err != nil {
		return nil, ccMapError(err)
	}

	for _, g := range out.LogGroups {
		if aws.ToString(g.LogGroupName) != id {
			continue
		}

		model := map[string]any{
			"LogGroupName": id,
			ccKeyArn:       strings.TrimSuffix(aws.ToString(g.Arn), ":*"),
		}

		if g.RetentionInDays != nil {
			model["RetentionInDays"] = *g.RetentionInDays
		}

		if g.KmsKeyId != nil {
			model["KmsKeyId"] = *g.KmsKeyId
		}

		if g.LogGroupClass != "" {
			model["LogGroupClass"] = string(g.LogGroupClass)
		}

		return model, nil
	}

	return nil, fmt.Errorf("%w: log group %s", cloudcontrolbackend.ErrNotFound, id)
}

func (h *ccLogGroup) Update(ctx context.Context, id string, current, desired map[string]any) error {
	if err := ccRejectUnsupportedChanges(current, desired, "RetentionInDays"); err != nil {
		return err
	}

	days, _ := desired["RetentionInDays"].(float64)

	return h.setRetention(ctx, id, int32(days))
}

func (h *ccLogGroup) Delete(ctx context.Context, id string) error {
	_, err := h.client.DeleteLogGroup(ctx, &cloudwatchlogs.DeleteLogGroupInput{LogGroupName: aws.String(id)})

	return ccMapError(err)
}

func (h *ccLogGroup) List(ctx context.Context) ([]string, error) {
	var names []string

	p := cloudwatchlogs.NewDescribeLogGroupsPaginator(h.client, &cloudwatchlogs.DescribeLogGroupsInput{})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		for _, g := range out.LogGroups {
			names = append(names, aws.ToString(g.LogGroupName))
		}
	}

	return names, nil
}
