package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	resourceTypeDynamoDB = "DynamoDB"
	resourceTypeS3       = "S3"
	resourceTypeEBS      = "EBS"
)

// resourceEngine snapshots a live resource at backup time and recreates it from that snapshot on restore.
type resourceEngine interface {
	// capture returns nil data when the resource does not exist.
	capture(ctx context.Context, resourceArn string) (json.RawMessage, error)
	restore(ctx context.Context, data json.RawMessage, metadata map[string]string) (string, error)
}

// DynamoDBBackend is the DynamoDB surface used to snapshot and restore tables.
type DynamoDBBackend interface {
	CreateTable(ctx context.Context, in *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error)
	DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error)
	Scan(ctx context.Context, in *dynamodb.ScanInput) (*dynamodb.ScanOutput, error)
	PutItem(ctx context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
}

// ObjectStore is the S3 surface used to deliver reports and snapshot and restore buckets.
type ObjectStore interface {
	S3Backend
	CreateBucket(ctx context.Context, in *sdk_s3.CreateBucketInput) (*sdk_s3.CreateBucketOutput, error)
	PutObject(ctx context.Context, in *sdk_s3.PutObjectInput) (*sdk_s3.PutObjectOutput, error)
	GetObject(ctx context.Context, in *sdk_s3.GetObjectInput) (*sdk_s3.GetObjectOutput, error)
	ListObjectsV2(ctx context.Context, in *sdk_s3.ListObjectsV2Input) (*sdk_s3.ListObjectsV2Output, error)
}

// VolumeInfo is the EBS volume shape a backup captures.
type VolumeInfo struct {
	AZ         string `json:"az"`
	VolumeType string `json:"volumeType"`
	Size       int    `json:"size"`
}

// VolumeService is the EC2 surface used to snapshot and restore EBS volumes.
type VolumeService interface {
	DescribeVolume(id string) (VolumeInfo, bool)
	CreateVolume(info VolumeInfo) (string, error)
}

// SetObjectStore wires S3 for report delivery and S3 backup and restore; it also serves the bucket checks.
func (b *InMemoryBackend) SetObjectStore(store ObjectStore) {
	b.mu.Lock("SetObjectStore")
	defer b.mu.Unlock()

	b.objects = store
	b.s3 = store
	b.engines[resourceTypeS3] = &s3Engine{store: store}
}

// SetDynamoDBBackend wires DynamoDB so tables are snapshotted at backup time and recreated on restore.
func (b *InMemoryBackend) SetDynamoDBBackend(ddb DynamoDBBackend) {
	b.mu.Lock("SetDynamoDBBackend")
	defer b.mu.Unlock()

	b.engines[resourceTypeDynamoDB] = &ddbEngine{ddb: ddb}
}

// SetVolumeService wires EC2 so EBS volumes are recreated on restore.
func (b *InMemoryBackend) SetVolumeService(vols VolumeService, region, accountID string) {
	b.mu.Lock("SetVolumeService")
	defer b.mu.Unlock()

	b.engines[resourceTypeEBS] = &ebsEngine{vols: vols, region: region, accountID: accountID}
}

type ddbEngine struct{ ddb DynamoDBBackend }

type ddbSnapshot struct {
	Provisioned *ddbtypes.ProvisionedThroughput `json:"provisioned,omitempty"`
	BillingMode ddbtypes.BillingMode            `json:"billingMode"`
	TableName   string                          `json:"tableName"`
	KeySchema   []ddbtypes.KeySchemaElement     `json:"keySchema"`
	Attributes  []ddbtypes.AttributeDefinition  `json:"attributes"`
	GSIs        []ddbtypes.GlobalSecondaryIndex `json:"gsis,omitempty"`
	LSIs        []ddbtypes.LocalSecondaryIndex  `json:"lsis,omitempty"`
	Items       []map[string]json.RawMessage    `json:"items"`
}

func (e *ddbEngine) capture(ctx context.Context, resourceArn string) (json.RawMessage, error) {
	name := resourceNameFromARN(resourceArn)

	desc, err := e.ddb.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
	if err != nil || desc.Table == nil {
		return nil, nil //nolint:nilerr // a missing table simply has nothing to snapshot
	}

	t := desc.Table
	snap := ddbSnapshot{
		TableName: name, KeySchema: t.KeySchema, Attributes: t.AttributeDefinitions,
		BillingMode: ddbtypes.BillingModeProvisioned,
	}
	if t.BillingModeSummary != nil && t.BillingModeSummary.BillingMode != "" {
		snap.BillingMode = t.BillingModeSummary.BillingMode
	}
	if snap.BillingMode == ddbtypes.BillingModeProvisioned && t.ProvisionedThroughput != nil {
		snap.Provisioned = &ddbtypes.ProvisionedThroughput{
			ReadCapacityUnits:  t.ProvisionedThroughput.ReadCapacityUnits,
			WriteCapacityUnits: t.ProvisionedThroughput.WriteCapacityUnits,
		}
	}
	for _, g := range t.GlobalSecondaryIndexes {
		gsi := ddbtypes.GlobalSecondaryIndex{IndexName: g.IndexName, KeySchema: g.KeySchema, Projection: g.Projection}
		if snap.Provisioned != nil && g.ProvisionedThroughput != nil {
			gsi.ProvisionedThroughput = &ddbtypes.ProvisionedThroughput{
				ReadCapacityUnits:  g.ProvisionedThroughput.ReadCapacityUnits,
				WriteCapacityUnits: g.ProvisionedThroughput.WriteCapacityUnits,
			}
		}
		snap.GSIs = append(snap.GSIs, gsi)
	}
	for _, l := range t.LocalSecondaryIndexes {
		snap.LSIs = append(snap.LSIs, ddbtypes.LocalSecondaryIndex{
			IndexName: l.IndexName, KeySchema: l.KeySchema, Projection: l.Projection,
		})
	}

	if snap.Items, err = e.scanAll(ctx, name); err != nil {
		return nil, err
	}

	return json.Marshal(snap)
}

func (e *ddbEngine) scanAll(ctx context.Context, table string) ([]map[string]json.RawMessage, error) {
	var (
		items []map[string]json.RawMessage
		start map[string]ddbtypes.AttributeValue
	)

	for {
		out, err := e.ddb.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(table), ExclusiveStartKey: start})
		if err != nil {
			return nil, fmt.Errorf("scan %s: %w", table, err)
		}

		for _, item := range out.Items {
			enc, encErr := encodeDDBItem(item)
			if encErr != nil {
				return nil, encErr
			}
			items = append(items, enc)
		}

		if len(out.LastEvaluatedKey) == 0 {
			return items, nil
		}
		start = out.LastEvaluatedKey
	}
}

func (e *ddbEngine) restore(ctx context.Context, data json.RawMessage, md map[string]string) (string, error) {
	var snap ddbSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return "", fmt.Errorf("decode DynamoDB snapshot: %w", err)
	}

	target := snap.TableName
	if v := md["targetTableName"]; v != "" {
		target = v
	}

	out, err := e.ddb.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(target), KeySchema: snap.KeySchema, AttributeDefinitions: snap.Attributes,
		BillingMode: snap.BillingMode, ProvisionedThroughput: snap.Provisioned,
		GlobalSecondaryIndexes: snap.GSIs, LocalSecondaryIndexes: snap.LSIs,
	})
	if err != nil {
		return "", fmt.Errorf("create table %s: %w", target, err)
	}

	for _, raw := range snap.Items {
		item, decErr := decodeDDBItem(raw)
		if decErr != nil {
			return "", decErr
		}
		if _, err = e.ddb.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(target), Item: item}); err != nil {
			return "", fmt.Errorf("restore item into %s: %w", target, err)
		}
	}

	return aws.ToString(out.TableDescription.TableArn), nil
}

type s3Engine struct{ store ObjectStore }

type s3Snapshot struct {
	Bucket  string         `json:"bucket"`
	Objects []s3SnapObject `json:"objects"`
}

type s3SnapObject struct {
	Key         string `json:"key"`
	ContentType string `json:"contentType,omitempty"`
	Body        []byte `json:"body"`
}

func (e *s3Engine) capture(ctx context.Context, resourceArn string) (json.RawMessage, error) {
	bucket, ok := s3BucketFromARN(resourceArn)
	if !ok {
		return nil, nil
	}
	if _, err := e.store.HeadBucket(ctx, &sdk_s3.HeadBucketInput{Bucket: aws.String(bucket)}); err != nil {
		return nil, nil //nolint:nilerr // a missing bucket simply has nothing to snapshot
	}

	snap := s3Snapshot{Bucket: bucket}
	var token *string

	for {
		page, err := e.store.ListObjectsV2(ctx, &sdk_s3.ListObjectsV2Input{
			Bucket: aws.String(bucket), ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list bucket %s: %w", bucket, err)
		}

		for _, obj := range page.Contents {
			so, getErr := e.readObject(ctx, bucket, aws.ToString(obj.Key))
			if getErr != nil {
				return nil, getErr
			}
			snap.Objects = append(snap.Objects, so)
		}

		if !aws.ToBool(page.IsTruncated) || page.NextContinuationToken == nil {
			break
		}
		token = page.NextContinuationToken
	}

	return json.Marshal(snap)
}

func (e *s3Engine) readObject(ctx context.Context, bucket, key string) (s3SnapObject, error) {
	out, err := e.store.GetObject(ctx, &sdk_s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return s3SnapObject{}, fmt.Errorf("get %s/%s: %w", bucket, key, err)
	}
	defer out.Body.Close()

	body, err := io.ReadAll(out.Body)
	if err != nil {
		return s3SnapObject{}, fmt.Errorf("read %s/%s: %w", bucket, key, err)
	}

	return s3SnapObject{Key: key, ContentType: aws.ToString(out.ContentType), Body: body}, nil
}

func (e *s3Engine) restore(ctx context.Context, data json.RawMessage, md map[string]string) (string, error) {
	var snap s3Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return "", fmt.Errorf("decode S3 snapshot: %w", err)
	}

	target := snap.Bucket
	if v := md["DestinationBucketName"]; v != "" {
		target = v
	}

	if _, err := e.store.HeadBucket(ctx, &sdk_s3.HeadBucketInput{Bucket: aws.String(target)}); err != nil {
		if _, err = e.store.CreateBucket(ctx, &sdk_s3.CreateBucketInput{Bucket: aws.String(target)}); err != nil {
			return "", fmt.Errorf("create bucket %s: %w", target, err)
		}
	}

	for _, o := range snap.Objects {
		in := &sdk_s3.PutObjectInput{Bucket: aws.String(target), Key: aws.String(o.Key), Body: bytes.NewReader(o.Body)}
		if o.ContentType != "" {
			in.ContentType = aws.String(o.ContentType)
		}
		if _, err := e.store.PutObject(ctx, in); err != nil {
			return "", fmt.Errorf("restore %s/%s: %w", target, o.Key, err)
		}
	}

	return "arn:aws:s3:::" + target, nil
}

type ebsEngine struct {
	vols      VolumeService
	region    string
	accountID string
}

func (e *ebsEngine) capture(_ context.Context, resourceArn string) (json.RawMessage, error) {
	info, ok := e.vols.DescribeVolume(resourceNameFromARN(resourceArn))
	if !ok {
		return nil, nil
	}

	return json.Marshal(info)
}

func (e *ebsEngine) restore(_ context.Context, data json.RawMessage, md map[string]string) (string, error) {
	var info VolumeInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return "", fmt.Errorf("decode EBS snapshot: %w", err)
	}
	if v := md["availabilityZone"]; v != "" {
		info.AZ = v
	}
	if v := md["volumeType"]; v != "" {
		info.VolumeType = v
	}

	id, err := e.vols.CreateVolume(info)
	if err != nil {
		return "", fmt.Errorf("create volume: %w", err)
	}

	return arn.Build("ec2", e.region, e.accountID, "volume/"+id), nil
}
