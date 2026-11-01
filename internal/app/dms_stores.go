package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/dmsengine"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	dmsbackend "github.com/blackbirdworks/gopherstack/services/dms"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// wireRDSIntegrations connects the RDS Data API and DMS to the services they read and write.
func wireRDSIntegrations(byName map[string]service.Registerable) {
	wireRDSData(byName["RDSData"], byName["RDS"], byName["SecretsManager"])
	wireDMSStores(byName)
}

// wireDMSStores lets DMS replication tasks write into the in-process S3, Kinesis and DynamoDB backends.
func wireDMSStores(byName map[string]service.Registerable) {
	dmsH, ok := byName["DMS"].(*dmsbackend.Handler)
	if !ok {
		return
	}

	var stores dmsengine.Stores

	if h, hok := byName["S3"].(*s3backend.S3Handler); hok {
		if bk, bok := h.Backend.(*s3backend.InMemoryBackend); bok {
			stores.Objects = &dmsObjects{bk: bk}
		}
	}

	if h, hok := byName["Kinesis"].(*kinesisbackend.Handler); hok {
		if bk, bok := h.Backend.(*kinesisbackend.InMemoryBackend); bok {
			stores.Streams = &dmsStreams{bk: bk}
		}
	}

	if h, hok := byName["DynamoDB"].(*ddbbackend.DynamoDBHandler); hok {
		if bk, bok := h.Backend.(*ddbbackend.InMemoryDB); bok {
			stores.Dynamo = &dmsDynamo{db: bk}
		}
	}

	dmsH.Backend.SetStores(stores)

	if h, hok := byName["SecretsManager"].(*secretsmanagerbackend.Handler); hok {
		dmsH.Backend.SetSecrets(dmsSecrets{bk: h.Backend})
	}
}

// dmsSecrets resolves endpoint SecretsManagerSecretId values against the in-process Secrets Manager.
type dmsSecrets struct {
	bk secretsmanagerbackend.StorageBackend
}

func (d dmsSecrets) SecretString(ctx context.Context, region, secretID string) (string, error) {
	out, err := d.bk.GetSecretValue(secretsmanagerbackend.WithRegion(ctx, region),
		&secretsmanagerbackend.GetSecretValueInput{SecretID: secretID})
	if err != nil {
		return "", err
	}

	return out.SecretString, nil
}

type dmsObjects struct{ bk *s3backend.InMemoryBackend }

func (o *dmsObjects) PutObject(ctx context.Context, bucket, key string, body []byte) error {
	_, err := o.bk.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body),
	})

	return err
}

func (o *dmsObjects) ListObjects(ctx context.Context, bucket, prefix string) ([]string, error) {
	var (
		keys  []string
		token *string
	)

	for {
		out, err := o.bk.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket), Prefix: aws.String(prefix), ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}

		for _, obj := range out.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}

		if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
			return keys, nil
		}

		token = out.NextContinuationToken
	}
}

func (o *dmsObjects) GetObject(ctx context.Context, bucket, key string) ([]byte, error) {
	out, err := o.bk.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()

	return io.ReadAll(out.Body)
}

func (o *dmsObjects) CheckBucket(ctx context.Context, bucket string) error {
	_, err := o.bk.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})

	return err
}

type dmsStreams struct {
	bk *kinesisbackend.InMemoryBackend
}

func (s *dmsStreams) PutRecord(ctx context.Context, stream, partitionKey string, data []byte) error {
	_, err := s.bk.PutRecord(ctx, &kinesisbackend.PutRecordInput{
		StreamName: stream, PartitionKey: partitionKey, Data: data,
	})

	return err
}

func (s *dmsStreams) CheckStream(ctx context.Context, stream string) error {
	_, err := s.bk.DescribeStream(ctx, &kinesisbackend.DescribeStreamInput{StreamName: stream})

	return err
}

type dmsDynamo struct{ db *ddbbackend.InMemoryDB }

var errDynamoKeys = errors.New("DynamoDB target tables take one or two key attributes")

func attrValue(v dmsengine.DynamoValue) ddbtypes.AttributeValue {
	switch v.Type {
	case "N":
		return &ddbtypes.AttributeValueMemberN{Value: v.S}
	case "B":
		return &ddbtypes.AttributeValueMemberB{Value: v.B}
	default:
		return &ddbtypes.AttributeValueMemberS{Value: v.S}
	}
}

func attrMap(m map[string]dmsengine.DynamoValue) map[string]ddbtypes.AttributeValue {
	out := make(map[string]ddbtypes.AttributeValue, len(m))
	for k, v := range m {
		out[k] = attrValue(v)
	}

	return out
}

func (d *dmsDynamo) CreateTable(ctx context.Context, name string, keys []dmsengine.DynamoKey) error {
	if len(keys) == 0 || len(keys) > 2 {
		return errDynamoKeys
	}

	schema := make([]ddbtypes.KeySchemaElement, len(keys))
	defs := make([]ddbtypes.AttributeDefinition, len(keys))

	for i, k := range keys {
		kt, st := ddbtypes.KeyTypeHash, ddbtypes.ScalarAttributeTypeS
		if i == 1 {
			kt = ddbtypes.KeyTypeRange
		}

		if k.Number {
			st = ddbtypes.ScalarAttributeTypeN
		}

		schema[i] = ddbtypes.KeySchemaElement{AttributeName: aws.String(k.Name), KeyType: kt}
		defs[i] = ddbtypes.AttributeDefinition{AttributeName: aws.String(k.Name), AttributeType: st}
	}

	_, err := d.db.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(name), KeySchema: schema, AttributeDefinitions: defs,
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	if err != nil {
		return fmt.Errorf("create DynamoDB table %s: %w", name, err)
	}

	return nil
}

func (d *dmsDynamo) DropTable(ctx context.Context, name string) error {
	_, err := d.db.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(name)})

	return err
}

func (d *dmsDynamo) PutItem(ctx context.Context, table string, item map[string]dmsengine.DynamoValue) error {
	_, err := d.db.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(table), Item: attrMap(item)})

	return err
}

func (d *dmsDynamo) DeleteItem(ctx context.Context, table string, key map[string]dmsengine.DynamoValue) error {
	_, err := d.db.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(table), Key: attrMap(key)})

	return err
}
