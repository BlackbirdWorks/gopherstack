package app

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sdks3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"

	dynamodbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const (
	e2eBucket     = "e2e-bench-bucket"
	e2eVerBucket  = "e2e-bench-versioned"
	e2eListKeys   = 10_000
	e2eDDBTable   = "e2e-bench-table"
	e2eDDBItems   = 10_000
	e2eDDBBatchTb = "e2e-bench-batch"
)

//nolint:gochecknoglobals // one-time fixtures shared across benchmark calibration runs
var e2eFixtureOnce sync.Once

func e2eSizes() map[string]int {
	return map[string]int{"1KiB": 1 << 10, "64KiB": 64 << 10, "1MiB": 1 << 20}
}

func e2eS3Client(b *testing.B) *sdks3.Client {
	b.Helper()

	e, _ := benchServer(b)
	cfg := newBenchAWSConfig(b, benchTransport{e: e})

	return sdks3.NewFromConfig(cfg, func(o *sdks3.Options) {
		o.BaseEndpoint = aws.String(benchEndpoint)
		o.UsePathStyle = true
	})
}

func e2eDDBClient(b *testing.B) *sdkdynamodb.Client {
	b.Helper()

	e, _ := benchServer(b)
	cfg := newBenchAWSConfig(b, benchTransport{e: e})

	return sdkdynamodb.NewFromConfig(cfg, func(o *sdkdynamodb.Options) {
		o.BaseEndpoint = aws.String(benchEndpoint)
	})
}

func e2eFixtures(b *testing.B) {
	b.Helper()

	_, byName := benchServer(b)

	e2eFixtureOnce.Do(func() {
		ctx := context.Background()

		s3H, ok := byName["S3"].(*s3backend.S3Handler)
		require.True(b, ok)

		for _, bk := range []string{e2eBucket, e2eVerBucket} {
			_, err := s3H.Backend.CreateBucket(ctx, &sdks3.CreateBucketInput{Bucket: aws.String(bk)})
			require.NoError(b, err)
		}

		_, err := s3H.Backend.PutBucketVersioning(ctx, &sdks3.PutBucketVersioningInput{
			Bucket: aws.String(e2eVerBucket),
			VersioningConfiguration: &s3types.VersioningConfiguration{
				Status: s3types.BucketVersioningStatusEnabled,
			},
		})
		require.NoError(b, err)

		sc := e2eS3Client(b)

		for name, n := range e2eSizes() {
			_, err = sc.PutObject(ctx, &sdks3.PutObjectInput{
				Bucket: aws.String(e2eBucket), Key: aws.String("get-" + name),
				Body: bytes.NewReader(bytes.Repeat([]byte("a"), n)),
			})
			require.NoError(b, err)
		}

		_, err = s3H.Backend.PutObject(ctx, &sdks3.PutObjectInput{
			Bucket: aws.String(e2eVerBucket), Key: aws.String("vkey"), Body: bytes.NewReader([]byte("v")),
		})
		require.NoError(b, err)

		for i := range e2eListKeys {
			_, err = s3H.Backend.PutObject(ctx, &sdks3.PutObjectInput{
				Bucket: aws.String(e2eBucket),
				Key:    aws.String(fmt.Sprintf("list/d%03d/f%04d", i/100, i%100)),
				Body:   bytes.NewReader([]byte("x")),
			})
			require.NoError(b, err)
		}

		ddbH, ok := byName["DynamoDB"].(*dynamodbbackend.DynamoDBHandler)
		require.True(b, ok)

		for _, name := range []string{e2eDDBTable, e2eDDBBatchTb} {
			ddbBenchCreateTable(ctx, b, ddbH.Backend, name, true)
		}

		for i := range e2eDDBItems {
			_, err = ddbH.Backend.PutItem(ctx, &sdkdynamodb.PutItemInput{
				TableName: aws.String(e2eDDBTable),
				Item: map[string]ddbtypes.AttributeValue{
					"pk":   &ddbtypes.AttributeValueMemberS{Value: "u" + strconv.Itoa(i%100)},
					"sk":   &ddbtypes.AttributeValueMemberS{Value: fmt.Sprintf("o%05d", i)},
					"val":  &ddbtypes.AttributeValueMemberN{Value: strconv.Itoa(i)},
					"name": &ddbtypes.AttributeValueMemberS{Value: "name " + strconv.Itoa(i)},
				},
			})
			require.NoError(b, err)
		}
	})
}

func BenchmarkE2E_S3(b *testing.B) {
	e2eFixtures(b)
	c := e2eS3Client(b)
	ctx := context.Background()

	for name, n := range e2eSizes() {
		payload := bytes.Repeat([]byte("p"), n)

		b.Run("put_"+name, func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; b.Loop(); i++ {
				_, err := c.PutObject(ctx, &sdks3.PutObjectInput{
					Bucket: aws.String(e2eBucket), Key: aws.String("put-" + name + strconv.Itoa(i%64)),
					Body: bytes.NewReader(payload),
				})
				require.NoError(b, err)
			}
		})

		b.Run("get_"+name, func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				out, err := c.GetObject(ctx, &sdks3.GetObjectInput{
					Bucket: aws.String(e2eBucket), Key: aws.String("get-" + name),
				})
				require.NoError(b, err)
				_, _ = io.Copy(io.Discard, out.Body)
				_ = out.Body.Close()
			}
		})
	}

	b.Run("head", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			_, err := c.HeadObject(ctx, &sdks3.HeadObjectInput{
				Bucket: aws.String(e2eBucket), Key: aws.String("get-1KiB"),
			})
			require.NoError(b, err)
		}
	})

	b.Run("list_prefix_delim_10k", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			out, err := c.ListObjectsV2(ctx, &sdks3.ListObjectsV2Input{
				Bucket: aws.String(e2eBucket), Prefix: aws.String("list/"), Delimiter: aws.String("/"),
			})
			require.NoError(b, err)
			require.NotEmpty(b, out.CommonPrefixes)
		}
	})

	b.Run("list_prefix_page", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			out, err := c.ListObjectsV2(ctx, &sdks3.ListObjectsV2Input{
				Bucket: aws.String(e2eBucket), Prefix: aws.String("list/d042/"),
			})
			require.NoError(b, err)
			require.Len(b, out.Contents, 100)
		}
	})

	b.Run("copy", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			_, err := c.CopyObject(ctx, &sdks3.CopyObjectInput{
				Bucket: aws.String(e2eBucket), Key: aws.String("copy-dst"),
				CopySource: aws.String(e2eBucket + "/get-64KiB"),
			})
			require.NoError(b, err)
		}
	})

	b.Run("delete_objects_1000", func(b *testing.B) { e2eDeleteObjects(b, c) })

	b.Run("multipart_3parts", func(b *testing.B) {
		b.ReportAllocs()

		part := bytes.Repeat([]byte("m"), 5<<20)

		for b.Loop() {
			cm, err := c.CreateMultipartUpload(ctx, &sdks3.CreateMultipartUploadInput{
				Bucket: aws.String(e2eBucket), Key: aws.String("mp"),
			})
			require.NoError(b, err)

			var parts []s3types.CompletedPart

			for pn := int32(1); pn <= 3; pn++ {
				body := part
				if pn == 3 {
					body = part[:1024]
				}

				up, uerr := c.UploadPart(ctx, &sdks3.UploadPartInput{
					Bucket: aws.String(e2eBucket), Key: aws.String("mp"), UploadId: cm.UploadId,
					PartNumber: aws.Int32(pn), Body: bytes.NewReader(body),
				})
				require.NoError(b, uerr)

				parts = append(parts, s3types.CompletedPart{ETag: up.ETag, PartNumber: aws.Int32(pn)})
			}

			_, err = c.CompleteMultipartUpload(ctx, &sdks3.CompleteMultipartUploadInput{
				Bucket: aws.String(e2eBucket), Key: aws.String("mp"), UploadId: cm.UploadId,
				MultipartUpload: &s3types.CompletedMultipartUpload{Parts: parts},
			})
			require.NoError(b, err)
		}
	})

	b.Run("versioned_put_get", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			_, err := c.PutObject(ctx, &sdks3.PutObjectInput{
				Bucket: aws.String(
					e2eVerBucket,
				),
				Key:  aws.String("vkey"),
				Body: bytes.NewReader([]byte("version body")),
			})
			require.NoError(b, err)

			out, err := c.GetObject(ctx, &sdks3.GetObjectInput{
				Bucket: aws.String(e2eVerBucket), Key: aws.String("vkey"),
			})
			require.NoError(b, err)
			_, _ = io.Copy(io.Discard, out.Body)
			_ = out.Body.Close()
		}
	})
}

func e2eDeleteObjects(b *testing.B, c *sdks3.Client) {
	b.Helper()
	b.ReportAllocs()

	ctx := context.Background()
	_, byName := benchServer(b)
	s3H, ok := byName["S3"].(*s3backend.S3Handler)
	require.True(b, ok)

	ids := make([]s3types.ObjectIdentifier, 1000)
	for i := range ids {
		ids[i] = s3types.ObjectIdentifier{Key: aws.String("del/k" + strconv.Itoa(i))}
	}

	for b.Loop() {
		b.StopTimer()

		for i := range ids {
			_, err := s3H.Backend.PutObject(ctx, &sdks3.PutObjectInput{
				Bucket: aws.String(e2eBucket), Key: ids[i].Key, Body: bytes.NewReader([]byte("d")),
			})
			require.NoError(b, err)
		}

		b.StartTimer()

		out, err := c.DeleteObjects(ctx, &sdks3.DeleteObjectsInput{
			Bucket: aws.String(e2eBucket), Delete: &s3types.Delete{Objects: ids},
		})
		require.NoError(b, err)
		require.Len(b, out.Deleted, 1000)
	}
}

func e2eItem(pk, sk string, v int) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		"pk":   &ddbtypes.AttributeValueMemberS{Value: pk},
		"sk":   &ddbtypes.AttributeValueMemberS{Value: sk},
		"val":  &ddbtypes.AttributeValueMemberN{Value: strconv.Itoa(v)},
		"name": &ddbtypes.AttributeValueMemberS{Value: "name " + strconv.Itoa(v)},
	}
}

func e2eKey(pk, sk string) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		"pk": &ddbtypes.AttributeValueMemberS{Value: pk},
		"sk": &ddbtypes.AttributeValueMemberS{Value: sk},
	}
}

func BenchmarkE2E_DynamoDB(b *testing.B) {
	e2eFixtures(b)
	c := e2eDDBClient(b)
	ctx := context.Background()

	b.Run("putitem", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; b.Loop(); i++ {
			_, err := c.PutItem(ctx, &sdkdynamodb.PutItemInput{
				TableName: aws.String(e2eDDBBatchTb), Item: e2eItem("p"+strconv.Itoa(i%256), "s", i),
			})
			require.NoError(b, err)
		}
	})

	b.Run("getitem", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			out, err := c.GetItem(ctx, &sdkdynamodb.GetItemInput{
				TableName: aws.String(e2eDDBTable), Key: e2eKey("u7", "o00007"),
			})
			require.NoError(b, err)
			require.NotEmpty(b, out.Item)
		}
	})

	b.Run("updateitem_expr", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			_, err := c.UpdateItem(ctx, &sdkdynamodb.UpdateItemInput{
				TableName: aws.String(e2eDDBTable), Key: e2eKey("u8", "o00008"),
				UpdateExpression:         aws.String("SET val = val + :inc, #n = :nm"),
				ExpressionAttributeNames: map[string]string{"#n": "name"},
				ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
					":inc": &ddbtypes.AttributeValueMemberN{Value: "1"},
					":nm":  &ddbtypes.AttributeValueMemberS{Value: "updated"},
				},
			})
			require.NoError(b, err)
		}
	})

	b.Run("query_filter", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			out, err := c.Query(ctx, &sdkdynamodb.QueryInput{
				TableName:              aws.String(e2eDDBTable),
				KeyConditionExpression: aws.String("pk = :pk AND begins_with(sk, :p)"),
				FilterExpression:       aws.String("val > :v"),
				ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
					":pk": &ddbtypes.AttributeValueMemberS{Value: "u3"},
					":p":  &ddbtypes.AttributeValueMemberS{Value: "o"},
					":v":  &ddbtypes.AttributeValueMemberN{Value: "5000"},
				},
			})
			require.NoError(b, err)
			require.NotEmpty(b, out.Items)
		}
	})

	b.Run("scan_filter_10k", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			_, err := c.Scan(ctx, &sdkdynamodb.ScanInput{
				TableName:        aws.String(e2eDDBTable),
				FilterExpression: aws.String("val BETWEEN :a AND :b"),
				ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
					":a": &ddbtypes.AttributeValueMemberN{Value: "100"},
					":b": &ddbtypes.AttributeValueMemberN{Value: "200"},
				},
			})
			require.NoError(b, err)
		}
	})

	b.Run("batchwrite_25", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; b.Loop(); i++ {
			reqs := make([]ddbtypes.WriteRequest, 25)
			for j := range reqs {
				reqs[j] = ddbtypes.WriteRequest{PutRequest: &ddbtypes.PutRequest{
					Item: e2eItem("bw"+strconv.Itoa(j), "s"+strconv.Itoa(i%16), i),
				}}
			}

			_, err := c.BatchWriteItem(ctx, &sdkdynamodb.BatchWriteItemInput{
				RequestItems: map[string][]ddbtypes.WriteRequest{e2eDDBBatchTb: reqs},
			})
			require.NoError(b, err)
		}
	})

	b.Run("batchget_100", func(b *testing.B) {
		b.ReportAllocs()

		keys := make([]map[string]ddbtypes.AttributeValue, 100)
		for i := range keys {
			keys[i] = e2eKey("u"+strconv.Itoa(i), fmt.Sprintf("o%05d", i))
		}

		for b.Loop() {
			out, err := c.BatchGetItem(ctx, &sdkdynamodb.BatchGetItemInput{
				RequestItems: map[string]ddbtypes.KeysAndAttributes{e2eDDBTable: {Keys: keys}},
			})
			require.NoError(b, err)
			require.Len(b, out.Responses[e2eDDBTable], 100)
		}
	})

	b.Run("transactwrite_10", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; b.Loop(); i++ {
			items := make([]ddbtypes.TransactWriteItem, 10)
			for j := range items {
				items[j] = ddbtypes.TransactWriteItem{Put: &ddbtypes.Put{
					TableName: aws.String(e2eDDBBatchTb), Item: e2eItem("tx"+strconv.Itoa(j), "s", i),
				}}
			}

			_, err := c.TransactWriteItems(ctx, &sdkdynamodb.TransactWriteItemsInput{TransactItems: items})
			require.NoError(b, err)
		}
	})

	b.Run("conditional_put_fail", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			_, err := c.PutItem(ctx, &sdkdynamodb.PutItemInput{
				TableName: aws.String(e2eDDBTable), Item: e2eItem("u1", "o00001", 1),
				ConditionExpression: aws.String("attribute_not_exists(pk)"),
			})
			require.Error(b, err)
		}
	})
}
