package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"net/http/httptest"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdkdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sdkstreams "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	sdks3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cloudtrailbackend "github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

const (
	leakWarmupOps       = 1500
	leakMeasureOps      = 2000
	leakMaxGoroutines   = 12
	leakSettleTimeout   = 60 * time.Second
	leakJanitorInterval = 50 * time.Millisecond
	leakRegions         = 30
)

// skipLeakInShort keeps the minutes-long -race leak suite out of the sharded
// -short unit run; the dedicated `leak-test` target and CI job run it.
func skipLeakInShort(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("leak suite runs via `task leak-test`")
	}
}

type leakSnapshot struct {
	goroutines int
	heapAlloc  uint64
	heapObjs   uint64
	series     int
}

type leakEnv struct {
	// resetBounded clears the intentionally bounded event stores (CloudTrail
	// keeps up to 100k events) so they do not mask real growth.
	resetBounded func()
	cfg          aws.Config
}

func newLeakEnv(t *testing.T) *leakEnv {
	t.Helper()

	log := buildLogger("")
	cli := CLI{AccountID: "000000000000", Region: "us-east-1"}
	cli.S3.JanitorInterval = leakJanitorInterval
	cli.DynamoDB.JanitorInterval = leakJanitorInterval
	cli.portAlloc = setupPortAllocatorWithReservations(t.Context(), log, cli)
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServicesT(t, &service.AppContext{
		Logger: log, Config: &cli, JanitorCtx: t.Context(), PortAlloc: cli.portAlloc,
	})
	require.NoError(t, err)

	e := buildEchoServer(t.Context(), log, nil, services, cli)
	require.NoError(t, setupChaosAndRegistry(e, log, &cli, services))

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awscfg.WithBaseEndpoint(srv.URL),
	)
	require.NoError(t, err)

	byName := serviceByName(services)
	startBackgroundWorkers(t.Context(), []service.Registerable{
		byName["S3"], byName["DynamoDB"], byName["DynamoDBStreams"],
	})

	ct, ok := byName["CloudTrail"].(*cloudtrailbackend.Handler)
	require.True(t, ok)

	return &leakEnv{cfg: cfg, resetBounded: ct.Reset}
}

// leakOwnedSeries reports whether the series carries a label value naming a
// resource created by this suite (all prefixed "leak"). The default registry
// is process-wide, so unrelated tests creating or sweeping series concurrently
// must not count toward a leak test's budget.
func leakOwnedSeries(m *dto.Metric) bool {
	for _, lp := range m.GetLabel() {
		if strings.Contains(lp.GetValue(), "leak") {
			return true
		}
	}

	return false
}

func takeLeakSnapshot(t *testing.T, env *leakEnv) leakSnapshot {
	t.Helper()

	env.resetBounded()

	for range 3 {
		runtime.GC()
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	series := 0
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			if leakOwnedSeries(m) {
				series++
			}
		}
	}

	return leakSnapshot{
		goroutines: runtime.NumGoroutine(),
		heapAlloc:  ms.HeapAlloc,
		heapObjs:   ms.HeapObjects,
		series:     series,
	}
}

type leakBudget struct {
	heapBytes uint64
	heapObjs  uint64
	series    int
	// settle waits for asynchronous janitor reclamation (deleted buckets and
	// tables are swept on a timer) before the final snapshot.
	settle bool
}

func assertNoLeak(t *testing.T, env *leakEnv, budget leakBudget, op func(i int)) {
	t.Helper()

	for i := range leakWarmupOps {
		op(i)
	}

	before := takeLeakSnapshot(t, env)

	for i := range leakMeasureOps {
		op(leakWarmupOps + i)
	}

	after := takeLeakSnapshot(t, env)

	if budget.settle {
		require.Eventually(t, func() bool {
			after = takeLeakSnapshot(t, env)

			return after.series-before.series <= budget.series
		}, leakSettleTimeout, time.Second, "series never returned to baseline: %d -> %d", before.series, after.series)
	}

	growth := int64(after.heapAlloc) - int64(before.heapAlloc)
	objs := int64(after.heapObjs) - int64(before.heapObjs)
	t.Logf("goroutines %d->%d heapAlloc %+d B heapObjs %+d series %d->%d",
		before.goroutines, after.goroutines, growth, objs, before.series, after.series)

	require.LessOrEqual(t, after.goroutines-before.goroutines, leakMaxGoroutines, "goroutine growth")
	require.LessOrEqual(t, growth, int64(budget.heapBytes), "live heap growth")
	require.LessOrEqual(t, objs, int64(budget.heapObjs), "live heap objects growth")
	require.LessOrEqual(t, after.series-before.series, budget.series, "prometheus series growth")
}

func leakS3(env *leakEnv) *sdks3.Client {
	return sdks3.NewFromConfig(env.cfg, func(o *sdks3.Options) { o.UsePathStyle = true })
}

func leakDDB(env *leakEnv) *sdkdynamodb.Client {
	return sdkdynamodb.NewFromConfig(env.cfg)
}

//nolint:paralleltest // measures process-wide heap and goroutine counts
func TestLeakS3(t *testing.T) {
	skipLeakInShort(t)

	budget := leakBudget{heapBytes: 1 << 20, heapObjs: 10_000, series: 40}
	ctx := context.Background()
	payload := bytes.Repeat([]byte("l"), 8<<10)

	cases := []struct {
		op     func(t *testing.T, c *sdks3.Client) func(i int)
		name   string
		settle bool
	}{
		{name: "put_get_delete", op: func(t *testing.T, c *sdks3.Client) func(int) {
			t.Helper()
			mustS3Bucket(t, c, "leak-pgd", false)

			return func(i int) {
				k := aws.String("k" + strconv.Itoa(i%8))
				_, err := c.PutObject(ctx, &sdks3.PutObjectInput{
					Bucket: aws.String("leak-pgd"), Key: k, Body: bytes.NewReader(payload),
				})
				require.NoError(t, err)
				out, err := c.GetObject(ctx, &sdks3.GetObjectInput{Bucket: aws.String("leak-pgd"), Key: k})
				require.NoError(t, err)
				_ = out.Body.Close()
				_, err = c.DeleteObject(ctx, &sdks3.DeleteObjectInput{Bucket: aws.String("leak-pgd"), Key: k})
				require.NoError(t, err)
			}
		}},
		{name: "unique_key_put_delete", op: func(t *testing.T, c *sdks3.Client) func(int) {
			t.Helper()
			mustS3Bucket(t, c, "leak-ukpd", false)

			return func(i int) {
				k := aws.String("unique-" + strconv.Itoa(i))
				_, err := c.PutObject(ctx, &sdks3.PutObjectInput{
					Bucket: aws.String("leak-ukpd"), Key: k, Body: bytes.NewReader(payload),
				})
				require.NoError(t, err)
				_, err = c.DeleteObject(ctx, &sdks3.DeleteObjectInput{Bucket: aws.String("leak-ukpd"), Key: k})
				require.NoError(t, err)
			}
		}},
		{name: "error_paths", op: func(t *testing.T, c *sdks3.Client) func(int) {
			t.Helper()
			mustS3Bucket(t, c, "leak-err", false)

			return func(i int) {
				_, err := c.GetObject(ctx, &sdks3.GetObjectInput{
					Bucket: aws.String("leak-err"), Key: aws.String("missing" + strconv.Itoa(i)),
				})
				require.Error(t, err)
				_, err = c.HeadObject(ctx, &sdks3.HeadObjectInput{
					Bucket: aws.String("leak-err"), Key: aws.String("missing"),
				})
				require.Error(t, err)
			}
		}},
		{name: "multipart_complete_abort", op: func(t *testing.T, c *sdks3.Client) func(int) {
			t.Helper()
			mustS3Bucket(t, c, "leak-mp", false)

			return func(i int) {
				cm, err := c.CreateMultipartUpload(ctx, &sdks3.CreateMultipartUploadInput{
					Bucket: aws.String("leak-mp"), Key: aws.String("mp"),
				})
				require.NoError(t, err)

				up, err := c.UploadPart(ctx, &sdks3.UploadPartInput{
					Bucket: aws.String("leak-mp"), Key: aws.String("mp"), UploadId: cm.UploadId,
					PartNumber: aws.Int32(1), Body: bytes.NewReader(payload),
				})
				require.NoError(t, err)

				if i%2 == 0 {
					_, err = c.CompleteMultipartUpload(ctx, &sdks3.CompleteMultipartUploadInput{
						Bucket: aws.String("leak-mp"), Key: aws.String("mp"), UploadId: cm.UploadId,
						MultipartUpload: &s3types.CompletedMultipartUpload{
							Parts: []s3types.CompletedPart{{ETag: up.ETag, PartNumber: aws.Int32(1)}},
						},
					})
				} else {
					_, err = c.AbortMultipartUpload(ctx, &sdks3.AbortMultipartUploadInput{
						Bucket: aws.String("leak-mp"), Key: aws.String("mp"), UploadId: cm.UploadId,
					})
				}

				require.NoError(t, err)
			}
		}},
		{name: "bucket_create_delete", op: func(t *testing.T, c *sdks3.Client) func(int) {
			t.Helper()

			return func(i int) {
				b := aws.String("leak-b-" + strconv.Itoa(i))
				_, err := c.CreateBucket(ctx, &sdks3.CreateBucketInput{Bucket: b})
				require.NoError(t, err)
				_, err = c.PutObject(ctx, &sdks3.PutObjectInput{
					Bucket: b, Key: aws.String("o"), Body: bytes.NewReader(payload),
				})
				require.NoError(t, err)
				_, err = c.DeleteObject(ctx, &sdks3.DeleteObjectInput{Bucket: b, Key: aws.String("o")})
				require.NoError(t, err)
				_, err = c.DeleteBucket(ctx, &sdks3.DeleteBucketInput{Bucket: b})
				require.NoError(t, err)
			}
		}, settle: true},
		{name: "versioned_put_delete_version", op: func(t *testing.T, c *sdks3.Client) func(int) {
			t.Helper()
			mustS3Bucket(t, c, "leak-ver", true)

			return func(int) {
				put, err := c.PutObject(ctx, &sdks3.PutObjectInput{
					Bucket: aws.String("leak-ver"), Key: aws.String("v"), Body: bytes.NewReader(payload),
				})
				require.NoError(t, err)
				_, err = c.DeleteObject(ctx, &sdks3.DeleteObjectInput{
					Bucket: aws.String("leak-ver"), Key: aws.String("v"), VersionId: put.VersionId,
				})
				require.NoError(t, err)
			}
		}},
		{name: "tagging_churn", op: func(t *testing.T, c *sdks3.Client) func(int) {
			t.Helper()
			mustS3Bucket(t, c, "leak-tag", false)
			_, err := c.PutObject(ctx, &sdks3.PutObjectInput{
				Bucket: aws.String("leak-tag"), Key: aws.String("o"), Body: bytes.NewReader(payload),
			})
			require.NoError(t, err)

			return func(i int) {
				tags := &s3types.Tagging{
					TagSet: []s3types.Tag{{Key: aws.String("k"), Value: aws.String(strconv.Itoa(i))}},
				}
				_, err = c.PutObjectTagging(ctx, &sdks3.PutObjectTaggingInput{
					Bucket: aws.String("leak-tag"), Key: aws.String("o"), Tagging: tags,
				})
				require.NoError(t, err)
				_, err = c.PutBucketTagging(
					ctx,
					&sdks3.PutBucketTaggingInput{Bucket: aws.String("leak-tag"), Tagging: tags},
				)
				require.NoError(t, err)
				_, err = c.DeleteObjectTagging(ctx, &sdks3.DeleteObjectTaggingInput{
					Bucket: aws.String("leak-tag"), Key: aws.String("o"),
				})
				require.NoError(t, err)
				_, err = c.DeleteBucketTagging(ctx, &sdks3.DeleteBucketTaggingInput{Bucket: aws.String("leak-tag")})
				require.NoError(t, err)
			}
		}},
		{name: "unique_missing_bucket", op: func(t *testing.T, c *sdks3.Client) func(int) {
			t.Helper()

			return func(i int) {
				_, err := c.GetObject(ctx, &sdks3.GetObjectInput{
					Bucket: aws.String("leak-nobucket-" + strconv.Itoa(i)), Key: aws.String("k"),
				})
				require.Error(t, err)
			}
		}},
		{name: "copy_list_deleteobjects", op: func(t *testing.T, c *sdks3.Client) func(int) {
			t.Helper()
			mustS3Bucket(t, c, "leak-cld", false)

			return func(i int) {
				src := aws.String("src" + strconv.Itoa(i%4))
				_, err := c.PutObject(ctx, &sdks3.PutObjectInput{
					Bucket: aws.String("leak-cld"), Key: src, Body: bytes.NewReader(payload),
				})
				require.NoError(t, err)
				_, err = c.CopyObject(ctx, &sdks3.CopyObjectInput{
					Bucket: aws.String("leak-cld"), Key: aws.String("dst"),
					CopySource: aws.String("leak-cld/" + *src),
				})
				require.NoError(t, err)
				_, err = c.ListObjectsV2(ctx, &sdks3.ListObjectsV2Input{Bucket: aws.String("leak-cld")})
				require.NoError(t, err)
				_, err = c.DeleteObjects(ctx, &sdks3.DeleteObjectsInput{
					Bucket: aws.String("leak-cld"),
					Delete: &s3types.Delete{Objects: []s3types.ObjectIdentifier{{Key: src}, {Key: aws.String("dst")}}},
				})
				require.NoError(t, err)
			}
		}},
	}

	for _, tc := range cases {
		//nolint:paralleltest // measures process-wide heap and goroutine counts
		t.Run(tc.name, func(t *testing.T) {
			env := newLeakEnv(t)
			budget := budget
			budget.settle = tc.settle
			assertNoLeak(t, env, budget, tc.op(t, leakS3(env)))
		})
	}
}

func mustS3Bucket(t *testing.T, c *sdks3.Client, name string, versioned bool) {
	t.Helper()

	_, err := c.CreateBucket(context.Background(), &sdks3.CreateBucketInput{Bucket: aws.String(name)})
	require.NoError(t, err)

	if !versioned {
		return
	}

	_, err = c.PutBucketVersioning(context.Background(), &sdks3.PutBucketVersioningInput{
		Bucket: aws.String(name),
		VersioningConfiguration: &s3types.VersioningConfiguration{
			Status: s3types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)
}

func leakDDBTable(t *testing.T, c *sdkdynamodb.Client, name string, stream bool) {
	t.Helper()

	in := &sdkdynamodb.CreateTableInput{
		TableName: aws.String(name),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	}

	if stream {
		in.StreamSpecification = &ddbtypes.StreamSpecification{
			StreamEnabled: aws.Bool(true), StreamViewType: ddbtypes.StreamViewTypeNewAndOldImages,
		}
	}

	_, err := c.CreateTable(context.Background(), in)
	require.NoError(t, err)
}

func leakItem(i int) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		"pk":  &ddbtypes.AttributeValueMemberS{Value: "k" + strconv.Itoa(i)},
		"val": &ddbtypes.AttributeValueMemberN{Value: strconv.Itoa(i)},
	}
}

func leakKey(i int) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{"pk": &ddbtypes.AttributeValueMemberS{Value: "k" + strconv.Itoa(i)}}
}

//nolint:paralleltest // measures process-wide heap and goroutine counts
func TestLeakDynamoDB(t *testing.T) {
	skipLeakInShort(t)

	budget := leakBudget{heapBytes: 1 << 20, heapObjs: 10_000, series: 40}
	ctx := context.Background()

	cases := []struct {
		op     func(t *testing.T, c *sdkdynamodb.Client) func(i int)
		name   string
		settle bool
	}{
		{name: "item_crud", op: func(t *testing.T, c *sdkdynamodb.Client) func(int) {
			t.Helper()
			leakDDBTable(t, c, "leak-crud", false)

			return func(i int) {
				j := i % 16
				_, err := c.PutItem(
					ctx,
					&sdkdynamodb.PutItemInput{TableName: aws.String("leak-crud"), Item: leakItem(j)},
				)
				require.NoError(t, err)
				_, err = c.UpdateItem(ctx, &sdkdynamodb.UpdateItemInput{
					TableName:        aws.String("leak-crud"),
					Key:              leakKey(j),
					UpdateExpression: aws.String("SET val = val + :i"),
					ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
						":i": &ddbtypes.AttributeValueMemberN{Value: "1"},
					},
				})
				require.NoError(t, err)
				_, err = c.GetItem(ctx, &sdkdynamodb.GetItemInput{TableName: aws.String("leak-crud"), Key: leakKey(j)})
				require.NoError(t, err)
				_, err = c.DeleteItem(
					ctx,
					&sdkdynamodb.DeleteItemInput{TableName: aws.String("leak-crud"), Key: leakKey(j)},
				)
				require.NoError(t, err)
			}
		}},
		{name: "conditional_failure", op: func(t *testing.T, c *sdkdynamodb.Client) func(int) {
			t.Helper()
			leakDDBTable(t, c, "leak-cond", false)
			_, err := c.PutItem(ctx, &sdkdynamodb.PutItemInput{TableName: aws.String("leak-cond"), Item: leakItem(1)})
			require.NoError(t, err)

			return func(int) {
				_, err = c.PutItem(ctx, &sdkdynamodb.PutItemInput{
					TableName: aws.String("leak-cond"), Item: leakItem(1),
					ConditionExpression: aws.String("attribute_not_exists(pk)"),
				})
				require.Error(t, err)
			}
		}},
		{name: "query_scan_batch_transact", op: func(t *testing.T, c *sdkdynamodb.Client) func(int) {
			t.Helper()
			leakDDBTable(t, c, "leak-qsb", false)

			return func(i int) {
				_, err := c.BatchWriteItem(ctx, &sdkdynamodb.BatchWriteItemInput{
					RequestItems: map[string][]ddbtypes.WriteRequest{"leak-qsb": {
						{PutRequest: &ddbtypes.PutRequest{Item: leakItem(i % 8)}},
						{PutRequest: &ddbtypes.PutRequest{Item: leakItem(8 + i%8)}},
					}},
				})
				require.NoError(t, err)
				_, err = c.TransactWriteItems(ctx, &sdkdynamodb.TransactWriteItemsInput{
					TransactItems: []ddbtypes.TransactWriteItem{
						{Put: &ddbtypes.Put{TableName: aws.String("leak-qsb"), Item: leakItem(16)}},
						{Put: &ddbtypes.Put{TableName: aws.String("leak-qsb"), Item: leakItem(17)}},
					},
				})
				require.NoError(t, err)
				_, err = c.Scan(ctx, &sdkdynamodb.ScanInput{
					TableName:        aws.String("leak-qsb"),
					FilterExpression: aws.String("val > :v"),
					ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
						":v": &ddbtypes.AttributeValueMemberN{Value: "3"},
					},
				})
				require.NoError(t, err)
				_, err = c.Query(ctx, &sdkdynamodb.QueryInput{
					TableName:              aws.String("leak-qsb"),
					KeyConditionExpression: aws.String("pk = :p"),
					ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
						":p": &ddbtypes.AttributeValueMemberS{Value: "k1"},
					},
				})
				require.NoError(t, err)
				_, err = c.BatchGetItem(ctx, &sdkdynamodb.BatchGetItemInput{
					RequestItems: map[string]ddbtypes.KeysAndAttributes{
						"leak-qsb": {Keys: []map[string]ddbtypes.AttributeValue{leakKey(1), leakKey(2)}},
					},
				})
				require.NoError(t, err)
			}
		}},
		{name: "table_create_delete", op: func(t *testing.T, c *sdkdynamodb.Client) func(int) {
			t.Helper()

			return func(i int) {
				name := fmt.Sprintf("leak-tbl-%d", i)
				leakDDBTable(t, c, name, i%2 == 0)
				_, err := c.PutItem(ctx, &sdkdynamodb.PutItemInput{TableName: aws.String(name), Item: leakItem(1)})
				require.NoError(t, err)
				_, err = c.DeleteTable(ctx, &sdkdynamodb.DeleteTableInput{TableName: aws.String(name)})
				require.NoError(t, err)
			}
		}, settle: true},
		{name: "transact_unique_token", op: func(t *testing.T, c *sdkdynamodb.Client) func(int) {
			t.Helper()
			leakDDBTable(t, c, "leak-tok", false)

			return func(i int) {
				_, err := c.TransactWriteItems(ctx, &sdkdynamodb.TransactWriteItemsInput{
					ClientRequestToken: aws.String("tok-" + strconv.Itoa(i)),
					TransactItems: []ddbtypes.TransactWriteItem{
						{Put: &ddbtypes.Put{TableName: aws.String("leak-tok"), Item: leakItem(i % 4)}},
					},
				})
				require.NoError(t, err)
			}
		}},
		{name: "backup_create_delete", op: func(t *testing.T, c *sdkdynamodb.Client) func(int) {
			t.Helper()
			leakDDBTable(t, c, "leak-bak", false)

			return func(i int) {
				out, err := c.CreateBackup(ctx, &sdkdynamodb.CreateBackupInput{
					TableName: aws.String("leak-bak"), BackupName: aws.String("b" + strconv.Itoa(i)),
				})
				require.NoError(t, err)
				_, err = c.DeleteBackup(ctx, &sdkdynamodb.DeleteBackupInput{BackupArn: out.BackupDetails.BackupArn})
				require.NoError(t, err)
			}
		}},
		{name: "tag_untag", op: func(t *testing.T, c *sdkdynamodb.Client) func(int) {
			t.Helper()
			leakDDBTable(t, c, "leak-tag", false)
			d, err := c.DescribeTable(ctx, &sdkdynamodb.DescribeTableInput{TableName: aws.String("leak-tag")})
			require.NoError(t, err)

			return func(i int) {
				k := "k" + strconv.Itoa(i)
				_, err = c.TagResource(ctx, &sdkdynamodb.TagResourceInput{
					ResourceArn: d.Table.TableArn, Tags: []ddbtypes.Tag{{Key: aws.String(k), Value: aws.String("v")}},
				})
				require.NoError(t, err)
				_, err = c.UntagResource(ctx, &sdkdynamodb.UntagResourceInput{
					ResourceArn: d.Table.TableArn, TagKeys: []string{k},
				})
				require.NoError(t, err)
			}
		}},
		{name: "unique_missing_table", op: func(t *testing.T, c *sdkdynamodb.Client) func(int) {
			t.Helper()

			return func(i int) {
				_, err := c.GetItem(ctx, &sdkdynamodb.GetItemInput{
					TableName: aws.String("leak-notable-" + strconv.Itoa(i)), Key: leakKey(1),
				})
				require.Error(t, err)
			}
		}},
		{name: "stream_table_writes", op: func(t *testing.T, c *sdkdynamodb.Client) func(int) {
			t.Helper()
			leakDDBTable(t, c, "leak-stream", true)

			return func(i int) {
				j := i % 8
				_, err := c.PutItem(
					ctx,
					&sdkdynamodb.PutItemInput{TableName: aws.String("leak-stream"), Item: leakItem(j)},
				)
				require.NoError(t, err)
				_, err = c.DeleteItem(
					ctx,
					&sdkdynamodb.DeleteItemInput{TableName: aws.String("leak-stream"), Key: leakKey(j)},
				)
				require.NoError(t, err)
			}
		}},
	}

	for _, tc := range cases {
		//nolint:paralleltest // measures process-wide heap and goroutine counts
		t.Run(tc.name, func(t *testing.T) {
			env := newLeakEnv(t)
			budget := budget
			budget.settle = tc.settle
			assertNoLeak(t, env, budget, tc.op(t, leakDDB(env)))
		})
	}
}

//nolint:paralleltest // measures process-wide heap and goroutine counts
func TestLeakDynamoDBStreamsRead(t *testing.T) {
	skipLeakInShort(t)

	ctx := context.Background()
	env := newLeakEnv(t)
	c := leakDDB(env)

	leakDDBTable(t, c, "leak-sread", true)
	desc, err := c.DescribeTable(ctx, &sdkdynamodb.DescribeTableInput{TableName: aws.String("leak-sread")})
	require.NoError(t, err)

	sc := sdkstreams.NewFromConfig(env.cfg)
	ds, err := sc.DescribeStream(ctx, &sdkstreams.DescribeStreamInput{StreamArn: desc.Table.LatestStreamArn})
	require.NoError(t, err)
	require.NotEmpty(t, ds.StreamDescription.Shards)

	shard := ds.StreamDescription.Shards[0].ShardId

	// Iterators stay valid for their 15-minute TTL, so ~2 objects per issued iterator is expected.
	assertNoLeak(t, env, leakBudget{heapBytes: 1 << 20, heapObjs: 12_000, series: 40}, func(i int) {
		_, perr := c.PutItem(ctx, &sdkdynamodb.PutItemInput{TableName: aws.String("leak-sread"), Item: leakItem(i % 8)})
		require.NoError(t, perr)

		it, ierr := sc.GetShardIterator(ctx, &sdkstreams.GetShardIteratorInput{
			StreamArn: desc.Table.LatestStreamArn, ShardId: shard,
			ShardIteratorType: streamtypes.ShardIteratorTypeTrimHorizon,
		})
		require.NoError(t, ierr)

		_, gerr := sc.GetRecords(ctx, &sdkstreams.GetRecordsInput{ShardIterator: it.ShardIterator, Limit: aws.Int32(5)})

		if _, trimmed := errors.AsType[*streamtypes.TrimmedDataAccessException](gerr); !trimmed {
			require.NoError(t, gerr)
		}
	})
}

//nolint:paralleltest // measures process-wide heap and goroutine counts
func TestLeakMiddlewareCardinality(t *testing.T) {
	skipLeakInShort(t)

	ctx := context.Background()
	env := newLeakEnv(t)
	s3c := leakS3(env)
	ddbc := leakDDB(env)

	tests := []struct {
		s3   func(i int) func(*sdks3.Options)
		ddb  func(i int) func(*sdkdynamodb.Options)
		name string
	}{
		{
			name: "unique_region",
			s3: func(i int) func(*sdks3.Options) {
				return func(o *sdks3.Options) { o.Region = "xx-region-" + strconv.Itoa(i%leakRegions) }
			},
			ddb: func(i int) func(*sdkdynamodb.Options) {
				return func(o *sdkdynamodb.Options) { o.Region = "xx-region-" + strconv.Itoa(i%leakRegions) }
			},
		},
		{
			name: "unique_access_key",
			s3: func(i int) func(*sdks3.Options) {
				return func(o *sdks3.Options) {
					o.Credentials = credentials.NewStaticCredentialsProvider("AKIA"+strconv.Itoa(i), "secret", "")
				}
			},
			ddb: func(i int) func(*sdkdynamodb.Options) {
				return func(o *sdkdynamodb.Options) {
					o.Credentials = credentials.NewStaticCredentialsProvider("AKIA"+strconv.Itoa(i), "secret", "")
				}
			},
		},
	}

	for _, tt := range tests {
		//nolint:paralleltest // measures process-wide heap and goroutine counts
		t.Run(tt.name, func(t *testing.T) {
			assertNoLeak(t, env, leakBudget{heapBytes: 1 << 20, heapObjs: 10_000, series: 40}, func(i int) {
				_, err := s3c.ListBuckets(ctx, &sdks3.ListBucketsInput{}, tt.s3(i))
				require.NoError(t, err)

				_, err = ddbc.ListTables(ctx, &sdkdynamodb.ListTablesInput{}, tt.ddb(i))
				require.NoError(t, err)
			})
		})
	}
}
