package gopherstack_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"google.golang.org/api/option"

	gopherstack "github.com/blackbirdworks/gopherstack/testcontainers/go"
)

func dockerAvailable(ctx context.Context) bool {
	p, err := testcontainers.NewDockerProvider()
	if err != nil {
		return false
	}
	defer p.Close()

	return p.Health(ctx) == nil
}

func TestGopherstackContainer(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)

	if !dockerAvailable(ctx) {
		t.Skip("docker unavailable")
	}

	gs, err := gopherstack.Run(ctx, os.Getenv("GOPHERSTACK_TEST_IMAGE"),
		gopherstack.WithServices("s3", "sqs", "azureblob", "gcpstorage"))
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, gs.Container)

	cfg, err := gs.AWSConfig(ctx)
	require.NoError(t, err)

	tests := []struct {
		fn   func(*testing.T, *gopherstack.Container, aws.Config)
		name string
	}{
		{name: "endpoints", fn: checkEndpoints},
		{name: "s3", fn: checkS3},
		{name: "sqs", fn: checkSQS},
		{name: "azure blob", fn: checkAzureBlob},
		{name: "gcp storage", fn: checkGCPStorage},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.fn(t, gs, cfg)
		})
	}
}

func checkEndpoints(t *testing.T, gs *gopherstack.Container, _ aws.Config) {
	t.Helper()

	ep, err := gs.Endpoint(t.Context())
	require.NoError(t, err)
	require.Contains(t, ep, "http://")

	_, err = gs.GCPEndpoint(t.Context(), "nope")
	require.ErrorIs(t, err, gopherstack.ErrUnknownGCPService)

	cs, err := gs.AzureConnectionString(t.Context())
	require.NoError(t, err)
	require.Contains(t, cs, "AccountName=devstoreaccount1")
}

func checkS3(t *testing.T, _ *gopherstack.Container, cfg aws.Config) {
	t.Helper()

	ctx := t.Context()
	cl := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	bucket := aws.String("tc-bucket")

	_, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket})
	require.NoError(t, err)

	_, err = cl.PutObject(ctx, &s3.PutObjectInput{
		Bucket: bucket, Key: aws.String("k"), Body: bytes.NewReader([]byte("hello")),
	})
	require.NoError(t, err)

	out, err := cl.GetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: aws.String("k")})
	require.NoError(t, err)

	b, err := io.ReadAll(out.Body)
	require.NoError(t, err)
	require.Equal(t, "hello", string(b))
}

func checkSQS(t *testing.T, _ *gopherstack.Container, cfg aws.Config) {
	t.Helper()

	ctx := t.Context()
	cl := sqs.NewFromConfig(cfg)

	q, err := cl.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("tc-queue")})
	require.NoError(t, err)

	_, err = cl.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: q.QueueUrl, MessageBody: aws.String("ping")})
	require.NoError(t, err)

	rm, err := cl.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: q.QueueUrl, WaitTimeSeconds: 2})
	require.NoError(t, err)
	require.Len(t, rm.Messages, 1)
	require.Equal(t, "ping", aws.ToString(rm.Messages[0].Body))
}

func checkAzureBlob(t *testing.T, gs *gopherstack.Container, _ aws.Config) {
	t.Helper()

	cs, err := gs.AzureConnectionString(t.Context())
	require.NoError(t, err)

	cl, err := azblob.NewClientFromConnectionString(cs, nil)
	require.NoError(t, err)

	_, err = cl.CreateContainer(t.Context(), "tc", nil)
	require.NoError(t, err)
	_, err = cl.UploadBuffer(t.Context(), "tc", "b", []byte("azure"), nil)
	require.NoError(t, err)

	dl, err := cl.DownloadStream(t.Context(), "tc", "b", nil)
	require.NoError(t, err)

	b, err := io.ReadAll(dl.Body)
	require.NoError(t, err)
	require.Equal(t, "azure", string(b))
}

func checkGCPStorage(t *testing.T, gs *gopherstack.Container, _ aws.Config) {
	t.Helper()

	hp, err := gs.GCPEndpoint(t.Context(), "storage")
	require.NoError(t, err)

	cl, err := storage.NewClient(t.Context(),
		option.WithEndpoint("http://"+hp+"/storage/v1/"), option.WithoutAuthentication())
	require.NoError(t, err)

	defer cl.Close()

	bkt := cl.Bucket("tc-gcp")
	require.NoError(t, bkt.Create(t.Context(), "proj", nil))

	w := bkt.Object("o").NewWriter(t.Context())
	_, err = w.Write([]byte("gcp"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	r, err := bkt.Object("o").NewReader(t.Context())
	require.NoError(t, err)

	b, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "gcp", string(b))
}
