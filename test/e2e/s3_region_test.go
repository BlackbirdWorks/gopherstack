//go:build e2e

package e2e_test

import (
"bytes"
"context"
"io"
"net/http/httptest"
"net/url"
"testing"

"github.com/aws/aws-sdk-go-v2/aws"
awscfg "github.com/aws/aws-sdk-go-v2/config"
"github.com/aws/aws-sdk-go-v2/credentials"
"github.com/aws/aws-sdk-go-v2/service/s3"
"github.com/aws/aws-sdk-go-v2/service/s3/types"
"github.com/stretchr/testify/assert"
"github.com/stretchr/testify/require"
)

// makeS3Client builds an AWS SDK S3 client for the given region pointed at
// the provided test server.
func makeS3Client(t *testing.T, serverURL, region string) *s3.Client {
t.Helper()

cfg, err := awscfg.LoadDefaultConfig(context.Background(),
awscfg.WithRegion(region),
awscfg.WithCredentialsProvider(
credentials.NewStaticCredentialsProvider("test", "test", ""),
),
)
require.NoError(t, err)

return s3.NewFromConfig(cfg, func(o *s3.Options) {
o.UsePathStyle = true
o.BaseEndpoint = aws.String(serverURL)
})
}

// TestE2E_S3_LocationConstraint_PutObjectAfterCreate verifies that a bucket
// created with a non-default LocationConstraint (e.g. us-west-2) can be written
// to and read from without a 404 (regression for the region-scoped getBucket bug).
func TestE2E_S3_LocationConstraint_PutObjectAfterCreate(t *testing.T) {
stack := newStack(t)

server := httptest.NewServer(stack.Echo)
defer server.Close()

if u, err := url.Parse(server.URL); err == nil {
stack.S3Handler.Endpoint = u.Host
}

// Use a client configured for us-west-2.
client := makeS3Client(t, server.URL, "us-west-2")
ctx := t.Context()

// 1. Create the bucket with LocationConstraint = us-west-2.
_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
Bucket: aws.String("west-bucket"),
CreateBucketConfiguration: &types.CreateBucketConfiguration{
LocationConstraint: types.BucketLocationConstraint("us-west-2"),
},
})
require.NoError(t, err, "CreateBucket must succeed")

// 2. PutObject — must not return 404.
_, err = client.PutObject(ctx, &s3.PutObjectInput{
Bucket: aws.String("west-bucket"),
Key:    aws.String("greeting.txt"),
Body:   bytes.NewReader([]byte("hello region")),
})
require.NoError(t, err, "PutObject must succeed after CreateBucket with LocationConstraint")

// 3. GetObject — content must round-trip correctly.
out, err := client.GetObject(ctx, &s3.GetObjectInput{
Bucket: aws.String("west-bucket"),
Key:    aws.String("greeting.txt"),
})
require.NoError(t, err)
defer out.Body.Close()

body, readErr := io.ReadAll(out.Body)
	require.NoError(t, readErr)
assert.Equal(t, "hello region", string(body))
}
