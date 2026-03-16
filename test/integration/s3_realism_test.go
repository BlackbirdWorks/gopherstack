package integration_test

import (
"io"
"net/http"
"strings"
"testing"
"time"

smithyhttp "github.com/aws/smithy-go/transport/http"

"github.com/aws/aws-sdk-go-v2/aws"
"github.com/aws/aws-sdk-go-v2/service/s3"
"github.com/aws/aws-sdk-go-v2/service/s3/types"
"github.com/google/uuid"
"github.com/stretchr/testify/assert"
"github.com/stretchr/testify/require"
)

func TestIntegration_S3_Realism_DeleteMarkers(t *testing.T) {
t.Parallel()
dumpContainerLogsOnFailure(t)
client := createS3Client(t)
ctx := t.Context()

bucket := "ver-bkt-" + uuid.NewString()

// 1. Setup bucket with versioning
_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
Bucket: aws.String(bucket),
})
require.NoError(t, err)

_, err = client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
Bucket: aws.String(bucket),
VersioningConfiguration: &types.VersioningConfiguration{
Status: types.BucketVersioningStatusEnabled,
},
})
require.NoError(t, err)

key := "key"
content := "data"

// 2. Put object
putOut, err := client.PutObject(ctx, &s3.PutObjectInput{
Bucket: aws.String(bucket),
Key:    aws.String(key),
Body:   strings.NewReader(content),
})
require.NoError(t, err)
dataVersionID := *putOut.VersionId

// 3. Delete object (creates delete marker)
delOut, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
Bucket: aws.String(bucket),
Key:    aws.String(key),
})
require.NoError(t, err)
require.True(t, *delOut.DeleteMarker)
dmVersionID := *delOut.VersionId

// 4. Get object (should return 404 with x-amz-delete-marker: true)
_, err = client.GetObject(ctx, &s3.GetObjectInput{
Bucket: aws.String(bucket),
Key:    aws.String(key),
})
require.Error(t, err)
var noSuchKey *types.NoSuchKey
require.ErrorAs(t, err, &noSuchKey)

// 5. Head object (should return 404 Not Found)
_, err = client.HeadObject(ctx, &s3.HeadObjectInput{
Bucket: aws.String(bucket),
Key:    aws.String(key),
})
require.Error(t, err)
var notFound *types.NotFound
require.ErrorAs(t, err, &notFound)

// 6. Get specific version (the one we put) should still work
got, err := client.GetObject(ctx, &s3.GetObjectInput{
Bucket:    aws.String(bucket),
Key:       aws.String(key),
VersionId: aws.String(dataVersionID),
})
require.NoError(t, err)
defer got.Body.Close()

body, err := io.ReadAll(got.Body)
require.NoError(t, err)
assert.Equal(t, content, string(body))

// 7. HEAD on specific DM version ID should return 405 (Method Not Allowed)
_, err = client.HeadObject(ctx, &s3.HeadObjectInput{
Bucket:    aws.String(bucket),
Key:       aws.String(key),
VersionId: aws.String(dmVersionID),
})
require.Error(t, err)
// Extract HTTP status code via smithy ResponseError.
var respErr *smithyhttp.ResponseError
require.ErrorAs(t, err, &respErr)
assert.Equal(t, http.StatusMethodNotAllowed, respErr.HTTPStatusCode())
}

func TestIntegration_S3_Realism_Headers(t *testing.T) {
t.Parallel()
dumpContainerLogsOnFailure(t)
client := createS3Client(t)
ctx := t.Context()

bucket := "real-bkt-" + uuid.NewString()

_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
Bucket: aws.String(bucket),
})
require.NoError(t, err)

// 1. Region header on Bucket (HEAD /bucket)
_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
Bucket: aws.String(bucket),
})
require.NoError(t, err)

// 2. Expiration header on Object
_, err = client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
Bucket: aws.String(bucket),
LifecycleConfiguration: &types.BucketLifecycleConfiguration{
Rules: []types.LifecycleRule{
{
ID:     aws.String("exp-rule"),
Status: types.ExpirationStatusEnabled,
Filter: &types.LifecycleRuleFilter{Prefix: aws.String("logs/")},
Expiration: &types.LifecycleExpiration{
Days: aws.Int32(1),
},
},
},
},
})
require.NoError(t, err)

key := "logs/test.log"
_, err = client.PutObject(ctx, &s3.PutObjectInput{
Bucket: aws.String(bucket),
Key:    aws.String(key),
Body:   strings.NewReader("data"),
})
require.NoError(t, err)

headO, err := client.HeadObject(ctx, &s3.HeadObjectInput{
Bucket: aws.String(bucket),
Key:    aws.String(key),
})
require.NoError(t, err)
assert.NotNil(t, headO.Expiration)
assert.Contains(t, *headO.Expiration, "exp-rule")

// 3. Object not matching prefix should not have expiration
_, err = client.PutObject(ctx, &s3.PutObjectInput{
Bucket: aws.String(bucket),
Key:    aws.String("other/file.txt"),
Body:   strings.NewReader("data"),
})
require.NoError(t, err)

headO2, err := client.HeadObject(ctx, &s3.HeadObjectInput{
Bucket: aws.String(bucket),
Key:    aws.String("other/file.txt"),
})
require.NoError(t, err)
assert.Nil(t, headO2.Expiration)
}

func TestIntegration_S3_Realism_RequestIDs(t *testing.T) {
t.Parallel()
dumpContainerLogsOnFailure(t)
_ = createS3Client(t)
ctx := t.Context()

bucket := "reqid-bkt-" + uuid.NewString()
client := createS3Client(t)
_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
Bucket: aws.String(bucket),
})
require.NoError(t, err)

url := endpoint + "/" + bucket
t.Logf("Testing realism headers at: %s", url)

httpClient := &http.Client{Timeout: 10 * time.Second}
req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
require.NoError(t, err)

resp, err := httpClient.Do(req)
require.NoError(t, err)
defer resp.Body.Close()

assert.NotEmpty(t, resp.Header.Get("X-Amz-Request-Id"), "X-Amz-Request-Id header should be set")
assert.NotEmpty(t, resp.Header.Get("X-Amz-Id-2"), "X-Amz-Id-2 header should be set")
}
