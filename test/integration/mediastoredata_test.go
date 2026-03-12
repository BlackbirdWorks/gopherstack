//go:build integration
// +build integration

package integration_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	mediastoredatasdk "github.com/aws/aws-sdk-go-v2/service/mediastoredata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createMediaStoreDataClient returns a MediaStore Data client pointed at the shared test container.
func createMediaStoreDataClient(t *testing.T) *mediastoredatasdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return mediastoredatasdk.NewFromConfig(cfg, func(o *mediastoredatasdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_MediaStoreData_ObjectLifecycle tests the full object CRUD lifecycle.
func TestIntegration_MediaStoreData_ObjectLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		objectPath  string
		body        []byte
		contentType string
	}{
		{
			name:        "video_object",
			objectPath:  "/videos/test-clip.mp4",
			body:        []byte("video content data"),
			contentType: "video/mp4",
		},
		{
			name:        "audio_object",
			objectPath:  "/audio/test-song.mp3",
			body:        []byte("audio content data"),
			contentType: "audio/mpeg",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createMediaStoreDataClient(t)

			// Put object.
			putOut, err := client.PutObject(ctx, &mediastoredatasdk.PutObjectInput{
				Path:        aws.String(tt.objectPath),
				Body:        bytes.NewReader(tt.body),
				ContentType: aws.String(tt.contentType),
			})
			require.NoError(t, err, "PutObject should succeed")
			require.NotNil(t, putOut)
			assert.NotEmpty(t, aws.ToString(putOut.ETag), "PutObject should return an ETag")

			// Describe object.
			descOut, err := client.DescribeObject(ctx, &mediastoredatasdk.DescribeObjectInput{
				Path: aws.String(tt.objectPath),
			})
			require.NoError(t, err, "DescribeObject should succeed")
			assert.NotEmpty(t, aws.ToString(descOut.ETag), "DescribeObject should return an ETag")
			require.NotNil(t, descOut.ContentLength, "DescribeObject should return ContentLength")
			assert.Equal(t, int64(len(tt.body)), aws.ToInt64(descOut.ContentLength))

			// Get object and validate body.
			getOut, err := client.GetObject(ctx, &mediastoredatasdk.GetObjectInput{
				Path: aws.String(tt.objectPath),
			})
			require.NoError(t, err, "GetObject should succeed")
			require.NotNil(t, getOut.Body)

			gotBody, readErr := io.ReadAll(getOut.Body)
			getOut.Body.Close()
			require.NoError(t, readErr)
			assert.Equal(t, tt.body, gotBody, "GetObject body should match uploaded content")
			assert.NotEmpty(t, aws.ToString(getOut.ETag), "GetObject should return an ETag")

			// List items at root.
			listOut, err := client.ListItems(ctx, &mediastoredatasdk.ListItemsInput{})
			require.NoError(t, err, "ListItems should succeed")
			assert.NotEmpty(t, listOut.Items, "ListItems should return at least one item")

			// Delete object.
			_, err = client.DeleteObject(ctx, &mediastoredatasdk.DeleteObjectInput{
				Path: aws.String(tt.objectPath),
			})
			require.NoError(t, err, "DeleteObject should succeed")

			// Verify deletion.
			_, err = client.GetObject(ctx, &mediastoredatasdk.GetObjectInput{
				Path: aws.String(tt.objectPath),
			})
			require.Error(t, err, "GetObject after delete should fail")
		})
	}
}
