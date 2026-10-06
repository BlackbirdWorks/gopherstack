package kafkaconnect_test

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kafkaconnectsdk "github.com/aws/aws-sdk-go-v2/service/kafkaconnect"
	"github.com/aws/aws-sdk-go-v2/service/kafkaconnect/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kafkaconnect"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

type s3Sibling struct{ h *s3backend.S3Handler }

func (s s3Sibling) GetS3Handler() service.Registerable { return s.h }

func TestCustomPluginReadsEmulatedS3(t *testing.T) {
	t.Parallel()

	payload := []byte("plugin-archive-bytes")
	sum := md5.Sum(payload)

	tests := []struct {
		name        string
		fileKey     string
		wantState   types.CustomPluginState
		wantSize    int64
		wantMessage bool
	}{
		{
			name: "object_present", fileKey: "plugins/real.zip",
			wantState: types.CustomPluginStateActive, wantSize: int64(len(payload)),
		},
		{
			name: "object_missing", fileKey: "plugins/absent.zip",
			wantState: types.CustomPluginStateCreateFailed, wantMessage: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s3h := s3backend.NewHandler(s3backend.NewInMemoryBackend(nil))
			_, err := s3h.Backend.CreateBucket(
				t.Context(), &awss3.CreateBucketInput{Bucket: aws.String("plugin-bucket")},
			)
			require.NoError(t, err)

			_, err = s3h.Backend.PutObject(t.Context(), &awss3.PutObjectInput{
				Bucket: aws.String("plugin-bucket"),
				Key:    aws.String("plugins/real.zip"),
				Body:   bytes.NewReader(payload),
			})
			require.NoError(t, err)

			backend := kafkaconnect.NewInMemoryBackend()
			backend.SetAppConfig(s3Sibling{h: s3h})
			h := kafkaconnect.NewHandler(backend)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion
			client := newTestClient(t, h)

			in := minimalCreateCustomPluginInput("s3-plugin-" + tt.name)
			in.Location.S3Location.BucketArn = aws.String("arn:aws:s3:::plugin-bucket")
			in.Location.S3Location.FileKey = aws.String(tt.fileKey)

			created, err := client.CreateCustomPlugin(t.Context(), in)
			require.NoError(t, err)

			var desc *kafkaconnectsdk.DescribeCustomPluginOutput

			require.Eventually(t, func() bool {
				desc, err = client.DescribeCustomPlugin(t.Context(), &kafkaconnectsdk.DescribeCustomPluginInput{
					CustomPluginArn: created.CustomPluginArn,
				})

				return err == nil && desc.CustomPluginState == tt.wantState
			}, waitTimeout, waitTick)

			assert.Equal(t, tt.wantMessage, desc.StateDescription != nil)

			if tt.wantMessage {
				return
			}

			assert.Equal(t, hex.EncodeToString(sum[:]), aws.ToString(desc.LatestRevision.FileDescription.FileMd5))
			assert.Equal(t, tt.wantSize, desc.LatestRevision.FileDescription.FileSize)
		})
	}
}
