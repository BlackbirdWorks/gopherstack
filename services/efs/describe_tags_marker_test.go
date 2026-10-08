package efs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	efssdk "github.com/aws/aws-sdk-go-v2/service/efs"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

func TestDescribeTags_Marker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		marker   *string
		maxItems *int32
		wantKeys []string
		wantErr  bool
	}{
		{name: "no marker", wantKeys: []string{"a", "b", "c"}},
		{name: "marker resumes", marker: aws.String("b"), wantKeys: []string{"b", "c"}},
		{name: "max items ignored", maxItems: aws.Int32(1), wantKeys: []string{"a", "b", "c"}},
		{name: "unknown marker", marker: aws.String("zz"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEFSSDKClient(t, efs.NewHandler(efs.NewInMemoryBackend("123456789012", "us-east-1")))
			ctx := t.Context()

			fs, err := client.CreateFileSystem(ctx, &efssdk.CreateFileSystemInput{CreationToken: aws.String("tok")})
			require.NoError(t, err)

			_, err = client.CreateTags(ctx, &efssdk.CreateTagsInput{ //nolint:staticcheck // legacy op under test
				FileSystemId: fs.FileSystemId,
				Tags: []efstypes.Tag{
					{Key: aws.String("c"), Value: aws.String("3")},
					{Key: aws.String("a"), Value: aws.String("1")},
					{Key: aws.String("b"), Value: aws.String("2")},
				},
			})
			require.NoError(t, err)

			out, err := client.DescribeTags(ctx, &efssdk.DescribeTagsInput{ //nolint:staticcheck // legacy op under test
				FileSystemId: fs.FileSystemId, Marker: tt.marker, MaxItems: tt.maxItems,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			keys := make([]string, 0, len(out.Tags))
			for _, tag := range out.Tags {
				keys = append(keys, aws.ToString(tag.Key))
			}

			assert.Equal(t, tt.wantKeys, keys)
			assert.Equal(t, tt.marker, out.Marker)
			assert.Nil(t, out.NextMarker)
		})
	}
}
