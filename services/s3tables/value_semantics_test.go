package s3tables_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3tablessdk "github.com/aws/aws-sdk-go-v2/service/s3tables"
	s3tablestypes "github.com/aws/aws-sdk-go-v2/service/s3tables/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateTable_StorageClassInheritsBucket(t *testing.T) {
	t.Parallel()

	cases := []struct {
		override s3tablestypes.StorageClass
		name     string
		want     s3tablestypes.StorageClass
	}{
		{name: "omitted inherits bucket", want: s3tablestypes.StorageClassIntelligentTiering},
		{
			name:     "explicit overrides bucket",
			override: s3tablestypes.StorageClassStandard,
			want:     s3tablestypes.StorageClassStandard,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestS3TablesClient(t, newRealClientHandler())
			ctx := t.Context()

			bucket, err := client.CreateTableBucket(ctx, &s3tablessdk.CreateTableBucketInput{
				Name: aws.String("inherit-bucket"),
				StorageClassConfiguration: &s3tablestypes.StorageClassConfiguration{
					StorageClass: s3tablestypes.StorageClassIntelligentTiering,
				},
			})
			require.NoError(t, err)

			_, err = client.CreateNamespace(ctx, &s3tablessdk.CreateNamespaceInput{
				TableBucketARN: bucket.Arn, Namespace: []string{"ns1"},
			})
			require.NoError(t, err)

			in := &s3tablessdk.CreateTableInput{
				TableBucketARN: bucket.Arn, Namespace: aws.String("ns1"), Name: aws.String("t1"),
				Format: s3tablestypes.OpenTableFormatIceberg,
			}
			if tc.override != "" {
				in.StorageClassConfiguration = &s3tablestypes.StorageClassConfiguration{StorageClass: tc.override}
			}

			_, err = client.CreateTable(ctx, in)
			require.NoError(t, err)

			got, err := client.GetTableStorageClass(ctx, &s3tablessdk.GetTableStorageClassInput{
				TableBucketARN: bucket.Arn, Namespace: aws.String("ns1"), Name: aws.String("t1"),
			})
			require.NoError(t, err)
			require.NotNil(t, got.StorageClassConfiguration)
			assert.Equal(t, tc.want, got.StorageClassConfiguration.StorageClass)
		})
	}
}
