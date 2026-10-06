package datasync_test

import (
	"context"
	"errors"
	"testing"

	datasyncsdk "github.com/aws/aws-sdk-go-v2/service/datasync"
	dstypes "github.com/aws/aws-sdk-go-v2/service/datasync/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func enumStrings[T ~string](vs []T) []string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = string(v)
	}

	return out
}

func TestSDK_EnumInputValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call   func(ctx context.Context, c *datasyncsdk.Client, v string) error
		name   string
		field  string
		values []string
	}{
		{
			name:   "CreateLocationAzureBlob.AuthenticationType",
			field:  "AuthenticationType",
			values: enumStrings(dstypes.AzureBlobAuthenticationType("").Values()),
			call: func(ctx context.Context, c *datasyncsdk.Client, v string) error {
				in := &datasyncsdk.CreateLocationAzureBlobInput{
					AuthenticationType: dstypes.AzureBlobAuthenticationType(v),
				}
				_, err := c.CreateLocationAzureBlob(ctx, in)

				return err
			},
		},
		{
			name:   "CreateLocationAzureBlob.AccessTier",
			field:  "AccessTier",
			values: enumStrings(dstypes.AzureAccessTier("").Values()),
			call: func(ctx context.Context, c *datasyncsdk.Client, v string) error {
				in := &datasyncsdk.CreateLocationAzureBlobInput{
					AccessTier: dstypes.AzureAccessTier(v),
				}
				_, err := c.CreateLocationAzureBlob(ctx, in)

				return err
			},
		},
		{
			name:   "CreateLocationAzureBlob.BlobType",
			field:  "BlobType",
			values: enumStrings(dstypes.AzureBlobType("").Values()),
			call: func(ctx context.Context, c *datasyncsdk.Client, v string) error {
				in := &datasyncsdk.CreateLocationAzureBlobInput{
					BlobType: dstypes.AzureBlobType(v),
				}
				_, err := c.CreateLocationAzureBlob(ctx, in)

				return err
			},
		},
		{
			name:   "CreateLocationEfs.InTransitEncryption",
			field:  "InTransitEncryption",
			values: enumStrings(dstypes.EfsInTransitEncryption("").Values()),
			call: func(ctx context.Context, c *datasyncsdk.Client, v string) error {
				in := &datasyncsdk.CreateLocationEfsInput{
					InTransitEncryption: dstypes.EfsInTransitEncryption(v),
				}
				_, err := c.CreateLocationEfs(ctx, in)

				return err
			},
		},
		{
			name:   "CreateLocationHdfs.AuthenticationType",
			field:  "AuthenticationType",
			values: enumStrings(dstypes.HdfsAuthenticationType("").Values()),
			call: func(ctx context.Context, c *datasyncsdk.Client, v string) error {
				in := &datasyncsdk.CreateLocationHdfsInput{
					AuthenticationType: dstypes.HdfsAuthenticationType(v),
				}
				_, err := c.CreateLocationHdfs(ctx, in)

				return err
			},
		},
		{
			name:   "CreateLocationObjectStorage.ServerProtocol",
			field:  "ServerProtocol",
			values: enumStrings(dstypes.ObjectStorageServerProtocol("").Values()),
			call: func(ctx context.Context, c *datasyncsdk.Client, v string) error {
				in := &datasyncsdk.CreateLocationObjectStorageInput{
					ServerProtocol: dstypes.ObjectStorageServerProtocol(v),
				}
				_, err := c.CreateLocationObjectStorage(ctx, in)

				return err
			},
		},
		{
			name:   "CreateLocationS3.S3StorageClass",
			field:  "S3StorageClass",
			values: enumStrings(dstypes.S3StorageClass("").Values()),
			call: func(ctx context.Context, c *datasyncsdk.Client, v string) error {
				in := &datasyncsdk.CreateLocationS3Input{
					S3StorageClass: dstypes.S3StorageClass(v),
				}
				_, err := c.CreateLocationS3(ctx, in)

				return err
			},
		},
		{
			name:   "CreateTask.TaskMode",
			field:  "TaskMode",
			values: enumStrings(dstypes.TaskMode("").Values()),
			call: func(ctx context.Context, c *datasyncsdk.Client, v string) error {
				in := &datasyncsdk.CreateTaskInput{
					TaskMode: dstypes.TaskMode(v),
				}
				_, err := c.CreateTask(ctx, in)

				return err
			},
		},
		{
			name:   "UpdateLocationAzureBlob.AccessTier",
			field:  "AccessTier",
			values: enumStrings(dstypes.AzureAccessTier("").Values()),
			call: func(ctx context.Context, c *datasyncsdk.Client, v string) error {
				in := &datasyncsdk.UpdateLocationAzureBlobInput{
					AccessTier: dstypes.AzureAccessTier(v),
				}
				_, err := c.UpdateLocationAzureBlob(ctx, in)

				return err
			},
		},
		{
			name:   "UpdateLocationAzureBlob.AuthenticationType",
			field:  "AuthenticationType",
			values: enumStrings(dstypes.AzureBlobAuthenticationType("").Values()),
			call: func(ctx context.Context, c *datasyncsdk.Client, v string) error {
				in := &datasyncsdk.UpdateLocationAzureBlobInput{
					AuthenticationType: dstypes.AzureBlobAuthenticationType(v),
				}
				_, err := c.UpdateLocationAzureBlob(ctx, in)

				return err
			},
		},
		{
			name:   "UpdateLocationAzureBlob.BlobType",
			field:  "BlobType",
			values: enumStrings(dstypes.AzureBlobType("").Values()),
			call: func(ctx context.Context, c *datasyncsdk.Client, v string) error {
				in := &datasyncsdk.UpdateLocationAzureBlobInput{
					BlobType: dstypes.AzureBlobType(v),
				}
				_, err := c.UpdateLocationAzureBlob(ctx, in)

				return err
			},
		},
		{
			name:   "UpdateLocationEfs.InTransitEncryption",
			field:  "InTransitEncryption",
			values: enumStrings(dstypes.EfsInTransitEncryption("").Values()),
			call: func(ctx context.Context, c *datasyncsdk.Client, v string) error {
				in := &datasyncsdk.UpdateLocationEfsInput{
					InTransitEncryption: dstypes.EfsInTransitEncryption(v),
				}
				_, err := c.UpdateLocationEfs(ctx, in)

				return err
			},
		},
		{
			name:   "UpdateLocationHdfs.AuthenticationType",
			field:  "AuthenticationType",
			values: enumStrings(dstypes.HdfsAuthenticationType("").Values()),
			call: func(ctx context.Context, c *datasyncsdk.Client, v string) error {
				in := &datasyncsdk.UpdateLocationHdfsInput{
					AuthenticationType: dstypes.HdfsAuthenticationType(v),
				}
				_, err := c.UpdateLocationHdfs(ctx, in)

				return err
			},
		},
		{
			name:   "UpdateLocationObjectStorage.ServerProtocol",
			field:  "ServerProtocol",
			values: enumStrings(dstypes.ObjectStorageServerProtocol("").Values()),
			call: func(ctx context.Context, c *datasyncsdk.Client, v string) error {
				in := &datasyncsdk.UpdateLocationObjectStorageInput{
					ServerProtocol: dstypes.ObjectStorageServerProtocol(v),
				}
				_, err := c.UpdateLocationObjectStorage(ctx, in)

				return err
			},
		},
		{
			name:   "UpdateLocationS3.S3StorageClass",
			field:  "S3StorageClass",
			values: enumStrings(dstypes.S3StorageClass("").Values()),
			call: func(ctx context.Context, c *datasyncsdk.Client, v string) error {
				in := &datasyncsdk.UpdateLocationS3Input{
					S3StorageClass: dstypes.S3StorageClass(v),
				}
				_, err := c.UpdateLocationS3(ctx, in)

				return err
			},
		},
	}

	base := newRealClient(t)
	client := datasyncsdk.New(base.Options(), func(o *datasyncsdk.Options) {
		o.APIOptions = append(o.APIOptions, func(s *middleware.Stack) error {
			_, _ = s.Initialize.Remove("OperationInputValidation")

			return nil
		})
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.NotEmpty(t, tt.values)

			t.Run("invalid", func(t *testing.T) {
				t.Parallel()

				err := tt.call(t.Context(), client, "NOT_A_REAL_VALUE")

				var invalid *dstypes.InvalidRequestException

				require.ErrorAs(t, err, &invalid)
				assert.Contains(t, invalid.ErrorMessage(), "invalid "+tt.field)
			})

			t.Run("every sdk value accepted", func(t *testing.T) {
				t.Parallel()

				for _, v := range tt.values {
					err := tt.call(t.Context(), client, v)

					if invalid, ok := errors.AsType[*dstypes.InvalidRequestException](err); ok {
						assert.NotContains(t, invalid.ErrorMessage(), "invalid "+tt.field,
							"%s rejected sdk value %q: %v", tt.field, v, err)
					}
				}
			})
		})
	}
}
