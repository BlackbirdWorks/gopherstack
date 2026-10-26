package s3

import (
	"context"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// The helpers below guard on logger.Enabled so disabled debug logging costs no
// logger load and no attribute boxing.

func logPutObjectInput(ctx context.Context, bucket, key, contentType string) {
	if logger.Enabled(ctx, slog.LevelDebug) {
		logger.Load(ctx).DebugContext(ctx, "S3 putObject input",
			"bucket", bucket, "key", key, "contentType", contentType)
	}
}

func logPutObjectOutput(ctx context.Context, bucket, key string, ver *s3.PutObjectOutput) {
	if logger.Enabled(ctx, slog.LevelDebug) {
		logger.Load(ctx).DebugContext(ctx, "S3 putObject output",
			"bucket", bucket, "key", key, "etag", aws.ToString(ver.ETag),
			"versionId", aws.ToString(ver.VersionId))
	}
}

func logGetObjectInput(ctx context.Context, bucket, key, versionID string) {
	if logger.Enabled(ctx, slog.LevelDebug) {
		logger.Load(ctx).DebugContext(ctx, "S3 getObject input",
			"bucket", bucket, "key", key, "versionId", versionID)
	}
}

func logGetObjectOutput(ctx context.Context, bucket, key string, ver *s3.GetObjectOutput) {
	if logger.Enabled(ctx, slog.LevelDebug) {
		logger.Load(ctx).DebugContext(ctx, "S3 getObject output",
			"bucket", bucket, "key", key, "etag", aws.ToString(ver.ETag),
			"contentLength", aws.ToInt64(ver.ContentLength))
	}
}
