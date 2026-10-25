package app

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	sagemakerbackend "github.com/blackbirdworks/gopherstack/services/sagemaker"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
)

// wireContainerEngines connects the container-backed engines to the emulator services they use.
func wireContainerEngines(byName map[string]service.Registerable) {
	wireCodeBuildEngine(byName)
	wireSageMakerEngine(byName)
}

// wireSageMakerEngine gives the container engine access to S3, CloudWatch Logs and SNS.
func wireSageMakerEngine(byName map[string]service.Registerable) {
	smH, ok := byName["SageMaker"].(*sagemakerbackend.Handler)
	if !ok || !smH.Backend.EngineEnabled() {
		return
	}

	var svc sagemakerbackend.EngineServices

	if s3H, s3Ok := byName["S3"].(*s3backend.S3Handler); s3Ok {
		svc.Store = sagemakerS3{codebuildS3{backend: s3H.Backend}}
	}

	if h, hOk := byName["CloudWatchLogs"].(*cwlogsbackend.Handler); hOk {
		if bk, bkOk := h.Backend.(*cwlogsbackend.InMemoryBackend); bkOk {
			svc.Logs = codebuildLogs{backend: bk}
		}
	}

	if snsH, snsOk := byName["SNS"].(*snsbackend.Handler); snsOk {
		if bk, bkOk := snsH.Backend.(*snsbackend.InMemoryBackend); bkOk {
			svc.Notifier = sagemakerSNS{sns: bk}
		}
	}

	smH.SetEngineServices(svc)
}

type sagemakerS3 struct{ codebuildS3 }

func (s sagemakerS3) ListObjectKeys(ctx context.Context, bucket, prefix string) ([]string, error) {
	var (
		keys  []string
		token *string
	)

	for {
		out, err := s.backend.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
			Bucket: aws.String(bucket), Prefix: aws.String(prefix), ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}

		for _, o := range out.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}

		if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
			return keys, nil
		}

		token = out.NextContinuationToken
	}
}

type sagemakerSNS struct{ sns *snsbackend.InMemoryBackend }

func (s sagemakerSNS) Publish(ctx context.Context, _, topicARN, message string) error {
	if _, err := s.sns.PublishContext(ctx, topicARN, message, "", "", nil); err != nil {
		return fmt.Errorf("sns publish: %w", err)
	}

	return nil
}
