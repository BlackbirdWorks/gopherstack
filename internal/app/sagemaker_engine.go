package app

import (
	"context"
	"fmt"

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

type sagemakerSNS struct{ sns *snsbackend.InMemoryBackend }

func (s sagemakerSNS) Publish(ctx context.Context, _, topicARN, message string) error {
	if _, err := s.sns.PublishContext(ctx, topicARN, message, "", "", nil); err != nil {
		return fmt.Errorf("sns publish: %w", err)
	}

	return nil
}
