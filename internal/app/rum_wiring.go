package app

import (
	"context"
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	rumbackend "github.com/blackbirdworks/gopherstack/services/rum"
)

// rumLogSink delivers CloudWatch RUM events to the vended CloudWatch Logs group of an app monitor.
type rumLogSink struct {
	logs *cwlogsbackend.InMemoryBackend
}

func (s rumLogSink) EnsureLogGroup(ctx context.Context, region, group string) error {
	_, err := s.logs.CreateLogGroup(cwlogsbackend.WithRegion(ctx, region), group, "", "")
	if err != nil && !errors.Is(err, cwlogsbackend.ErrLogGroupAlreadyExists) {
		return err
	}

	return nil
}

func (s rumLogSink) PutLogEvents(
	ctx context.Context, region, group, stream string, events []rumbackend.LogEvent,
) error {
	ctx = cwlogsbackend.WithRegion(ctx, region)

	if _, err := s.logs.CreateLogStream(ctx, group, stream); err != nil &&
		!errors.Is(err, cwlogsbackend.ErrLogStreamAlreadyExist) {
		return err
	}

	in := make([]cwlogsbackend.InputLogEvent, len(events))
	for i, e := range events {
		in[i] = cwlogsbackend.InputLogEvent{Message: e.Message, Timestamp: e.Timestamp}
	}

	_, err := s.logs.PutLogEvents(ctx, group, stream, "", in)

	return err
}

// wireRUMCloudWatch sends RUM events to CloudWatch Logs and RUM metrics to CloudWatch.
func wireRUMCloudWatch(byName map[string]service.Registerable) {
	rumH, ok := byName["RUM"].(*rumbackend.Handler)
	if !ok {
		return
	}

	if h, hok := byName["CloudWatchLogs"].(*cwlogsbackend.Handler); hok {
		if bk, bok := h.Backend.(*cwlogsbackend.InMemoryBackend); bok {
			rumH.Backend.SetLogSink(rumLogSink{logs: bk})
		}
	}

	if h, hok := byName["CloudWatch"].(*cwbackend.Handler); hok {
		rumH.Backend.SetMetricEmitter(regionalMetricEmitter(h))
	}
}
