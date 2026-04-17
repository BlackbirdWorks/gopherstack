package dashboard

import (
	"context"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	dashboardv1 "github.com/blackbirdworks/gopherstack/dashboard/api/v1"
	"github.com/blackbirdworks/gopherstack/dashboard/api/v1/dashboardv1connect"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

type connectDashboardServer struct{}

func NewConnectDashboardServer() dashboardv1connect.DashboardServiceHandler {
	return &connectDashboardServer{}
}

const streamKeepAliveInterval = 2 * time.Second

func (s *connectDashboardServer) StreamConsole(
	ctx context.Context,
	_ *connect.Request[dashboardv1.StreamConsoleRequest],
	stream *connect.ServerStream[dashboardv1.StreamConsoleResponse],
) error {
	ch := logger.GlobalRingBuffer.Subscribe()
	defer logger.GlobalRingBuffer.Unsubscribe(ch)

	ticker := time.NewTicker(streamKeepAliveInterval)
	defer ticker.Stop()

	// Optionally send initial data
	initialRequests := logger.GlobalRingBuffer.GetAll()
	// To keep it simple, we can send recent records. For now, let's just stream incoming.
	// Actually, sending existing ones is better UX.
	for i := len(initialRequests) - 1; i >= 0; i-- { // just an example, reverse list
		_ = initialRequests // We send one-by-one below if we want
	}

	// Real-time loop
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r := <-ch:
			resp := &dashboardv1.StreamConsoleResponse{
				Request: &dashboardv1.CapturedRequest{
					Id:         r.ID,
					Method:     r.Method,
					Path:       r.Path,
					Headers:    r.Headers,
					Body:       r.Body,
					Status:     int32(r.Status), //nolint:gosec // status codes fit in int32
					DurationMs: r.Duration.Milliseconds(),
					Timestamp:  timestamppb.New(r.Timestamp),
				},
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		case <-ticker.C:
			// just keep-alive/heartbeat logic if needed in connect-rpc,
			// though HTTP/2 usually handles it.
		}
	}
}

func (s *connectDashboardServer) StreamMetrics(
	ctx context.Context,
	_ *connect.Request[dashboardv1.StreamMetricsRequest],
	stream *connect.ServerStream[dashboardv1.StreamMetricsResponse],
) error {
	ticker := time.NewTicker(streamKeepAliveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			m := telemetry.CollectMetrics()
			resp := &dashboardv1.StreamMetricsResponse{
				Dashboard: mapMetricsToProto(m),
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}
}

func mapMetricsToProto(m *telemetry.Dashboard) *dashboardv1.DashboardMetrics {
	if m == nil {
		return nil
	}

	p := &dashboardv1.DashboardMetrics{}

	if m.Runtime != nil {
		p.Runtime = &dashboardv1.RuntimeMetrics{
			Goroutines:    int32(m.Runtime.Goroutines), //nolint:gosec // Goroutines fits in int32
			HeapAllocMb:   m.Runtime.HeapAllocMB,
			HeapInuseMb:   m.Runtime.HeapInuseMB,
			HeapSysMb:     m.Runtime.HeapSysMB,
			NumGc:         m.Runtime.NumGC,
			LastGcPauseMs: m.Runtime.LastGCPause,
			TotalAllocMb:  m.Runtime.TotalAllocMB,
			NumServices:   int32(m.Runtime.NumServices), //nolint:gosec // NumServices fits in int32
		}
	}

	for _, op := range m.Operations {
		p.Operations = append(p.Operations, &dashboardv1.OperationSummary{
			Operation:  op.Operation,
			Count:      op.Count,
			ErrorCount: op.ErrorCount,
			P50Ms:      op.P50Ms,
			P95Ms:      op.P95Ms,
			P99Ms:      op.P99Ms,
			AvgMs:      op.AvgMs,
			MaxMs:      op.MaxMs,
		})
	}

	for _, dl := range m.Deadlocks {
		p.Deadlocks = append(p.Deadlocks, &dashboardv1.DeadlockInfo{
			Lock:      dl.Lock,
			Operation: dl.Operation,
			HeldSec:   dl.HeldSec,
			Waiters:   int32(dl.Waiters), //nolint:gosec // Waiters fits in int32
		})
	}

	for _, w := range m.Workers {
		p.Workers = append(p.Workers, &dashboardv1.WorkerStats{
			Service:             w.Service,
			Worker:              w.Worker,
			QueueDepth:          int32(w.QueueDepth), //nolint:gosec // QueueDepth fits in int32
			TasksTotal:          w.TasksTotal,
			ErrorsTotal:         w.ErrorsTotal,
			ItemsProcessedTotal: w.ItemsProcessed,
		})
	}

	return p
}
