package rum_test

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrum "github.com/aws/aws-sdk-go-v2/service/rum"
	"github.com/aws/aws-sdk-go-v2/service/rum/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/rum"
)

func TestEvidentlyDestinationLogsWouldPublish(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		eventType string
		want      bool
	}{
		{name: "matching event", eventType: customCheckout, want: true},
		{name: "other event", eventType: evtNav, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			id := f.createMonitor(t, "web")
			ctx := t.Context()

			_, err := f.client.PutRumMetricsDestination(ctx, &awsrum.PutRumMetricsDestinationInput{
				AppMonitorName: aws.String("web"), Destination: types.MetricDestinationEvidently,
				DestinationArn: aws.String(evidentlyARN), IamRoleArn: aws.String(evidentlyRole),
			})
			require.NoError(t, err)

			created, err := f.client.BatchCreateRumMetricDefinitions(ctx, &awsrum.BatchCreateRumMetricDefinitionsInput{
				AppMonitorName: aws.String("web"), Destination: types.MetricDestinationEvidently,
				DestinationArn: aws.String(evidentlyARN),
				MetricDefinitions: []types.MetricDefinitionRequest{
					{
						Name:         aws.String("checkout"),
						Namespace:    aws.String("shop"),
						EventPattern: aws.String(`{"event_type":["` + customCheckout + `"]}`),
					},
				},
			})
			require.NoError(t, err)
			if len(created.Errors) > 0 {
				t.Fatalf(
					"%s %s",
					aws.ToString(created.Errors[0].ErrorMessage),
					aws.ToString(created.Errors[0].ErrorCode),
				)
			}
			require.Len(t, created.MetricDefinitions, 1)

			var buf bytes.Buffer

			lctx := logger.Save(ctx, slog.New(slog.NewTextHandler(&buf, nil)))
			require.NoError(t, f.backend.PutRumEvents(lctx, &rum.PutEventsInput{
				AppID: id, BatchID: "b1",
				AppMonitorDetails: &struct {
					ID      string `json:"id"`
					Name    string `json:"name"`
					Version string `json:"version"`
				}{ID: id, Name: "web"},
				UserDetails: &struct {
					SessionID string `json:"sessionId"`
					UserID    string `json:"userId"`
				}{SessionID: "s", UserID: "u"},
				RumEvents: []rum.WireRumEvent{
					{ID: "e1", Type: tt.eventType, Details: `{}`, Timestamp: f.base()},
				},
			}))

			if !tt.want {
				assert.NotContains(t, buf.String(), "would publish")

				return
			}

			assert.Contains(t, buf.String(), "would publish events to Evidently")
			assert.Contains(t, buf.String(), evidentlyARN)
		})
	}
}
