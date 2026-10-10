package appconfig_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
)

type sqsOnlyConfig struct{ h *sqsbackend.Handler }

func (c sqsOnlyConfig) GetSQSHandler() service.Registerable { return c.h }

func TestExtensionActions_SiblingSQSDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		wantN  int
		wired  bool
		wantOK bool
	}{
		{name: "wired", wired: true, wantN: 1, wantOK: true},
		{name: "unwired", wired: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sqs := sqsbackend.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			t.Cleanup(sqs.Close)
			q, err := sqs.CreateQueue(&sqsbackend.CreateQueueInput{QueueName: "queue", Region: "us-east-1"})
			require.NoError(t, err)

			f := newLifecycleFixture(t, map[string][]string{"ON_DEPLOYMENT_COMPLETE": {testSQSARN}}, 0, 0)
			f.b.SetExtensionTargetDeliverer(nil)

			if tt.wired {
				f.b.SetAppConfig(sqsOnlyConfig{h: sqsbackend.NewHandler(sqs)})
			}

			f.start(t)

			if !tt.wantOK {
				assert.Equal(t, "COMPLETE", f.state(t))

				return
			}

			require.Eventually(t, func() bool {
				out, rerr := sqs.ReceiveMessage(&sqsbackend.ReceiveMessageInput{
					QueueURL: q.QueueURL, Region: "us-east-1", MaxNumberOfMessages: 10,
				})
				if rerr != nil || len(out.Messages) != tt.wantN {
					return false
				}

				var m map[string]any
				require.NoError(t, json.Unmarshal([]byte(out.Messages[0].Body), &m))

				return m["Type"] == "OnDeploymentComplete"
			}, eventuallyWait, eventuallyTick)
		})
	}
}
