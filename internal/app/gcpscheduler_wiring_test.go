package app

import (
	"testing"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/code"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
	gcpschedulerbackend "github.com/blackbirdworks/gopherstack/services/gcpscheduler"
)

func TestWireGCPSchedulerPublishesIntoPubSub(t *testing.T) {
	t.Parallel()

	const (
		loc   = "projects/p/locations/us-central1"
		topic = "projects/p/topics/ticks"
		sub   = "projects/p/subscriptions/ticks-sub"
	)

	tests := []struct {
		name      string
		wired     bool
		wantCode  code.Code
		wantDeliv int
	}{
		{"wired", true, code.Code_OK, 1},
		{"not wired", false, code.Code_FAILED_PRECONDITION, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ps := gcppubsubbackend.NewHandler(gcppubsubbackend.NewInMemoryBackend())
			sched := gcpschedulerbackend.NewHandler(gcpschedulerbackend.NewInMemoryBackend())

			byName := map[string]service.Registerable{"GCPScheduler": sched}
			if tt.wired {
				byName["GCPPubSub"] = ps
			}

			wireGCPScheduler(byName)

			_, err := ps.Backend.CreateTopic(&pubsubpb.Topic{Name: topic})
			require.NoError(t, err)

			_, err = ps.Backend.CreateSubscription(&pubsubpb.Subscription{Name: sub, Topic: topic})
			require.NoError(t, err)

			job, err := sched.Backend.CreateJob(&schedulerpb.CreateJobRequest{Parent: loc, Job: &schedulerpb.Job{
				Name: loc + "/jobs/tick", Schedule: "* * * * *",
				Target: &schedulerpb.Job_PubsubTarget{PubsubTarget: &schedulerpb.PubsubTarget{
					TopicName: topic, Data: []byte("tick"),
				}},
			}})
			require.NoError(t, err)

			_, err = sched.Backend.RunJob(job.GetName())
			require.NoError(t, err)
			sched.Backend.Drain()

			got, err := sched.Backend.GetJob(job.GetName())
			require.NoError(t, err)
			assert.EqualValues(t, tt.wantCode, got.GetStatus().GetCode())

			resp, err := ps.SubscriberAPI().Pull(t.Context(), &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 10})
			require.NoError(t, err)
			require.Len(t, resp.GetReceivedMessages(), tt.wantDeliv)

			if tt.wantDeliv > 0 {
				msg := resp.GetReceivedMessages()[0].GetMessage()
				assert.Equal(t, "tick", string(msg.GetData()))
				assert.Equal(t, job.GetName(), msg.GetAttributes()["googclient_schedulejobname"])
			}
		})
	}
}
