package gcpcloudbuild_test

import (
	"context"
	"encoding/base64"
	"errors"
	"maps"
	"testing"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/blackbirdworks/gopherstack/services/gcppubsub"
)

const (
	triggerTopic = "projects/" + project + "/topics/deploys"
	otherTopic   = "projects/" + project + "/topics/other"
)

type publishEnv struct {
	*env
	client *pubsub.Client
}

// newPublishEnv wires a Pub/Sub emulator's publishes into the Cloud Build backend.
func newPublishEnv(t *testing.T) *publishEnv {
	t.Helper()

	e := newEnv(t, map[string]behavior{"img": exits(0)})
	ps := gcppubsub.NewInMemoryBackend()

	ps.ObservePublishes(func(topic string, m *pubsubpb.PubsubMessage) {
		e.backend.HandlePubsubMessage(
			topic, m.GetData(), m.GetAttributes(), m.GetMessageId(), m.GetPublishTime().AsTime(),
		)
	})

	h := gcppubsub.NewHandler(ps)
	h.Port = 0

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	c, err := pubsub.NewClient(t.Context(), project,
		option.WithEndpoint(h.Addr()), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	for _, name := range []string{triggerTopic, otherTopic} {
		_, err = c.TopicAdminClient.CreateTopic(t.Context(), &pubsubpb.Topic{Name: name})
		require.NoError(t, err)
	}

	return &publishEnv{env: e, client: c}
}

func (p *publishEnv) publish(t *testing.T, topic string, msg *pubsub.Message) {
	t.Helper()

	pub := p.client.Publisher(topic)
	defer pub.Stop()

	_, err := pub.Publish(t.Context(), msg).Get(t.Context())
	require.NoError(t, err)
}

func (p *publishEnv) builds(t *testing.T) []*cloudbuildpb.Build {
	t.Helper()
	p.backend.Wait()

	var out []*cloudbuildpb.Build

	it := p.env.client.ListBuilds(t.Context(), &cloudbuildpb.ListBuildsRequest{Parent: parent})

	for {
		b, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out
		}

		require.NoError(t, err)

		out = append(out, b)
	}
}

func TestPubsubTriggerFires(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		trigger  *cloudbuildpb.BuildTrigger
		topic    string
		msg      *pubsub.Message
		wantArgs []string
		wantFire bool
	}{
		{
			name: "binds data and attributes", topic: triggerTopic, wantFire: true,
			msg: &pubsub.Message{Data: []byte("hello"), Attributes: map[string]string{"env": "prod"}},
			trigger: &cloudbuildpb.BuildTrigger{
				Substitutions: map[string]string{
					"_BODY": "$(body.message.data)", "_ENV": "$(body.message.attributes.env)",
				},
			},
			wantArgs: []string{base64.StdEncoding.EncodeToString([]byte("hello")), "prod"},
		},
		{
			name: "other topic", topic: otherTopic, msg: &pubsub.Message{Data: []byte("x")},
			trigger: &cloudbuildpb.BuildTrigger{},
		},
		{
			name: "disabled", topic: triggerTopic, msg: &pubsub.Message{Data: []byte("x")},
			trigger: &cloudbuildpb.BuildTrigger{Disabled: true},
		},
		{
			name: "filter matches", topic: triggerTopic, wantFire: true,
			msg: &pubsub.Message{Data: []byte("x"), Attributes: map[string]string{"env": "prod"}},
			trigger: &cloudbuildpb.BuildTrigger{
				Substitutions: map[string]string{"_BODY": "b", "_ENV": "$(body.message.attributes.env)"},
				Filter:        "_ENV.matches('^pr') && _ENV != 'prodx'",
			},
			wantArgs: []string{"b", "prod"},
		},
		{
			name: "filter rejects", topic: triggerTopic,
			msg: &pubsub.Message{Data: []byte("x"), Attributes: map[string]string{"env": "dev"}},
			trigger: &cloudbuildpb.BuildTrigger{
				Substitutions: map[string]string{"_BODY": "b", "_ENV": "$(body.message.attributes.env)"},
				Filter:        "_ENV == 'prod'",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := newPublishEnv(t)

			subs := map[string]string{"_BODY": "b", "_ENV": "e"}
			maps.Copy(subs, tt.trigger.GetSubstitutions())

			tt.trigger.Name, tt.trigger.Substitutions = "on-deploy", subs
			tt.trigger.PubsubConfig = &cloudbuildpb.PubsubConfig{Topic: triggerTopic}
			tt.trigger.BuildTemplate = &cloudbuildpb.BuildTrigger_Build{Build: buildOf(
				step("img", withArgs("$_BODY", "$_ENV")),
			)}

			created, err := p.env.client.CreateBuildTrigger(t.Context(), &cloudbuildpb.CreateBuildTriggerRequest{
				Parent: parent, Trigger: tt.trigger,
			})
			require.NoError(t, err)

			p.publish(t, tt.topic, tt.msg)

			got := p.builds(t)
			if !tt.wantFire {
				assert.Empty(t, got)

				return
			}

			require.Len(t, got, 1)
			assert.Equal(t, created.GetId(), got[0].GetBuildTriggerId())
			assert.Equal(t, cloudbuildpb.Build_SUCCESS, got[0].GetStatus())
			assert.Equal(t, tt.wantArgs, got[0].GetSteps()[0].GetArgs())
		})
	}
}

func TestPubsubTriggerFilterValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, nil)

	_, err := e.client.CreateBuildTrigger(t.Context(), &cloudbuildpb.CreateBuildTriggerRequest{
		Parent: parent,
		Trigger: &cloudbuildpb.BuildTrigger{
			Name: "bad", Filter: "_A.size() > 3",
			PubsubConfig:  &cloudbuildpb.PubsubConfig{Topic: triggerTopic},
			BuildTemplate: &cloudbuildpb.BuildTrigger_Build{Build: buildOf(step("img"))},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid trigger filter")
}

func TestWebhookTriggerDisabledAndFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filter   string
		disabled bool
		wantFire bool
	}{
		{name: "enabled", wantFire: true},
		{name: "disabled", disabled: true},
		{name: "filter matches", filter: "_V == 'x'", wantFire: true},
		{name: "filter rejects", filter: "_V == 'y'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, map[string]behavior{"img": exits(0)})

			created, err := e.client.CreateBuildTrigger(t.Context(), &cloudbuildpb.CreateBuildTriggerRequest{
				Parent: parent,
				Trigger: &cloudbuildpb.BuildTrigger{
					Name: "hook", Disabled: tt.disabled, Filter: tt.filter,
					Substitutions: map[string]string{"_V": "$(body.v)"},
					BuildTemplate: &cloudbuildpb.BuildTrigger_Build{Build: buildOf(step("img", withArgs("$_V")))},
					WebhookConfig: &cloudbuildpb.WebhookConfig{AuthMethod: &cloudbuildpb.WebhookConfig_Secret{
						Secret: "projects/p/secrets/hook/versions/1",
					}},
				},
			})
			require.NoError(t, err)

			_, err = e.client.ReceiveTriggerWebhook(t.Context(), &cloudbuildpb.ReceiveTriggerWebhookRequest{
				Name: created.GetResourceName(), Secret: "whsec", Body: &httpbody.HttpBody{Data: []byte(`{"v":"x"}`)},
			})
			require.NoError(t, err)
			e.backend.Wait()

			it := e.client.ListBuilds(t.Context(), &cloudbuildpb.ListBuildsRequest{Parent: parent})
			b, err := it.Next()

			if !tt.wantFire {
				assert.ErrorIs(t, err, iterator.Done)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, created.GetId(), b.GetBuildTriggerId())
		})
	}
}
