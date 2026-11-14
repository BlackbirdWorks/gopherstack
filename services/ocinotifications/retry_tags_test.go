package ocinotifications_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/ons"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ocitags"
)

func TestCreateRetryToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, e *env, topic ons.NotificationTopic, name string) (string, error)
		remove func(t *testing.T, e *env, topic ons.NotificationTopic, id string)
		name   string
	}{
		{
			name: "topic",
			create: func(t *testing.T, e *env, _ ons.NotificationTopic, name string) (string, error) {
				t.Helper()

				r, err := e.ctl.CreateTopic(t.Context(), ons.CreateTopicRequest{
					OpcRetryToken: new("tok"),
					Name:          new(name), CompartmentId: new(testCompartment),
				})
				if err != nil {
					return "", err
				}

				return *r.TopicId, nil
			},
			remove: func(t *testing.T, e *env, _ ons.NotificationTopic, id string) {
				t.Helper()

				_, err := e.ctl.DeleteTopic(t.Context(), ons.DeleteTopicRequest{TopicId: &id})
				require.NoError(t, err)
			},
		},
		{
			name: "subscription",
			create: func(t *testing.T, e *env, topic ons.NotificationTopic, _ string) (string, error) {
				t.Helper()

				r, err := e.dp.CreateSubscription(t.Context(), ons.CreateSubscriptionRequest{
					OpcRetryToken: new("tok"),
					TopicId:       topic.TopicId, CompartmentId: new(testCompartment),
					Protocol: new("PAGERDUTY"), Endpoint: new("integration-key"),
				})
				if err != nil {
					return "", err
				}

				return *r.Id, nil
			},
			remove: func(t *testing.T, e *env, _ ons.NotificationTopic, id string) {
				t.Helper()

				_, err := e.dp.DeleteSubscription(t.Context(), ons.DeleteSubscriptionRequest{SubscriptionId: &id})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+" replay", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			topic := e.mkTopic(t, "base")

			id1, err := tt.create(t, e, topic, "rt")
			require.NoError(t, err)

			id2, err := tt.create(t, e, topic, "rt")
			require.NoError(t, err)
			assert.Equal(t, id1, id2)
		})

		t.Run(tt.name+" restored", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			topic := e.mkTopic(t, "base")

			id1, err := tt.create(t, e, topic, "rt")
			require.NoError(t, err)

			fresh := newEnv(t, envOpts{})
			require.NoError(t, fresh.backend.Restore(t.Context(), e.backend.Snapshot(t.Context())))

			id2, err := tt.create(t, fresh, topic, "rt")
			require.NoError(t, err)
			assert.Equal(t, id1, id2)
		})

		t.Run(tt.name+" expired", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			topic := e.mkTopic(t, "base")

			id1, err := tt.create(t, e, topic, "rt")
			require.NoError(t, err)

			e.clock.Advance(25 * time.Hour)
			tt.remove(t, e, topic, id1)
			e.settle()

			id2, err := tt.create(t, e, topic, "rt")
			require.NoError(t, err)
			assert.NotEqual(t, id1, id2)
		})

		t.Run(tt.name+" invalidated", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			topic := e.mkTopic(t, "base")

			id, err := tt.create(t, e, topic, "rt")
			require.NoError(t, err)

			e.settle()
			tt.remove(t, e, topic, id)
			e.settle()

			_, err = tt.create(t, e, topic, "rt")
			require.Error(t, err)
			assert.Equal(t, "InvalidatedRetryToken", svcErr(t, err).GetCode())
			assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestChangeCompartmentRetryToken(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	topic := e.mkTopic(t, "base")

	move := func(to string) error {
		_, err := e.ctl.ChangeTopicCompartment(t.Context(), ons.ChangeTopicCompartmentRequest{
			TopicId: topic.TopicId, OpcRetryToken: new("mv"),
			ChangeTopicCompartmentDetails: ons.ChangeCompartmentDetails{CompartmentId: new(to)},
		})

		return err
	}

	require.NoError(t, move("ocid1.compartment.oc1..a"))
	require.NoError(t, move("ocid1.compartment.oc1..b"))

	got, err := e.ctl.GetTopic(t.Context(), ons.GetTopicRequest{TopicId: topic.TopicId})
	require.NoError(t, err)
	assert.Equal(t, "ocid1.compartment.oc1..a", *got.CompartmentId)
}

func TestTagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		free   map[string]string
		defd   map[string]map[string]any
		name   string
		code   string
		status int
	}{
		{
			name: "valid", free: map[string]string{"a": "b"},
			defd: map[string]map[string]any{"Ops": {"Cost": "1"}}, status: http.StatusOK,
		},
		{
			name: "long value", free: map[string]string{"a": strings.Repeat("x", 257)},
			status: http.StatusBadRequest, code: "InvalidParameter",
		},
		{
			name: "unknown namespace", defd: map[string]map[string]any{"Nope": {"k": "v"}},
			status: http.StatusNotFound, code: "NotAuthorizedOrNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.backend.SetTagLookup(func(ns, key string) (ocitags.Def, bool) {
				return ocitags.Def{}, strings.EqualFold(ns, "ops") && strings.EqualFold(key, "cost")
			})

			topic := e.mkTopic(t, "base")
			sub := e.subscribe(t, topic, "PAGERDUTY", "base-key")

			check := func(err error) {
				t.Helper()

				if tt.code == "" {
					require.NoError(t, err)

					return
				}

				assert.Equal(t, tt.code, svcErr(t, err).GetCode())
				assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
			}

			_, err := e.ctl.CreateTopic(t.Context(), ons.CreateTopicRequest{
				Name: new("tagged"), CompartmentId: new(testCompartment),
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = e.dp.CreateSubscription(t.Context(), ons.CreateSubscriptionRequest{
				TopicId: topic.TopicId, CompartmentId: new(testCompartment),
				Protocol: new("PAGERDUTY"), Endpoint: new("other-key"),
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = e.ctl.UpdateTopic(t.Context(), ons.UpdateTopicRequest{
				TopicId:      topic.TopicId,
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = e.dp.UpdateSubscription(t.Context(), ons.UpdateSubscriptionRequest{
				SubscriptionId: sub.Id,
				FreeformTags:   tt.free, DefinedTags: tt.defd,
			})
			check(err)
		})
	}
}
