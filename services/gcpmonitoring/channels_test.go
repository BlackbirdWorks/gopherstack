package gcpmonitoring_test

import (
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type hookRequest struct {
	query string
	auth  string
	user  string
	pass  string
	body  string
}

type hookServer struct {
	got []hookRequest
	mu  sync.Mutex
}

func newHookServer(t *testing.T, status int) (*hookServer, string) {
	t.Helper()

	hs := &hookServer{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		user, pass, _ := r.BasicAuth()

		hs.mu.Lock()
		hs.got = append(hs.got, hookRequest{
			query: r.URL.Query().Get("auth_token"), auth: r.Header.Get("Content-Type"),
			user: user, pass: pass, body: string(body),
		})
		hs.mu.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)

	return hs, srv.URL + "/hook"
}

func (h *hookServer) requests() []hookRequest {
	h.mu.Lock()
	defer h.mu.Unlock()

	return append([]hookRequest(nil), h.got...)
}

func TestWebhookChannelsDeliverIncidents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		labels map[string]string
		check  func(*testing.T, hookRequest)
		name   string
		typ    string
	}{
		{
			name: "token auth", typ: "webhook_tokenauth", labels: map[string]string{"auth_token": "s3cret"},
			check: func(t *testing.T, r hookRequest) {
				t.Helper()
				assert.Equal(t, "s3cret", r.query)
			},
		},
		{
			name: "basic auth", typ: "webhook_basicauth",
			labels: map[string]string{"username": "bob", "password": "pw"},
			check: func(t *testing.T, r hookRequest) {
				t.Helper()
				assert.Equal(t, "bob", r.user)
				assert.Equal(t, "pw", r.pass)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			hs, url := newHookServer(t, http.StatusOK)
			labels := map[string]string{"url": url}

			maps.Copy(labels, tt.labels)

			ch, err := e.channels(t).
				CreateNotificationChannel(t.Context(), &monitoringpb.CreateNotificationChannelRequest{
					Name: proj,
					NotificationChannel: &monitoringpb.NotificationChannel{
						Type:        tt.typ,
						DisplayName: "hook",
						Labels:      labels,
					},
				})
			require.NoError(t, err)
			assert.NotContains(t, ch.GetLabels(), "auth_token")
			assert.NotContains(t, ch.GetLabels(), "password")

			e.createPolicy(t, queuePolicy([]string{ch.GetName()}, monitoringpb.ComparisonType_COMPARISON_GT, 3))

			for m := 1; m <= 2; m++ {
				e.writeAt(t, epoch().Add(time.Duration(m)*time.Minute), "gold", map[string]float64{"inst-1": 9})
			}

			e.handler.Backend.Tick(t.Context())

			reqs := hs.requests()
			require.Len(t, reqs, 1)
			tt.check(t, reqs[0])
			assert.Contains(t, reqs[0].auth, "application/json")
			assert.Contains(t, reqs[0].body, `"state":"open"`)

			ds := e.handler.Backend.Deliveries()
			require.Len(t, ds, 1)
			assert.Equal(t, "delivered", ds[0].Result)
		})
	}
}

func TestChannelDeliveryFailuresAreRecorded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		labels map[string]string
		name   string
		typ    string
		want   string
		status int
	}{
		{name: "webhook 500", typ: "webhook_tokenauth", status: http.StatusInternalServerError, want: "failed"},
		{
			name: "slack unsupported", typ: "slack", labels: map[string]string{"channel_name": "#ops"},
			want: "failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			labels := tt.labels

			if labels == nil {
				_, url := newHookServer(t, tt.status)
				labels = map[string]string{"url": url}
			}

			ch, err := e.channels(t).
				CreateNotificationChannel(t.Context(), &monitoringpb.CreateNotificationChannelRequest{
					Name:                proj,
					NotificationChannel: &monitoringpb.NotificationChannel{Type: tt.typ, Labels: labels},
				})
			require.NoError(t, err)
			e.createPolicy(t, queuePolicy([]string{ch.GetName()}, monitoringpb.ComparisonType_COMPARISON_GT, 3))

			for m := 1; m <= 2; m++ {
				e.writeAt(t, epoch().Add(time.Duration(m)*time.Minute), "gold", map[string]float64{"inst-1": 9})
			}

			e.handler.Backend.Tick(t.Context())

			ds := e.handler.Backend.Deliveries()
			require.Len(t, ds, 1)
			assert.Equal(t, tt.want, ds[0].Result)
			assert.NotEmpty(t, ds[0].Error)
			assert.Len(t, e.handler.Backend.Incidents(testProject), 1)
		})
	}
}

func TestDisabledChannelReceivesNothing(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ch := e.pubsubChannel(t)

	_, err := e.channels(t).UpdateNotificationChannel(t.Context(), &monitoringpb.UpdateNotificationChannelRequest{
		UpdateMask:          &fieldmaskpb.FieldMask{Paths: []string{"enabled"}},
		NotificationChannel: &monitoringpb.NotificationChannel{Name: ch, Enabled: wrapperspb.Bool(false)},
	})
	require.NoError(t, err)

	e.createPolicy(t, queuePolicy([]string{ch}, monitoringpb.ComparisonType_COMPARISON_GT, 3))

	for m := 1; m <= 2; m++ {
		e.writeAt(t, epoch().Add(time.Duration(m)*time.Minute), "gold", map[string]float64{"inst-1": 9})
	}

	e.handler.Backend.Tick(t.Context())
	assert.Empty(t, e.pub.all())
	assert.Len(t, e.handler.Backend.Incidents(testProject), 1)
}

func TestNotificationChannelCRUD(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	c := e.channels(t)

	created, err := c.CreateNotificationChannel(t.Context(), &monitoringpb.CreateNotificationChannelRequest{
		Name: proj,
		NotificationChannel: &monitoringpb.NotificationChannel{
			Type: "email", DisplayName: "oncall", Labels: map[string]string{"email_address": "a@example.com"},
			UserLabels: map[string]string{"team": "x"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, monitoringpb.NotificationChannel_UNVERIFIED, created.GetVerificationStatus())
	assert.True(t, created.GetEnabled().GetValue())
	assert.NotNil(t, created.GetCreationRecord())

	got, err := c.GetNotificationChannel(
		t.Context(),
		&monitoringpb.GetNotificationChannelRequest{Name: created.GetName()},
	)
	require.NoError(t, err)
	assert.Equal(t, "oncall", got.GetDisplayName())

	upd, err := c.UpdateNotificationChannel(t.Context(), &monitoringpb.UpdateNotificationChannelRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name", "user_labels"}},
		NotificationChannel: &monitoringpb.NotificationChannel{
			Name: created.GetName(), DisplayName: "renamed", UserLabels: map[string]string{"team": "y"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", upd.GetDisplayName())
	assert.Equal(t, "a@example.com", upd.GetLabels()["email_address"])
	assert.Len(t, upd.GetMutationRecords(), 1)

	it := c.ListNotificationChannels(t.Context(), &monitoringpb.ListNotificationChannelsRequest{
		Name: proj, Filter: `type = "email" AND display_name = "renamed"`,
	})
	first, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, created.GetName(), first.GetName())
	_, err = it.Next()
	assert.Equal(t, iterator.Done, err)

	require.NoError(
		t,
		c.DeleteNotificationChannel(
			t.Context(),
			&monitoringpb.DeleteNotificationChannelRequest{Name: created.GetName()},
		),
	)

	_, err = c.GetNotificationChannel(t.Context(), &monitoringpb.GetNotificationChannelRequest{Name: created.GetName()})
	assert.Equal(t, "NotFound", codeOf(err).String())
}

func TestNotificationChannelValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	c := e.channels(t)

	tests := []struct {
		ch   *monitoringpb.NotificationChannel
		name string
	}{
		{&monitoringpb.NotificationChannel{Type: "carrier_pigeon"}, "unknown type"},
		{&monitoringpb.NotificationChannel{Type: "email"}, "missing label"},
		{
			&monitoringpb.NotificationChannel{
				Type:   "email",
				Labels: map[string]string{"email_address": "a@b.c", "x": "y"},
			},
			"unknown label",
		},
		{
			&monitoringpb.NotificationChannel{Type: "pubsub", Labels: map[string]string{"topic": "not-a-topic"}},
			"bad topic",
		},
		{
			&monitoringpb.NotificationChannel{Type: "webhook_tokenauth", Labels: map[string]string{"url": "ftp://x"}},
			"bad url",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := c.CreateNotificationChannel(t.Context(),
				&monitoringpb.CreateNotificationChannelRequest{Name: proj, NotificationChannel: tt.ch})
			assert.Equal(t, "InvalidArgument", codeOf(err).String())
		})
	}
}

func TestNotificationChannelVerification(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	c := e.channels(t)

	sms, err := c.CreateNotificationChannel(t.Context(), &monitoringpb.CreateNotificationChannelRequest{
		Name: proj,
		NotificationChannel: &monitoringpb.NotificationChannel{
			Type:   "sms",
			Labels: map[string]string{"number": "+15555550100"},
		},
	})
	require.NoError(t, err)

	_, err = c.VerifyNotificationChannel(
		t.Context(),
		&monitoringpb.VerifyNotificationChannelRequest{Name: sms.GetName(), Code: "000000"},
	)
	assert.Equal(t, "InvalidArgument", codeOf(err).String())

	require.NoError(t, c.SendNotificationChannelVerificationCode(t.Context(),
		&monitoringpb.SendNotificationChannelVerificationCodeRequest{Name: sms.GetName()}))

	code, err := c.GetNotificationChannelVerificationCode(t.Context(),
		&monitoringpb.GetNotificationChannelVerificationCodeRequest{Name: sms.GetName()})
	require.NoError(t, err)
	assert.Len(t, code.GetCode(), 6)

	verified, err := c.VerifyNotificationChannel(
		t.Context(),
		&monitoringpb.VerifyNotificationChannelRequest{Name: sms.GetName(), Code: code.GetCode()},
	)
	require.NoError(t, err)
	assert.Equal(t, monitoringpb.NotificationChannel_VERIFIED, verified.GetVerificationStatus())

	ps := e.pubsubChannel(t)
	err = c.SendNotificationChannelVerificationCode(
		t.Context(),
		&monitoringpb.SendNotificationChannelVerificationCodeRequest{Name: ps},
	)
	assert.Equal(t, "FailedPrecondition", codeOf(err).String())
}

func TestNotificationChannelDeleteInUse(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	c := e.channels(t)
	ch := e.pubsubChannel(t)
	policy := e.createPolicy(t, queuePolicy([]string{ch}, monitoringpb.ComparisonType_COMPARISON_GT, 3))

	err := c.DeleteNotificationChannel(t.Context(), &monitoringpb.DeleteNotificationChannelRequest{Name: ch})
	assert.Equal(t, "FailedPrecondition", codeOf(err).String())

	require.NoError(
		t,
		c.DeleteNotificationChannel(t.Context(), &monitoringpb.DeleteNotificationChannelRequest{Name: ch, Force: true}),
	)

	got, err := e.alerts(t).GetAlertPolicy(t.Context(), &monitoringpb.GetAlertPolicyRequest{Name: policy.GetName()})
	require.NoError(t, err)
	assert.Empty(t, got.GetNotificationChannels())
}

func TestNotificationChannelDescriptors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	c := e.channels(t)

	d, err := c.GetNotificationChannelDescriptor(t.Context(), &monitoringpb.GetNotificationChannelDescriptorRequest{
		Name: proj + "/notificationChannelDescriptors/pubsub",
	})
	require.NoError(t, err)
	assert.Equal(t, "pubsub", d.GetType())
	require.Len(t, d.GetLabels(), 1)
	assert.Equal(t, "topic", d.GetLabels()[0].GetKey())

	var types []string

	it := c.ListNotificationChannelDescriptors(
		t.Context(),
		&monitoringpb.ListNotificationChannelDescriptorsRequest{Name: proj},
	)

	for {
		nd, nextErr := it.Next()
		if errors.Is(nextErr, iterator.Done) {
			break
		}

		require.NoError(t, nextErr)

		types = append(types, nd.GetType())
	}

	assert.Subset(t, types, []string{"email", "pubsub", "webhook_tokenauth", "webhook_basicauth", "slack"})

	_, err = c.GetNotificationChannelDescriptor(t.Context(), &monitoringpb.GetNotificationChannelDescriptorRequest{
		Name: proj + "/notificationChannelDescriptors/nope",
	})
	assert.Equal(t, "NotFound", codeOf(err).String())
}
