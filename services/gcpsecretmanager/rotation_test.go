package gcpsecretmanager_test

import (
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/services/gcppubsub"
)

const (
	centralHost   = "secretmanager.us-central1.rep.googleapis.com"
	centralParent = proj + "/locations/us-central1"
)

type pubsubBridge struct{ b *gcppubsub.InMemoryBackend }

func (p pubsubBridge) Publish(topic string, data []byte, attrs map[string]string) error {
	_, err := p.b.Publish(&pubsubpb.PublishRequest{
		Topic:    topic,
		Messages: []*pubsubpb.PubsubMessage{{Data: data, Attributes: attrs}},
	})

	return err
}

type sink struct {
	msgs []*pubsubpb.PubsubMessage
	mu   sync.Mutex
}

func (s *sink) take() []*pubsubpb.PubsubMessage {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := s.msgs
	s.msgs = nil

	return out
}

func rotationEnv(t *testing.T) (*env, *sink) {
	t.Helper()

	e := newEnv(t)
	ps := gcppubsub.NewInMemoryBackend()

	_, err := ps.CreateTopic(&pubsubpb.Topic{Name: "projects/test-project/topics/rot"})
	require.NoError(t, err)

	s := &sink{}
	ps.ObservePublishes(func(_ string, m *pubsubpb.PubsubMessage) {
		s.mu.Lock()
		defer s.mu.Unlock()

		s.msgs = append(s.msgs, m)
	})
	e.handler.Backend.SetPublisher(pubsubBridge{ps})

	return e, s
}

func rotating(e *env, period time.Duration, lead time.Duration) func(*secretmanagerpb.Secret) {
	return func(s *secretmanagerpb.Secret) {
		s.Topics = []*secretmanagerpb.Topic{{Name: "projects/test-project/topics/rot"}}
		s.Rotation = &secretmanagerpb.Rotation{NextRotationTime: timestamppb.New(e.clk.now().Add(lead))}

		if period > 0 {
			s.Rotation.RotationPeriod = durationpb.New(period)
		}
	}
}

func TestRotationPublishesSecretRotate(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		period   time.Duration
		wantNext time.Duration
	}{
		"periodic": {time.Hour, 5*time.Hour + 10*time.Minute},
		"one shot": {0, 0},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			e, s := rotationEnv(t)
			sec := e.secret(t, "rot", rotating(e, tc.period, 10*time.Minute))
			start := e.clk.now()

			e.handler.Backend.FireRotations()
			assert.Empty(t, s.take(), "not due yet")

			e.clk.advance(4*time.Hour + 20*time.Minute)
			e.handler.Backend.FireRotations()

			msgs := s.take()
			require.Len(t, msgs, 1, "missed periods collapse into one notification")
			assert.Equal(t, "SECRET_ROTATE", msgs[0].GetAttributes()["eventType"])
			assert.Equal(t, sec.GetName(), msgs[0].GetAttributes()["secretId"])
			assert.Equal(t, "JSON_API_V1", msgs[0].GetAttributes()["dataFormat"])
			assert.NotEmpty(t, msgs[0].GetAttributes()["timestamp"])
			assert.Contains(t, string(msgs[0].GetData()), "projects/test-project/secrets/rot")

			got, err := e.client.GetSecret(t.Context(), &secretmanagerpb.GetSecretRequest{Name: sec.GetName()})
			require.NoError(t, err)

			if tc.wantNext == 0 {
				assert.Nil(t, got.GetRotation().GetNextRotationTime())
			} else {
				assert.Equal(t, start.Add(tc.wantNext), got.GetRotation().GetNextRotationTime().AsTime())
			}

			e.handler.Backend.FireRotations()
			assert.Empty(t, s.take(), "fires once per due time")
		})
	}
}

func TestRotationValidation(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		rot  func(now time.Time) *secretmanagerpb.Rotation
		code codes.Code
	}{
		"too soon": {func(n time.Time) *secretmanagerpb.Rotation {
			return &secretmanagerpb.Rotation{NextRotationTime: timestamppb.New(n.Add(4 * time.Minute))}
		}, codes.InvalidArgument},
		"five minutes": {func(n time.Time) *secretmanagerpb.Rotation {
			return &secretmanagerpb.Rotation{NextRotationTime: timestamppb.New(n.Add(5 * time.Minute))}
		}, codes.OK},
		"period over 100 years": {func(n time.Time) *secretmanagerpb.Rotation {
			return &secretmanagerpb.Rotation{
				NextRotationTime: timestamppb.New(n.Add(time.Hour)),
				RotationPeriod:   durationpb.New(3153600001 * time.Second),
			}
		}, codes.InvalidArgument},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			_, err := e.client.CreateSecret(t.Context(), &secretmanagerpb.CreateSecretRequest{
				Parent: proj, SecretId: "v", Secret: &secretmanagerpb.Secret{
					Replication: auto(), Rotation: tc.rot(e.clk.now()),
					Topics: []*secretmanagerpb.Topic{{Name: "projects/p/topics/t"}},
				},
			})
			assert.Equal(t, tc.code, status.Code(err))
		})
	}
}

func cloudSQL(s *secretmanagerpb.Secret) {
	s.SecretType = secretmanagerpb.Secret_CLOUD_SQL_DB_CREDENTIALS
}

func enableReq(parent string) *secretmanagerpb.EnableManagedRotationRequest {
	return &secretmanagerpb.EnableManagedRotationRequest{
		Parent: parent,
		Credentials: &secretmanagerpb.EnableManagedRotationRequest_CloudSqlSingleUserCredentials{
			CloudSqlSingleUserCredentials: &secretmanagerpb.EnableManagedRotationRequest_CloudSQLSingleUserCredentials{
				InstanceId: "db1", Username: "app",
			},
		},
	}
}

func TestManagedRotation(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	plain := e.secret(t, "plain")
	sql := e.secret(t, "sql", cloudSQL)

	_, err := e.client.RotateSecret(ctx, &secretmanagerpb.RotateSecretRequest{Parent: sql.GetName()})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "rotate before enable")

	_, err = e.client.EnableManagedRotation(ctx, enableReq(plain.GetName()))
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "wrong secret type")

	_, err = e.client.EnableManagedRotation(ctx, &secretmanagerpb.EnableManagedRotationRequest{Parent: sql.GetName()})
	assert.Equal(t, codes.InvalidArgument, status.Code(err), "credentials required")

	_, err = e.client.EnableManagedRotation(ctx, enableReq(proj+"/secrets/nope"))
	assert.Equal(t, codes.NotFound, status.Code(err))

	v1, err := e.client.EnableManagedRotation(ctx, enableReq(sql.GetName()))
	require.NoError(t, err)
	assert.Equal(t, sql.GetName()+"/versions/1", v1.GetName())

	_, err = e.client.EnableManagedRotation(ctx, enableReq(sql.GetName()))
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "enable only once")

	got, err := e.client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: sql.GetName()})
	require.NoError(t, err)
	assert.Equal(t, secretmanagerpb.Rotation_ManagedRotationStatus_ACTIVE,
		got.GetRotation().GetManagedRotationStatus().GetState())

	v2, err := e.client.RotateSecret(ctx, &secretmanagerpb.RotateSecretRequest{Parent: sql.GetName()})
	require.NoError(t, err)
	assert.Equal(t, sql.GetName()+"/versions/2", v2.GetName())

	p1, err := e.client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: v1.GetName()})
	require.NoError(t, err)
	p2, err := e.client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: v2.GetName()})
	require.NoError(t, err)
	assert.NotEmpty(t, p2.GetPayload().GetData())
	assert.NotEqual(t, p1.GetPayload().GetData(), p2.GetPayload().GetData())

	e2 := newEnv(t)
	require.NoError(t, e2.handler.Restore(ctx, e.handler.Snapshot(ctx)))

	_, err = e2.client.RotateSecret(ctx, &secretmanagerpb.RotateSecretRequest{Parent: sql.GetName()})
	require.NoError(t, err, "managed state survives snapshot")
}

func (e *env) endpointClient(t *testing.T, authority string) *secretmanager.Client {
	t.Helper()

	c, err := secretmanager.NewClient(t.Context(),
		option.WithEndpoint(e.handler.Addr()),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithGRPCDialOption(grpc.WithAuthority(authority)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func TestRegionalSecrets(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	parent := centralParent

	created, err := e.client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent: parent, SecretId: "reg", Secret: &secretmanagerpb.Secret{Labels: map[string]string{"k": "v"}},
	})
	require.NoError(t, err)
	assert.Equal(t, parent+"/secrets/reg", created.GetName())
	assert.Nil(t, created.GetReplication())

	_, err = e.client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent: parent, SecretId: "reg", Secret: &secretmanagerpb.Secret{},
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	_, err = e.client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent: parent, SecretId: "withrepl", Secret: &secretmanagerpb.Secret{Replication: auto()},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err), "replication is not applicable to regional secrets")

	_, err = e.client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent: proj, SecretId: "reg", Secret: &secretmanagerpb.Secret{Replication: auto()},
	})
	require.NoError(t, err, "same id is free in the global namespace")

	v := e.version(t, created.GetName(), "hello")
	assert.Equal(t, created.GetName()+"/versions/1", v.GetName())
	assert.Nil(t, v.GetReplicationStatus())

	acc, err := e.client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{
		Name: created.GetName() + "/versions/latest",
	})
	require.NoError(t, err)
	assert.Equal(t, "hello", string(acc.GetPayload().GetData()))

	regional := e.client.ListSecrets(ctx, &secretmanagerpb.ListSecretsRequest{Parent: parent})
	s, err := regional.Next()
	require.NoError(t, err)
	assert.Equal(t, created.GetName(), s.GetName())

	_, err = regional.Next()
	require.ErrorIs(t, err, iterator.Done, "global secrets are not listed under a location")

	upd, err := e.client.UpdateSecret(ctx, &secretmanagerpb.UpdateSecretRequest{
		Secret:     &secretmanagerpb.Secret{Name: created.GetName(), Labels: map[string]string{"k": "w"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	require.NoError(t, err)
	assert.Equal(t, "w", upd.GetLabels()["k"])

	pol, err := e.client.IAM(created.GetName()).V3().Policy(ctx)
	require.NoError(t, err)
	assert.Empty(t, pol.Bindings)

	e2 := newEnv(t)
	require.NoError(t, e2.handler.Restore(ctx, e.handler.Snapshot(ctx)))

	got, err := e2.client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: created.GetName()})
	require.NoError(t, err)
	assert.Equal(t, "w", got.GetLabels()["k"])

	require.NoError(t, e.client.DeleteSecret(ctx, &secretmanagerpb.DeleteSecretRequest{Name: created.GetName()}))

	_, err = e.client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: created.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestRegionalEndpointLocationCheck(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	for name, tc := range map[string]struct {
		authority string
		parent    string
		repl      bool
		code      codes.Code
	}{
		"matching region":    {centralHost, centralParent, false, codes.OK},
		"other region":       {centralHost, proj + "/locations/us-east1", false, codes.InvalidArgument},
		"global on regional": {centralHost, proj, true, codes.InvalidArgument},
		"global endpoint":    {"secretmanager.googleapis.com", proj, true, codes.OK},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			c := e.endpointClient(t, tc.authority)
			sec := &secretmanagerpb.Secret{}

			if tc.repl {
				sec.Replication = auto()
			}

			_, err := c.CreateSecret(t.Context(), &secretmanagerpb.CreateSecretRequest{
				Parent: tc.parent, SecretId: "ep", Secret: sec,
			})
			assert.Equal(t, tc.code, status.Code(err))
		})
	}
}
