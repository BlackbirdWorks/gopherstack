package gcpsecretmanager_test

import (
	"context"
	"fmt"
	"hash/crc32"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
)

const proj = "projects/test-project"

type clock struct{ ns atomic.Int64 }

func (c *clock) now() time.Time          { return time.Unix(0, c.ns.Load()).UTC() }
func (c *clock) advance(d time.Duration) { c.ns.Add(int64(d)) }

type env struct {
	client  *secretmanager.Client
	handler *gcpsecretmanager.Handler
	clk     *clock
	opts    []option.ClientOption
}

func newEnv(t *testing.T) *env {
	t.Helper()

	h := gcpsecretmanager.NewHandler(gcpsecretmanager.NewInMemoryBackend())
	h.Port = 0

	clk := &clock{}
	clk.ns.Store(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	h.Backend.SetClock(clk.now)

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	opts := []option.ClientOption{
		option.WithEndpoint(h.Addr()),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}

	client, err := secretmanager.NewClient(t.Context(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return &env{client: client, handler: h, clk: clk, opts: opts}
}

func auto() *secretmanagerpb.Replication {
	return &secretmanagerpb.Replication{Replication: &secretmanagerpb.Replication_Automatic_{
		Automatic: &secretmanagerpb.Replication_Automatic{},
	}}
}

func (e *env) secret(t *testing.T, id string, mut ...func(*secretmanagerpb.Secret)) *secretmanagerpb.Secret {
	t.Helper()

	s := &secretmanagerpb.Secret{Replication: auto()}
	for _, m := range mut {
		m(s)
	}

	out, err := e.client.CreateSecret(t.Context(), &secretmanagerpb.CreateSecretRequest{
		Parent: proj, SecretId: id, Secret: s,
	})
	require.NoError(t, err)

	return out
}

func (e *env) version(t *testing.T, secret, payload string) *secretmanagerpb.SecretVersion {
	t.Helper()

	v, err := e.client.AddSecretVersion(t.Context(), &secretmanagerpb.AddSecretVersionRequest{
		Parent: secret, Payload: &secretmanagerpb.SecretPayload{Data: []byte(payload)},
	})
	require.NoError(t, err)

	return v
}

func crc(data string) int64 {
	return int64(crc32.Checksum([]byte(data), crc32.MakeTable(crc32.Castagnoli)))
}

func TestCreateSecret(t *testing.T) {
	t.Parallel()

	topic := &secretmanagerpb.Topic{Name: "projects/test-project/topics/t"}
	future := timestamppb.New(time.Date(2031, 1, 1, 0, 0, 0, 0, time.UTC))

	tests := map[string]struct {
		secret *secretmanagerpb.Secret
		code   codes.Code
	}{
		"automatic": {&secretmanagerpb.Secret{Replication: auto(), Labels: map[string]string{"env": "dev"}}, codes.OK},
		"user-managed": {&secretmanagerpb.Secret{Replication: &secretmanagerpb.Replication{
			Replication: &secretmanagerpb.Replication_UserManaged_{
				UserManaged: &secretmanagerpb.Replication_UserManaged{
					Replicas: []*secretmanagerpb.Replication_UserManaged_Replica{
						{
							Location: "us-east1",
						},
						{
							Location:                  "europe-west1",
							CustomerManagedEncryption: &secretmanagerpb.CustomerManagedEncryption{KmsKeyName: "k"},
						},
					},
				},
			},
		}}, codes.OK},
		"duplicate-replica": {&secretmanagerpb.Secret{Replication: &secretmanagerpb.Replication{
			Replication: &secretmanagerpb.Replication_UserManaged_{
				UserManaged: &secretmanagerpb.Replication_UserManaged{
					Replicas: []*secretmanagerpb.Replication_UserManaged_Replica{{Location: "x"}, {Location: "x"}},
				},
			},
		}}, codes.InvalidArgument},
		"missing-replication": {&secretmanagerpb.Secret{}, codes.InvalidArgument},
		"bad id!":             {&secretmanagerpb.Secret{Replication: auto()}, codes.InvalidArgument},
		"ttl": {
			&secretmanagerpb.Secret{
				Replication: auto(),
				Expiration:  &secretmanagerpb.Secret_Ttl{Ttl: durationpb.New(time.Hour)},
			},
			codes.OK,
		},
		"negative-ttl": {
			&secretmanagerpb.Secret{
				Replication: auto(),
				Expiration:  &secretmanagerpb.Secret_Ttl{Ttl: durationpb.New(-time.Hour)},
			},
			codes.InvalidArgument,
		},
		"past-expire": {
			&secretmanagerpb.Secret{
				Replication: auto(),
				Expiration:  &secretmanagerpb.Secret_ExpireTime{ExpireTime: timestamppb.New(time.Unix(1, 0))},
			},
			codes.InvalidArgument,
		},
		"rotation": {
			&secretmanagerpb.Secret{
				Replication: auto(),
				Topics:      []*secretmanagerpb.Topic{topic},
				Rotation: &secretmanagerpb.Rotation{
					NextRotationTime: future, RotationPeriod: durationpb.New(2 * time.Hour),
				},
			},
			codes.OK,
		},
		"rotation-without-topics": {
			&secretmanagerpb.Secret{Replication: auto(), Rotation: &secretmanagerpb.Rotation{NextRotationTime: future}},
			codes.InvalidArgument,
		},
		"rotation-period-without-time": {
			&secretmanagerpb.Secret{
				Replication: auto(),
				Topics:      []*secretmanagerpb.Topic{topic},
				Rotation: &secretmanagerpb.Rotation{
					RotationPeriod: durationpb.New(2 * time.Hour),
				},
			},
			codes.InvalidArgument,
		},
		"short-rotation-period": {
			&secretmanagerpb.Secret{
				Replication: auto(),
				Topics:      []*secretmanagerpb.Topic{topic},
				Rotation: &secretmanagerpb.Rotation{
					NextRotationTime: future, RotationPeriod: durationpb.New(time.Minute),
				},
			},
			codes.InvalidArgument,
		},
		"bad-topic": {
			&secretmanagerpb.Secret{Replication: auto(), Topics: []*secretmanagerpb.Topic{{Name: "nope"}}},
			codes.InvalidArgument,
		},
		"short-destroy-ttl": {
			&secretmanagerpb.Secret{Replication: auto(), VersionDestroyTtl: durationpb.New(time.Hour)},
			codes.InvalidArgument,
		},
		"alias-to-missing-version": {
			&secretmanagerpb.Secret{Replication: auto(), VersionAliases: map[string]int64{"prod": 1}},
			codes.NotFound,
		},
		"empty-kms-key": {
			&secretmanagerpb.Secret{
				Replication:               auto(),
				CustomerManagedEncryption: &secretmanagerpb.CustomerManagedEncryption{},
			},
			codes.InvalidArgument,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)

			got, err := e.client.CreateSecret(t.Context(), &secretmanagerpb.CreateSecretRequest{
				Parent: proj, SecretId: name, Secret: tt.secret,
			})
			if tt.code != codes.OK {
				assert.Equal(t, tt.code, status.Code(err), "%v", err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, proj+"/secrets/"+name, got.GetName())
			assert.NotEmpty(t, got.GetEtag())
			assert.True(t, got.GetCreateTime().AsTime().Equal(e.clk.now()))

			if name == "ttl" {
				assert.True(t, got.GetExpireTime().AsTime().Equal(e.clk.now().Add(time.Hour)))
				assert.Nil(t, got.GetTtl())
			}

			again, err := e.client.GetSecret(t.Context(), &secretmanagerpb.GetSecretRequest{Name: got.GetName()})
			require.NoError(t, err)
			assert.Equal(t, got.GetEtag(), again.GetEtag())
		})
	}
}

func TestCreateSecretDuplicateAndMissing(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.secret(t, "dup")

	_, err := e.client.CreateSecret(t.Context(), &secretmanagerpb.CreateSecretRequest{
		Parent: proj, SecretId: "dup", Secret: &secretmanagerpb.Secret{Replication: auto()},
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	_, err = e.client.GetSecret(t.Context(), &secretmanagerpb.GetSecretRequest{Name: proj + "/secrets/none"})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.client.GetSecret(t.Context(), &secretmanagerpb.GetSecretRequest{Name: "garbage"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = e.client.CreateSecret(t.Context(), &secretmanagerpb.CreateSecretRequest{
		Parent: "bad", SecretId: "x", Secret: &secretmanagerpb.Secret{Replication: auto()},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestUpdateSecret(t *testing.T) {
	t.Parallel()

	topic := &secretmanagerpb.Topic{Name: "projects/test-project/topics/t"}

	tests := map[string]struct {
		check func(t *testing.T, before, after *secretmanagerpb.Secret)
		patch *secretmanagerpb.Secret
		paths []string
		code  codes.Code
	}{
		"labels": {func(t *testing.T, before, after *secretmanagerpb.Secret) {
			t.Helper()
			assert.Equal(t, map[string]string{"a": "b"}, after.GetLabels())
			assert.NotEqual(t, before.GetEtag(), after.GetEtag())
			assert.Equal(t, before.GetAnnotations(), after.GetAnnotations())
		}, &secretmanagerpb.Secret{Labels: map[string]string{"a": "b"}}, []string{"labels"}, codes.OK},
		"annotations": {func(t *testing.T, _, after *secretmanagerpb.Secret) {
			t.Helper()
			assert.Equal(t, map[string]string{"n": "v"}, after.GetAnnotations())
			assert.Equal(t, map[string]string{"keep": "me"}, after.GetLabels())
		}, &secretmanagerpb.Secret{Annotations: map[string]string{"n": "v"}}, []string{"annotations"}, codes.OK},
		"ttl": {func(t *testing.T, _, after *secretmanagerpb.Secret) {
			t.Helper()
			assert.NotNil(t, after.GetExpireTime())
		}, &secretmanagerpb.Secret{
			Expiration: &secretmanagerpb.Secret_Ttl{Ttl: durationpb.New(48 * time.Hour)},
		}, []string{"ttl"}, codes.OK},
		"clear expire": {func(t *testing.T, _, after *secretmanagerpb.Secret) {
			t.Helper()
			assert.Nil(t, after.GetExpireTime())
		}, &secretmanagerpb.Secret{}, []string{"expire_time"}, codes.OK},
		"rotation": {func(t *testing.T, _, after *secretmanagerpb.Secret) {
			t.Helper()
			assert.Equal(t, 24*time.Hour, after.GetRotation().GetRotationPeriod().AsDuration())
			assert.Len(t, after.GetTopics(), 1)
		}, &secretmanagerpb.Secret{
			Topics: []*secretmanagerpb.Topic{topic},
			Rotation: &secretmanagerpb.Rotation{
				NextRotationTime: timestamppb.New(time.Date(2031, 1, 1, 0, 0, 0, 0, time.UTC)),
				RotationPeriod:   durationpb.New(24 * time.Hour),
			},
		}, []string{"topics", "rotation"}, codes.OK},
		"destroy ttl": {func(t *testing.T, _, after *secretmanagerpb.Secret) {
			t.Helper()
			assert.Equal(t, 72*time.Hour, after.GetVersionDestroyTtl().AsDuration())
		}, &secretmanagerpb.Secret{
			VersionDestroyTtl: durationpb.New(72 * time.Hour),
		}, []string{"version_destroy_ttl"}, codes.OK},
		"alias": {func(t *testing.T, _, after *secretmanagerpb.Secret) {
			t.Helper()
			assert.Equal(t, int64(1), after.GetVersionAliases()["prod"])
		}, &secretmanagerpb.Secret{VersionAliases: map[string]int64{"prod": 1}}, []string{"version_aliases"}, codes.OK},
		"alias missing version": {
			nil,
			&secretmanagerpb.Secret{VersionAliases: map[string]int64{"prod": 9}},
			[]string{"version_aliases"},
			codes.NotFound,
		},
		"alias latest reserved": {
			nil,
			&secretmanagerpb.Secret{VersionAliases: map[string]int64{"latest": 1}},
			[]string{"version_aliases"},
			codes.InvalidArgument,
		},
		"replication immutable": {
			nil,
			&secretmanagerpb.Secret{Replication: auto()},
			[]string{"replication"},
			codes.InvalidArgument,
		},
		"empty mask": {nil, &secretmanagerpb.Secret{}, nil, codes.InvalidArgument},
		"rotation without topics": {
			nil,
			&secretmanagerpb.Secret{Rotation: &secretmanagerpb.Rotation{}},
			[]string{"rotation"},
			codes.InvalidArgument,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			before := e.secret(t, "s", func(s *secretmanagerpb.Secret) { s.Labels = map[string]string{"keep": "me"} })
			e.version(t, before.GetName(), "v1")

			tt.patch.Name = before.GetName()

			after, err := e.client.UpdateSecret(t.Context(), &secretmanagerpb.UpdateSecretRequest{
				Secret: tt.patch, UpdateMask: &fieldmaskpb.FieldMask{Paths: tt.paths},
			})
			if tt.code != codes.OK {
				assert.Equal(t, tt.code, status.Code(err), "%v", err)

				return
			}

			require.NoError(t, err)
			tt.check(t, before, after)
		})
	}
}

func TestUpdateAndDeleteEtag(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	s := e.secret(t, "s")

	_, err := e.client.UpdateSecret(t.Context(), &secretmanagerpb.UpdateSecretRequest{
		Secret:     &secretmanagerpb.Secret{Name: s.GetName(), Etag: `"stale"`, Labels: map[string]string{"a": "b"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	err = e.client.DeleteSecret(t.Context(), &secretmanagerpb.DeleteSecretRequest{Name: s.GetName(), Etag: `"stale"`})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	require.NoError(
		t,
		e.client.DeleteSecret(t.Context(), &secretmanagerpb.DeleteSecretRequest{Name: s.GetName(), Etag: s.GetEtag()}),
	)

	_, err = e.client.GetSecret(t.Context(), &secretmanagerpb.GetSecretRequest{Name: s.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))

	err = e.client.DeleteSecret(t.Context(), &secretmanagerpb.DeleteSecretRequest{Name: s.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestDeleteSecretRemovesVersions(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	s := e.secret(t, "s")
	e.version(t, s.GetName(), "v1")
	require.NoError(t, e.client.DeleteSecret(t.Context(), &secretmanagerpb.DeleteSecretRequest{Name: s.GetName()}))

	e.secret(t, "s")

	_, err := e.client.GetSecretVersion(
		t.Context(),
		&secretmanagerpb.GetSecretVersionRequest{Name: s.GetName() + "/versions/1"},
	)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestListSecrets(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		filter   string
		want     []string
		code     codes.Code
		pageSize int32
	}{
		"all":                 {"", []string{"alpha", "beta", "gamma"}, codes.OK, 0},
		"name substring":      {"name:alp", []string{"alpha"}, codes.OK, 0},
		"label equals":        {"labels.env=prod", []string{"beta", "gamma"}, codes.OK, 0},
		"label presence":      {"labels.team:*", []string{"gamma"}, codes.OK, 0},
		"and not":             {"labels.env=prod AND NOT labels.team:*", []string{"beta"}, codes.OK, 0},
		"or":                  {"name:alpha OR name:gamma", []string{"alpha", "gamma"}, codes.OK, 0},
		"parens implicit and": {"(name:beta OR name:gamma) labels.team=x", []string{"gamma"}, codes.OK, 0},
		"negated term":        {"-labels.env=prod", []string{"alpha"}, codes.OK, 0},
		"bare term":           {"gamma", []string{"gamma"}, codes.OK, 0},
		"annotation":          {"annotations.owner=me", []string{"alpha"}, codes.OK, 0},
		"create time":         {"create_time>2029-01-01", []string{"alpha", "beta", "gamma"}, codes.OK, 0},
		"create time none":    {`create_time<"2029-01-01T00:00:00Z"`, nil, codes.OK, 0},
		"bad filter":          {"name:(", nil, codes.InvalidArgument, 0},
		"negative page":       {"", nil, codes.InvalidArgument, -1},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.secret(t, "alpha", func(s *secretmanagerpb.Secret) { s.Annotations = map[string]string{"owner": "me"} })
			e.secret(t, "beta", func(s *secretmanagerpb.Secret) { s.Labels = map[string]string{"env": "prod"} })
			e.secret(
				t,
				"gamma",
				func(s *secretmanagerpb.Secret) { s.Labels = map[string]string{"env": "prod", "team": "x"} },
			)

			resp, err := e.handlerList(t, tt.filter, tt.pageSize, "")
			if tt.code != codes.OK {
				assert.Equal(t, tt.code, status.Code(err), "%v", err)

				return
			}

			require.NoError(t, err)

			var got []string
			for _, s := range resp.GetSecrets() {
				got = append(got, strings.TrimPrefix(s.GetName(), proj+"/secrets/"))
			}

			assert.Equal(t, tt.want, got)
			assert.Equal(t, int32(len(tt.want)), resp.GetTotalSize())
		})
	}
}

func (e *env) handlerList(
	t *testing.T,
	filter string,
	size int32,
	token string,
) (*secretmanagerpb.ListSecretsResponse, error) {
	t.Helper()

	conn, err := grpc.NewClient(e.handler.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return secretmanagerpb.NewSecretManagerServiceClient(conn).
		ListSecrets(t.Context(), &secretmanagerpb.ListSecretsRequest{
			Parent: proj, Filter: filter, PageSize: size, PageToken: token,
		})
}

func TestListSecretsPagination(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	for i := range 5 {
		e.secret(t, fmt.Sprintf("s%d", i))
	}

	e.secret(t, "other")

	var (
		seen  []string
		token string
		pages int
	)

	for {
		resp, err := e.handlerList(t, "name:s", 2, token)
		require.NoError(t, err)

		pages++

		for _, s := range resp.GetSecrets() {
			seen = append(seen, s.GetName())
		}

		if token = resp.GetNextPageToken(); token == "" {
			break
		}
	}

	assert.Equal(t, 3, pages)
	assert.Len(t, seen, 6)

	it := e.client.ListSecrets(t.Context(), &secretmanagerpb.ListSecretsRequest{Parent: proj, PageSize: 2})
	count := 0

	for {
		_, err := it.Next()
		if err != nil {
			break
		}

		count++
	}

	assert.Equal(t, 6, count)
}

func TestSecretVersions(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	s := e.secret(t, "s")

	v1 := e.version(t, s.GetName(), "one")
	v2 := e.version(t, s.GetName(), "two")

	assert.Equal(t, s.GetName()+"/versions/1", v1.GetName())
	assert.Equal(t, s.GetName()+"/versions/2", v2.GetName())
	assert.Equal(t, secretmanagerpb.SecretVersion_ENABLED, v1.GetState())
	assert.NotNil(t, v1.GetReplicationStatus().GetAutomatic())
	assert.False(t, v1.GetClientSpecifiedPayloadChecksum())

	acc, err := e.client.AccessSecretVersion(
		t.Context(),
		&secretmanagerpb.AccessSecretVersionRequest{Name: s.GetName() + "/versions/latest"},
	)
	require.NoError(t, err)
	assert.Equal(t, "two", string(acc.GetPayload().GetData()))
	assert.Equal(t, v2.GetName(), acc.GetName())
	assert.Equal(t, crc("two"), acc.GetPayload().GetDataCrc32C())

	got, err := e.client.GetSecretVersion(
		t.Context(),
		&secretmanagerpb.GetSecretVersionRequest{Name: s.GetName() + "/versions/latest"},
	)
	require.NoError(t, err)
	assert.Equal(t, v2.GetName(), got.GetName())

	list := e.client.ListSecretVersions(t.Context(), &secretmanagerpb.ListSecretVersionsRequest{Parent: s.GetName()})

	var names []string

	for {
		v, nerr := list.Next()
		if nerr != nil {
			break
		}

		names = append(names, v.GetName())
	}

	assert.Equal(t, []string{v2.GetName(), v1.GetName()}, names)

	for name, code := range map[string]codes.Code{
		s.GetName() + "/versions/9":       codes.NotFound,
		s.GetName() + "/versions/ghost":   codes.NotFound,
		s.GetName() + "/versions/0":       codes.InvalidArgument,
		proj + "/secrets/none/versions/1": codes.NotFound,
		"junk":                            codes.InvalidArgument,
	} {
		_, err = e.client.AccessSecretVersion(t.Context(), &secretmanagerpb.AccessSecretVersionRequest{Name: name})
		assert.Equal(t, code, status.Code(err), name)
	}

	_, err = e.client.AccessSecretVersion(
		t.Context(),
		&secretmanagerpb.AccessSecretVersionRequest{Name: e.emptyLatest(t)},
	)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func (e *env) emptyLatest(t *testing.T) string {
	t.Helper()

	return e.secret(t, "empty").GetName() + "/versions/latest"
}

func TestAddSecretVersionValidation(t *testing.T) {
	t.Parallel()

	good := crc("data")
	bad := good + 1

	tests := map[string]struct {
		payload *secretmanagerpb.SecretPayload
		code    codes.Code
		checked bool
	}{
		"plain":    {&secretmanagerpb.SecretPayload{Data: []byte("data")}, codes.OK, false},
		"good crc": {&secretmanagerpb.SecretPayload{Data: []byte("data"), DataCrc32C: &good}, codes.OK, true},
		"bad crc": {
			&secretmanagerpb.SecretPayload{Data: []byte("data"), DataCrc32C: &bad},
			codes.InvalidArgument,
			false,
		},
		"empty":       {&secretmanagerpb.SecretPayload{}, codes.OK, false},
		"max size":    {&secretmanagerpb.SecretPayload{Data: make([]byte, 64<<10)}, codes.OK, false},
		"too big":     {&secretmanagerpb.SecretPayload{Data: make([]byte, 64<<10+1)}, codes.InvalidArgument, false},
		"nil payload": {nil, codes.InvalidArgument, false},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			s := e.secret(t, "s")

			v, err := e.client.AddSecretVersion(
				t.Context(),
				&secretmanagerpb.AddSecretVersionRequest{Parent: s.GetName(), Payload: tt.payload},
			)
			if tt.code != codes.OK {
				assert.Equal(t, tt.code, status.Code(err), "%v", err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.checked, v.GetClientSpecifiedPayloadChecksum())
		})
	}

	e := newEnv(t)
	_, err := e.client.AddSecretVersion(t.Context(), &secretmanagerpb.AddSecretVersionRequest{
		Parent: proj + "/secrets/none", Payload: &secretmanagerpb.SecretPayload{Data: []byte("x")},
	})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestVersionStateMachine(t *testing.T) {
	t.Parallel()

	type op func(e *env, name, etag string) (*secretmanagerpb.SecretVersion, error)

	disable := func(e *env, name, etag string) (*secretmanagerpb.SecretVersion, error) {
		return e.client.DisableSecretVersion(
			context.Background(),
			&secretmanagerpb.DisableSecretVersionRequest{Name: name, Etag: etag},
		)
	}
	enable := func(e *env, name, etag string) (*secretmanagerpb.SecretVersion, error) {
		return e.client.EnableSecretVersion(
			context.Background(),
			&secretmanagerpb.EnableSecretVersionRequest{Name: name, Etag: etag},
		)
	}
	destroy := func(e *env, name, etag string) (*secretmanagerpb.SecretVersion, error) {
		return e.client.DestroySecretVersion(
			context.Background(),
			&secretmanagerpb.DestroySecretVersionRequest{Name: name, Etag: etag},
		)
	}

	tests := []struct {
		name  string
		steps []op
		state secretmanagerpb.SecretVersion_State
		last  codes.Code
	}{
		{"disable", []op{disable}, secretmanagerpb.SecretVersion_DISABLED, codes.OK},
		{"disable twice", []op{disable, disable}, secretmanagerpb.SecretVersion_DISABLED, codes.OK},
		{"enable noop", []op{enable}, secretmanagerpb.SecretVersion_ENABLED, codes.OK},
		{"disable enable", []op{disable, enable}, secretmanagerpb.SecretVersion_ENABLED, codes.OK},
		{"destroy", []op{destroy}, secretmanagerpb.SecretVersion_DESTROYED, codes.OK},
		{"destroy twice", []op{destroy, destroy}, secretmanagerpb.SecretVersion_DESTROYED, codes.FailedPrecondition},
		{"enable destroyed", []op{destroy, enable}, secretmanagerpb.SecretVersion_DESTROYED, codes.FailedPrecondition},
		{
			"disable destroyed",
			[]op{destroy, disable},
			secretmanagerpb.SecretVersion_DESTROYED,
			codes.FailedPrecondition,
		},
		{"destroy disabled", []op{disable, destroy}, secretmanagerpb.SecretVersion_DESTROYED, codes.OK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			s := e.secret(t, "s")
			v := e.version(t, s.GetName(), "secret")

			var err error

			for _, step := range tt.steps {
				_, err = step(e, v.GetName(), "")
			}

			assert.Equal(t, tt.last, status.Code(err), "%v", err)

			got, err := e.client.GetSecretVersion(
				t.Context(),
				&secretmanagerpb.GetSecretVersionRequest{Name: v.GetName()},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.state, got.GetState())

			_, err = e.client.AccessSecretVersion(
				t.Context(),
				&secretmanagerpb.AccessSecretVersionRequest{Name: v.GetName()},
			)
			if tt.state == secretmanagerpb.SecretVersion_ENABLED {
				require.NoError(t, err)
			} else {
				assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			}

			if tt.state == secretmanagerpb.SecretVersion_DESTROYED {
				assert.NotNil(t, got.GetDestroyTime())
			}
		})
	}
}

func TestVersionEtagAndAlias(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	s := e.secret(t, "s")
	v1 := e.version(t, s.GetName(), "one")
	e.version(t, s.GetName(), "two")

	_, err := e.client.DisableSecretVersion(
		t.Context(),
		&secretmanagerpb.DisableSecretVersionRequest{Name: v1.GetName(), Etag: `"bogus"`},
	)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	dis, err := e.client.DisableSecretVersion(
		t.Context(),
		&secretmanagerpb.DisableSecretVersionRequest{Name: v1.GetName(), Etag: v1.GetEtag()},
	)
	require.NoError(t, err)
	assert.NotEqual(t, v1.GetEtag(), dis.GetEtag())

	_, err = e.client.UpdateSecret(t.Context(), &secretmanagerpb.UpdateSecretRequest{
		Secret:     &secretmanagerpb.Secret{Name: s.GetName(), VersionAliases: map[string]int64{"stable": 2}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"version_aliases"}},
	})
	require.NoError(t, err)

	acc, err := e.client.AccessSecretVersion(
		t.Context(),
		&secretmanagerpb.AccessSecretVersionRequest{Name: s.GetName() + "/versions/stable"},
	)
	require.NoError(t, err)
	assert.Equal(t, "two", string(acc.GetPayload().GetData()))
	assert.Equal(t, s.GetName()+"/versions/2", acc.GetName())

	list := e.client.ListSecretVersions(
		t.Context(),
		&secretmanagerpb.ListSecretVersionsRequest{Parent: s.GetName(), Filter: "state=DISABLED"},
	)
	got, err := list.Next()
	require.NoError(t, err)
	assert.Equal(t, v1.GetName(), got.GetName())

	_, err = list.Next()
	assert.Error(t, err)
}

func TestScheduledDestroy(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	s := e.secret(t, "s", func(s *secretmanagerpb.Secret) { s.VersionDestroyTtl = durationpb.New(48 * time.Hour) })
	v := e.version(t, s.GetName(), "later")

	des, err := e.client.DestroySecretVersion(
		t.Context(),
		&secretmanagerpb.DestroySecretVersionRequest{Name: v.GetName()},
	)
	require.NoError(t, err)
	assert.Equal(t, secretmanagerpb.SecretVersion_DISABLED, des.GetState())
	assert.True(t, des.GetScheduledDestroyTime().AsTime().Equal(e.clk.now().Add(48*time.Hour)))

	_, err = e.client.AccessSecretVersion(t.Context(), &secretmanagerpb.AccessSecretVersionRequest{Name: v.GetName()})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	en, err := e.client.EnableSecretVersion(t.Context(), &secretmanagerpb.EnableSecretVersionRequest{Name: v.GetName()})
	require.NoError(t, err)
	assert.Equal(t, secretmanagerpb.SecretVersion_ENABLED, en.GetState())
	assert.Nil(t, en.GetScheduledDestroyTime())

	_, err = e.client.DestroySecretVersion(t.Context(), &secretmanagerpb.DestroySecretVersionRequest{Name: v.GetName()})
	require.NoError(t, err)

	e.clk.advance(47 * time.Hour)

	got, err := e.client.GetSecretVersion(t.Context(), &secretmanagerpb.GetSecretVersionRequest{Name: v.GetName()})
	require.NoError(t, err)
	assert.Equal(t, secretmanagerpb.SecretVersion_DISABLED, got.GetState())

	e.clk.advance(2 * time.Hour)
	e.handler.Backend.Sweep()

	got, err = e.client.GetSecretVersion(t.Context(), &secretmanagerpb.GetSecretVersionRequest{Name: v.GetName()})
	require.NoError(t, err)
	assert.Equal(t, secretmanagerpb.SecretVersion_DESTROYED, got.GetState())
	assert.Nil(t, got.GetScheduledDestroyTime())
	assert.NotNil(t, got.GetDestroyTime())
}

func TestScheduledDestroyLazy(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	s := e.secret(t, "s", func(s *secretmanagerpb.Secret) { s.VersionDestroyTtl = durationpb.New(24 * time.Hour) })
	v := e.version(t, s.GetName(), "later")

	_, err := e.client.DestroySecretVersion(
		t.Context(),
		&secretmanagerpb.DestroySecretVersionRequest{Name: v.GetName()},
	)
	require.NoError(t, err)

	e.clk.advance(25 * time.Hour)

	got, err := e.client.GetSecretVersion(t.Context(), &secretmanagerpb.GetSecretVersionRequest{Name: v.GetName()})
	require.NoError(t, err)
	assert.Equal(t, secretmanagerpb.SecretVersion_DESTROYED, got.GetState())
}

func TestSecretExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		sweep bool
	}{
		{"lazy on read", false},
		{"janitor sweep", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			s := e.secret(t, "temp", func(s *secretmanagerpb.Secret) {
				s.Expiration = &secretmanagerpb.Secret_Ttl{Ttl: durationpb.New(time.Hour)}
			})
			e.secret(t, "forever")
			e.version(t, s.GetName(), "x")

			e.clk.advance(30 * time.Minute)

			_, err := e.client.GetSecret(t.Context(), &secretmanagerpb.GetSecretRequest{Name: s.GetName()})
			require.NoError(t, err)

			e.clk.advance(31 * time.Minute)

			if tt.sweep {
				e.handler.Backend.Sweep()
			}

			_, err = e.client.GetSecret(t.Context(), &secretmanagerpb.GetSecretRequest{Name: s.GetName()})
			assert.Equal(t, codes.NotFound, status.Code(err))

			_, err = e.client.AccessSecretVersion(
				t.Context(),
				&secretmanagerpb.AccessSecretVersionRequest{Name: s.GetName() + "/versions/1"},
			)
			assert.Equal(t, codes.NotFound, status.Code(err))

			resp, err := e.handlerList(t, "", 0, "")
			require.NoError(t, err)
			assert.Len(t, resp.GetSecrets(), 1)
		})
	}
}

func TestIamPolicy(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	s := e.secret(t, "s")

	empty, err := e.client.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: s.GetName()})
	require.NoError(t, err)
	assert.Empty(t, empty.GetBindings())
	assert.NotEmpty(t, empty.GetEtag())

	policy := &iampb.Policy{
		Bindings: []*iampb.Binding{
			{Role: "roles/secretmanager.secretAccessor", Members: []string{"user:a@example.com"}},
		},
	}

	set, err := e.client.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{Resource: s.GetName(), Policy: policy})
	require.NoError(t, err)
	assert.Equal(t, policy.GetBindings()[0].GetRole(), set.GetBindings()[0].GetRole())
	assert.NotEqual(t, empty.GetEtag(), set.GetEtag())

	got, err := e.client.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: s.GetName()})
	require.NoError(t, err)
	assert.Equal(t, set.GetEtag(), got.GetEtag())
	assert.Equal(t, []string{"user:a@example.com"}, got.GetBindings()[0].GetMembers())

	_, err = e.client.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
		Resource: s.GetName(), Policy: &iampb.Policy{Etag: []byte("stale")},
	})
	assert.Equal(t, codes.Aborted, status.Code(err))

	_, err = e.client.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
		Resource: s.GetName(), Policy: &iampb.Policy{Bindings: []*iampb.Binding{{Role: "r"}}},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	perms, err := e.client.TestIamPermissions(t.Context(), &iampb.TestIamPermissionsRequest{
		Resource: s.GetName(), Permissions: []string{"secretmanager.versions.access", "secretmanager.secrets.delete"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"secretmanager.versions.access", "secretmanager.secrets.delete"}, perms.GetPermissions())

	_, err = e.client.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: proj + "/secrets/none"})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.client.TestIamPermissions(
		t.Context(),
		&iampb.TestIamPermissionsRequest{Resource: proj + "/secrets/none"},
	)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	s := src.secret(t, "s", func(s *secretmanagerpb.Secret) {
		s.Labels = map[string]string{"k": "v"}
		s.VersionDestroyTtl = durationpb.New(24 * time.Hour)
	})
	src.version(t, s.GetName(), "one")
	v2 := src.version(t, s.GetName(), "two")
	_, err := src.client.DisableSecretVersion(
		t.Context(),
		&secretmanagerpb.DisableSecretVersionRequest{Name: s.GetName() + "/versions/1"},
	)
	require.NoError(t, err)
	_, err = src.client.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
		Resource: s.GetName(),
		Policy:   &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/x", Members: []string{"user:a@b.c"}}}},
	})
	require.NoError(t, err)

	data := src.handler.Snapshot(t.Context())
	require.NotEmpty(t, data)

	dst := newEnv(t)
	require.NoError(t, dst.handler.Restore(t.Context(), data))

	got, err := dst.client.GetSecret(t.Context(), &secretmanagerpb.GetSecretRequest{Name: s.GetName()})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"k": "v"}, got.GetLabels())

	acc, err := dst.client.AccessSecretVersion(
		t.Context(),
		&secretmanagerpb.AccessSecretVersionRequest{Name: v2.GetName()},
	)
	require.NoError(t, err)
	assert.Equal(t, "two", string(acc.GetPayload().GetData()))

	_, err = dst.client.AccessSecretVersion(
		t.Context(),
		&secretmanagerpb.AccessSecretVersionRequest{Name: s.GetName() + "/versions/1"},
	)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	pol, err := dst.client.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: s.GetName()})
	require.NoError(t, err)
	assert.Equal(t, "roles/x", pol.GetBindings()[0].GetRole())

	next := dst.version(t, s.GetName(), "three")
	assert.Equal(t, s.GetName()+"/versions/3", next.GetName())

	dst.handler.Reset()

	_, err = dst.client.GetSecret(t.Context(), &secretmanagerpb.GetSecretRequest{Name: s.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}
