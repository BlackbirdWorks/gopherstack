package gcppubsub_test

import (
	"context"
	"strings"
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"
	vkit "cloud.google.com/go/pubsub/v2/apiv1"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/type/expr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type iamAPI interface {
	GetIamPolicy(context.Context, *iampb.GetIamPolicyRequest, ...gax.CallOption) (*iampb.Policy, error)
	SetIamPolicy(context.Context, *iampb.SetIamPolicyRequest, ...gax.CallOption) (*iampb.Policy, error)
	TestIamPermissions(
		context.Context, *iampb.TestIamPermissionsRequest, ...gax.CallOption,
	) (*iampb.TestIamPermissionsResponse, error)
}

type iamTarget struct {
	api  iamAPI
	name string
	perm string
	del  func()
}

// iamTargets creates one resource of every IAM-capable kind.
func iamTargets(t *testing.T, e *env) map[string]iamTarget {
	t.Helper()

	sc, err := vkit.NewSchemaClient(t.Context(), e.opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sc.Close() })

	tp := e.topic(t, "iam-topic")
	sub := e.sub(t, "iam-sub", tp)
	admin := e.client.SubscriptionAdminClient

	snap, err := admin.CreateSnapshot(t.Context(), &pubsubpb.CreateSnapshotRequest{
		Name: proj + "/snapshots/iam-snap", Subscription: sub,
	})
	require.NoError(t, err)

	def := `{"type":"record","name":"U","fields":[{"name":"id","type":"long"}]}`
	sch, err := sc.CreateSchema(t.Context(), &pubsubpb.CreateSchemaRequest{
		Parent: proj, SchemaId: "iam-schema", Schema: &pubsubpb.Schema{Type: pubsubpb.Schema_AVRO, Definition: def},
	})
	require.NoError(t, err)

	return map[string]iamTarget{
		"topic": {e.client.TopicAdminClient, tp, "pubsub.topics.publish", func() {
			require.NoError(t, e.client.TopicAdminClient.DeleteTopic(t.Context(), &pubsubpb.DeleteTopicRequest{Topic: tp}))
		}},
		"subscription": {admin, sub, "pubsub.subscriptions.consume", func() {
			require.NoError(t, admin.DeleteSubscription(t.Context(), &pubsubpb.DeleteSubscriptionRequest{Subscription: sub}))
		}},
		"snapshot": {admin, snap.GetName(), "pubsub.snapshots.seek", func() {
			require.NoError(t, admin.DeleteSnapshot(t.Context(), &pubsubpb.DeleteSnapshotRequest{Snapshot: snap.GetName()}))
		}},
		"schema": {sc, sch.GetName(), "pubsub.schemas.get", func() {
			require.NoError(t, sc.DeleteSchema(t.Context(), &pubsubpb.DeleteSchemaRequest{Name: sch.GetName()}))
		}},
	}
}

func viewer(member string) []*iampb.Binding {
	return []*iampb.Binding{{Role: "roles/pubsub.viewer", Members: []string{member}}}
}

func TestIAMPolicyRoundTrip(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{"topic", "subscription", "snapshot", "schema"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			tg := iamTargets(t, e)[kind]
			ctx := t.Context()

			empty, err := tg.api.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: tg.name})
			require.NoError(t, err)
			assert.Empty(t, empty.GetBindings())
			assert.Equal(t, []byte{0x00, 0x20, 0x01}, empty.GetEtag())

			set, err := tg.api.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
				Resource: tg.name, Policy: &iampb.Policy{Bindings: viewer("user:a@example.com")},
			})
			require.NoError(t, err)
			assert.NotEqual(t, empty.GetEtag(), set.GetEtag())
			assert.EqualValues(t, 1, set.GetVersion())

			got, err := tg.api.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: tg.name})
			require.NoError(t, err)
			assert.True(t, proto.Equal(set, got))

			_, err = tg.api.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
				Resource: tg.name, Policy: &iampb.Policy{Bindings: viewer("user:b@example.com"), Etag: empty.GetEtag()},
			})
			assert.Equal(t, codes.Aborted, status.Code(err))

			next, err := tg.api.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
				Resource: tg.name, Policy: &iampb.Policy{Bindings: viewer("user:b@example.com"), Etag: got.GetEtag()},
			})
			require.NoError(t, err)
			assert.NotEqual(t, got.GetEtag(), next.GetEtag())

			_, err = tg.api.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
				Resource: tg.name, Policy: &iampb.Policy{Bindings: viewer("user:c@example.com")},
			})
			require.NoError(t, err, "no etag overwrites unconditionally")

			_, err = tg.api.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
				Resource: tg.name, Policy: &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/x"}}},
			})
			assert.Equal(t, codes.InvalidArgument, status.Code(err))

			tg.del()

			_, err = tg.api.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: tg.name})
			assert.Equal(t, codes.NotFound, status.Code(err))
		})
	}
}

func TestIAMPolicyMissingAndInvalidResource(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	api := e.client.TopicAdminClient

	for name, tc := range map[string]struct {
		resource string
		code     codes.Code
	}{
		"missing topic":  {proj + "/topics/nope", codes.NotFound},
		"missing sub":    {proj + "/subscriptions/nope", codes.NotFound},
		"bad collection": {proj + "/widgets/nope", codes.InvalidArgument},
		"malformed":      {"nope", codes.InvalidArgument},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := api.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: tc.resource})
			assert.Equal(t, tc.code, status.Code(err))

			_, err = api.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
				Resource: tc.resource, Policy: &iampb.Policy{},
			})
			assert.Equal(t, tc.code, status.Code(err))
		})
	}
}

func TestIAMTestPermissions(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	targets := iamTargets(t, e)

	for kind, tg := range targets {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()

			resp, err := tg.api.TestIamPermissions(t.Context(), &iampb.TestIamPermissionsRequest{
				Resource: tg.name, Permissions: []string{tg.perm, "storage.objects.get", "pubsub.bogus.thing"},
			})
			require.NoError(t, err)
			assert.Equal(t, []string{tg.perm}, resp.GetPermissions())

			_, err = tg.api.TestIamPermissions(t.Context(), &iampb.TestIamPermissionsRequest{Resource: tg.name})
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

func TestIAMPolicyConditions(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tp := e.topic(t, "cond-topic")
	api := e.client.TopicAdminClient
	cond := &expr.Expr{Title: "soon", Expression: `request.time < timestamp("2030-01-01T00:00:00Z")`}
	bindings := []*iampb.Binding{
		{Role: "roles/pubsub.viewer", Members: []string{"user:a@example.com"}},
		{Role: "roles/pubsub.publisher", Members: []string{"user:a@example.com"}, Condition: cond},
	}

	for name, tc := range map[string]struct {
		version int32
		code    codes.Code
	}{
		"v1 rejected": {1, codes.InvalidArgument},
		"unset":       {0, codes.InvalidArgument},
		"v3 accepted": {3, codes.OK},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := api.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
				Resource: tp, Policy: &iampb.Policy{Version: tc.version, Bindings: bindings},
			})
			assert.Equal(t, tc.code, status.Code(err))
		})
	}

	v3, err := api.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{
		Resource: tp, Options: &iampb.GetPolicyOptions{RequestedPolicyVersion: 3},
	})
	require.NoError(t, err)
	assert.EqualValues(t, 3, v3.GetVersion())
	require.Len(t, v3.GetBindings(), 2)
	assert.Equal(t, cond.GetExpression(), v3.GetBindings()[1].GetCondition().GetExpression())

	v1, err := api.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: tp})
	require.NoError(t, err)
	assert.EqualValues(t, 1, v1.GetVersion())
	require.Len(t, v1.GetBindings(), 2)
	assert.Nil(t, v1.GetBindings()[1].GetCondition())
	assert.True(t, strings.HasPrefix(v1.GetBindings()[1].GetRole(), "roles/pubsub.publisher_withcond_"))

	plain, err := api.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
		Resource: tp, Policy: &iampb.Policy{Bindings: viewer("user:a@example.com")},
	})
	require.NoError(t, err)
	assert.EqualValues(t, 1, plain.GetVersion(), "a policy without conditions is always version 1")
}

func TestIAMPolicyPersists(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := iamTargets(t, e)["topic"]

	set, err := tg.api.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
		Resource: tg.name, Policy: &iampb.Policy{Bindings: viewer("user:a@example.com")},
	})
	require.NoError(t, err)

	data := e.handler.Snapshot(t.Context())

	e2 := newEnv(t)
	require.NoError(t, e2.handler.Restore(t.Context(), data))

	got, err := e2.client.TopicAdminClient.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: tg.name})
	require.NoError(t, err)
	assert.True(t, proto.Equal(set, got))
}
