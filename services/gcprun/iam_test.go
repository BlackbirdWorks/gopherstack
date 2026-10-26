package gcprun_test

import (
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIAM_ServiceAndJob(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	svc := e.createService(t, "web", "example/web:1")
	job := e.createJob(t, "etl", jobSpec("example/etl:1", 1, 1, 0))

	tests := []struct {
		name     string
		resource string
		get      func(*testing.T, string) *iampb.Policy
		set      func(*testing.T, *iampb.SetIamPolicyRequest) (*iampb.Policy, error)
		test     func(*testing.T, string, []string) []string
		perms    []string
		wantPerm []string
	}{
		{
			name: "service", resource: svc.GetName(),
			get: func(t *testing.T, r string) *iampb.Policy {
				t.Helper()
				p, err := e.services.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: r})
				require.NoError(t, err)

				return p
			},
			set: func(t *testing.T, in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
				t.Helper()

				return e.services.SetIamPolicy(t.Context(), in)
			},
			test: func(t *testing.T, r string, perms []string) []string {
				t.Helper()
				out, err := e.services.TestIamPermissions(
					t.Context(), &iampb.TestIamPermissionsRequest{Resource: r, Permissions: perms})
				require.NoError(t, err)

				return out.GetPermissions()
			},
			perms:    []string{"run.services.get", "run.jobs.run", "storage.objects.get"},
			wantPerm: []string{"run.services.get"},
		},
		{
			name: "job", resource: job.GetName(),
			get: func(t *testing.T, r string) *iampb.Policy {
				t.Helper()
				p, err := e.jobs.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: r})
				require.NoError(t, err)

				return p
			},
			set: func(t *testing.T, in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
				t.Helper()

				return e.jobs.SetIamPolicy(t.Context(), in)
			},
			test: func(t *testing.T, r string, perms []string) []string {
				t.Helper()
				out, err := e.jobs.TestIamPermissions(
					t.Context(), &iampb.TestIamPermissionsRequest{Resource: r, Permissions: perms})
				require.NoError(t, err)

				return out.GetPermissions()
			},
			perms:    []string{"run.jobs.run", "run.services.get"},
			wantPerm: []string{"run.jobs.run"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			empty := tt.get(t, tt.resource)
			assert.Empty(t, empty.GetBindings())
			assert.NotEmpty(t, empty.GetEtag())

			binding := &iampb.Binding{Role: "roles/run.invoker", Members: []string{"allUsers"}}
			set, err := tt.set(t, &iampb.SetIamPolicyRequest{
				Resource: tt.resource, Policy: &iampb.Policy{Bindings: []*iampb.Binding{binding}},
			})
			require.NoError(t, err)
			assert.Equal(t, "roles/run.invoker", set.GetBindings()[0].GetRole())
			assert.NotEqual(t, empty.GetEtag(), set.GetEtag())
			assert.Equal(t, set.GetEtag(), tt.get(t, tt.resource).GetEtag())

			_, err = tt.set(t, &iampb.SetIamPolicyRequest{
				Resource: tt.resource, Policy: &iampb.Policy{Etag: empty.GetEtag(), Bindings: []*iampb.Binding{binding}},
			})
			assert.Equal(t, codes.Aborted, status.Code(err), "stale etag")

			_, err = tt.set(t, &iampb.SetIamPolicyRequest{
				Resource: tt.resource, Policy: &iampb.Policy{Bindings: []*iampb.Binding{{Role: "r"}}},
			})
			assert.Equal(t, codes.InvalidArgument, status.Code(err), "binding without members")

			assert.Equal(t, tt.wantPerm, tt.test(t, tt.resource, tt.perms))
		})
	}
}

func TestIAM_Errors(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	missing := parent + "/services/nope"

	_, err := e.services.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: missing})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.services.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: "bogus"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = e.jobs.TestIamPermissions(t.Context(),
		&iampb.TestIamPermissionsRequest{Resource: parent + "/jobs/nope", Permissions: []string{"run.jobs.run"}})
	assert.Equal(t, codes.NotFound, status.Code(err))

	svc := e.createService(t, "web", "example/web:1")
	_, err = e.services.TestIamPermissions(t.Context(), &iampb.TestIamPermissionsRequest{Resource: svc.GetName()})
	assert.Equal(t, codes.InvalidArgument, status.Code(err), "no permissions")
}

func TestIAM_DroppedWithResource(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	svc := e.createService(t, "web", "example/web:1")

	_, err := e.services.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
		Resource: svc.GetName(),
		Policy:   &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/run.invoker", Members: []string{"allUsers"}}}},
	})
	require.NoError(t, err)

	op, err := e.services.DeleteService(t.Context(), &runpb.DeleteServiceRequest{Name: svc.GetName()})
	require.NoError(t, err)
	_, err = op.Wait(t.Context())
	require.NoError(t, err)

	e.createService(t, "web", "example/web:1")

	p, err := e.services.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: svc.GetName()})
	require.NoError(t, err)
	assert.Empty(t, p.GetBindings(), "a recreated service starts with an empty policy")
}
