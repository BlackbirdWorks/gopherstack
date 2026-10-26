package gcpiampolicy_test

import (
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/type/expr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy"
)

func binding(role string, members ...string) *iampb.Binding {
	return &iampb.Binding{Role: role, Members: members}
}

func TestStore_SetGet(t *testing.T) {
	t.Parallel()

	cond := &iampb.Binding{Role: "r", Members: []string{"m"}, Condition: &expr.Expr{Expression: "true"}}

	tests := []struct {
		in       *iampb.Policy
		name     string
		wantCode codes.Code
		wantRole string
		read     int32
	}{
		{in: &iampb.Policy{Bindings: []*iampb.Binding{binding("r", "m")}}, name: "plain", wantRole: "r", read: 1},
		{in: &iampb.Policy{}, name: "empty", read: 1},
		{in: &iampb.Policy{Bindings: []*iampb.Binding{binding("", "m")}}, name: "no role", wantCode: codes.InvalidArgument},
		{in: &iampb.Policy{Bindings: []*iampb.Binding{binding("r")}}, name: "no member", wantCode: codes.InvalidArgument},
		{in: &iampb.Policy{Bindings: []*iampb.Binding{cond}}, name: "condition needs v3", wantCode: codes.InvalidArgument},
		{
			in: &iampb.Policy{Version: 3, Bindings: []*iampb.Binding{cond}}, name: "condition v3 read v1",
			wantRole: "r_withcond_", read: 1,
		},
		{
			in: &iampb.Policy{Version: 3, Bindings: []*iampb.Binding{cond}}, name: "condition v3 read v3",
			wantRole: "r", read: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := gcpiampolicy.NewStore()
			_, err := s.Set("res", tt.in)

			if tt.wantCode != codes.OK {
				assert.Equal(t, tt.wantCode, status.Code(err))

				return
			}

			require.NoError(t, err)

			got := s.Get("res", tt.read)
			if tt.wantRole == "" {
				assert.Empty(t, got.GetBindings())

				return
			}

			assert.Contains(t, got.GetBindings()[0].GetRole(), tt.wantRole)
		})
	}
}

func TestStore_EtagAndSnapshot(t *testing.T) {
	t.Parallel()

	s := gcpiampolicy.NewStore()
	assert.Equal(t, gcpiampolicy.EmptyEtag(), s.Get("res", 0).GetEtag())

	first, err := s.Set("res", &iampb.Policy{Etag: gcpiampolicy.EmptyEtag()})
	require.NoError(t, err)

	_, err = s.Set("res", &iampb.Policy{Etag: gcpiampolicy.EmptyEtag()})
	assert.Equal(t, codes.Aborted, status.Code(err), "stale etag")

	policies, seq := s.Snapshot()
	restored := gcpiampolicy.NewStore()
	restored.Restore(policies, seq)

	next, err := restored.Set("res", &iampb.Policy{Etag: first.GetEtag()})
	require.NoError(t, err)
	assert.NotEqual(t, first.GetEtag(), next.GetEtag())

	restored.Drop("res")
	assert.Equal(t, gcpiampolicy.EmptyEtag(), restored.Get("res", 0).GetEtag())

	restored.Reset()
	assert.Empty(t, restored.Get("other", 0).GetBindings())
}

func TestGranted(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{"run.jobs.run"}, gcpiampolicy.Granted([]string{"run.jobs.run", "x.y"}, "run.jobs."))
	assert.Empty(t, gcpiampolicy.Granted(nil, "run."))
}
