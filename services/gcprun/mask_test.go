package gcprun_test

import (
	"testing"

	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestService_UpdateMaskNestedPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tmpl  *runpb.RevisionTemplate
		check func(t *testing.T, got *runpb.RevisionTemplate)
		name  string
		path  string
		code  codes.Code
	}{
		{
			name: "leaf",
			path: "template.scaling.max_instance_count",
			tmpl: &runpb.RevisionTemplate{Scaling: &runpb.RevisionScaling{MaxInstanceCount: 7}},
			check: func(t *testing.T, got *runpb.RevisionTemplate) {
				t.Helper()

				assert.Equal(t, int32(7), got.GetScaling().GetMaxInstanceCount())
				require.Len(t, got.GetContainers(), 1)
				assert.Equal(t, "img", got.GetContainers()[0].GetImage())
			},
		},
		{name: "unknown leaf", path: "template.nope", tmpl: &runpb.RevisionTemplate{}, code: codes.InvalidArgument},
		{
			name: "through repeated", path: "template.containers.image", tmpl: &runpb.RevisionTemplate{},
			code: codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			svc := e.createService(t, "app", "img")

			op, err := e.services.UpdateService(t.Context(), &runpb.UpdateServiceRequest{
				Service:    &runpb.Service{Name: svc.GetName(), Template: tt.tmpl},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{tt.path}},
			})
			if tt.code != codes.OK {
				assert.Equal(t, tt.code, status.Code(err))

				return
			}

			require.NoError(t, err)

			got, err := op.Wait(t.Context())
			require.NoError(t, err)
			tt.check(t, got.GetTemplate())
		})
	}
}
